package process

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ReapOrphans kills opencode serve processes listening on the manager's port
// range that occa did not spawn (leftovers from a previous run). Safe to call
// once at startup before any instance is created. Returns the number killed.
func (m *Manager) ReapOrphans(ctx context.Context) (int, error) {
	reaped := 0
	for port := m.ports.lo; port <= m.ports.hi; port++ {
		select {
		case <-ctx.Done():
			return reaped, ctx.Err()
		default:
		}
		if probePort(port) == nil {
			continue
		}
		pid, ok := openCodeOnPort(port)
		if !ok {
			slog.Warn("agent orphan sweep: port in use by a foreign process", "port", port)
			continue
		}
		if err := killPID(pid); err != nil {
			return reaped, fmt.Errorf("reap orphan opencode on port %d (pid %d): %w", port, pid, err)
		}
		reaped++
	}
	return reaped, nil
}

// ensurePortFree verifies nothing listens on port before a spawn. An orphan
// opencode serve is killed and the port re-probed; a foreign listener is left
// untouched and reported as an error.
func ensurePortFree(ctx context.Context, port int, stopGrace time.Duration) error {
	if probePort(port) == nil {
		return nil
	}
	pid, ok := openCodeOnPort(port)
	if !ok {
		return fmt.Errorf("port %d in use by a foreign process", port)
	}
	if err := killPID(pid); err != nil {
		return fmt.Errorf("kill orphan opencode pid %d on port %d: %w", pid, port, err)
	}
	deadline := time.Now().Add(stopGrace)
	for {
		if err := probePort(port); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("port %d still in use after reaping orphan opencode: %w", port, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("port %d still in use after reaping orphan opencode", port)
		}
	}
}

func probePort(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	return ln.Close()
}

func openCodeOnPort(port int) (int, bool) {
	inodes := listenerInodes(port)
	if len(inodes) == 0 {
		return 0, false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		argv, err := readCmdline(pid)
		if err != nil || !openCodeServeOnPort(argv, port) {
			continue
		}
		if pidHoldsSocket(pid, inodes) {
			return pid, true
		}
	}
	return 0, false
}

// listenerInodes returns the socket inodes of listeners bound to port, parsed
// from /proc/net/tcp where the local address is hex-encoded and 0A is LISTEN.
func listenerInodes(port int) map[uint64]bool {
	inodes := make(map[uint64]bool)
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			f := strings.Fields(line)
			if len(f) < 10 || f[3] != "0A" {
				continue
			}
			hexPort := f[1][strings.LastIndex(f[1], ":")+1:]
			p, err := strconv.ParseUint(hexPort, 16, 32)
			if err != nil || int(p) != port {
				continue
			}
			inode, err := strconv.ParseUint(f[9], 10, 64)
			if err != nil {
				continue
			}
			inodes[inode] = true
		}
	}
	return inodes
}

func pidHoldsSocket(pid int, inodes map[uint64]bool) bool {
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		link, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil || !strings.HasPrefix(link, "socket:[") {
			continue
		}
		var inode uint64
		if _, err := fmt.Sscanf(link, "socket:[%d]", &inode); err == nil && inodes[inode] {
			return true
		}
	}
	return false
}

func readCmdline(pid int) ([]string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return nil, err
	}
	return strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00"), nil
}

func openCodeServeOnPort(argv []string, port int) bool {
	portStr := strconv.Itoa(port)
	var hasOpenCode, hasServe, hasPort bool
	for i := 0; i < len(argv); i++ {
		switch {
		case strings.Contains(argv[i], "opencode"):
			hasOpenCode = true
		case argv[i] == "serve":
			hasServe = true
		case argv[i] == "--port":
			if i+1 < len(argv) && argv[i+1] == portStr {
				hasPort = true
			}
		case argv[i] == "--port="+portStr:
			hasPort = true
		}
	}
	return hasOpenCode && hasServe && hasPort
}

func killPID(pid int) error {
	if !processAlive(pid) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	deadline := time.Now().Add(defaultStopGrace)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// processAlive reports whether pid exists and is not a zombie. Zombies hold no
// port and cannot be signalled, so they count as dead for reaping purposes.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	switch {
	case errors.Is(err, syscall.EPERM):
		return true
	case err != nil:
		return false
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// stat layout: pid (comm) state ... — comm may contain spaces, so find the
	// last ')' and read the state field right after it.
	i := strings.LastIndex(string(data), ")")
	if i < 0 || i+2 >= len(data) {
		return true
	}
	return data[i+2] != 'Z'
}
