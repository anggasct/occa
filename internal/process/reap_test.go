package process

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// spawnArgvLike starts a helper process whose argv looks like
// "<binName> serve --port <p> --hostname 127.0.0.1" and which listens on the
// port, so the reaper treats it as a process of that identity.
func spawnArgvLike(t *testing.T, binName string, port int) *exec.Cmd {
	t.Helper()
	target, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatalf("abs test binary: %v", err)
	}
	bin := filepath.Join(t.TempDir(), binName)
	if err := os.Symlink(target, bin); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	cmd := exec.Command(bin, "-test.run=TestAgentHelperProcess", "serve", "--port", strconv.Itoa(port), "--hostname", "127.0.0.1")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fake %s: %v", binName, err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd
}

// spawnFakeOpenCode starts a helper process whose argv looks like
// "opencode serve --port <p> --hostname 127.0.0.1" and which listens on the
// port, so the reaper treats it as an orphaned agent.
func spawnFakeOpenCode(t *testing.T, port int) *exec.Cmd {
	return spawnArgvLike(t, "opencode", port)
}

func waitPortOccupied(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return
		}
		_ = ln.Close()
		if time.Now().After(deadline) {
			t.Fatalf("port %d never became occupied", port)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReapOrphansKillsOpenCodeOnPort(t *testing.T) {
	port := freePort(t)
	cmd := spawnFakeOpenCode(t, port)
	waitPortOccupied(t, port)

	m := newTestManager(t, testConfig(1, fmt.Sprintf("%d-%d", port, port), time.Minute), newFakeSpawner())
	reaped, err := m.ReapOrphans(context.Background())
	if err != nil {
		t.Fatalf("ReapOrphans: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("fake opencode still running after reap")
	}
	if err := probePort(port); err != nil {
		t.Fatalf("port %d still occupied after reap: %v", port, err)
	}
}

func TestReapOrphansSkipsFreePorts(t *testing.T) {
	lo := freePort(t)
	m := newTestManager(t, testConfig(1, fmt.Sprintf("%d-%d", lo, lo+2), time.Minute), newFakeSpawner())

	reaped, err := m.ReapOrphans(context.Background())
	if err != nil {
		t.Fatalf("ReapOrphans: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped = %d, want 0", reaped)
	}
}

func TestReapOrphansSkipsForeignProcess(t *testing.T) {
	port := freePort(t)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	m := newTestManager(t, testConfig(1, fmt.Sprintf("%d-%d", port, port), time.Minute), newFakeSpawner())
	reaped, err := m.ReapOrphans(context.Background())
	if err != nil {
		t.Fatalf("ReapOrphans: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped = %d, want 0", reaped)
	}
	if probePort(port) == nil {
		t.Fatal("foreign listener was killed")
	}
}

// TestReapOrphansSkipsForeignOpenCodeSubstring is the regression case for the
// strict executable-identity rule: a listener whose executable basename merely
// CONTAINS the "opencode" substring (not-opencode) and whose argv otherwise
// satisfies the serve + port checks must NOT be matched or killed. The sweep
// leaves it running and continues.
func TestReapOrphansSkipsForeignOpenCodeSubstring(t *testing.T) {
	port := freePort(t)
	cmd := spawnArgvLike(t, "not-opencode", port)
	waitPortOccupied(t, port)

	m := newTestManager(t, testConfig(1, fmt.Sprintf("%d-%d", port, port), time.Minute), newFakeSpawner())
	reaped, err := m.ReapOrphans(context.Background())
	if err != nil {
		t.Fatalf("ReapOrphans: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped = %d, want 0", reaped)
	}
	if !processAlive(cmd.Process.Pid) {
		t.Fatal("foreign listener with opencode substring was killed")
	}
	if probePort(port) == nil {
		t.Fatal("foreign listener with opencode substring was killed")
	}
}

func TestOpenCodeOnPortMatch(t *testing.T) {
	serve := []string{"/home/ubuntu/.opencode/bin/opencode", "serve", "--port", "4096", "--hostname", "127.0.0.1"}
	cases := []struct {
		name string
		argv []string
		port int
		want bool
	}{
		{"serve with separate port flag", serve, 4096, true},
		{"serve with equals port", []string{"opencode", "serve", "--port=4096", "--hostname", "127.0.0.1"}, 4096, true},
		{"different port", serve, 4097, false},
		{"not serve", []string{"opencode", "run", "--port", "4096"}, 4096, false},
		{"no opencode binary", []string{"node", "serve", "--port", "4096"}, 4096, false},
		{"foreign binary with opencode substring", []string{"/usr/local/bin/not-opencode", "serve", "--port", "4096"}, 4096, false},
		{"opencode substring in argument", []string{"node", "opencode-agent.js", "serve", "--port", "4096"}, 4096, false},
		{"empty argv", nil, 4096, false},
		{"unrelated process", []string{"nginx"}, 4096, false},
	}
	for _, tc := range cases {
		if got := openCodeServeOnPort(tc.argv, tc.port); got != tc.want {
			t.Errorf("%s: openCodeServeOnPort(%v, %d) = %v, want %v", tc.name, tc.argv, tc.port, got, tc.want)
		}
	}
}

func TestEnsurePortFreeKillsOrphanAndRetries(t *testing.T) {
	port := freePort(t)
	cmd := spawnFakeOpenCode(t, port)
	waitPortOccupied(t, port)

	if err := ensurePortFree(context.Background(), port, time.Second); err != nil {
		t.Fatalf("ensurePortFree: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("orphan still running after ensurePortFree")
	}
}

func TestEnsurePortFreeRejectsForeignProcess(t *testing.T) {
	port := freePort(t)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	if err := ensurePortFree(context.Background(), port, time.Second); err == nil {
		t.Fatal("expected error for foreign process on port")
	}
}
