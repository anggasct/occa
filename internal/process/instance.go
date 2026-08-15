package process

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/anggasct/occa/internal/relay"
)

const (
	defaultReadinessTimeout = 30 * time.Second
	defaultStopGrace        = 5 * time.Second
)

type instanceFactory func(ctx context.Context, workdir string, port int) (*Instance, error)

type Instance struct {
	workdir  string
	addr     string
	port     int
	pid      int
	client   relay.Client
	lastUsed atomic.Int64
	inflight atomic.Int32
	dead     atomic.Bool
	stop     func()
}

func (i *Instance) Client() relay.Client { return i.client }
func (i *Instance) Addr() string         { return i.addr }
func (i *Instance) Workdir() string      { return i.workdir }
func (i *Instance) PID() int             { return i.pid }

func (i *Instance) Touch() { i.lastUsed.Store(time.Now().Unix()) }

func (i *Instance) End() { i.inflight.Add(-1) }

func (i *Instance) begin()       { i.inflight.Add(1); i.Touch() }
func (i *Instance) isIdle() bool { return i.inflight.Load() == 0 }

func productionFactory(binary string, readinessTimeout, stopGrace time.Duration) instanceFactory {
	if readinessTimeout <= 0 {
		readinessTimeout = defaultReadinessTimeout
	}
	if stopGrace <= 0 {
		stopGrace = defaultStopGrace
	}
	return func(ctx context.Context, workdir string, port int) (*Instance, error) {
		addr := fmt.Sprintf("http://127.0.0.1:%d", port)
		cmd := exec.Command(binary, "serve", "--port", strconv.Itoa(port), "--hostname", "127.0.0.1")
		cmd.Dir = workdir
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("process: spawn %q in %q: %w", binary, workdir, err)
		}

		waitDone := make(chan struct{})
		inst := &Instance{
			workdir: workdir,
			addr:    addr,
			port:    port,
			pid:     cmd.Process.Pid,
			client:  relay.NewHTTPClient(addr),
			stop: func() {
				if cmd.Process == nil {
					return
				}
				pgid := cmd.Process.Pid
				_ = syscall.Kill(-pgid, syscall.SIGTERM)
				select {
				case <-waitDone:
				case <-time.After(stopGrace):
					_ = syscall.Kill(-pgid, syscall.SIGKILL)
				}
			},
		}
		go func() {
			cmd.Wait()
			inst.dead.Store(true)
			close(waitDone)
		}()

		if err := waitReady(ctx, addr, readinessTimeout); err != nil {
			inst.stop()
			return nil, fmt.Errorf("process: readiness for %q: %w", workdir, err)
		}
		inst.Touch()
		return inst, nil
	}
}

func waitReady(ctx context.Context, addr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := addr + "/global/health"
	client := &http.Client{Timeout: 2 * time.Second}
	healthy := func() bool {
		resp, err := client.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}

	if healthy() {
		return nil
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("not ready within %s: %w", timeout, ctx.Err())
		case <-ticker.C:
			if healthy() {
				return nil
			}
		}
	}
}

// NormalizeWorkdir expands a leading "~" and returns a cleaned absolute path so
// equivalent paths (e.g. "/repo/x" and "/repo/x/") map to the same instance.
func NormalizeWorkdir(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, strings.TrimPrefix(path, "~"))
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}
