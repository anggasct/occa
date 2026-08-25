//go:build linux || darwin

package process

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAgentHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	args := flag.Args()
	for i, a := range args {
		if a == "--port" && i+1 < len(args) {
			port, err := strconv.Atoi(args[i+1])
			if err == nil && port != 0 {
				go func() {
					_ = http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
					}))
				}()
			}
		}
	}

	switch mode := os.Getenv("GO_HELPER_MODE"); mode {
	case "parent-with-child":
		child := exec.Command(os.Args[0], "-test.run=TestAgentHelperProcess")
		child.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "GO_HELPER_MODE=child-ignore-term")
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		_ = os.WriteFile(os.Getenv("GO_HELPER_PID_FILE"), []byte(strconv.Itoa(child.Process.Pid)), 0o600)
		signal.Ignore(syscall.SIGTERM)
	case "child-ignore-term":
		signal.Ignore(syscall.SIGTERM)
	case "exit-on-term":
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM)
		<-sigCh
		_ = os.WriteFile(os.Getenv("GO_HELPER_MARKER"), []byte("term"), 0o600)
		os.Exit(0)
	}
	select {}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func TestStopSignalsProcessGroup(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("GO_HELPER_PID_FILE", pidFile)
	t.Setenv("GO_HELPER_MODE", "parent-with-child")

	factory := productionFactory(os.Args[0], 5*time.Second, 1*time.Second)
	inst, err := factory(context.Background(), t.TempDir(), freePort(t))
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	var childPID int
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, err := os.ReadFile(pidFile); err == nil {
			childPID, _ = strconv.Atoi(strings.TrimSpace(string(data)))
			if childPID != 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("child pid file never appeared")
		}
		time.Sleep(50 * time.Millisecond)
	}

	inst.stop()

	if err := waitFor(func() bool { return inst.dead.Load() }, 2*time.Second); err != nil {
		t.Fatal("agent still running after stop")
	}
	if err := syscall.Kill(childPID, 0); err == nil {
		t.Fatal("agent child survived group stop")
	}
}

func TestStopGracefulBeforeKill(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	marker := filepath.Join(t.TempDir(), "got-term")
	t.Setenv("GO_HELPER_MARKER", marker)
	t.Setenv("GO_HELPER_MODE", "exit-on-term")

	factory := productionFactory(os.Args[0], 5*time.Second, 30*time.Second)
	inst, err := factory(context.Background(), t.TempDir(), freePort(t))
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	start := time.Now()
	inst.stop()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("stop took %v despite a 30s grace; graceful exit should win", elapsed)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("graceful signal never reached the agent")
	}
}

func waitFor(cond func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}
