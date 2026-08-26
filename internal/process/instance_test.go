package process

import (
	"context"
	"errors"
	"slices"
	"syscall"
	"testing"
	"time"
)

func TestProductionFactorySetsPdeathSig(t *testing.T) {
	cmd := openCodeCommand("opencode", 4096, "/tmp/workdir")

	if cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Errorf("Pdeathsig = %v, want SIGKILL", cmd.SysProcAttr.Pdeathsig)
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Error("Setpgid = false, want true")
	}

	want := []string{"opencode", "serve", "--port", "4096", "--hostname", "127.0.0.1"}
	if !slices.Equal(cmd.Args, want) {
		t.Errorf("Args = %v, want %v", cmd.Args, want)
	}
	if cmd.Dir != "/tmp/workdir" {
		t.Errorf("Dir = %q, want %q", cmd.Dir, "/tmp/workdir")
	}
}

func TestWaitReadyTimeoutWrapsSentinel(t *testing.T) {
	// Port 1 is never served locally; readiness must fail fast with the
	// sentinel so callers can classify readiness timeouts distinctly.
	err := waitReady(context.Background(), "http://127.0.0.1:1", 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitReady unexpectedly succeeded")
	}
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("waitReady error = %v, want ErrReadinessTimeout in chain", err)
	}
}
