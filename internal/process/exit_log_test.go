package process

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestInstanceExitUnexpectedIsWarned(t *testing.T) {
	logs := captureLogs(t)
	inst := &Instance{pid: 4242, workdir: "/repo/a"}

	logInstanceExit(inst, errors.New("signal: killed"))

	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "agent instance exited unexpectedly") {
		t.Fatalf("log = %q, want one unexpected-exit warning", out)
	}
	for _, want := range []string{"pid=4242", "workdir=/repo/a", `exit="signal: killed"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log = %q, missing %s", out, want)
		}
	}
}

func TestInstanceExitRequestedIsInfo(t *testing.T) {
	logs := captureLogs(t)
	inst := &Instance{pid: 7, workdir: "/repo/b"}
	inst.stopReason.Store("idle")

	logInstanceExit(inst, nil)

	out := logs.String()
	if strings.Contains(out, "level=WARN") {
		t.Fatalf("log = %q, want no warning for a requested stop", out)
	}
	if !strings.Contains(out, "agent instance stopped") || !strings.Contains(out, "reason=idle") || !strings.Contains(out, "exit=ok") {
		t.Fatalf("log = %q, want a stopped line with reason=idle", out)
	}
}

func stopReasonOf(t *testing.T, inst *Instance) string {
	t.Helper()
	reason, ok := inst.requestedStop()
	if !ok {
		t.Fatal("instance has no recorded stop reason")
	}
	return reason
}

func TestManagerRecordsStopReasons(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), newFakeSpawner())
		inst, err := m.Instance(context.Background(), "/repo/idle")
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		inst.End()
		inst.lastUsed.Store(time.Now().Add(-2 * time.Minute).Unix())
		m.reapOnce(time.Now())
		if got := stopReasonOf(t, inst); got != "idle" {
			t.Fatalf("reason = %q, want idle", got)
		}
	})
	t.Run("evicted", func(t *testing.T) {
		m := newTestManager(t, testConfig(1, "4096-4100", time.Minute), newFakeSpawner())
		old, err := m.Instance(context.Background(), "/repo/old")
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		old.End()
		next, err := m.Instance(context.Background(), "/repo/new")
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		next.End()
		if got := stopReasonOf(t, old); got != "evicted" {
			t.Fatalf("reason = %q, want evicted", got)
		}
	})
	t.Run("forced", func(t *testing.T) {
		m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), newFakeSpawner())
		inst, err := m.Instance(context.Background(), "/repo/forced")
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		inst.End()
		m.ForceStop("/repo/forced")
		if got := stopReasonOf(t, inst); got != "forced" {
			t.Fatalf("reason = %q, want forced", got)
		}
	})
	t.Run("shutdown", func(t *testing.T) {
		m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), newFakeSpawner())
		inst, err := m.Instance(context.Background(), "/repo/close")
		if err != nil {
			t.Fatalf("Instance: %v", err)
		}
		inst.End()
		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := stopReasonOf(t, inst); got != "shutdown" {
			t.Fatalf("reason = %q, want shutdown", got)
		}
	})
}
