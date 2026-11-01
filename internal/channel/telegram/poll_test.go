package telegram

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type pollLogRecord struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

type pollLogs struct {
	mu      sync.Mutex
	records []pollLogRecord
}

func (h *pollLogs) Enabled(context.Context, slog.Level) bool { return true }
func (h *pollLogs) Handle(_ context.Context, r slog.Record) error {
	rec := pollLogRecord{level: r.Level, msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool { rec.attrs[a.Key] = a.Value.Any(); return true })
	h.mu.Lock()
	h.records = append(h.records, rec)
	h.mu.Unlock()
	return nil
}
func (h *pollLogs) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pollLogs) WithGroup(string) slog.Handler      { return h }

func (h *pollLogs) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.msg == msg {
			n++
		}
	}
	return n
}

func (h *pollLogs) last(msg string) pollLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.records) - 1; i >= 0; i-- {
		if h.records[i].msg == msg {
			return h.records[i]
		}
	}
	return pollLogRecord{}
}

func capturePollLogs(t *testing.T) *pollLogs {
	t.Helper()
	prev := slog.Default()
	h := &pollLogs{}
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

var conflictErr = &tgbotapi.Error{Code: 409, Message: "Conflict: terminated by other getUpdates request; make sure that only one bot instance is running"}

const (
	msgConflict      = "telegram: another bot instance is polling with this token — updates are being split between them"
	msgConflictClear = "telegram: polling conflict cleared"
	msgTransient     = "telegram: getUpdates failed, retrying"
	msgRecovered     = "telegram: getUpdates recovered"
)

func TestIsPollConflict(t *testing.T) {
	if !isPollConflict(conflictErr) {
		t.Fatal("409 api error must be a conflict")
	}
	if !isPollConflict(errors.New("Conflict: terminated by other getUpdates request; make sure that only one bot instance is running")) {
		t.Fatal("conflict text must be a conflict")
	}
	if isPollConflict(&tgbotapi.Error{Code: 502, Message: "Bad Gateway"}) || isPollConflict(nil) {
		t.Fatal("502 / nil must not be a conflict")
	}
}

// AC-01: 5 conflicts then success → one ERROR, one INFO, backoff 3,6,12,24,30.
func TestPollConflictEpisode(t *testing.T) {
	logs := capturePollLogs(t)
	p := &pollHealth{botName: "occacodebot"}
	now := time.Unix(0, 0)
	want := []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second, 24 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := p.fail(conflictErr, now); got != w {
			t.Fatalf("delay %d = %v, want %v", i+1, got, w)
		}
		if !p.inConflict() {
			t.Fatal("inConflict must be true during the episode")
		}
		now = now.Add(w)
	}
	p.recover(now)
	if n := logs.count(msgConflict); n != 1 {
		t.Fatalf("conflict ERROR lines = %d, want 1", n)
	}
	if rec := logs.last(msgConflict); rec.level != slog.LevelError || rec.attrs["bot_username"] != "occacodebot" {
		t.Fatalf("conflict record = %+v", rec)
	}
	if rec := logs.last(msgConflictClear); rec.attrs["conflicts"] != int64(5) {
		t.Fatalf("clear record = %+v, want conflicts=5", rec)
	}
	if logs.count(msgTransient) != 0 {
		t.Fatal("a conflict episode must not log transient WARNs")
	}
	if p.inConflict() {
		t.Fatal("inConflict must be false after recovery")
	}
}

// AC-02: 20× 502 over five minutes → at most 6 WARN lines, each later one with failures=.
func TestPollTransientWarnsAreRateLimited(t *testing.T) {
	logs := capturePollLogs(t)
	p := &pollHealth{}
	now := time.Unix(0, 0)
	bad := &tgbotapi.Error{Code: 502, Message: "Bad Gateway"}
	for i := 0; i < 20; i++ {
		p.fail(bad, now)
		now = now.Add(15 * time.Second)
	}
	p.recover(now)
	if n := logs.count(msgTransient); n < 2 || n > 6 {
		t.Fatalf("transient WARN lines = %d, want 2..6", n)
	}
	if _, ok := logs.last(msgTransient).attrs["failures"]; !ok {
		t.Fatal("rate-limited WARN must carry failures=")
	}
	if rec := logs.last(msgRecovered); rec.attrs["failures"] != int64(20) {
		t.Fatalf("recovered record = %+v, want failures=20", rec)
	}
}

// AC-03: a success resets the delay.
func TestPollSuccessResetsBackoff(t *testing.T) {
	capturePollLogs(t)
	p := &pollHealth{}
	now := time.Unix(0, 0)
	p.fail(errors.New("timeout"), now)
	p.fail(errors.New("timeout"), now)
	p.recover(now)
	if got := p.fail(errors.New("timeout"), now); got != pollBackoffBase {
		t.Fatalf("delay after success = %v, want %v", got, pollBackoffBase)
	}
}

// AC-04: a cancelled context ends the wait at once.
func TestWaitOrDoneHonoursCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if waitOrDone(ctx, pollBackoffMax) {
		t.Fatal("wait must report cancellation")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("wait took %v after cancel, want < 100ms", elapsed)
	}
}

// AC-05: Connected reports the conflict while it lasts.
func TestConnectedReportsPollConflict(t *testing.T) {
	a := &Adapter{}
	a.connected.Store(true)
	a.pollConflict.Store(true)
	if ok, reason := a.Connected(); ok || reason != pollConflictReason {
		t.Fatalf("Connected = %v %q, want false + conflict reason", ok, reason)
	}
	a.pollConflict.Store(false)
	if ok, reason := a.Connected(); !ok || reason != "" {
		t.Fatalf("Connected = %v %q, want true", ok, reason)
	}
}
