package router

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/loop"
	"github.com/anggasct/occa/internal/store"
)

type loopFixture struct {
	r        *Router
	reply    *fakeReplyCtx
	overides *fakeOverrideRepo

	mu        sync.Mutex
	execCalls int
	outputs   []string
	notified  []loopPosted
	busy      bool
}

type loopPosted struct {
	conv loop.Conversation
	text string
}

func newLoopFixture() *loopFixture {
	f := &loopFixture{}
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	overrides.overrides["telegram:chat1:user2"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user2", Role: "allow",
	}
	r.SetLooper(loop.New(f.execute, f.notify, f.isBusy))
	f.r = r
	f.reply = reply
	f.overides = overrides
	return f
}

func (f *loopFixture) execute(_ context.Context, _ loop.Conversation, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls++
	if len(f.outputs) == 0 {
		return "", nil
	}
	out := f.outputs[0]
	f.outputs = f.outputs[1:]
	return out, nil
}

func (f *loopFixture) notify(conv loop.Conversation, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notified = append(f.notified, loopPosted{conv: conv, text: text})
}

func (f *loopFixture) isBusy(loop.Conversation) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.busy
}

func (f *loopFixture) route(t *testing.T, text string) {
	t.Helper()
	if err := f.r.Route(context.Background(), msg(text, f.reply)); err != nil {
		t.Fatalf("Route(%q): %v", text, err)
	}
}

func (f *loopFixture) routeAs(t *testing.T, userID, text string) {
	t.Helper()
	if err := f.r.Route(context.Background(), msgFrom(userID, text, f.reply)); err != nil {
		t.Fatalf("Route(%q): %v", text, err)
	}
}

func (f *loopFixture) lastSend() string {
	sends := f.reply.sends
	if len(sends) == 0 {
		return ""
	}
	return sends[len(sends)-1]
}

func (f *loopFixture) notices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, p := range f.notified {
		out = append(out, p.text)
	}
	return out
}

func TestLoopCreateConfirm(t *testing.T) {
	f := newLoopFixture()
	f.route(t, "/loop every 2m x3 check PR status")
	got := f.lastSend()
	for _, want := range []string{"Loop 1", "every 2m", "3 runs", "check PR status", "/loop stop 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("confirm = %q, want substring %q", got, want)
		}
	}
}

func TestLoopCreateDurationConfirm(t *testing.T) {
	f := newLoopFixture()
	f.route(t, "/loop every 30s for 1h watch deploy")
	got := f.lastSend()
	for _, want := range []string{"Loop 1", "every 30s", "for 1h"} {
		if !strings.Contains(got, want) {
			t.Errorf("confirm = %q, want substring %q", got, want)
		}
	}
}

func TestLoopUsageInputs(t *testing.T) {
	for _, text := range []string{
		"/loop",
		"/loop every",
		"/loop every 5s x3 hi",
		"/loop every 2m x1 hi",
		"/loop every 2m check status",
		"/loop nonsense here",
		"/loop stop",
		"/loop stop abc",
		"/loop stop 0",
	} {
		f := newLoopFixture()
		f.route(t, text)
		if got := f.lastSend(); !strings.Contains(got, "Usage") {
			t.Errorf("Route(%q) = %q, want Usage reply", text, got)
		}
		if n := len(f.r.loops.List(loopConv(msg(text, f.reply)))); n != 0 {
			t.Errorf("Route(%q) created %d loops, want 0", text, n)
		}
	}
}

func TestLoopListAndStop(t *testing.T) {
	f := newLoopFixture()
	f.route(t, "/loop every 1m x2 poll target")
	f.route(t, "/loops")
	if got := f.lastSend(); !strings.Contains(got, "[1]") || !strings.Contains(got, "poll target") {
		t.Errorf("list = %q, want loop row", got)
	}
	sendsBefore := len(f.reply.sends)
	f.route(t, "/loop stop 1")
	if len(f.reply.sends) != sendsBefore {
		t.Errorf("stop posted %d command replies, want 0 (terminal goes through notifier)", len(f.reply.sends)-sendsBefore)
	}
	notices := f.notices()
	if len(notices) != 1 || !strings.Contains(notices[0], "stopped") {
		t.Errorf("notices = %q, want single stopped terminal", notices)
	}
	f.route(t, "/loops")
	if got := f.lastSend(); !strings.Contains(got, "No active loops") {
		t.Errorf("list after stop = %q, want empty text", got)
	}
	f.route(t, "/loop stop 1")
	if got := f.lastSend(); !strings.Contains(got, "Unknown loop") {
		t.Errorf("second stop = %q, want unknown-id reply", got)
	}
}

func TestLoopConversationIsolation(t *testing.T) {
	f := newLoopFixture()
	f.routeAs(t, "user1", "/loop every 1m x3 private poll")
	f.routeAs(t, "user2", "/loops")
	if got := f.lastSend(); !strings.Contains(got, "No active loops") {
		t.Errorf("user2 list = %q, want empty (isolation)", got)
	}
	f.routeAs(t, "user2", "/loop stop 1")
	if got := f.lastSend(); !strings.Contains(got, "Unknown loop") {
		t.Errorf("user2 stop = %q, want unknown-id (no cross-stop oracle)", got)
	}
	f.routeAs(t, "user1", "/loops")
	if got := f.lastSend(); !strings.Contains(got, "[1]") {
		t.Errorf("user1 list = %q, want intact loop", got)
	}
	if len(f.notices()) != 0 {
		t.Errorf("notices = %q, want none after foreign stop", f.notices())
	}
}

func TestLoopSecondLoopLimit(t *testing.T) {
	f := newLoopFixture()
	f.route(t, "/loop every 1m x2 first")
	f.route(t, "/loop every 1m x2 second")
	got := f.lastSend()
	if !strings.Contains(got, "already has an active loop") || !strings.Contains(got, "/loop stop 1") {
		t.Errorf("second create = %q, want limit reply with existing id", got)
	}
}

func TestLoopResetCancels(t *testing.T) {
	f := newLoopFixture()
	f.route(t, "/loop every 1m x4 doomed poll")
	f.route(t, "/reset")
	notices := f.notices()
	if len(notices) != 1 || !strings.Contains(notices[0], "stopped") {
		t.Errorf("notices after reset = %q, want single stopped terminal", notices)
	}
	if got := f.lastSend(); !strings.Contains(got, "Session reset") {
		t.Errorf("reset reply = %q, want session reset confirmation", got)
	}
}

func TestLoopBusyGate(t *testing.T) {
	f := newLoopFixture()
	conv := loopConv(msg("x", f.reply))
	if f.r.LoopBusy(conv) {
		t.Fatal("LoopBusy on idle conversation, want false")
	}
	key := responseKey{platform: "telegram", channelID: "chat1", userID: "user1"}
	if !f.r.responses.acquire(key, func() {}) {
		t.Fatal("acquire failed")
	}
	defer f.r.responses.release(key)
	if !f.r.LoopBusy(conv) {
		t.Error("LoopBusy on active conversation, want true")
	}
	other := loop.Conversation{Platform: "telegram", ChannelID: "chat1", UserID: "user2"}
	if f.r.LoopBusy(other) {
		t.Error("LoopBusy on other user, want false")
	}
}

func TestLoopStopHandlerDirect(t *testing.T) {
	f := newLoopFixture()
	got, err := f.r.handleLoop(context.Background(), msg("x", f.reply), "stop 42")
	if err != nil {
		t.Fatalf("stop unknown err = %v", err)
	}
	if !strings.Contains(got, "Unknown loop") {
		t.Errorf("direct stop = %q", got)
	}
}
