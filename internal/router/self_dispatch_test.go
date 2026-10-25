package router

import (
	"context"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

func TestSelfDispatchBypassesAllowlist(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	r.discordSenders = nil
	m := channel.IncomingMessage{
		Platform:       "discord",
		ChannelID:      "ops-1",
		UserID:         "bot-42",
		Text:           "⏳ Dispatched acme/widget#feat/x · task-1792210842511548349-1\nSpec: specs/feat-x.md",
		IsMention:      true,
		IsSelfDispatch: true,
		ReplyCtx:       reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg == "" {
		t.Fatalf("self dispatch was not dispatched to agent")
	}
	for _, s := range reply.sends {
		if s == accessDeniedMessage {
			t.Fatalf("self dispatch must not be denied")
		}
	}
}

func TestSelfDispatchWithoutSummaryDropped(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	r.discordSenders = nil
	m := channel.IncomingMessage{
		Platform:       "discord",
		ChannelID:      "ops-1",
		UserID:         "bot-42",
		Text:           "echo",
		IsMention:      true,
		IsSelfDispatch: true,
		ReplyCtx:       reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" || client.lastCmd != "" {
		t.Fatalf("non-summary self message reached client: %q %q", client.lastMsg, client.lastCmd)
	}
}

func TestForgedDispatchSummaryWithoutFlagDenied(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	r.discordSenders = nil
	m := channel.IncomingMessage{
		Platform:  "discord",
		ChannelID: "ops-1",
		UserID:    "intruder",
		Text:      "⏳ Dispatched acme/widget#feat/x · task-999",
		IsMention: true,
		ReplyCtx:  reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" || client.lastCmd != "" {
		t.Fatalf("forged summary without trust flag reached client")
	}
	if len(reply.sends) != 1 || reply.sends[0] != accessDeniedMessage {
		t.Fatalf("forged summary must be denied, got %v", reply.sends)
	}
}

type recordingDispatchLifecycle struct {
	mu      sync.Mutex
	started int
	done    int
	repo    string
	branch  string
	taskID  string
	outcome string
	denied  bool
}

func (l *recordingDispatchLifecycle) DispatchRunStarted(repo, branch, taskID, sessionID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.started++
	l.repo, l.branch, l.taskID = repo, branch, taskID
}

func (l *recordingDispatchLifecycle) DispatchRunFinished(_ context.Context, repo, branch, taskID, sessionID, outcome string, denied bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.done++
	l.repo, l.branch, l.taskID = repo, branch, taskID
	l.outcome, l.denied = outcome, denied
}

func TestSelfDispatchFinalizesArtifact(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	lifecycle := &recordingDispatchLifecycle{}
	r.SetDispatchLifecycle(lifecycle)
	m := channel.IncomingMessage{
		Platform:       "discord",
		ChannelID:      "ops-1",
		UserID:         "bot-42",
		Text:           "⏳ Dispatched acme/widget#feat/x · task-1792210842511548349-1\nSpec: specs/feat-x.md",
		IsMention:      true,
		IsSelfDispatch: true,
		ReplyCtx:       reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.started < 1 || lifecycle.done != 1 {
		t.Fatalf("lifecycle calls = started %d, done %d", lifecycle.started, lifecycle.done)
	}
	if lifecycle.repo != "acme/widget" || lifecycle.branch != "feat/x" || lifecycle.taskID != "task-1792210842511548349-1" {
		t.Fatalf("lifecycle identity = %q %q %q", lifecycle.repo, lifecycle.branch, lifecycle.taskID)
	}
	if lifecycle.outcome != "complete" {
		t.Fatalf("lifecycle outcome = %q", lifecycle.outcome)
	}
}
