package router

import (
	"context"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

func waitPendingPermission(t *testing.T, r *Router) (string, channel.MessageRef) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.permissions.mu.Lock()
		for token, record := range r.permissions.records {
			if record.state == permissionPending && record.origin != nil {
				r.permissions.mu.Unlock()
				return token, record.origin
			}
		}
		r.permissions.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("permission prompt was not registered")
	return "", nil
}

func routeDenialPrompt(t *testing.T, r *Router, client *responseClient, events chan relay.Event, reply *responseReply) {
	t.Helper()
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
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	events <- relay.Event{Type: "permission_asked", Permission: &relay.PermissionRequest{ID: "perm-1", SessionID: "session", Permission: "bash", Tool: "bash"}}
}

func TestDispatchDenial_AC13_RejectedPromptMarksDenied(t *testing.T) {
	client := newResponseClient(func(context.Context, chan<- relay.Event) error { return nil })
	r, _ := newResponseRouter(client)
	r.permissions.window = 5 * time.Millisecond
	lifecycle := &recordingDispatchLifecycle{}
	r.SetDispatchLifecycle(lifecycle)
	events := make(chan relay.Event, 8)
	client.eventsHook = func(int) (<-chan relay.Event, error) { return events, nil }
	reply := newResponseReply()

	routeDenialPrompt(t, r, client, events, reply)
	token, origin := waitPendingPermission(t, r)
	callback := channel.IncomingMessage{
		Platform:     "discord",
		ChannelID:    "ops-1",
		IsCallback:   true,
		CallbackData: "permission:" + token + ":reject",
		CallbackRef:  origin,
		ReplyCtx:     reply,
	}
	if err := r.permissions.handle(context.Background(), callback); err != nil {
		t.Fatalf("reject callback: %v", err)
	}
	events <- relay.Event{Type: "done"}
	close(events)
	waitForResponse(t, r)

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.done != 1 {
		t.Fatalf("DispatchRunFinished calls = %d, want 1", lifecycle.done)
	}
	if !lifecycle.denied {
		t.Fatal("a rejected permission must reach DispatchRunFinished as denied")
	}
}

func TestDispatchDenial_AC11_PendingPromptIsNotDenial(t *testing.T) {
	client := newResponseClient(func(context.Context, chan<- relay.Event) error { return nil })
	r, _ := newResponseRouter(client)
	r.permissions.window = 5 * time.Millisecond
	lifecycle := &recordingDispatchLifecycle{}
	r.SetDispatchLifecycle(lifecycle)
	events := make(chan relay.Event, 8)
	client.eventsHook = func(int) (<-chan relay.Event, error) { return events, nil }
	reply := newResponseReply()

	routeDenialPrompt(t, r, client, events, reply)
	waitPendingPermission(t, r)
	events <- relay.Event{Type: "done"}
	close(events)
	waitForResponse(t, r)

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.done != 1 {
		t.Fatalf("DispatchRunFinished calls = %d, want 1", lifecycle.done)
	}
	if lifecycle.denied {
		t.Fatal("an unanswered permission is a timeout, not a denial")
	}
}
