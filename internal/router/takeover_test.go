package router

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

func TestFailedThreadAdoptsFailedSession(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	ctx := context.Background()

	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "telegram", "chat1", "thread-1", "failed-sess", 999, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}

	reply := newResponseReply()
	msg := channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		ThreadID:  "thread-1",
		IsThread:  true,
		UserID:    "user1",
		Text:      "continue the fix",
		IsMention: true,
		ReplyCtx:  reply,
	}
	if err := r.Route(ctx, msg); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.sendSessions) != 1 || client.sendSessions[0] != "failed-sess" {
		t.Fatalf("prompt went to sessions %v, want exactly [failed-sess]", client.sendSessions)
	}
	if !reply.contains("response") {
		t.Fatalf("continued turn produced no response: %v", reply.texts())
	}
	if got, _, _ := st.SessionRepo().Active(ctx, "telegram", "chat1", "thread-1", ""); got != "failed-sess" {
		t.Fatalf("thread key resolves to %q after adoption, want failed-sess", got)
	}
}

func TestFailedThreadDeniedSenderDoesNotAdopt(t *testing.T) {
	r, _, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	ctx := context.Background()

	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "discord", "parent-1", "thread-orphan", "failed-sess", 999, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}

	if err := r.Route(ctx, orphanMsg("thread-orphan", "parent-1", "user1", "continue", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || reply.sends[0] != accessDeniedMessage {
		t.Fatalf("expected deny, got %v", reply.sends)
	}
	candidate, err := st.SessionRepo().TakeoverCandidate(ctx, "discord", "parent-1", "thread-orphan")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate == nil || candidate.SessionID != "failed-sess" {
		t.Fatalf("denied press disturbed the mark: %+v", candidate)
	}
	if got, _, _ := st.SessionRepo().Active(ctx, "discord", "parent-1", "thread-orphan", "user1"); got != "" {
		t.Fatalf("denied sender gained a session: %q", got)
	}
}
