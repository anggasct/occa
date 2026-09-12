package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
)

type rootCardEdit struct {
	platform  string
	channelID string
	messageID string
	text      string
}

type rootCardEditorCapture struct {
	mu    sync.Mutex
	edits []rootCardEdit
	err   error
}

func (c *rootCardEditorCapture) edit(_ context.Context, platform, channelID, messageID, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.edits = append(c.edits, rootCardEdit{platform: platform, channelID: channelID, messageID: messageID, text: text})
	return c.err
}

func (c *rootCardEditorCapture) snapshot() []rootCardEdit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rootCardEdit(nil), c.edits...)
}

func linkedThreadMessage(reply channel.ReplyContext) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		ThreadID:  "thread-1",
		IsThread:  true,
		UserID:    "user1",
		Text:      "continue the fix",
		IsMention: true,
		ReplyCtx:  reply,
	}
}

func TestThreadTurnUpdatesRootCardFollowup(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	ctx := context.Background()

	baseCard := "📨 GitHub Webhook · github_fix\nStatus: ⚠️ FAILED\nDelivery: del-1"
	if err := st.SessionRepo().LinkThreadRoot(ctx, "telegram", "chat1", "thread-1", "root-77", "chat1:thread-1", baseCard); err != nil {
		t.Fatalf("link root: %v", err)
	}
	capture := &rootCardEditorCapture{}
	r.SetRootCardEditor(capture.edit)

	reply := newResponseReply()
	if err := r.Route(ctx, linkedThreadMessage(reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	edits := capture.snapshot()
	if len(edits) != 2 {
		t.Fatalf("root card edits = %d, want exactly 2 (running + terminal): %+v", len(edits), edits)
	}
	first, last := edits[0], edits[1]
	for _, e := range edits {
		if e.platform != "telegram" || e.channelID != "chat1:thread-1" || e.messageID != "root-77" {
			t.Fatalf("edit targeted %+v, want telegram/chat1:thread-1/root-77", e)
		}
		if !strings.HasPrefix(e.text, baseCard+"\n") {
			t.Fatalf("edit lost the audit base card: %q", e.text)
		}
	}
	if first.text != baseCard+"\n"+followupRunningLine {
		t.Fatalf("running edit = %q, want base card + running line", first.text)
	}
	if !strings.Contains(last.text, "Follow-up: ✅ completed (") {
		t.Fatalf("terminal edit missing completed line: %q", last.text)
	}
	if !reply.contains("response") {
		t.Fatalf("turn response missing: %v", reply.texts())
	}
}

func TestThreadTurnWithoutRootCardSkipsEdits(t *testing.T) {
	client := newResponseClient(nil)
	r, _ := newResponseRouter(client)

	capture := &rootCardEditorCapture{}
	r.SetRootCardEditor(capture.edit)

	reply := newResponseReply()
	if err := r.Route(context.Background(), linkedThreadMessage(reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	if edits := capture.snapshot(); len(edits) != 0 {
		t.Fatalf("unlinked thread produced %d root card edits, want 0", len(edits))
	}
	if !reply.contains("response") {
		t.Fatalf("turn response missing: %v", reply.texts())
	}
}

func TestThreadTurnEditorFailureDoesNotBreakTurn(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	ctx := context.Background()

	if err := st.SessionRepo().LinkThreadRoot(ctx, "telegram", "chat1", "thread-1", "root-77", "chat1:thread-1", "card"); err != nil {
		t.Fatalf("link root: %v", err)
	}
	capture := &rootCardEditorCapture{err: errors.New("edit rejected")}
	r.SetRootCardEditor(capture.edit)

	reply := newResponseReply()
	if err := r.Route(ctx, linkedThreadMessage(reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	if !reply.contains("response") {
		t.Fatalf("editor failure broke the turn: %v", reply.texts())
	}
}

func TestFollowupTerminalLineMapping(t *testing.T) {
	if got := followupTerminalLine("complete", 90*time.Second); got != "Follow-up: ✅ completed (1m 30s)" {
		t.Fatalf("complete line = %q", got)
	}
	if got := followupTerminalLine("cancelled", 0); got != "Follow-up: ⏹️ cancelled" {
		t.Fatalf("cancelled line = %q", got)
	}
	for _, outcome := range []string{"dispatch_error", "stream_error", "incomplete"} {
		if got := followupTerminalLine(outcome, 0); got != "Follow-up: ⚠️ failed" {
			t.Fatalf("%s line = %q", outcome, got)
		}
	}
}

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
	if len(client.sendTexts) != 1 || client.sendTexts[0] != "continue the fix" {
		t.Fatalf("adopted prompt = %v, want raw operator text without envelope seed", client.sendTexts)
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

func TestFailedThreadWithoutSessionSeedsEnvelope(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	ctx := context.Background()

	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "telegram", "chat1", "thread-1", "", 0, "envelope-seed-xyz"); err != nil {
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
	if len(client.sendSessions) != 1 || client.sendSessions[0] != "session" {
		t.Fatalf("prompt went to sessions %v, want exactly one fresh [session]", client.sendSessions)
	}
	if len(client.sendTexts) != 1 || !strings.Contains(client.sendTexts[0], "envelope-seed-xyz") || !strings.Contains(client.sendTexts[0], "continue the fix") {
		t.Fatalf("seeded prompt = %v, want envelope seed plus operator text", client.sendTexts)
	}
}
