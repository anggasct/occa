package discord

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

type capturedRequest struct {
	method string
	path   string
}

// TestSetReactionEmojiSequence asserts the exact emoji sequence including
// the removal of the previous state, using the real transition order.
func TestSetReactionEmojiSequence(t *testing.T) {
	var calls []capturedRequest
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		calls = append(calls, capturedRequest{method: r.Method, path: r.URL.Path})
		return []byte(`{}`), http.StatusOK
	})

	rc := &replyContext{session: session, channelID: "ch-1"}
	ref := messageRef{id: "msg-1"}

	if err := rc.SetReaction(ref, channel.ReactionProcessing); err != nil {
		t.Fatalf("SetReaction processing: %v", err)
	}
	if err := rc.SetReaction(ref, channel.ReactionSuccess); err != nil {
		t.Fatalf("SetReaction success: %v", err)
	}
	if err := rc.SetReaction(ref, channel.ReactionError); err != nil {
		t.Fatalf("SetReaction error: %v", err)
	}

	if len(calls) != 5 {
		t.Fatalf("expected 5 calls, got %d: %+v", len(calls), calls)
	}
	want := []struct{ method, emoji string }{
		{http.MethodPut, "👀"},
		{http.MethodDelete, "👀"},
		{http.MethodPut, "✅"},
		{http.MethodDelete, "✅"},
		{http.MethodPut, "❌"},
	}
	for i, w := range want {
		method, emoji := calls[i].method, ""
		decoded, err := url.PathUnescape(calls[i].path)
		if err == nil {
			emoji = reactionFromPath(decoded)
		}
		if method != w.method || emoji != w.emoji {
			t.Fatalf("call %d = %s %q, want %s %q (all: %+v)", i, method, emoji, w.method, w.emoji, calls)
		}
	}
}

func reactionFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "reactions" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// TestSetReactionSameStateNoOp: repeating the current state issues no request.
func TestSetReactionSameStateNoOp(t *testing.T) {
	var calls []capturedRequest
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		calls = append(calls, capturedRequest{method: r.Method, path: r.URL.Path})
		return []byte(`{}`), http.StatusOK
	})

	rc := &replyContext{session: session, channelID: "ch-1"}
	ref := messageRef{id: "msg-1"}

	if err := rc.SetReaction(ref, channel.ReactionProcessing); err != nil {
		t.Fatalf("SetReaction: %v", err)
	}
	if err := rc.SetReaction(ref, channel.ReactionProcessing); err != nil {
		t.Fatalf("SetReaction repeat: %v", err)
	}

	if len(calls) != 1 {
		t.Fatalf("same-state repeat must be a no-op, got %d calls: %+v", len(calls), calls)
	}
}
