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

// TestSetReactionUsesReactionChannelID: when a reactionChannelID is set (the
// source message's channel) both the add and remove calls target it rather
// than the reply channel, so the auto-thread case reacts on the parent.
func TestSetReactionUsesReactionChannelID(t *testing.T) {
	var paths []string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		paths = append(paths, r.URL.Path)
		return []byte(`{}`), http.StatusOK
	})

	rc := &replyContext{session: session, channelID: "reply-ch", reactionChannelID: "source-ch"}
	ref := messageRef{id: "msg-1"}

	if err := rc.SetReaction(ref, channel.ReactionProcessing); err != nil {
		t.Fatalf("SetReaction: %v", err)
	}
	if err := rc.SetReaction(ref, channel.ReactionSuccess); err != nil {
		t.Fatalf("SetReaction success: %v", err)
	}

	if len(paths) != 3 {
		t.Fatalf("expected 3 calls, got %d: %+v", len(paths), paths)
	}
	for _, p := range paths {
		if !strings.Contains(p, "/channels/source-ch/messages/") {
			t.Fatalf("reaction path %q did not target the source channel", p)
		}
		if strings.Contains(p, "/channels/reply-ch/") {
			t.Fatalf("reaction path %q targeted the reply channel instead of the source", p)
		}
	}
}
