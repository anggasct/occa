package router

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

type reactingReplyCtx struct {
	*fakeReplyCtx
	reactions []channel.ReactionState
}

func (r *reactingReplyCtx) SetReaction(_ channel.MessageRef, state channel.ReactionState) error {
	r.reactions = append(r.reactions, state)
	return nil
}

// TestRouterWiresReactionSetter: a reply context that implements
// channel.ReactionSetter receives the 👀→✅ lifecycle for a completed stream.
func TestRouterWiresReactionSetter(t *testing.T) {
	r, client, reply := newTestRouter()
	client.deltaBeforeDone = "hello"
	rr := &reactingReplyCtx{fakeReplyCtx: reply}

	m := msg("hello world", rr.fakeReplyCtx)
	m.ReplyCtx = rr
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	if len(rr.reactions) != 2 || rr.reactions[0] != channel.ReactionProcessing || rr.reactions[1] != channel.ReactionSuccess {
		t.Fatalf("reactions = %v, want [processing success]", rr.reactions)
	}
}

// TestRouterSkipsReactionSetterWhenUnsupported: a plain reply context
// without ReactionSetter is unaffected (silent no-op).
func TestRouterSkipsReactionSetterWhenUnsupported(t *testing.T) {
	r, client, reply := newTestRouter()
	client.deltaBeforeDone = "hello"

	if err := r.Route(context.Background(), msg("hello world", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	if client.lastMsg != "hello world" {
		t.Fatalf("expected the reply to be delivered, got %q", client.lastMsg)
	}
}
