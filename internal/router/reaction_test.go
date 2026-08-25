package router

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

type reactingReplyCtx struct {
	*fakeReplyCtx
	reactions []channel.ReactionState
	refs      []channel.MessageRef
}

func (r *reactingReplyCtx) SetReaction(ref channel.MessageRef, state channel.ReactionState) error {
	r.reactions = append(r.reactions, state)
	r.refs = append(r.refs, ref)
	return nil
}

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
	for i, ref := range rr.refs {
		if ref.ID() != "1" {
			t.Fatalf("reaction %d targeted %q, want the first reply ref", i, ref.ID())
		}
	}
}

func TestRouterWiresReactionTargetToSourceMessage(t *testing.T) {
	r, client, reply := newTestRouter()
	client.deltaBeforeDone = "hello"
	rr := &reactingReplyCtx{fakeReplyCtx: reply}

	m := msg("hello world", rr.fakeReplyCtx)
	m.ReplyCtx = rr
	m.SourceRef = fakeRef{id: "source-1"}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	if len(rr.reactions) != 2 || rr.reactions[0] != channel.ReactionProcessing || rr.reactions[1] != channel.ReactionSuccess {
		t.Fatalf("reactions = %v, want [processing success]", rr.reactions)
	}
	for i, ref := range rr.refs {
		if ref.ID() != "source-1" {
			t.Fatalf("reaction %d targeted %q, want the source message", i, ref.ID())
		}
	}
}

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
