package relay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type fakeReactionSetter struct {
	mu     sync.Mutex
	states []channel.ReactionState
	refs   []string
}

func (f *fakeReactionSetter) SetReaction(ref channel.MessageRef, state channel.ReactionState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = append(f.states, state)
	f.refs = append(f.refs, ref.ID())
	return nil
}

func (f *fakeReactionSetter) record() []channel.ReactionState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]channel.ReactionState(nil), f.states...)
}

func (f *fakeReactionSetter) recordRefs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.refs...)
}

// waitReactions polls the fake setter until it has seen the given number of
// transitions, failing the test otherwise. Needed because the first flush
// happens on the 500ms edit timer.
func waitReactions(t *testing.T, r *fakeReactionSetter, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(r.record()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reactions did not reach %d transitions, have %v", want, r.record())
}

// TestStreamerReactionDone: the reply gains 👀 on first send and ✅ on done.
func TestStreamerReactionDone(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 10)
	events <- Event{Type: EventDelta, Delta: "hello"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := reactions.record()
	if len(got) != 2 || got[0] != channel.ReactionProcessing || got[1] != channel.ReactionSuccess {
		t.Fatalf("reactions = %v, want [processing success]", got)
	}
}

// TestStreamerReactionErrorState: an error event after a reply replaces 👀
// with ❌.
func TestStreamerReactionErrorState(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 10)
	events <- Event{Type: EventDelta, Delta: "partial"}

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background(), events) }()

	waitReactions(t, reactions, 1)
	events <- Event{Type: EventError, Delta: "boom"}

	if err := <-done; !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("Run error = %v, want ErrStreamFailed", err)
	}
	got := reactions.record()
	if len(got) != 2 || got[0] != channel.ReactionProcessing || got[1] != channel.ReactionError {
		t.Fatalf("reactions = %v, want [processing error]", got)
	}
}

// TestStreamerReactionIncompleteState: an incomplete stream replaces 👀 with ❌.
func TestStreamerReactionIncompleteState(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 10)
	events <- Event{Type: EventDelta, Delta: "partial"}
	close(events)

	if err := s.Run(context.Background(), events); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("Run error = %v, want ErrIncompleteStream", err)
	}
	got := reactions.record()
	if len(got) != 2 || got[0] != channel.ReactionProcessing || got[1] != channel.ReactionError {
		t.Fatalf("reactions = %v, want [processing error]", got)
	}
}

// TestStreamerReactionTimeoutNoMessage: a timeout before any reply message
// exists sends no reactions.
func TestStreamerReactionTimeoutNoMessage(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.noEventTimeout = 30 * time.Millisecond
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 10)
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background(), events) }()

	if err := <-done; !errors.Is(err, ErrTimeout) {
		t.Fatalf("Run error = %v, want ErrTimeout", err)
	}
	if got := reactions.record(); len(got) != 0 {
		t.Fatalf("timeout with no reply message: reactions = %v, want none", got)
	}
}

// TestStreamerReactionCancelLeavesProcessing: cancellation does not change
// the reaction (the task may still be running in the backend).
func TestStreamerReactionCancelLeavesProcessing(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 10)
	events <- Event{Type: EventDelta, Delta: "partial"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, events) }()

	waitReactions(t, reactions, 1)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	got := reactions.record()
	if len(got) != 1 || got[0] != channel.ReactionProcessing {
		t.Fatalf("reactions = %v, want [processing] only", got)
	}
}

// TestStreamerReactionNoSetterIsNoOp: without a setter the stream behaves
// exactly as before.
func TestStreamerReactionNoSetterIsNoOp(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventDelta, Delta: "hello"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := reply.finalMessages()
	if len(msgs) != 1 || msgs[0] != "hello" {
		t.Fatalf("messages = %v, want the reply unchanged", msgs)
	}
}

// TestStreamerReactionTargetsSourceMessage: with a source target set via
// SetReactionTarget, the 👀→✅ lifecycle lands on the source message (a
// read-receipt) and never on the reply.
func TestStreamerReactionTargetsSourceMessage(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Discord)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)
	s.SetReactionTarget(fakeRef{id: "source-1"})

	events := make(chan Event, 4)
	events <- Event{Type: EventDelta, Delta: "hello"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := reactions.record()
	if len(got) != 2 || got[0] != channel.ReactionProcessing || got[1] != channel.ReactionSuccess {
		t.Fatalf("reactions = %v, want [processing success]", got)
	}
	for i, ref := range reactions.recordRefs() {
		if ref != "source-1" {
			t.Fatalf("reaction %d targeted %q, want the source message", i, ref)
		}
	}
}

// TestStreamerReactionFallsBackToReplyRef: without a source target the
// 👀→✅ lifecycle falls back onto the first reply message.
func TestStreamerReactionFallsBackToReplyRef(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Discord)
	reactions := &fakeReactionSetter{}
	s.SetReactionSetter(reactions)

	events := make(chan Event, 4)
	events <- Event{Type: EventDelta, Delta: "hello"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := reactions.record()
	if len(got) != 2 || got[0] != channel.ReactionProcessing || got[1] != channel.ReactionSuccess {
		t.Fatalf("reactions = %v, want [processing success]", got)
	}
	// The reply context issues message refs starting at "msg-1".
	for i, ref := range reactions.recordRefs() {
		if ref != "msg-1" {
			t.Fatalf("reaction %d targeted %q, want the first reply ref", i, ref)
		}
	}
}
