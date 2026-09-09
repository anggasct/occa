package relay

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func TestStreamerReasoningOnlyWithoutTools(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	var now atomic.Int64
	now.Store(100)
	s.now = func() time.Time { return time.Unix(now.Load(), 0) }

	events := make(chan Event)
	go func() {
		events <- Event{Type: EventReasoning}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(104)
		events <- Event{Type: EventDelta, Delta: "Here is your answer."}
		events <- Event{Type: EventDone}
		close(events)
	}()

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if len(reply.sends) != 2 {
		t.Fatalf("sends = %v, want 2 (thinking card + answer message)", reply.sends)
	}
	if reply.sends[0] != "🧠 Thinking…" {
		t.Fatalf("initial send = %q, want '🧠 Thinking…'", reply.sends[0])
	}

	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != "🧠 Thought for 4s" {
		t.Fatalf("thinking resolution = %v, want '🧠 Thought for 4s'", edits)
	}

	for _, m := range reply.sends {
		if m == "✅ Task completed" {
			t.Fatalf("unexpected completedNotice sent when thought card present: %v", reply.sends)
		}
	}
}

func TestStreamerReasoningFollowedByTools(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	var now atomic.Int64
	now.Store(100)
	s.now = func() time.Time { return time.Unix(now.Load(), 0) }

	events := make(chan Event)
	go func() {
		events <- Event{Type: EventReasoning}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(105)
		events <- Event{Type: EventTool, Delta: "bash"}
		for {
			reply.mu.Lock()
			edits := len(reply.edits["msg-1"])
			reply.mu.Unlock()
			if edits > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(107)
		events <- Event{Type: EventTool, Delta: "read"}
		events <- Event{Type: EventDone}
		close(events)
	}()

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want exactly 1 progress card", reply.sends)
	}

	wantRollup := "✅ 2 tool calls · 🧠 Thought for 5s\n\n<blockquote expandable>\n• bash ×1\n• read ×1\n</blockquote>"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("final rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerReasoningInterleavedBetweenTools(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	var now atomic.Int64
	now.Store(100)
	s.now = func() time.Time { return time.Unix(now.Load(), 0) }

	events := make(chan Event)
	go func() {
		events <- Event{Type: EventTool, Delta: "bash"}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(102)
		events <- Event{Type: EventReasoning}
		for {
			reply.mu.Lock()
			edits := len(reply.edits["msg-1"])
			reply.mu.Unlock()
			if edits > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(105)
		events <- Event{Type: EventTool, Delta: "grep"}
		events <- Event{Type: EventDone}
		close(events)
	}()

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want 1 progress card", reply.sends)
	}

	wantRollup := "✅ 2 tool calls · 🧠 Thought for 3s\n\n<blockquote expandable>\n• bash ×1\n• grep ×1\n</blockquote>"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("final rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerReasoningErrorIncludesThoughtDuration(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	var now atomic.Int64
	now.Store(100)
	s.now = func() time.Time { return time.Unix(now.Load(), 0) }

	events := make(chan Event)
	go func() {
		events <- Event{Type: EventReasoning}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(107)
		events <- Event{Type: EventTool, Delta: "deploy"}
		events <- Event{Type: EventError, Delta: "failed to connect"}
		close(events)
	}()

	if err := s.Run(context.Background(), events); err == nil {
		t.Fatal("expected error from Run")
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	wantRollup := "⚠️ 1 tool call · deploy ×1 · 🧠 Thought for 7s"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("error rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerReasoningEditThrottle(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	now := time.Unix(200, 0)
	s.now = func() time.Time { return now }
	working := workingState{
		ref:             fakeRef{id: "working"},
		reasoningActive: true,
		reasoningStart:  now,
		rendered:        "🧠 Thinking…",
		pending:         "🧠 Thinking… (1s)",
		lastEditAt:      now,
		hasLastEditAt:   true,
	}

	s.maybeEditWorking(&working)
	if got := reply.editCountFor("working"); got != 0 {
		t.Fatalf("edit count before interval = %d, want 0", got)
	}

	now = now.Add(2 * time.Second)
	s.maybeEditWorking(&working)
	if got := reply.editCountFor("working"); got != 1 {
		t.Fatalf("edit count at interval = %d, want 1", got)
	}
}
