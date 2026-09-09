package relay

import (
	"context"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func TestStreamerStopButtonLifecycleTool(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:sess-123")
	s.workingEditInterval = -1

	events := make(chan Event, 5)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	sentBtns := reply.sentButtons["msg-1"]
	if len(sentBtns) != 1 || sentBtns[0].Label != "🛑 Stop" || sentBtns[0].Value != "stop:sess-123" {
		t.Fatalf("sentButtons = %+v, want [🛑 Stop / stop:sess-123]", sentBtns)
	}

	edits := reply.editButtons["msg-1"]
	if len(edits) == 0 {
		t.Fatalf("expected edit buttons recorded, got none")
	}

	finalBtns := edits[len(edits)-1]
	if len(finalBtns) != 0 {
		t.Fatalf("terminal edit buttons = %+v, want empty/cleared", finalBtns)
	}
}

func TestStreamerStopButtonLifecycleReasoning(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:sess-456")
	s.workingEditInterval = -1

	events := make(chan Event, 5)
	events <- Event{Type: EventReasoning}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	sentBtns := reply.sentButtons["msg-1"]
	if len(sentBtns) != 1 || sentBtns[0].Label != "🛑 Stop" || sentBtns[0].Value != "stop:sess-456" {
		t.Fatalf("sentButtons = %+v, want [🛑 Stop / stop:sess-456]", sentBtns)
	}

	edits := reply.editButtons["msg-1"]
	if len(edits) == 0 {
		t.Fatalf("expected edit buttons recorded, got none")
	}

	finalBtns := edits[len(edits)-1]
	if len(finalBtns) != 0 {
		t.Fatalf("terminal edit buttons = %+v, want empty/cleared", finalBtns)
	}
}

func TestStreamerStopButtonClearedOnContextCanceled(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:sess-789")
	s.workingEditInterval = -1

	ctx, cancel := context.WithCancel(context.Background())
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
		cancel()
	}()

	_ = s.Run(ctx, events)

	reply.mu.Lock()
	defer reply.mu.Unlock()

	edits := reply.editButtons["msg-1"]
	if len(edits) == 0 {
		t.Fatalf("expected edit on cancel, got none")
	}
	lastBtns := edits[len(edits)-1]
	if len(lastBtns) != 0 {
		t.Fatalf("cancelled edit buttons = %+v, want empty/cleared", lastBtns)
	}
}

func TestStreamerStopButtonOmittedWhenNotConfigured(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 5)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if len(reply.sentButtons["msg-1"]) != 0 {
		t.Fatalf("sentButtons must be empty when stopCallbackData is unset, got %+v", reply.sentButtons["msg-1"])
	}
}
