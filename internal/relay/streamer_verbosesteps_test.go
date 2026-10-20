package relay

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func TestVerboseStepsOffSuppressesStepLines(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Discord, 15*time.Minute)
	s.SetVerboseSteps(false)

	events := make(chan Event, 8)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()
	for _, text := range reply.sends {
		if strings.Contains(text, "[Step ") {
			t.Errorf("per-step line must stay off by default: %q", text)
		}
	}
	for id, edits := range reply.edits {
		for _, text := range edits {
			if strings.Contains(text, "[Step ") {
				t.Errorf("per-step line must stay off by default: %q", text)
			}
		}
		_ = id
	}
	summaries := 0
	for _, text := range reply.sends {
		if strings.Contains(text, "2 tool calls") {
			summaries++
		}
	}
	if summaries != 1 {
		t.Fatalf("tool summary lines = %d, want exactly one: %v", summaries, reply.sends)
	}
}

func TestVerboseStepsOnRestoresStepLines(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Discord, 15*time.Minute)
	s.SetVerboseSteps(true)

	events := make(chan Event, 8)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()
	found := false
	for _, text := range reply.sends {
		if strings.Contains(text, "[Step 1] bash") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verbose steps must restore the per-step lines: %v", reply.sends)
	}
}
