package relay

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/render"
)

// TestStreamerToolSamePartSkipsIdenticalEdit: a ToolSamePart follow-up that
// renders the same label (same tool, same context) must NOT call Edit again —
// Telegram rejects no-op edits with "message is not modified" (observed live:
// "streaming: tool notice context edit failed" WARN spam).
func TestStreamerToolSamePartSkipsIdenticalEdit(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "read"}                                             // bubble ⚙️ read
	events <- Event{Type: EventTool, Delta: "read", ToolContext: "foo.txt", ToolSamePart: true} // context arrives → edit
	events <- Event{Type: EventTool, Delta: "read", ToolContext: "foo.txt", ToolSamePart: true} // identical → skip
	events <- Event{Type: EventTool, Delta: "read", ToolContext: "bar.txt", ToolSamePart: true} // changed → edit
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// One bubble was sent; only the context changes should have edited it.
	edits := reply.edits["msg-1"]
	if len(edits) != 2 {
		t.Fatalf("edits on msg-1 = %d (%v), want 2 (context foo.txt, then bar.txt)", len(edits), edits)
	}
	if edits[0] != "⚙️ read: foo.txt" {
		t.Fatalf("edit[0] = %q, want %q", edits[0], "⚙️ read: foo.txt")
	}
	if edits[1] != "⚙️ read: bar.txt" {
		t.Fatalf("edit[1] = %q, want %q", edits[1], "⚙️ read: bar.txt")
	}
}
