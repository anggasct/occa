package relay

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/render"
)

// TestToolNoticeAggregatesCounts: a contiguous tool run of the same tool
// renders as one notice with a count.
func TestToolNoticeAggregatesCounts(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "result"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") {
			notices = append(notices, m)
		}
	}
	if len(notices) != 1 || notices[0] != "⚙️ bash ×2" {
		t.Fatalf("notices = %v, want one '⚙️ bash ×2' (messages: %v)", notices, msgs)
	}
}

// TestToolNoticeDistinctNamesInOrder: different tools in one run are listed
// in order of first use with per-name counts.
func TestToolNoticeDistinctNamesInOrder(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "edit"}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "done"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") {
			notices = append(notices, m)
		}
	}
	if len(notices) != 1 || notices[0] != "⚙️ bash ×2, edit" {
		t.Fatalf("notices = %v, want '⚙️ bash ×2, edit'", notices)
	}
}

// TestToolNoticeSeparateRuns: text between tool runs produces one notice per
// run.
func TestToolNoticeSeparateRuns(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "first result"}
	events <- Event{Type: EventTool, Delta: "edit"}
	events <- Event{Type: EventTool, Delta: "edit"}
	events <- Event{Type: EventDelta, Delta: "second result"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") {
			notices = append(notices, m)
		}
	}
	if len(notices) != 2 || notices[0] != "⚙️ bash" || notices[1] != "⚙️ edit ×2" {
		t.Fatalf("notices = %v, want '⚙️ bash' then '⚙️ edit ×2'", notices)
	}
}

// TestToolNoticeFlushedOnDone: a tool run at the end of the stream (no text
// after) still renders its notice before the terminal reaction.
func TestToolNoticeFlushedOnDone(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	if len(msgs) != 1 || msgs[0] != "⚙️ bash ×2" {
		t.Fatalf("messages = %v, want one '⚙️ bash ×2'", msgs)
	}
}

// TestToolNoticeEmptyNameFallback: tool parts without a name fall back to a
// generic label, still aggregated.
func TestToolNoticeEmptyNameFallback(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool}
	events <- Event{Type: EventTool}
	events <- Event{Type: EventDelta, Delta: "result"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") {
			notices = append(notices, m)
		}
	}
	if len(notices) != 1 || notices[0] != "⚙️ Tool call ×2" {
		t.Fatalf("notices = %v, want '⚙️ Tool call ×2'", notices)
	}
}

// TestFormatToolRunCapsNames: more than 4 distinct tools are truncated with
// a remainder count.
func TestFormatToolRunCapsNames(t *testing.T) {
	order := []string{"a", "b", "c", "d", "e", "f"}
	counts := map[string]int{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1}
	got := formatToolRun(order, counts)
	if got != "⚙️ a, b, c, d, … +2 more" {
		t.Fatalf("formatToolRun = %q", got)
	}
}

// TestToolNoticeSinglePerRunWithSegment: the decoder emits a segment event
// between a tool run and the next text — the run must produce exactly one
// notice despite the intervening segment.
func TestToolNoticeSinglePerRunWithSegment(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventDelta, Delta: "result"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") {
			notices = append(notices, m)
		}
	}
	if len(notices) != 1 || notices[0] != "⚙️ bash ×2" {
		t.Fatalf("notices = %v, want exactly one '⚙️ bash ×2' (messages: %v)", notices, msgs)
	}
}
