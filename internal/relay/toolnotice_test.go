package relay

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func toolNoticesOf(msgs []string) []string {
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") || strings.HasPrefix(m, "🔄 ") {
			notices = append(notices, m)
		}
	}
	return notices
}

func runToolEvents(t *testing.T, events ...Event) []string {
	t.Helper()
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	ch := make(chan Event, len(events)+1)
	for _, e := range events {
		ch <- e
	}
	ch <- Event{Type: EventDone}
	close(ch)

	if err := s.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return toolNoticesOf(reply.finalMessages())
}

// TestToolBubbleEditsInPlace: repeats of the same tool within one phase edit
// the same bubble with a count.
func TestToolBubbleEditsInPlace(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventTool, Delta: "glob"},
	)
	want := []string{"⚙️ glob ×2"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

// TestToolBubblePhaseReset: text ends the phase — the same tool after a
// message starts a fresh bubble instead of incrementing the old one.
func TestToolBubblePhaseReset(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventDelta, Delta: "answer"},
		Event{Type: EventTool, Delta: "glob"},
	)
	want := []string{"⚙️ glob ×2", "⚙️ glob"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

// TestToolBubbleDistinctTools: different tools get separate bubbles.
func TestToolBubbleDistinctTools(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventTool, Delta: "grep"},
	)
	want := []string{"⚙️ glob", "⚙️ grep"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

// TestToolBubbleCapShowsWorkingIndicator: after 5 distinct bubbles, further
// new tools stop creating bubbles and a single working indicator appears.
func TestToolBubbleCapShowsWorkingIndicator(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f", "g"}
	events := make([]Event, 0, len(names)+1)
	for _, n := range names {
		events = append(events, Event{Type: EventTool, Delta: n})
	}
	got := runToolEvents(t, events...)

	var bubbles, working []string
	for _, n := range got {
		if strings.HasPrefix(n, "🔄 ") {
			working = append(working, n)
		} else {
			bubbles = append(bubbles, n)
		}
	}
	if len(bubbles) != 5 {
		t.Fatalf("bubbles = %v, want exactly 5", bubbles)
	}
	if len(working) != 1 || working[0] != "🔄 Working…" {
		t.Fatalf("working = %v, want exactly one indicator", working)
	}
}

// TestToolBubbleEmptyNameFallback: tool parts without a name fall back to a
// generic label, still edited in place with counts.
func TestToolBubbleEmptyNameFallback(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool},
		Event{Type: EventTool},
	)
	want := []string{"⚙️ Tool call ×2"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

// TestToolBubblesPersistAfterDone: bubbles remain after the response ends.
func TestToolBubblesPersistAfterDone(t *testing.T) {
	got := runToolEvents(t, Event{Type: EventTool, Delta: "glob"})
	want := []string{"⚙️ glob"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

// TestFormatToolLabel: counts render only above one.
func TestFormatToolLabel(t *testing.T) {
	if got := formatToolLabel("glob", 1); got != "⚙️ glob" {
		t.Fatalf("formatToolLabel(1) = %q", got)
	}
	if got := formatToolLabel("glob", 3); got != "⚙️ glob ×3" {
		t.Fatalf("formatToolLabel(3) = %q", got)
	}
}

// TestToolBubbleNonConsecutiveReset: the same tool separated by other tools
// starts a fresh bubble instead of incrementing the old one.
func TestToolBubbleNonConsecutiveReset(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "bash"},
		Event{Type: EventTool, Delta: "grep"},
		Event{Type: EventTool, Delta: "read"},
		Event{Type: EventTool, Delta: "bash"},
	)
	want := []string{"⚙️ bash", "⚙️ grep", "⚙️ read", "⚙️ bash"}
	if len(got) != len(want) {
		t.Fatalf("notices = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("notices = %v, want %v", got, want)
		}
	}
}

// TestTypingHeartbeat: a short-typed stream emits the typing indicator at
// least once, including during a silent gap before any output.
func TestTypingHeartbeat(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.typingInterval = 10 * time.Millisecond

	ch := make(chan Event, 8)
	for i := 0; i < 5; i++ {
		ch <- Event{Type: EventDelta, Delta: "x"}
	}
	ch <- Event{Type: EventDone}
	close(ch)

	if err := s.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reply.mu.Lock()
	n := reply.typings
	reply.mu.Unlock()
	if n == 0 {
		t.Fatal("SendTyping was never called")
	}
}
