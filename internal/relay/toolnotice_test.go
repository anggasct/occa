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

// TestToolBubbleCapShowsWorkingIndicator: after 5 distinct bubbles in a phase,
// further new tools stop creating bubbles and a single working indicator appears.
// A substantial text block (EventSegment carrying >= maxToolBubbleResetRunes)
// resets the bubble count so subsequent tools start fresh bubbles again.
func TestToolBubbleCapShowsWorkingIndicator(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "a"},
		Event{Type: EventTool, Delta: "b"},
		Event{Type: EventTool, Delta: "c"},
		Event{Type: EventTool, Delta: "d"},
		Event{Type: EventTool, Delta: "e"},
		Event{Type: EventTool, Delta: "f"},
		Event{Type: EventDelta, Delta: strings.Repeat("x", 60)},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "g"},
		Event{Type: EventTool, Delta: "h"},
	)

	want := []string{
		"⚙️ a",
		"⚙️ b",
		"⚙️ c",
		"⚙️ d",
		"⚙️ e",
		"🔄 Working…",
		"⚙️ g",
		"⚙️ h",
	}

	if len(got) != len(want) {
		t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
		}
	}
}

// TestToolBubbleCapNotResetByEmptySegment: an EventSegment carrying no text
// never resets the budget, so a burst of 10 distinct tools still caps at 5
// bubbles plus a single working indicator.
func TestToolBubbleCapNotResetByEmptySegment(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "a"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "b"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "c"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "d"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "e"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "f"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "g"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "h"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "i"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "j"},
	)

	want := []string{
		"⚙️ a",
		"⚙️ b",
		"⚙️ c",
		"⚙️ d",
		"⚙️ e",
		"🔄 Working…",
	}

	if len(got) != len(want) {
		t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
		}
	}
}

// TestToolBubbleCapNotResetByShortText: an EventSegment carrying fewer runes
// than the threshold does not reset the budget, so later tools still collapse
// to the working indicator.
func TestToolBubbleCapNotResetByShortText(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "a"},
		Event{Type: EventTool, Delta: "b"},
		Event{Type: EventTool, Delta: "c"},
		Event{Type: EventTool, Delta: "d"},
		Event{Type: EventTool, Delta: "e"},
		Event{Type: EventDelta, Delta: strings.Repeat("x", 20)},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "f"},
	)

	want := []string{
		"⚙️ a",
		"⚙️ b",
		"⚙️ c",
		"⚙️ d",
		"⚙️ e",
		"🔄 Working…",
	}

	if len(got) != len(want) {
		t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
		}
	}
}

// TestToolBubbleCapResetBySubstantialText: an EventSegment carrying text at or
// above the threshold resets the budget, so later tools render fresh bubbles.
func TestToolBubbleCapResetBySubstantialText(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "a"},
		Event{Type: EventTool, Delta: "b"},
		Event{Type: EventTool, Delta: "c"},
		Event{Type: EventTool, Delta: "d"},
		Event{Type: EventTool, Delta: "e"},
		Event{Type: EventTool, Delta: "f"},
		Event{Type: EventDelta, Delta: strings.Repeat("x", 60)},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "g"},
		Event{Type: EventTool, Delta: "h"},
	)

	want := []string{
		"⚙️ a",
		"⚙️ b",
		"⚙️ c",
		"⚙️ d",
		"⚙️ e",
		"🔄 Working…",
		"⚙️ g",
		"⚙️ h",
	}

	if len(got) != len(want) {
		t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
		}
	}
}

// TestToolBubbleCapThresholdBoundary: text of exactly maxToolBubbleResetRunes
// resets the budget; one rune fewer does not.
func TestToolBubbleCapThresholdBoundary(t *testing.T) {
	run := func(t *testing.T, textLen int) []string {
		t.Helper()
		return runToolEvents(t,
			Event{Type: EventTool, Delta: "a"},
			Event{Type: EventTool, Delta: "b"},
			Event{Type: EventTool, Delta: "c"},
			Event{Type: EventTool, Delta: "d"},
			Event{Type: EventTool, Delta: "e"},
			Event{Type: EventTool, Delta: "f"},
			Event{Type: EventDelta, Delta: strings.Repeat("x", textLen)},
			Event{Type: EventSegment},
			Event{Type: EventTool, Delta: "g"},
		)
	}

	t.Run("at threshold resets", func(t *testing.T) {
		got := run(t, maxToolBubbleResetRunes)
		want := []string{
			"⚙️ a",
			"⚙️ b",
			"⚙️ c",
			"⚙️ d",
			"⚙️ e",
			"🔄 Working…",
			"⚙️ g",
		}
		if len(got) != len(want) {
			t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
			}
		}
	})

	t.Run("below threshold does not reset", func(t *testing.T) {
		got := run(t, maxToolBubbleResetRunes-1)
		want := []string{
			"⚙️ a",
			"⚙️ b",
			"⚙️ c",
			"⚙️ d",
			"⚙️ e",
			"🔄 Working…",
		}
		if len(got) != len(want) {
			t.Fatalf("notices len = %d, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("notices[%d] = %q, want %q (got: %v)", i, got[i], want[i], got)
			}
		}
	})
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
	if got := formatToolLabel("glob", "", 1); got != "⚙️ glob" {
		t.Fatalf("formatToolLabel(1) = %q", got)
	}
	if got := formatToolLabel("glob", "", 3); got != "⚙️ glob ×3" {
		t.Fatalf("formatToolLabel(3) = %q", got)
	}
}

func TestNormalizeToolContext(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"empty", "", ""},
		{"whitespace only", "   \n\t  ", ""},
		{"normal text", "internal/relay/streamer.go", "internal/relay/streamer.go"},
		{"collapse whitespace and newlines", "  go   test\n\t ./...  ", "go test ./..."},
		{"exact 40 runes", "1234567890123456789012345678901234567890", "1234567890123456789012345678901234567890"},
		{"over 40 runes truncated to 40 with ellipsis", "12345678901234567890123456789012345678901", "123456789012345678901234567890123456789…"},
		{"unicode multi-byte runes over 40 truncated", "αβγδεζηθικλμνξοπρστυφχψωαβγδεζηθικλμνξοπρστυφχψω", "αβγδεζηθικλμνξοπρστυφχψωαβγδεζηθικλμνξο…"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeToolContext(c.raw)
			if got != c.want {
				t.Fatalf("normalizeToolContext(%q) = %q, want %q", c.raw, got, c.want)
			}
			if runeCount := len([]rune(got)); runeCount > maxToolContextRunes {
				t.Fatalf("rune count = %d, exceeds max %d", runeCount, maxToolContextRunes)
			}
		})
	}
}

func TestFormatToolLabelWithContext(t *testing.T) {
	if got := formatToolLabel("glob", "", 1); got != "⚙️ glob" {
		t.Fatalf("formatToolLabel(glob, empty, 1) = %q", got)
	}
	if got := formatToolLabel("glob", "", 3); got != "⚙️ glob ×3" {
		t.Fatalf("formatToolLabel(glob, empty, 3) = %q", got)
	}
	if got := formatToolLabel("read", "main.go", 1); got != "⚙️ read: main.go" {
		t.Fatalf("formatToolLabel(read, main.go, 1) = %q", got)
	}
	if got := formatToolLabel("read", "main.go", 3); got != "⚙️ read: main.go ×3" {
		t.Fatalf("formatToolLabel(read, main.go, 3) = %q", got)
	}
}

func TestToolBubbleWithContext(t *testing.T) {
	t.Run("same tool and context edits in place", func(t *testing.T) {
		got := runToolEvents(t,
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
		)
		want := []string{"⚙️ read: main.go ×2"}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("notices = %v, want %v", got, want)
		}
	})

	t.Run("same tool with different context starts new bubble", func(t *testing.T) {
		got := runToolEvents(t,
			Event{Type: EventTool, Delta: "read", ToolContext: "file1.go"},
			Event{Type: EventTool, Delta: "read", ToolContext: "file2.go"},
		)
		want := []string{"⚙️ read: file1.go", "⚙️ read: file2.go"}
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("notices = %v, want %v", got, want)
		}
	})

	t.Run("unnormalized context whitespace collapse matches same bubble", func(t *testing.T) {
		got := runToolEvents(t,
			Event{Type: EventTool, Delta: "read", ToolContext: " main.go \n"},
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
		)
		want := []string{"⚙️ read: main.go ×2"}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("notices = %v, want %v", got, want)
		}
	})
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
