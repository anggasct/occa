package relay

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func toolNoticesOf(msgs []string) []string {
	var notices []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "⚙️ ") || strings.HasPrefix(m, "🔄 ") || strings.HasPrefix(m, "✅ ") || strings.HasPrefix(m, "⚠️ ") {
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

func TestToolBubbleEditsInPlace(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventTool, Delta: "glob"},
	)
	want := []string{"✅ 2 tool calls · glob ×2"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

func TestToolBubblePhaseReset(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventTool, Delta: "glob"},
		Event{Type: EventDelta, Delta: "done, next step"},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "glob"},
	)
	want := []string{"⚙️ [Step 1] glob ×2", "✅ 3 tool calls · glob ×3"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

func TestToolBubbleDistinctTools(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	events := make(chan Event, 3)
	events <- Event{Type: EventTool, Delta: "glob"}
	events <- Event{Type: EventTool, Delta: "grep"}
	events <- Event{Type: EventDone}
	close(events)
	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want exactly 1 progress card message", reply.sends)
	}
	got := toolNoticesOf(reply.finalMessages())
	want := []string{"✅ 2 tool calls · glob ×1 · grep ×1"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

func TestToolBubbleSingleProgressCard(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	events := make(chan Event, 10)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDone}
	close(events)
	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want exactly 1 progress card message", reply.sends)
	}
	want := "✅ 8 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1 · g ×1 · h ×1"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != want {
		t.Fatalf("final edit = %v, want %q", edits, want)
	}
}

func TestToolBubbleEmptySegmentResetsPhase(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "a"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventTool, Delta: "b"}
	events <- Event{Type: EventDone}
	close(events)
	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reply.sends) != 2 {
		t.Fatalf("sends = %v, want 2 progress cards for 2 phases", reply.sends)
	}
}

func TestToolBubbleShortTextResetsBudget(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool, Delta: "a"},
		Event{Type: EventTool, Delta: "b"},
		Event{Type: EventDelta, Delta: strings.Repeat("x", 20)},
		Event{Type: EventSegment},
		Event{Type: EventTool, Delta: "f"},
	)
	want := []string{"⚙️ [Step 2] b", "✅ 3 tool calls · a ×1 · b ×1 · f ×1"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

func TestToolBubbleEmptyNameFallback(t *testing.T) {
	got := runToolEvents(t,
		Event{Type: EventTool},
		Event{Type: EventTool},
	)
	want := []string{"✅ 2 tool calls · Tool call ×2"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

func TestToolBubblesPersistAfterDone(t *testing.T) {
	got := runToolEvents(t, Event{Type: EventTool, Delta: "glob"})
	want := []string{"✅ 1 tool call · glob ×1"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("notices = %v, want %v", got, want)
	}
}

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
	if got := formatToolLabel("read", "main.go", 3); got != "⚙️ read ×3: main.go" {
		t.Fatalf("formatToolLabel(read, main.go, 3) = %q", got)
	}
}

func TestToolBubbleWithContext(t *testing.T) {
	runTest := func(t *testing.T, wantIntermediate string, events ...Event) {
		t.Helper()
		reply := newFakeReplyContext()
		s := NewStreamer(reply, render.New(), render.Telegram)
		s.workingEditInterval = -1

		ch := make(chan Event, len(events)+1)
		for _, e := range events {
			ch <- e
		}
		ch <- Event{Type: EventDone}
		close(ch)

		if err := s.Run(context.Background(), ch); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if len(reply.sends) != 1 {
			t.Fatalf("sends = %v, want 1", reply.sends)
		}
		edits := reply.edits["msg-1"]
		if len(edits) < 2 {
			t.Fatalf("edits on msg-1 = %v, want at least 2 (intermediate + rollup)", edits)
		}
		if edits[len(edits)-2] != wantIntermediate {
			t.Fatalf("intermediate edit = %q, want %q", edits[len(edits)-2], wantIntermediate)
		}
		if edits[len(edits)-1] != "✅ 2 tool calls · read ×2" {
			t.Fatalf("rollup edit = %q, want %q", edits[len(edits)-1], "✅ 2 tool calls · read ×2")
		}
	}

	t.Run("same tool and context edits in place", func(t *testing.T) {
		runTest(t, "⚙️ [Step 1] read ×2: main.go",
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
		)
	})

	t.Run("same tool with different context updates grouped bubble", func(t *testing.T) {
		runTest(t, "⚙️ [Step 1] read ×2: file2.go",
			Event{Type: EventTool, Delta: "read", ToolContext: "file1.go"},
			Event{Type: EventTool, Delta: "read", ToolContext: "file2.go"},
		)
	})

	t.Run("unnormalized context whitespace collapse matches same bubble", func(t *testing.T) {
		runTest(t, "⚙️ [Step 1] read ×2: main.go",
			Event{Type: EventTool, Delta: "read", ToolContext: " main.go \n"},
			Event{Type: EventTool, Delta: "read", ToolContext: "main.go"},
		)
	})
}

func TestToolBubbleContiguousRunGrouping(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 5)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventTool, Delta: "grep"}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want 1 progress card", reply.sends)
	}
	want := "✅ 4 tool calls · bash ×2 · grep ×1 · read ×1"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != want {
		t.Fatalf("rollup = %v, want last %q", edits, want)
	}
}

func TestTerminalRollupResolvesWorkingBubble(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want 1 progress card", reply.sends)
	}
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != "✅ 6 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1" {
		t.Fatalf("terminal rollup = %v", edits)
	}
	for _, m := range reply.finalMessages() {
		if strings.HasPrefix(m, "🔄 ") || strings.HasPrefix(m, "⚙️ ") {
			t.Fatalf("stale working/progress card survived done: %q", m)
		}
	}
}

func TestTerminalRollupErrorPrefixAndCountOrdering(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 12)
	for _, name := range []string{"bash", "bash", "read", "grep", "edit", "write", "deploy"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventError, Delta: "boom"}
	close(events)

	if err := s.Run(context.Background(), events); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("Run error = %v", err)
	}

	if len(reply.sends) != 2 {
		t.Fatalf("sends = %v, want 2 (progress card + agent error notice)", reply.sends)
	}
	edits := reply.edits["msg-1"]
	want := "⚠️ 7 tool calls · bash ×2 · deploy ×1 · edit ×1 · grep ×1 · read ×1 · write ×1"
	if len(edits) == 0 || edits[len(edits)-1] != want {
		t.Fatalf("error rollup = %v, want last %q", edits, want)
	}
}

func TestTerminalRollupOverflowListsEightTypes(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 12)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want 1 progress card", reply.sends)
	}
	edits := reply.edits["msg-1"]
	want := "✅ 9 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1 · g ×1 · h ×1 · +1 more"
	if len(edits) == 0 || edits[len(edits)-1] != want {
		t.Fatalf("overflow rollup = %v, want last %q", edits, want)
	}
}

func TestTerminalRollupCountsAcrossPhases(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 20)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "some narration"}
	events <- Event{Type: EventSegment}
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	edits := reply.edits["msg-3"]
	wantRollup := "✅ 7 tool calls · a ×1 · b ×1 · bash ×1 · c ×1 · d ×1 · e ×1 · f ×1"
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("cross-phase rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestSingleProgressCardResolvesRollup(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "glob"}
	events <- Event{Type: EventTool, Delta: "grep"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %v, want 1 progress card message", reply.sends)
	}
	want := "✅ 2 tool calls · glob ×1 · grep ×1"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != want {
		t.Fatalf("terminal rollup = %v, want %q", edits, want)
	}
	for _, send := range reply.sends {
		if send == "✅ Task completed" {
			t.Fatalf("redundant completedNotice sent: %v", reply.sends)
		}
	}
}

func TestWorkingBubbleSingleMessageAndPendingFlush(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	var now atomic.Int64
	now.Store(int64(100 * time.Second))
	s.now = func() time.Time { return time.Unix(0, now.Load()) }

	events := make(chan Event)
	go func() {
		for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
			events <- Event{Type: EventTool, Delta: name}
		}
		now.Add(int64(time.Second))
		events <- Event{Type: EventTool, Delta: "g"}
		now.Add(int64(500 * time.Millisecond))
		events <- Event{Type: EventTool, Delta: "h"}
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
	if reply.sends[0] != "⚙️ [Step 1] a" {
		t.Fatalf("initial send = %q, want '⚙️ [Step 1] a'", reply.sends[0])
	}
	wantRollup := "✅ 8 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1 · g ×1 · h ×1"
	edits := reply.edits["msg-1"]
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("Working rollup = %v, want %q", edits, wantRollup)
	}
}

func TestWorkingBubbleEditThrottle(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	now := time.Unix(200, 0)
	s.now = func() time.Time { return now }
	working := workingState{
		ref:           fakeRef{id: "working"},
		rendered:      "old",
		pending:       "new",
		lastEditAt:    now,
		hasLastEditAt: true,
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

func TestWorkingRemovalStartsFreshPhase(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)

	events := make(chan Event, 20)
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDelta, Delta: strings.Repeat("x", 60)}
	events <- Event{Type: EventSegment}
	for _, name := range []string{"g", "h", "i", "j", "k", "l"} {
		events <- Event{Type: EventTool, Delta: name}
	}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()
	if len(reply.sends) != 3 {
		t.Fatalf("sends = %v, want 3 messages (phase 1 card, text, phase 2 card)", reply.sends)
	}

	edits := reply.edits["msg-3"]
	wantRollup := "✅ 12 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1 · g ×1 · h ×1 · +4 more"
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("second phase Working rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestWorkingFlushesOnTerminalEvents(t *testing.T) {
	rollup := func(icon string) string {
		return icon + " 7 tool calls · a ×1 · b ×1 · c ×1 · d ×1 · e ×1 · f ×1 · g ×1"
	}
	tests := []struct {
		name         string
		terminal     Event
		wantErr      error
		wantLastEdit string
		configure    func(*Streamer)
		cancelRun    bool
	}{
		{name: "done", terminal: Event{Type: EventDone}, wantLastEdit: rollup("✅")},
		{name: "error", terminal: Event{Type: EventError, Delta: "failed"}, wantErr: ErrStreamFailed, wantLastEdit: rollup("⚠️")},
		{
			name: "timeout", wantErr: ErrTimeout,
			wantLastEdit: "⚙️ [Step 7] g",
			configure:    func(s *Streamer) { s.noEventTimeout = 10 * time.Millisecond },
		},
		{
			name: "cancellation", wantErr: context.Canceled, cancelRun: true,
			wantLastEdit: "⚙️ [Step 7] g",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reply := newFakeReplyContext()
			s := NewStreamer(reply, render.New(), render.Telegram)
			if tc.configure != nil {
				tc.configure(s)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			events := make(chan Event, 10)
			if tc.cancelRun {
				events = make(chan Event)
				go func() {
					for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
						events <- Event{Type: EventTool, Delta: name}
					}
					cancel()
				}()
			} else {
				for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
					events <- Event{Type: EventTool, Delta: name}
				}
				if tc.terminal.Type != "" {
					events <- tc.terminal
					close(events)
				}
			}

			err := s.Run(ctx, events)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Run error = %v, want %v", err, tc.wantErr)
			}
			if got := reply.edits["msg-1"]; len(got) == 0 || got[len(got)-1] != tc.wantLastEdit {
				t.Fatalf("Working terminal edit = %v, want last %q", got, tc.wantLastEdit)
			}
		})
	}
}

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
