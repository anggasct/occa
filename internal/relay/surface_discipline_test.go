package relay

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"
)

func runSurfaceStreamer(t *testing.T, narration, verbose bool, events []Event) *mockSink {
	t.Helper()
	sink := &mockSink{}
	s := NewStreamerWithSink(sink, render.New(), render.Discord, time.Minute)
	s.SetVerboseSteps(verbose)
	s.SetNarration(narration)
	ch := make(chan Event, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)
	if err := s.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return sink
}

func TestSurfaceDiscipline_AC05(t *testing.T) {
	sink := runSurfaceStreamer(t, false, false, []Event{
		{Type: EventTool, Delta: "bash", ToolContext: "git status"},
		{Type: EventTool, Delta: "bash", ToolContext: "gh pr view"},
		{Type: EventTool, Delta: "read_file", ToolContext: "main.go"},
		{Type: EventTool, Delta: "read_file", ToolContext: "workflow.go"},
		{Type: EventDelta, Delta: "done"},
		{Type: EventDone},
	})
	var rollups int
	for _, text := range sink.sent {
		if strings.Contains(text, "[Step ") {
			t.Errorf("step line leaked: %q", text)
		}
		if strings.Contains(text, "4 tool calls") {
			rollups++
		}
	}
	if rollups != 1 {
		t.Fatalf("rollup lines = %d, want 1: %v", rollups, sink.sent)
	}
}

func TestSurfaceDiscipline_AC06(t *testing.T) {
	sink := runSurfaceStreamer(t, false, true, []Event{
		{Type: EventDelta, Delta: "first segment"},
		{Type: EventReasoning, Delta: "thinking"},
		{Type: EventDelta, Delta: "second segment"},
		{Type: EventReasoning, Delta: "thinking"},
		{Type: EventDelta, Delta: "third segment"},
		{Type: EventDone},
	})
	var assistant []string
	for _, text := range sink.sent {
		if strings.Contains(text, "segment") {
			assistant = append(assistant, text)
		}
	}
	if len(assistant) != 1 {
		t.Fatalf("assistant messages = %d, want exactly one outcome: %v", len(assistant), sink.sent)
	}
	if !strings.Contains(assistant[0], "third segment") {
		t.Fatalf("outcome = %q, want the last segment", assistant[0])
	}
	for _, banned := range []string{"first segment", "second segment"} {
		if strings.Contains(assistant[0], banned) {
			t.Fatalf("outcome carried narration %q: %q", banned, assistant[0])
		}
	}
}

func TestSurfaceDiscipline_AC07(t *testing.T) {
	sink := runSurfaceStreamer(t, true, true, []Event{
		{Type: EventDelta, Delta: "first segment"},
		{Type: EventReasoning, Delta: "thinking"},
		{Type: EventDelta, Delta: "second segment"},
		{Type: EventReasoning, Delta: "thinking"},
		{Type: EventDelta, Delta: "third segment"},
		{Type: EventDone},
	})
	var assistant []string
	for _, text := range sink.sent {
		if strings.Contains(text, "segment") {
			assistant = append(assistant, text)
		}
	}
	if len(assistant) != 3 {
		t.Fatalf("assistant messages = %d, want one per segment: %v", len(assistant), sink.sent)
	}
	for i, want := range []string{"first segment", "second segment", "third segment"} {
		if !strings.Contains(assistant[i], want) {
			t.Fatalf("message %d = %q, want %q", i, assistant[i], want)
		}
	}
}
