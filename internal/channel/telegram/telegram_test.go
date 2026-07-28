package telegram

import (
	"strings"
	"testing"
)

func TestSplitMessageShort(t *testing.T) {
	chunks := splitMessage("hello world", 4096)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0] != "hello world" {
		t.Fatalf("unexpected chunk: %q", chunks[0])
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 3000)
	para2 := strings.Repeat("b", 3000)
	text := para1 + "\n\n" + para2

	chunks := splitMessage(text, 4096)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds max: %d", i, len(chunk))
		}
	}
}

func TestSplitMessageCodeBlock(t *testing.T) {
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = strings.Repeat("x", 80)
	}
	text := strings.Join(lines, "\n")

	chunks := splitMessage(text, 4096)
	for i, chunk := range chunks {
		if len(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds max: %d", i, len(chunk))
		}
	}
	rejoined := strings.Join(chunks, "\n")
	if len(rejoined) < len(text)-100 {
		t.Fatal("lost significant content during split")
	}
}

func TestFindBreakPoint(t *testing.T) {
	text := "hello world\n\nsecond paragraph"
	bp := findBreakPoint(text, 20)
	if bp != 11 {
		t.Fatalf("expected break at 11 (double newline), got %d", bp)
	}
}
