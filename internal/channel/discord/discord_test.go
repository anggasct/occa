package discord

import (
	"strings"
	"testing"
)

func TestSplitMessageShort(t *testing.T) {
	chunks := splitMessage("hello", 2000)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 1500)
	para2 := strings.Repeat("b", 1500)
	text := para1 + "\n\n" + para2

	chunks := splitMessage(text, 2000)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 2000 {
			t.Fatalf("chunk %d exceeds 2000: %d", i, len(chunk))
		}
	}
}

func TestFindBreakPoint(t *testing.T) {
	text := "hello world\n\nsecond paragraph"
	bp := findBreakPoint(text, 20)
	if bp != 11 {
		t.Fatalf("expected break at 11, got %d", bp)
	}
}
