package render

import (
	"strings"
	"testing"
)

func TestTelegramCodeBlock(t *testing.T) {
	r := New()
	md := "```go\nfmt.Println(\"hello\")\n```"
	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if !strings.Contains(out, "<pre><code>") {
		t.Fatalf("expected <pre><code>, got: %q", out)
	}
	if !strings.Contains(out, "fmt.Println(&#34;hello&#34;)") && !strings.Contains(out, "fmt.Println(\"hello\")") {
		if !strings.Contains(out, "fmt.Println") {
			t.Fatalf("expected code content, got: %q", out)
		}
	}
}

func TestTelegramEscaping(t *testing.T) {
	r := New()
	md := "Use `<div>` & `span`"
	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if strings.Contains(out, "<div>") {
		t.Fatalf("unescaped <div> in output: %q", out)
	}
	if !strings.Contains(out, "&lt;div&gt;") {
		t.Fatalf("expected escaped div, got: %q", out)
	}
	if !strings.Contains(out, "&amp;") {
		t.Fatalf("expected escaped ampersand, got: %q", out)
	}
}

func TestTelegramBoldItalic(t *testing.T) {
	r := New()
	md := "This is **bold** and *italic*"
	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if !strings.Contains(out, "<b>bold</b>") {
		t.Fatalf("expected <b>bold</b>, got: %q", out)
	}
	if !strings.Contains(out, "<i>italic</i>") {
		t.Fatalf("expected <i>italic</i>, got: %q", out)
	}
}

func TestDiscordCodeBlock(t *testing.T) {
	r := New()
	md := "```python\nprint('hi')\n```"
	chunks, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if !strings.Contains(out, "```python") {
		t.Fatalf("expected fenced code, got: %q", out)
	}
}

func TestDiscordBoldItalic(t *testing.T) {
	r := New()
	md := "This is **bold** and *italic*"
	chunks, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if !strings.Contains(out, "**bold**") {
		t.Fatalf("expected **bold**, got: %q", out)
	}
	if !strings.Contains(out, "*italic*") {
		t.Fatalf("expected *italic*, got: %q", out)
	}
}

func TestSplitLongOutput(t *testing.T) {
	r := New()
	paras := make([]string, 50)
	for i := range paras {
		paras[i] = strings.Repeat("word ", 30)
	}
	md := strings.Join(paras, "\n\n")

	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds 4096: %d", i, len(chunk))
		}
	}
}

func TestSplitDoesNotBreakCodeBlock(t *testing.T) {
	r := New()
	code := strings.Repeat("x := 1\n", 200)
	md := "Before\n\n```go\n" + code + "```\n\nAfter"

	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, chunk := range chunks {
		openCount := strings.Count(chunk, "<pre><code>")
		closeCount := strings.Count(chunk, "</code></pre>")
		if openCount != closeCount {
			t.Fatalf("code block broken across chunks: open=%d close=%d in %q", openCount, closeCount, chunk[:min(100, len(chunk))])
		}
	}
}

func TestMalformedMarkdownNoPanic(t *testing.T) {
	r := New()
	inputs := []string{
		"```unclosed code block",
		"**unclosed bold",
		"<<<>>>&&&",
		"",
		"\x00\x01\x02",
	}
	for _, input := range inputs {
		_, err := r.Render(input, Telegram)
		if err != nil {
			t.Fatalf("Render(%q): %v", input, err)
		}
	}
}

func TestList(t *testing.T) {
	r := New()
	md := "- item one\n- item two\n- item three"
	chunks, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := chunks[0]
	if !strings.Contains(out, "• item one") {
		t.Fatalf("expected bullet list, got: %q", out)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
