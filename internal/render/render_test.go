package render

import (
	"strings"
	"testing"
	"unicode/utf8"
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

func TestSplitNeverEmitsInvalidUTF8(t *testing.T) {
	inputs := map[string]string{
		"ascii":            strings.Repeat("a", 5000),
		"cjk":              strings.Repeat("日", 3000),
		"thai":             strings.Repeat("ก", 3000),
		"emoji":            strings.Repeat("🎉", 2000),
		"mixed":            strings.Repeat("halo 日本 🎉 ", 500),
		"no-whitespace":    strings.Repeat("日本語テキスト", 800),
		"single-long-word": strings.Repeat("Z", 9000),
	}

	for name, in := range inputs {
		for _, limit := range []int{4, 17, 100, 2000, 4096} {
			chunks := Split(in, limit)
			for i, chunk := range chunks {
				if !utf8.ValidString(chunk) {
					t.Fatalf("%s/limit=%d: chunk %d is not valid UTF-8", name, limit, i)
				}
				if measure(chunk) > limit && utf8.RuneCountInString(chunk) > 1 {
					t.Fatalf("%s/limit=%d: chunk %d measures %d", name, limit, i, measure(chunk))
				}
			}
			if joined := strings.Join(chunks, ""); utf8.RuneCountInString(joined) > utf8.RuneCountInString(in) {
				t.Fatalf("%s/limit=%d: split added content", name, limit)
			}
		}
	}
}

func TestSplitPrefersBoundaries(t *testing.T) {
	if got := Split("hello world\n\nsecond paragraph", 20); got[0] != "hello world" {
		t.Fatalf("paragraph boundary not preferred: %q", got[0])
	}
	if got := Split("alpha beta\ngamma delta epsilon", 16); got[0] != "alpha beta" {
		t.Fatalf("line boundary not preferred: %q", got[0])
	}
	if got := Split("alpha beta gamma delta", 16); got[0] != "alpha beta" {
		t.Fatalf("word boundary not preferred: %q", got[0])
	}
	if got := Split("<pre><code>x</code></pre>tail text here", 30); got[0] != "<pre><code>x</code></pre>" {
		t.Fatalf("code block boundary not preferred: %q", got[0])
	}
}

func TestSplitMeasuresSurrogatePairs(t *testing.T) {
	// One astral rune is two UTF-16 units, which is what Telegram counts.
	if got := Split("🎉🎉", 2); len(got) != 2 {
		t.Fatalf("expected one chunk per surrogate pair, got %d", len(got))
	}
	if got := Split("🎉", 1); len(got) != 1 || got[0] != "🎉" {
		t.Fatalf("a rune larger than the limit must still advance: %v", got)
	}
}

func TestRenderUnknownPlatformFallsBack(t *testing.T) {
	chunks, err := New().Render("hello", Platform(99))
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(chunks) == 0 || chunks[0] == "" {
		t.Fatalf("unknown platform produced no output: %v", chunks)
	}
}

func TestPlatformFor(t *testing.T) {
	if PlatformFor("discord") != Discord || PlatformFor("telegram") != Telegram || PlatformFor("") != Telegram {
		t.Fatal("unexpected platform mapping")
	}
}

func TestLiteralMarkupIsEscapedNotDropped(t *testing.T) {
	chunks, err := New().Render("value <x> & <script>alert(1)</script> done", Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	got := strings.Join(chunks, "")
	for _, want := range []string{"&lt;x&gt;", "&amp;", "&lt;script&gt;", "alert(1)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "<script>") {
		t.Fatalf("raw markup survived: %q", got)
	}
}

func TestLiteralMarkupPreservedForDiscord(t *testing.T) {
	chunks, err := New().Render("value <x> & more", Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := strings.Join(chunks, ""); !strings.Contains(got, "<x>") || !strings.Contains(got, "&") {
		t.Fatalf("discord content altered: %q", got)
	}
}

func TestSplitNeverBreaksTagBalance(t *testing.T) {
	long := strings.Repeat("filler line\n", 500)
	md := "**bold start " + long + "bold end**\n\nnormal text here"

	chunks, err := New().RenderWithLimit(md, Telegram, 4096)
	if err != nil {
		t.Fatalf("RenderWithLimit: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected a multi-chunk split, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if !htmlBalanced(chunk) {
			t.Fatalf("chunk %d is not tag-balanced: %q", i, chunk[:60])
		}
		if measure(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds the limit: %d units", i, measure(chunk))
		}
	}
	// The hard cut must have repaired the tags exactly.
	if !strings.HasSuffix(chunks[0], "</b>") {
		t.Fatalf("chunk 0 must end with the repaired close tag: %q", chunks[0][len(chunks[0])-20:])
	}
	if !strings.HasPrefix(chunks[1], "<b>") {
		t.Fatalf("chunk 1 must reopen the repaired tag: %q", chunks[1][:20])
	}
}

func TestSplitStrayCloseTagDoesNotPanic(t *testing.T) {
	chunks := Split("</b>"+strings.Repeat("x", 5000), 4096)
	if len(chunks) == 0 {
		t.Fatal("expected chunks")
	}
	for i, chunk := range chunks {
		if measure(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds the limit: %d units", i, measure(chunk))
		}
	}
}

func TestSplitPrefersBalancedBoundaryInsideBoldSpan(t *testing.T) {
	// A multi-line bold span: the line boundary inside the span is not a safe
	// break, so the split must land after the closing tag.
	md := "**line one\nline two\nline three**\n\n" + strings.Repeat("pad ", 2000)
	chunks, err := New().RenderWithLimit(md, Telegram, 4096)
	if err != nil {
		t.Fatalf("RenderWithLimit: %v", err)
	}
	for i, chunk := range chunks {
		if !htmlBalanced(chunk) {
			t.Fatalf("chunk %d is not tag-balanced: %q", i, chunk[:60])
		}
	}
}

func TestStrikethrough(t *testing.T) {
	r := New()
	md := "This is ~~deleted~~ text."

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(tg[0], "<s>deleted</s>") {
		t.Fatalf("expected <s>deleted</s>, got: %q", tg[0])
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(dc[0], "~~deleted~~") {
		t.Fatalf("expected ~~deleted~~, got: %q", dc[0])
	}
}

func TestTable(t *testing.T) {
	r := New()
	md := "| A | B |\n|---|---|\n| 1 | 2 |\n"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "<pre><code>| A | B |\n| --- | --- |\n| 1 | 2 |\n</code></pre>"
	if tg[0] != want {
		t.Fatalf("telegram table mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	wantDC := "```\n| A | B |\n| --- | --- |\n| 1 | 2 |\n```"
	if dc[0] != wantDC {
		t.Fatalf("discord table mismatch:\n got:  %q\n want: %q", dc[0], wantDC)
	}
}

func TestTaskList(t *testing.T) {
	r := New()
	md := "- [ ] todo\n- [x] done\n"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "• ☐ todo\n• ☑ done"
	if tg[0] != want {
		t.Fatalf("telegram tasklist mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if dc[0] != want {
		t.Fatalf("discord tasklist mismatch:\n got:  %q\n want: %q", dc[0], want)
	}
}

func TestLink(t *testing.T) {
	r := New()
	md := "[docs](https://example.com)"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := `<a href="https://example.com">docs</a>`
	if tg[0] != want {
		t.Fatalf("telegram link mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	wantDC := "[docs](https://example.com)"
	if dc[0] != wantDC {
		t.Fatalf("discord link mismatch:\n got:  %q\n want: %q", dc[0], wantDC)
	}
}

func TestLinkDestinationWithQuoteIsAttributeEscaped(t *testing.T) {
	r := New()
	md := `[docs](https://example.com/"x)`

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := `<a href="https://example.com/&quot;x">docs</a>`
	if tg[0] != want {
		t.Fatalf("expected attribute-escaped quote:\n got:  %q\n want: %q", tg[0], want)
	}
	if strings.Contains(tg[0], `x">docs</a>"`) {
		t.Fatalf("destination broke out of the href attribute: %q", tg[0])
	}
}

func TestAutoLink(t *testing.T) {
	r := New()
	md := "<https://example.com>"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := `<a href="https://example.com">https://example.com</a>`
	if tg[0] != want {
		t.Fatalf("telegram autolink mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if dc[0] != "https://example.com" {
		t.Fatalf("discord autolink mismatch:\n got:  %q\n want: %q", dc[0], "https://example.com")
	}
}

func TestBlockquote(t *testing.T) {
	r := New()
	md := "> quoted\n> two lines\n\nAfter."

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "<blockquote>quoted\ntwo lines</blockquote>\n\nAfter."
	if tg[0] != want {
		t.Fatalf("telegram blockquote mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	wantDC := "> quoted\n> two lines\n\nAfter."
	if dc[0] != wantDC {
		t.Fatalf("discord blockquote mismatch:\n got:  %q\n want: %q", dc[0], wantDC)
	}
}

func TestThematicBreak(t *testing.T) {
	r := New()
	md := "Before\n\n---\n\nAfter"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if tg[0] == "Before\n\nAfter" {
		t.Fatalf("thematic break vanished: %q", tg[0])
	}
	want := "Before\n\n──────────\n\nAfter"
	if tg[0] != want {
		t.Fatalf("telegram thematic break mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	wantDC := "Before\n\n---\n\nAfter"
	if dc[0] != wantDC {
		t.Fatalf("discord thematic break mismatch:\n got:  %q\n want: %q", dc[0], wantDC)
	}
}

func TestOrderedList(t *testing.T) {
	r := New()
	md := "1. first\n2. second"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "1. first\n2. second"
	if tg[0] != want {
		t.Fatalf("expected sequential numbering, got: %q", tg[0])
	}
}

func TestNestedListDoesNotConcatenate(t *testing.T) {
	r := New()
	md := "- a\n  - b\n  - c\n- d\n"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "• a\n  • b\n  • c\n• d"
	if tg[0] != want {
		t.Fatalf("nested list mismatch:\n got:  %q\n want: %q", tg[0], want)
	}

	dc, err := r.Render(md, Discord)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if dc[0] != want {
		t.Fatalf("discord nested list mismatch:\n got:  %q\n want: %q", dc[0], want)
	}
}

func TestOrderedNestedList(t *testing.T) {
	r := New()
	md := "1. first\n2. second\n   1. nested\n"

	tg, err := r.Render(md, Telegram)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "1. first\n2. second\n  1. nested"
	if tg[0] != want {
		t.Fatalf("ordered nested list mismatch:\n got:  %q\n want: %q", tg[0], want)
	}
}

func TestSplitKeepsLinkTagBalanced(t *testing.T) {
	label := strings.Repeat("word ", 2000)
	md := "[" + label + "](https://example.com/very/long/destination)"

	chunks, err := New().RenderWithLimit(md, Telegram, 4096)
	if err != nil {
		t.Fatalf("RenderWithLimit: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected a multi-chunk split, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if !htmlBalanced(chunk) {
			t.Fatalf("chunk %d is not tag-balanced: %q", i, chunk[:min(80, len(chunk))])
		}
		if measure(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds the limit: %d units", i, measure(chunk))
		}
		if !strings.HasPrefix(chunk, `<a href="https://example.com/very/long/destination">`) {
			t.Fatalf("chunk %d does not reopen the full link tag: %q", i, chunk[:min(80, len(chunk))])
		}
		if !strings.HasSuffix(chunk, "</a>") {
			t.Fatalf("chunk %d does not close the link tag: %q", i, chunk[max(0, len(chunk)-20):])
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
