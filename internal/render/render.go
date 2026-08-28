package render

import (
	"bytes"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	east "github.com/yuin/goldmark/extension/ast"

	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
)

type Platform int

const (
	Telegram Platform = iota
	Discord
)

const (
	TelegramLimit = 4096
	DiscordLimit  = 2000
)

type Renderer interface {
	Render(markdown string, p Platform) ([]string, error)
	RenderWithLimit(markdown string, p Platform, limit int) ([]string, error)
}

type GoldmarkRenderer struct {
	md goldmark.Markdown
}

func New() *GoldmarkRenderer {
	return &GoldmarkRenderer{
		md: goldmark.New(
			goldmark.WithExtensions(extension.Table, extension.Strikethrough, extension.TaskList),
		),
	}
}

func (r *GoldmarkRenderer) Render(markdown string, p Platform) ([]string, error) {
	limit := TelegramLimit
	if p == Discord {
		limit = DiscordLimit
	}
	return r.RenderWithLimit(markdown, p, limit)
}

func (r *GoldmarkRenderer) RenderWithLimit(markdown string, p Platform, limit int) ([]string, error) {
	source := []byte(markdown)
	reader := text.NewReader(source)
	doc := r.md.Parser().Parse(reader)

	var output string
	switch p {
	case Discord:
		output = renderDiscord(doc, source)
	default:
		output = renderTelegramHTML(doc, source)
	}

	return Split(output, limit), nil
}

func PlatformFor(name string) Platform {
	if name == "discord" {
		return Discord
	}
	return Telegram
}

// Split cuts s into chunks that each measure at most limit, preferring a
// code-block, paragraph, line, then word boundary. Chunks never split a rune:
// both platforms reject invalid UTF-8, and text without ASCII whitespace
// (CJK, Thai, base64) otherwise gets cut mid-character. Boundaries never
// leave an HTML tag open in a chunk; a hard cut inside an open tag span
// closes the open tags at the cut and reopens them on the next chunk, which
// is the one case where concatenating the chunks differs from the input.
//
// Length is measured in UTF-16 code units, the unit Telegram counts against
// its limit; Discord counts code points, for which this is a safe upper bound.
func Split(s string, limit int) []string {
	if limit <= 0 || measure(s) <= limit {
		return []string{s}
	}

	var chunks []string
	remaining := s
	for len(remaining) > 0 {
		if measure(remaining) <= limit {
			chunks = append(chunks, remaining)
			break
		}

		breakAt, hardCut := findSafeBreak(remaining, limit)
		chunk := remaining[:breakAt]
		if hardCut {
			// The limit fell inside an open tag span with no balanced boundary
			// in range: close the open tags here and reopen them on the next
			// chunk so each chunk stays well-formed for the platform parser.
			if stack := openTagStack(chunk); len(stack) > 0 {
				closes := closeTags(stack)
				// The closing tags consume budget too: shorten the cut so the
				// chunk still measures at most limit.
				if budget := limit - measure(closes); budget > 0 {
					chunk = remaining[:maxPrefix(remaining, budget)]
				}
				chunk += closes
				consumed := len(chunk) - len(closes)
				remaining = openTags(stack) + strings.TrimLeft(remaining[consumed:], "\n")
				chunks = append(chunks, chunk)
				continue
			}
		}
		chunks = append(chunks, chunk)
		remaining = strings.TrimLeft(remaining[breakAt:], "\n")
	}
	return chunks
}

// Clamp returns s unchanged when it measures at most limit; otherwise it
// returns a single-message-safe prefix of s that measures at most limit:
// rune-safe, HTML-tag-balanced (open tags closed at a hard cut), with "…"
// appended so the truncation is visible. It is the button-message
// counterpart to Split: a message carrying buttons cannot be split across
// multiple messages, so its content must instead be clamped to the platform
// limit (Discord 2000, Telegram 4096).
func Clamp(s string, limit int) string {
	if limit <= 0 || measure(s) <= limit {
		return s
	}

	const marker = "…"

	end := maxPrefix(s, limit)
	if idx, ok := danglingTagStart(s[:end]); ok && idx > 0 {
		end = idx
	}

	// Prefer the largest tag-balanced boundary that still leaves room for
	// the marker; otherwise hard-cut and close any tags left open. The hard
	// cut never emits a partial or un-closed opening tag: if an open span
	// cannot be closed (with the marker) within the limit, its content is
	// dropped back toward plain text.
	cut := -1
	for _, idx := range candidateBreaks(s[:end]) {
		if measure(s[:idx])+measure(marker) <= limit && htmlBalanced(s[:idx]) {
			cut = idx
		}
	}
	if cut >= 0 {
		return s[:cut] + marker
	}

	return clampHardCut(s, end, limit, marker)
}

// clampHardCut builds the longest rune-safe result it can from a prefix of s
// (plus closing tags and the … marker) that measures at most limit and is
// tag-balanced. It walks the candidate end point backward, closing any open
// spans when they fit and omitting them when they do not — so a tag too big
// to fit with its close and the marker is dropped rather than left dangling
// (e.g. Clamp("<b>xstring", 2) == "…" instead of "<…"). The marker alone is
// the guaranteed floor, so the result is always balanced and within limit.
func clampHardCut(s string, end, limit int, marker string) string {
	// Closes for any cut are a subset of the tag text in the window, so
	// their units bound every candidate from above; that bound both skips
	// candidates that cannot fit and stops the backward walk once no
	// remaining candidate can measure longer than the best found.
	maxCloseUnits := tagUnits(s[:end])
	best := ""
	bestUnits := 0
	units := measure(s[:end])
	for e := end; ; e = prevRuneBoundary(s, e) {
		if best != "" && units+maxCloseUnits+1 <= bestUnits {
			break
		}
		if units+1 <= limit {
			cand := s[:e] + closeTags(openTagStack(s[:e])) + marker
			if m := measure(cand); m <= limit && htmlBalanced(cand) && m > bestUnits {
				best = cand
				bestUnits = m
			}
		}
		if e == 0 {
			break
		}
		r, _ := utf8.DecodeLastRuneInString(s[:e])
		units -= utf16Len(r)
	}
	if best == "" {
		return marker
	}
	return best
}

func tagUnits(s string) int {
	total := 0
	for i := 0; i < len(s); {
		if _, n, ok := matchOpenTag(s[i:]); ok {
			total += measure(s[i : i+n])
			i += n
			continue
		}
		if n, ok := matchCloseTag(s[i:]); ok {
			total += measure(s[i : i+n])
			i += n
			continue
		}
		i++
	}
	return total
}

// prevRuneBoundary returns the byte index of the rune boundary that precedes
// i. i must itself be a rune boundary (maxPrefix guarantees this); the
// returned index is the start of the previous rune (0 when i is 0).
func prevRuneBoundary(s string, i int) int {
	if i <= 0 {
		return 0
	}
	for i > 0 {
		i--
		if s[i]&0xC0 != 0x80 { // not a UTF-8 continuation byte
			break
		}
	}
	return i
}

func findSafeBreak(s string, limit int) (int, bool) {
	end := maxPrefix(s, limit)
	// A cut can land mid-formation of a tag itself (e.g. after "<a" but
	// before "href=...">", or after "<pr" but before "e>") — distinct from
	// landing inside an open-but-unclosed span, which htmlBalanced already
	// rejects below. Retreat before the dangling tag so no chunk boundary
	// ever splits a tag's own literal bytes.
	if idx, ok := danglingTagStart(s[:end]); ok && idx > 0 {
		end = idx
	}
	window := s[:end]

	// Each chunk is parsed by the platform independently, so a boundary that
	// leaves an HTML tag open in the chunk (e.g. a split inside a multi-line
	// <b> span) produces a message the platform rejects. Only break where the
	// prefix is tag-balanced.
	for _, idx := range candidateBreaks(window) {
		if htmlBalanced(window[:idx]) {
			return idx, false
		}
	}
	return end, true
}

// danglingTagStart reports the byte index where s ends mid-way through
// forming a recognized tag — s ran out before the tag could be confirmed as
// open, closed, or ruled out as a tag entirely.
func danglingTagStart(s string) (int, bool) {
	for i := 0; i < len(s); {
		if _, n, ok := matchOpenTag(s[i:]); ok {
			i += n
			continue
		}
		if n, ok := matchCloseTag(s[i:]); ok {
			i += n
			continue
		}
		if s[i] == '<' && looksLikeTagStart(s[i:]) {
			return i, true
		}
		i++
	}
	return 0, false
}

// looksLikeTagStart reports whether s could still become a longer recognized
// tag if more bytes followed — e.g. "<pr" (a prefix of "<pre>") or
// `<a href="https` (aOpenPrefix matched but no closing quote yet).
func looksLikeTagStart(s string) bool {
	for _, t := range openTagTokens {
		if len(s) < len(t) && strings.HasPrefix(t, s) {
			return true
		}
	}
	for _, t := range closeTagTokens {
		if len(s) < len(t) && strings.HasPrefix(t, s) {
			return true
		}
	}
	if len(s) < len("</a>") && strings.HasPrefix("</a>", s) {
		return true
	}
	if strings.HasPrefix(s, aOpenPrefix) {
		rest := s[len(aOpenPrefix):]
		idx := strings.IndexByte(rest, '"')
		return idx < 0 || idx+1 >= len(rest)
	}
	return len(s) < len(aOpenPrefix) && strings.HasPrefix(aOpenPrefix, s)
}

type openTag struct {
	name string
	text string
}

func openTagStack(s string) []openTag {
	var stack []openTag
	for i := 0; i < len(s); {
		if tag, n, ok := matchOpenTag(s[i:]); ok {
			stack = append(stack, tag)
			i += n
			continue
		}
		if n, ok := matchCloseTag(s[i:]); ok {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			i += n
			continue
		}
		i++
	}
	return stack
}

func closeTags(stack []openTag) string {
	var out string
	for i := len(stack) - 1; i >= 0; i-- {
		out += "</" + stack[i].name + ">"
	}
	return out
}

func openTags(stack []openTag) string {
	var out string
	for _, t := range stack {
		out += t.text
	}
	return out
}

func tagName(t string) string { return t[1 : len(t)-1] }

func candidateBreaks(window string) []int {
	var out []int
	collect := func(sep string, offset int) {
		for i := len(window) - len(sep); i > 0; i-- {
			if strings.HasPrefix(window[i:], sep) {
				out = append(out, i+offset)
			}
		}
	}
	collect("</pre>", len("</pre>"))
	collect("\n\n", 0)
	collect("\n", 0)
	collect(" ", 0)
	return out
}

func htmlBalanced(s string) bool {
	if _, ok := danglingTagStart(s); ok {
		return false
	}
	opens, closes := 0, 0
	for i := 0; i < len(s); {
		if _, n, ok := matchOpenTag(s[i:]); ok {
			opens++
			i += n
			continue
		}
		if n, ok := matchCloseTag(s[i:]); ok {
			closes++
			i += n
			continue
		}
		i++
	}
	return opens == closes
}

var (
	openTagTokens  = []string{"<b>", "<i>", "<code>", "<pre>", "<blockquote>", "<s>"}
	closeTagTokens = []string{"</b>", "</i>", "</code>", "</pre>", "</blockquote>", "</s>"}
)

const aOpenPrefix = `<a href="`

func matchOpenTag(s string) (tag openTag, length int, ok bool) {
	for _, t := range openTagTokens {
		if strings.HasPrefix(s, t) {
			return openTag{name: tagName(t), text: t}, len(t), true
		}
	}
	if strings.HasPrefix(s, aOpenPrefix) {
		if end := strings.IndexByte(s[len(aOpenPrefix):], '"'); end >= 0 {
			tagEnd := len(aOpenPrefix) + end + 1
			if tagEnd < len(s) && s[tagEnd] == '>' {
				return openTag{name: "a", text: s[:tagEnd+1]}, tagEnd + 1, true
			}
		}
	}
	return openTag{}, 0, false
}

func matchCloseTag(s string) (length int, ok bool) {
	for _, t := range closeTagTokens {
		if strings.HasPrefix(s, t) {
			return len(t), true
		}
	}
	if strings.HasPrefix(s, "</a>") {
		return len("</a>"), true
	}
	return 0, false
}

func maxPrefix(s string, limit int) int {
	units := 0
	for i, r := range s {
		n := utf16Len(r)
		if units+n > limit {
			if i == 0 {
				return len(string(r))
			}
			return i
		}
		units += n
	}
	return len(s)
}

func measure(s string) int {
	n := 0
	for _, r := range s {
		n += utf16Len(r)
	}
	return n
}

func utf16Len(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}

type listCtx struct {
	ordered bool
	next    int
}

const telegramThematicBreak = "──────────\n\n"
const discordThematicBreak = "---\n\n"

func renderTelegramHTML(doc ast.Node, source []byte) string {
	root := &bytes.Buffer{}
	buf := root
	var bufStack []*bytes.Buffer
	var listStack []*listCtx
	tableFirstCell := false

	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		switch node := n.(type) {
		case *ast.CodeBlock:
			if entering {
				buf.WriteString("<pre><code>")
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(escapeHTML(line.Value(source)))
				}
			} else {
				buf.WriteString("</code></pre>\n")
			}
		case *ast.FencedCodeBlock:
			if entering {
				buf.WriteString("<pre><code>")
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(escapeHTML(line.Value(source)))
				}
			} else {
				buf.WriteString("</code></pre>\n")
			}
		case *ast.CodeSpan:
			if entering {
				buf.WriteString("<code>")
			} else {
				buf.WriteString("</code>")
			}
		case *ast.Emphasis:
			if entering {
				if node.Level == 2 {
					buf.WriteString("<b>")
				} else {
					buf.WriteString("<i>")
				}
			} else {
				if node.Level == 2 {
					buf.WriteString("</b>")
				} else {
					buf.WriteString("</i>")
				}
			}
		case *east.Strikethrough:
			if entering {
				buf.WriteString("<s>")
			} else {
				buf.WriteString("</s>")
			}
		case *ast.Link:
			if entering {
				buf.WriteString(`<a href="`)
				buf.Write(escapeHTMLAttr(node.Destination))
				buf.WriteString(`">`)
			} else {
				buf.WriteString("</a>")
			}
		case *ast.AutoLink:
			if entering {
				url := node.URL(source)
				buf.WriteString(`<a href="`)
				buf.Write(escapeHTMLAttr(url))
				buf.WriteString(`">`)
				buf.Write(escapeHTML(url))
				buf.WriteString("</a>")
			}
		case *ast.Blockquote:
			if entering {
				bufStack = append(bufStack, buf)
				buf = &bytes.Buffer{}
			} else {
				inner := strings.TrimRight(buf.String(), "\n")
				buf = bufStack[len(bufStack)-1]
				bufStack = bufStack[:len(bufStack)-1]
				buf.WriteString("<blockquote>" + inner + "</blockquote>\n\n")
			}
		case *ast.ThematicBreak:
			if entering {
				buf.WriteString(telegramThematicBreak)
			}
		case *ast.Text:
			if entering {
				buf.Write(escapeHTML(node.Value(source)))
				if node.SoftLineBreak() {
					buf.WriteByte('\n')
				}
			}
		case *ast.RawHTML:
			if entering {
				for i := 0; i < node.Segments.Len(); i++ {
					seg := node.Segments.At(i)
					buf.Write(escapeHTML(seg.Value(source)))
				}
			}
		case *ast.HTMLBlock:
			if entering {
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(escapeHTML(line.Value(source)))
				}
				if node.HasClosure() {
					closure := node.ClosureLine
					buf.Write(escapeHTML(closure.Value(source)))
				}
			}
		case *ast.Paragraph:
			if !entering {
				buf.WriteString("\n\n")
			}
		case *ast.TextBlock:
			if !entering {
				buf.WriteByte('\n')
			}
		case *ast.Heading:
			if entering {
				buf.WriteString("<b>")
			} else {
				buf.WriteString("</b>\n\n")
			}
		case *ast.List:
			if entering {
				listStack = append(listStack, &listCtx{ordered: node.IsOrdered(), next: node.Start})
			} else {
				listStack = listStack[:len(listStack)-1]
				if len(listStack) == 0 {
					buf.WriteByte('\n')
				}
			}
		case *ast.ListItem:
			if entering {
				depth := len(listStack)
				ctx := listStack[depth-1]
				buf.WriteString(strings.Repeat("  ", depth-1))
				if ctx.ordered {
					buf.WriteString(strconv.Itoa(ctx.next) + ". ")
					ctx.next++
				} else {
					buf.WriteString("• ")
				}
			}
		case *east.TaskCheckBox:
			if entering {
				if node.IsChecked {
					buf.WriteString("☑ ")
				} else {
					buf.WriteString("☐ ")
				}
			}
		case *east.Table:
			if entering {
				buf.WriteString("<pre><code>")
			} else {
				buf.WriteString("</code></pre>\n")
			}
		case *east.TableHeader:
			if entering {
				buf.WriteString("| ")
				tableFirstCell = true
			} else {
				buf.WriteString(" |\n")
				buf.WriteString("|" + strings.Repeat(" --- |", node.ChildCount()) + "\n")
			}
		case *east.TableRow:
			if entering {
				buf.WriteString("| ")
				tableFirstCell = true
			} else {
				buf.WriteString(" |\n")
			}
		case *east.TableCell:
			if entering {
				if !tableFirstCell {
					buf.WriteString(" | ")
				}
				tableFirstCell = false
			}
		}
		return ast.WalkContinue, nil
	})
	return strings.TrimRight(root.String(), "\n")
}

func renderDiscord(doc ast.Node, source []byte) string {
	root := &bytes.Buffer{}
	buf := root
	var bufStack []*bytes.Buffer
	var listStack []*listCtx
	tableFirstCell := false

	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		switch node := n.(type) {
		case *ast.FencedCodeBlock:
			if entering {
				lang := string(node.Language(source))
				buf.WriteString("```" + lang + "\n")
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(line.Value(source))
				}
			} else {
				buf.WriteString("```\n")
			}
		case *ast.CodeBlock:
			if entering {
				buf.WriteString("```\n")
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(line.Value(source))
				}
			} else {
				buf.WriteString("```\n")
			}
		case *ast.CodeSpan:
			if entering {
				buf.WriteByte('`')
			} else {
				buf.WriteByte('`')
			}
		case *ast.Emphasis:
			if entering {
				if node.Level == 2 {
					buf.WriteString("**")
				} else {
					buf.WriteByte('*')
				}
			} else {
				if node.Level == 2 {
					buf.WriteString("**")
				} else {
					buf.WriteByte('*')
				}
			}
		case *east.Strikethrough:
			buf.WriteString("~~")
		case *ast.Link:
			if entering {
				buf.WriteByte('[')
			} else {
				buf.WriteString("](")
				buf.Write(node.Destination)
				buf.WriteByte(')')
			}
		case *ast.AutoLink:
			if entering {
				buf.Write(node.URL(source))
			}
		case *ast.Blockquote:
			if entering {
				bufStack = append(bufStack, buf)
				buf = &bytes.Buffer{}
			} else {
				inner := strings.TrimRight(buf.String(), "\n")
				buf = bufStack[len(bufStack)-1]
				bufStack = bufStack[:len(bufStack)-1]
				buf.WriteString(quoteLines(inner) + "\n\n")
			}
		case *ast.ThematicBreak:
			if entering {
				buf.WriteString(discordThematicBreak)
			}
		case *ast.Text:
			if entering {
				buf.Write(node.Value(source))
				if node.SoftLineBreak() {
					buf.WriteByte('\n')
				}
			}
		case *ast.RawHTML:
			if entering {
				for i := 0; i < node.Segments.Len(); i++ {
					seg := node.Segments.At(i)
					buf.Write(seg.Value(source))
				}
			}
		case *ast.HTMLBlock:
			if entering {
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					buf.Write(line.Value(source))
				}
				if node.HasClosure() {
					closure := node.ClosureLine
					buf.Write(closure.Value(source))
				}
			}
		case *ast.Paragraph:
			if !entering {
				buf.WriteString("\n\n")
			}
		case *ast.TextBlock:
			if !entering {
				buf.WriteByte('\n')
			}
		case *ast.Heading:
			if entering {
				buf.WriteString("**")
			} else {
				buf.WriteString("**\n\n")
			}
		case *ast.List:
			if entering {
				listStack = append(listStack, &listCtx{ordered: node.IsOrdered(), next: node.Start})
			} else {
				listStack = listStack[:len(listStack)-1]
				if len(listStack) == 0 {
					buf.WriteByte('\n')
				}
			}
		case *ast.ListItem:
			if entering {
				depth := len(listStack)
				ctx := listStack[depth-1]
				buf.WriteString(strings.Repeat("  ", depth-1))
				if ctx.ordered {
					buf.WriteString(strconv.Itoa(ctx.next) + ". ")
					ctx.next++
				} else {
					buf.WriteString("• ")
				}
			}
		case *east.TaskCheckBox:
			if entering {
				if node.IsChecked {
					buf.WriteString("☑ ")
				} else {
					buf.WriteString("☐ ")
				}
			}
		case *east.Table:
			buf.WriteString("```\n")
		case *east.TableHeader:
			if entering {
				buf.WriteString("| ")
				tableFirstCell = true
			} else {
				buf.WriteString(" |\n")
				buf.WriteString("|" + strings.Repeat(" --- |", node.ChildCount()) + "\n")
			}
		case *east.TableRow:
			if entering {
				buf.WriteString("| ")
				tableFirstCell = true
			} else {
				buf.WriteString(" |\n")
			}
		case *east.TableCell:
			if entering {
				if !tableFirstCell {
					buf.WriteString(" | ")
				}
				tableFirstCell = false
			}
		}
		return ast.WalkContinue, nil
	})
	return strings.TrimRight(root.String(), "\n")
}

func quoteLines(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = "> " + line
	}
	return strings.Join(lines, "\n")
}

func escapeHTML(b []byte) []byte {
	s := string(b)
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return []byte(s)
}

func escapeHTMLAttr(b []byte) []byte {
	return []byte(strings.ReplaceAll(string(escapeHTML(b)), `"`, "&quot;"))
}

var _ Renderer = (*GoldmarkRenderer)(nil)
