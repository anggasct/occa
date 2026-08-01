package render

import (
	"bytes"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
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
		md: goldmark.New(),
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
// (CJK, Thai, base64) otherwise gets cut mid-character.
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
				chunk += closeTags(stack)
				remaining = openTags(stack) + strings.TrimLeft(remaining[breakAt:], "\n")
				chunks = append(chunks, chunk)
				continue
			}
		}
		chunks = append(chunks, chunk)
		remaining = strings.TrimLeft(remaining[breakAt:], "\n")
	}
	return chunks
}

func findSafeBreak(s string, limit int) (int, bool) {
	end := maxPrefix(s, limit)
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

// openTagStack returns the tags currently open in s, innermost last.
func openTagStack(s string) []string {
	var stack []string
	for i := 0; i < len(s); {
		matched := false
		for _, t := range openTagTokens {
			if strings.HasPrefix(s[i:], t) {
				stack = append(stack, tagName(t))
				i += len(t)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		for _, t := range closeTagTokens {
			if strings.HasPrefix(s[i:], t) {
				stack = stack[:len(stack)-1]
				i += len(t)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		i++
	}
	return stack
}

func closeTags(stack []string) string {
	var out string
	for i := len(stack) - 1; i >= 0; i-- {
		out += "</" + stack[i] + ">"
	}
	return out
}

func openTags(stack []string) string {
	var out string
	for _, name := range stack {
		out += "<" + name + ">"
	}
	return out
}

func tagName(t string) string { return t[1:] }

// candidateBreaks lists boundary indices in preference order — after a closed
// code block, then paragraph, line, and word boundaries, each from the last
// occurrence backwards.
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

// htmlBalanced reports whether every tag opened in s is closed and no closing
// tag appears without its opener. Plain text with no tags is trivially
// balanced.
func htmlBalanced(s string) bool {
	opens, closes := 0, 0
	for i := 0; i < len(s); {
		matched := false
		for _, t := range openTagTokens {
			if strings.HasPrefix(s[i:], t) {
				opens++
				i += len(t)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		for _, t := range closeTagTokens {
			if strings.HasPrefix(s[i:], t) {
				closes++
				i += len(t)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		i++
	}
	return opens == closes
}

var (
	openTagTokens  = []string{"<b>", "<code>", "<pre>"}
	closeTagTokens = []string{"</b>", "</code>", "</pre>"}
)

// maxPrefix returns the largest rune-aligned byte index whose prefix measures
// at most limit. It always advances by at least one rune so Split terminates
// even when a single rune exceeds the limit.
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

func renderTelegramHTML(doc ast.Node, source []byte) string {
	var buf bytes.Buffer
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
		case *ast.Text:
			if entering {
				buf.Write(escapeHTML(node.Value(source)))
				if node.SoftLineBreak() {
					buf.WriteByte('\n')
				}
			}
		// Markup the author wrote literally is escaped, not passed through:
		// the platform accepts only a small tag subset, and dropping these
		// nodes would silently delete the user's text.
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
		case *ast.Heading:
			if entering {
				buf.WriteString("<b>")
			} else {
				buf.WriteString("</b>\n\n")
			}
		case *ast.List:
			if !entering {
				buf.WriteByte('\n')
			}
		case *ast.ListItem:
			if entering {
				buf.WriteString("• ")
			} else {
				buf.WriteByte('\n')
			}
		}
		return ast.WalkContinue, nil
	})
	return strings.TrimRight(buf.String(), "\n")
}

func renderDiscord(doc ast.Node, source []byte) string {
	var buf bytes.Buffer
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
		case *ast.Heading:
			if entering {
				buf.WriteString("**")
			} else {
				buf.WriteString("**\n\n")
			}
		case *ast.List:
			if !entering {
				buf.WriteByte('\n')
			}
		case *ast.ListItem:
			if entering {
				buf.WriteString("• ")
			} else {
				buf.WriteByte('\n')
			}
		}
		return ast.WalkContinue, nil
	})
	return strings.TrimRight(buf.String(), "\n")
}

func escapeHTML(b []byte) []byte {
	s := string(b)
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return []byte(s)
}

var _ Renderer = (*GoldmarkRenderer)(nil)
