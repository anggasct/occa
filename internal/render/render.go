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

// PlatformFor maps a platform name to its renderer.
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

		breakAt := findSafeBreak(remaining, limit)
		chunks = append(chunks, remaining[:breakAt])
		remaining = strings.TrimLeft(remaining[breakAt:], "\n")
	}
	return chunks
}

func findSafeBreak(s string, limit int) int {
	end := maxPrefix(s, limit)
	window := s[:end]

	if idx := strings.LastIndex(window, "</pre>"); idx > 0 {
		return idx + len("</pre>")
	}
	if idx := strings.LastIndex(window, "\n\n"); idx > 0 {
		return idx
	}
	if idx := strings.LastIndex(window, "\n"); idx > 0 {
		return idx
	}
	if idx := strings.LastIndex(window, " "); idx > 0 {
		return idx
	}
	return end
}

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
