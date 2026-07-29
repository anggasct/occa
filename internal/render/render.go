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

type Renderer interface {
	Render(markdown string, p Platform) ([]string, error)
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
	source := []byte(markdown)
	reader := text.NewReader(source)
	doc := r.md.Parser().Parse(reader)

	var output string
	switch p {
	case Telegram:
		output = renderTelegramHTML(doc, source)
	case Discord:
		output = renderDiscord(doc, source)
	}

	limit := 4096
	if p == Discord {
		limit = 2000
	}

	return splitRendered(output, limit), nil
}

func splitRendered(text string, maxLen int) []string {
	if len(text) <= maxLen {
		return []string{text}
	}

	var chunks []string
	remaining := text
	for len(remaining) > 0 {
		if len(remaining) <= maxLen {
			chunks = append(chunks, remaining)
			break
		}

		breakAt := findSafeBreak(remaining, maxLen)
		chunks = append(chunks, remaining[:breakAt])
		remaining = strings.TrimLeft(remaining[breakAt:], "\n")
	}
	return chunks
}

func findSafeBreak(s string, maxLen int) int {
	if idx := strings.LastIndex(s[:maxLen], "</pre>"); idx > 0 {
		return idx + len("</pre>")
	}
	if idx := strings.LastIndex(s[:maxLen], "\n\n"); idx > 0 {
		return idx
	}
	if idx := strings.LastIndex(s[:maxLen], "\n"); idx > 0 {
		return idx
	}
	return maxLen
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
