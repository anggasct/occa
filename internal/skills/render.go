package skills

import (
	"strconv"
	"strings"
)

// Render produces the context block injected at the top of a session's first
// prompt. No applied skills renders "", so a run without skills sends exactly
// the prompt it would have sent before this package existed.
//
//	<occa-skills count="2">
//	<skill name="store-contract" scope="repo" trigger="paths: internal/store/**">
//	…body…
//	</skill>
//	</occa-skills>
func Render(applied []Applied) string {
	if len(applied) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(`<occa-skills count="`)
	b.WriteString(strconv.Itoa(len(applied)))
	b.WriteString("\">\n")
	for _, a := range applied {
		b.WriteString(`<skill name="`)
		b.WriteString(attr(a.Skill.Name))
		b.WriteString(`" scope="`)
		b.WriteString(attr(a.Skill.Scope))
		b.WriteString(`" trigger="`)
		b.WriteString(attr(a.Reason))
		b.WriteString("\">\n")
		b.WriteString(a.Skill.Body)
		b.WriteString("\n</skill>\n")
	}
	b.WriteString("</occa-skills>")
	return b.String()
}

// attr keeps an attribute value inside its quotes.
func attr(s string) string {
	return strings.NewReplacer(`"`, "'", "\n", " ", "\r", " ").Replace(s)
}
