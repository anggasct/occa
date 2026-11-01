package skills

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Input is what a run declares: its task text and its file scope.
type Input struct {
	Text  string
	Scope []string
}

// Applied is a selected skill and the trigger that fired it.
type Applied struct {
	Skill  Skill
	Reason string
}

// Selection is the outcome for one run.
type Selection struct {
	// Applied is in render order: priority descending, then name ascending.
	Applied []Applied
	// Dropped were triggered but cut by the context budget.
	Dropped []Applied
	// Runes counts the rendered block.
	Runes int
}

// Select picks the skills the input triggers and fits them to maxRunes.
// Over budget, the lowest priority is dropped first; within one priority
// the name that sorts first goes first.
func Select(set Set, in Input, maxRunes int) Selection {
	if maxRunes <= 0 {
		maxRunes = DefaultContextMaxRunes
	}
	scope := make([]string, 0, len(in.Scope))
	for _, p := range in.Scope {
		if n := normalizePath(p); n != "" {
			scope = append(scope, n)
		}
	}
	var picked []Applied
	for _, sk := range set.Skills {
		if reason, ok := triggered(sk, in.Text, scope); ok {
			picked = append(picked, Applied{Skill: sk, Reason: reason})
		}
	}
	sort.SliceStable(picked, func(i, j int) bool {
		if picked[i].Skill.Priority != picked[j].Skill.Priority {
			return picked[i].Skill.Priority > picked[j].Skill.Priority
		}
		return picked[i].Skill.Name < picked[j].Skill.Name
	})
	var dropped []Applied
	for len(picked) > 0 && utf8.RuneCountInString(Render(picked)) > maxRunes {
		victim := 0
		for i, a := range picked {
			v := picked[victim].Skill
			if a.Skill.Priority < v.Priority || (a.Skill.Priority == v.Priority && a.Skill.Name < v.Name) {
				victim = i
			}
		}
		dropped = append(dropped, picked[victim])
		picked = append(picked[:victim:victim], picked[victim+1:]...)
	}
	return Selection{Applied: picked, Dropped: dropped, Runes: utf8.RuneCountInString(Render(picked))}
}

func triggered(sk Skill, text string, scope []string) (string, bool) {
	switch sk.Trigger {
	case TriggerAlways:
		return "always", true
	case TriggerKeyword:
		for _, kw := range sk.Keywords {
			if containsToken(text, kw) {
				return "keyword: " + kw, true
			}
		}
	case TriggerPaths:
		for _, pattern := range sk.Paths {
			for _, file := range scope {
				if matchGlob(pattern, file) {
					return "paths: " + pattern, true
				}
			}
		}
	}
	return "", false
}

// containsToken reports whether keyword occurs in text, case-insensitively,
// with no letter or digit directly before or after it: "migrate" matches
// "please migrate the store" but not "migration" or "migrated".
func containsToken(text, keyword string) bool {
	hay := []rune(strings.ToLower(text))
	needle := []rune(strings.ToLower(strings.TrimSpace(keyword)))
	if len(needle) == 0 || len(needle) > len(hay) {
		return false
	}
	for i := 0; i+len(needle) <= len(hay); i++ {
		if !runesEqual(hay[i:i+len(needle)], needle) {
			continue
		}
		if i > 0 && isWordRune(hay[i-1]) {
			continue
		}
		if end := i + len(needle); end < len(hay) && isWordRune(hay[end]) {
			continue
		}
		return true
	}
	return false
}

func runesEqual(a, b []rune) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }
