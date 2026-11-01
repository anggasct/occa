package skills

import (
	"errors"
	"path"
	"strings"
)

// normalizePattern slash-normalizes a pattern and strips a leading "./".
func normalizePattern(p string) string {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	for strings.HasPrefix(p, "./") {
		p = p[2:]
	}
	return p
}

// normalizePath turns a declared scope entry into a workdir-relative,
// slash-separated path. It reports ok=false for absolute or ..-escaping
// entries: the declared scope is workdir-relative by contract, and a
// stripped or cleaned absolute path would match the wrong files or none,
// with no diagnostic.
func normalizePath(p string) (string, bool) {
	p = normalizePattern(p)
	if p == "" {
		return "", false
	}
	if strings.HasPrefix(p, "/") {
		return "", false
	}
	clean := path.Clean(p)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", false
	}
	return clean, true
}

// validatePattern rejects patterns path.Match would fail on, segment by
// segment ("**" is ours, not path.Match's).
func validatePattern(p string) error {
	for _, seg := range strings.Split(p, "/") {
		if seg == "**" {
			continue
		}
		if strings.Contains(seg, "**") {
			return errors.New(`"**" must be a whole path segment`)
		}
		if _, err := path.Match(seg, ""); err != nil {
			return err
		}
	}
	return nil
}

// matchGlob matches a workdir-relative path: "*" within one segment, "**"
// across any number of segments (including zero), "?" one non-slash
// character, "[a-z]" classes.
func matchGlob(pattern, name string) bool {
	return matchSegments(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

func matchSegments(pat, name []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			rest := pat[1:]
			if len(rest) == 0 {
				return true
			}
			for i := 0; i <= len(name); i++ {
				if matchSegments(rest, name[i:]) {
					return true
				}
			}
			return false
		}
		if len(name) == 0 {
			return false
		}
		ok, err := path.Match(pat[0], name[0])
		if err != nil || !ok {
			return false
		}
		pat, name = pat[1:], name[1:]
	}
	return len(name) == 0
}
