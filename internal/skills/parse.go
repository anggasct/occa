package skills

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

var (
	namePattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,63}$`)
	yamlLinePattern    = regexp.MustCompile(`line (\d+)`)
	yamlUnknownPattern = regexp.MustCompile(`field (\S+) not found in type`)
)

const (
	maxDescriptionRunes = 200
	maxListEntries      = 16
	maxKeywordRunes     = 64
	maxPriority         = 1000
)

type frontmatter struct {
	Name        *string  `yaml:"name"`
	Description *string  `yaml:"description"`
	Trigger     *string  `yaml:"trigger"`
	Keywords    []string `yaml:"keywords"`
	Paths       []string `yaml:"paths"`
	Priority    *int     `yaml:"priority"`
}

type parseError struct {
	line   int
	reason string
}

func (e *parseError) Error() string { return e.reason }

func failAt(line int, format string, args ...any) error {
	return &parseError{line: line, reason: fmt.Sprintf(format, args...)}
}

// Load reads both scopes and applies repo-over-global precedence. It never
// fails as a whole: a bad file, or a repo skills directory that exists but
// cannot be read, becomes a LoadError and is left out.
func Load(opts Options) Set {
	opts = opts.withDefaults()
	var set Set
	repo, repoErrs, rErr := loadScope(opts.repoPath(), ScopeRepo, opts.SkillMaxRunes)
	if rErr != nil {
		repoErrs = append(repoErrs, unreadableScope(opts.repoPath(), rErr))
	}
	set.Errors = append(set.Errors, repoErrs...)
	global, globalErrs, gErr := loadScope(opts.globalPath(), ScopeGlobal, opts.SkillMaxRunes)
	set.Errors = append(set.Errors, globalErrs...)
	set.GlobalUnreadable = gErr

	byName := make(map[string]bool, len(repo))
	for _, sk := range repo {
		byName[sk.Name] = true
	}
	set.Skills = append(set.Skills, repo...)
	for _, sk := range global {
		if byName[sk.Name] {
			set.Shadowed = append(set.Shadowed, sk)
			continue
		}
		set.Skills = append(set.Skills, sk)
	}
	return set
}

// LoadRepo reads only the repo scope, for lint. A repo skills directory
// that exists but cannot be read is a LoadError: lint is the enforcement
// point and must not pass a tree it could not read.
func LoadRepo(opts Options) ([]Skill, []LoadError) {
	opts = opts.withDefaults()
	skills, errs, err := loadScope(opts.repoPath(), ScopeRepo, opts.SkillMaxRunes)
	if err != nil {
		errs = append(errs, unreadableScope(opts.repoPath(), err))
	}
	return skills, errs
}

// unreadableScope reports a scope directory that exists but could not be
// listed, at line 1 of the directory path.
func unreadableScope(dir string, err error) LoadError {
	abs, _ := filepath.Abs(dir)
	return LoadError{Path: abs, Line: 1, Reason: "unreadable: " + err.Error()}
}

// loadScope reads <dir>/<name>/SKILL.md one level deep, sorted by directory
// name. A missing directory is an empty scope, not an error.
func loadScope(dir, scope string, maxRunes int) ([]Skill, []LoadError, error) {
	if dir == "" {
		return nil, nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var skills []Skill
	var errs []LoadError
	seen := map[string]string{}
	for _, entry := range entries {
		sub := filepath.Join(dir, entry.Name())
		info, err := os.Stat(sub) // follows a symlinked skill directory once
		if err != nil || !info.IsDir() {
			continue
		}
		path := filepath.Join(sub, skillFile)
		abs, _ := filepath.Abs(path)
		raw, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			errs = append(errs, LoadError{Path: abs, Line: 1, Reason: "unreadable: " + err.Error()})
			continue
		}
		sk, err := parseSkill(raw, entry.Name(), maxRunes)
		if err != nil {
			line := 1
			var pe *parseError
			if errors.As(err, &pe) && pe.line > 0 {
				line = pe.line
			}
			errs = append(errs, LoadError{Path: abs, Line: line, Reason: err.Error()})
			continue
		}
		if first, dup := seen[sk.Name]; dup {
			errs = append(errs, LoadError{Path: abs, Line: 1, Reason: "duplicate skill name " + strconv.Quote(sk.Name) + " (first: " + first + ")"})
			continue
		}
		seen[sk.Name] = abs
		sk.Scope = scope
		sk.Path = abs
		skills = append(skills, sk)
	}
	return skills, errs, nil
}

// parseSkill validates one SKILL.md against the frontmatter contract.
func parseSkill(raw []byte, dirName string, maxRunes int) (Skill, error) {
	if !utf8.Valid(raw) {
		return Skill{}, failAt(1, "invalid utf-8")
	}
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	if !strings.HasPrefix(text, "---\n") {
		return Skill{}, failAt(1, "missing frontmatter: file must start with ---")
	}
	rest := text[len("---\n"):]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return Skill{}, failAt(1, "unterminated frontmatter: no closing ---")
	}
	header := rest[:end+1]
	after := rest[end+len("\n---"):]
	if nl := strings.IndexByte(after, '\n'); nl >= 0 {
		if strings.TrimSpace(after[:nl]) != "" {
			return Skill{}, failAt(strings.Count(header, "\n")+2, "closing --- must be on its own line")
		}
		after = after[nl+1:]
	} else if strings.TrimSpace(after) != "" {
		return Skill{}, failAt(strings.Count(header, "\n")+2, "closing --- must be on its own line")
	} else {
		after = ""
	}

	var fm frontmatter
	dec := yaml.NewDecoder(bytes.NewReader([]byte(header)))
	dec.KnownFields(true)
	if err := dec.Decode(&fm); err != nil && !errors.Is(err, io.EOF) {
		return Skill{}, frontmatterError(err)
	}

	sk := Skill{Priority: DefaultPriority}
	if fm.Name == nil || strings.TrimSpace(*fm.Name) == "" {
		return Skill{}, failAt(1, "missing required key name")
	}
	sk.Name = strings.TrimSpace(*fm.Name)
	if !namePattern.MatchString(sk.Name) {
		return Skill{}, failAt(1, "name %q must match %s", sk.Name, namePattern.String())
	}
	if sk.Name != dirName {
		return Skill{}, failAt(1, "name %q must equal its directory %q", sk.Name, dirName)
	}
	if fm.Description == nil || strings.TrimSpace(*fm.Description) == "" {
		return Skill{}, failAt(1, "missing required key description")
	}
	sk.Description = strings.TrimSpace(*fm.Description)
	if strings.ContainsAny(sk.Description, "\n\r") {
		return Skill{}, failAt(1, "description must be a single line")
	}
	if utf8.RuneCountInString(sk.Description) > maxDescriptionRunes {
		return Skill{}, failAt(1, "description over %d runes", maxDescriptionRunes)
	}
	if fm.Trigger == nil || strings.TrimSpace(*fm.Trigger) == "" {
		return Skill{}, failAt(1, "missing required key trigger")
	}
	sk.Trigger = strings.TrimSpace(*fm.Trigger)
	switch sk.Trigger {
	case TriggerAlways, TriggerKeyword, TriggerPaths:
	default:
		return Skill{}, failAt(1, "trigger %q must be always, keyword or paths", sk.Trigger)
	}
	if sk.Trigger == TriggerKeyword {
		if len(fm.Keywords) == 0 {
			return Skill{}, failAt(1, "trigger keyword requires keywords")
		}
		if len(fm.Keywords) > maxListEntries {
			return Skill{}, failAt(1, "keywords: at most %d entries", maxListEntries)
		}
		for _, kw := range fm.Keywords {
			kw = strings.TrimSpace(kw)
			if kw == "" {
				return Skill{}, failAt(1, "keywords must not contain empty entries")
			}
			if utf8.RuneCountInString(kw) > maxKeywordRunes {
				return Skill{}, failAt(1, "keyword %q over %d runes", kw, maxKeywordRunes)
			}
			sk.Keywords = append(sk.Keywords, kw)
		}
	} else if len(fm.Keywords) > 0 {
		return Skill{}, failAt(1, "keywords is only allowed with trigger keyword")
	}
	if sk.Trigger == TriggerPaths {
		if len(fm.Paths) == 0 {
			return Skill{}, failAt(1, "trigger paths requires paths")
		}
		if len(fm.Paths) > maxListEntries {
			return Skill{}, failAt(1, "paths: at most %d entries", maxListEntries)
		}
		for _, p := range fm.Paths {
			p = normalizePattern(p)
			if p == "" {
				return Skill{}, failAt(1, "paths must not contain empty entries")
			}
			if err := validatePattern(p); err != nil {
				return Skill{}, failAt(1, "paths %q: %v", p, err)
			}
			sk.Paths = append(sk.Paths, p)
		}
	} else if len(fm.Paths) > 0 {
		return Skill{}, failAt(1, "paths is only allowed with trigger paths")
	}
	if fm.Priority != nil {
		if *fm.Priority < 0 || *fm.Priority > maxPriority {
			return Skill{}, failAt(1, "priority %d must be 0-%d", *fm.Priority, maxPriority)
		}
		sk.Priority = *fm.Priority
	}

	sk.Body = strings.TrimSpace(after)
	sk.Runes = utf8.RuneCountInString(sk.Body)
	if sk.Runes > maxRunes {
		return Skill{}, failAt(1, "body is %d runes, over %d", sk.Runes, maxRunes)
	}
	sum := sha256.Sum256(raw)
	sk.Digest = hex.EncodeToString(sum[:])
	return sk, nil
}

// frontmatterError turns a yaml decode error into one line with the file
// line it points at. The frontmatter starts on the file's second line.
func frontmatterError(err error) error {
	msg := err.Error()
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) && len(typeErr.Errors) > 0 {
		msg = typeErr.Errors[0]
	}
	line := 1
	if m := yamlLinePattern.FindStringSubmatch(msg); m != nil {
		n, _ := strconv.Atoi(m[1])
		line = n + 1
	}
	if m := yamlUnknownPattern.FindStringSubmatch(msg); m != nil {
		return failAt(line, "unknown key %s", m[1])
	}
	msg = strings.TrimPrefix(msg, "yaml: ")
	msg = yamlLinePattern.ReplaceAllString(msg, "")
	msg = strings.Trim(strings.Join(strings.Fields(msg), " "), ": ")
	return failAt(line, "frontmatter: %s", msg)
}
