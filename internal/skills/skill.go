// Package skills loads per-repo and global skill packs (SKILL.md files),
// selects the ones a run's task text and declared file scope trigger, and
// renders them as one delimited context block. Resolution is a pure function
// of the directory contents and the run's inputs: no model call, no
// randomness, no ordering by filesystem walk.
package skills

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	ScopeRepo   = "repo"
	ScopeGlobal = "global"

	TriggerAlways  = "always"
	TriggerKeyword = "keyword"
	TriggerPaths   = "paths"

	DefaultRepoDir         = ".occa/skills"
	DefaultGlobalDir       = "~/.occa/skills"
	DefaultContextMaxRunes = 8000
	DefaultSkillMaxRunes   = 4000
	DefaultPriority        = 100

	skillFile = "SKILL.md"
)

// Options locates the skill directories and bounds the injected text.
type Options struct {
	// Workdir is the run's repository checkout; RepoDir is relative to it.
	Workdir string
	RepoDir string
	// GlobalDir holds cross-repo skills; a leading "~/" is the home dir.
	// Empty disables the global scope.
	GlobalDir       string
	ContextMaxRunes int
	SkillMaxRunes   int
}

// DefaultOptions returns the documented defaults for a workdir.
func DefaultOptions(workdir string) Options {
	return Options{
		Workdir:         workdir,
		RepoDir:         DefaultRepoDir,
		GlobalDir:       DefaultGlobalDir,
		ContextMaxRunes: DefaultContextMaxRunes,
		SkillMaxRunes:   DefaultSkillMaxRunes,
	}
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.RepoDir) == "" {
		o.RepoDir = DefaultRepoDir
	}
	if o.ContextMaxRunes <= 0 {
		o.ContextMaxRunes = DefaultContextMaxRunes
	}
	if o.SkillMaxRunes <= 0 {
		o.SkillMaxRunes = DefaultSkillMaxRunes
	}
	return o
}

// repoPath is the absolute repo-scope directory, or "" without a workdir.
func (o Options) repoPath() string {
	if strings.TrimSpace(o.Workdir) == "" {
		return ""
	}
	return filepath.Join(o.Workdir, filepath.FromSlash(o.RepoDir))
}

// globalPath expands a leading "~/" against the home directory.
func (o Options) globalPath() string {
	dir := strings.TrimSpace(o.GlobalDir)
	if dir == "" {
		return ""
	}
	if dir == "~" || strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		return filepath.Join(home, strings.TrimPrefix(strings.TrimPrefix(dir, "~"), "/"))
	}
	return dir
}

// Skill is one parsed SKILL.md.
type Skill struct {
	Name        string
	Description string
	Trigger     string
	Keywords    []string
	Paths       []string
	Priority    int
	Body        string
	// Runes counts the trimmed body, frontmatter excluded.
	Runes  int
	Scope  string
	Path   string
	Digest string
}

// LoadError is a skill file that was skipped, with the line the reason
// refers to (1 when the reason is not tied to a line).
type LoadError struct {
	Path   string
	Line   int
	Reason string
}

// Set is every skill found for a workdir, after repo-over-global precedence.
type Set struct {
	// Skills are the candidates, repo first, each scope sorted by name.
	Skills []Skill
	// Shadowed are global skills hidden by a repo skill of the same name.
	Shadowed []Skill
	Errors   []LoadError
	// GlobalUnreadable is set when the global directory exists but could not
	// be listed; the global scope then contributes nothing.
	GlobalUnreadable error
}
