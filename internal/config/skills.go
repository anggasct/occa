package config

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/anggasct/occa/internal/skills"
)

// SkillsConfig is the optional top-level skills: block. An absent block is
// the documented defaults; a present block is validated strictly.
type SkillsConfig struct {
	Enabled         bool
	RepoDir         string
	GlobalDir       string
	ContextMaxRunes int
	SkillMaxRunes   int
}

// Options returns the resolver options for one run's workdir.
func (c SkillsConfig) Options(workdir string) skills.Options {
	return skills.Options{
		Workdir:         workdir,
		RepoDir:         c.RepoDir,
		GlobalDir:       c.GlobalDir,
		ContextMaxRunes: c.ContextMaxRunes,
		SkillMaxRunes:   c.SkillMaxRunes,
	}
}

// DefaultSkillsConfig is what an absent skills: block means.
func DefaultSkillsConfig() SkillsConfig {
	return SkillsConfig{
		Enabled:         true,
		RepoDir:         skills.DefaultRepoDir,
		GlobalDir:       skills.DefaultGlobalDir,
		ContextMaxRunes: skills.DefaultContextMaxRunes,
		SkillMaxRunes:   skills.DefaultSkillMaxRunes,
	}
}

type fileSkills struct {
	Enabled         *bool   `yaml:"enabled"`
	RepoDir         *string `yaml:"repo_dir"`
	GlobalDir       *string `yaml:"global_dir"`
	ContextMaxRunes *int    `yaml:"context_max_runes"`
	SkillMaxRunes   *int    `yaml:"skill_max_runes"`
	set             bool
}

var knownSkillsKeys = map[string]bool{
	"enabled": true, "repo_dir": true, "global_dir": true,
	"context_max_runes": true, "skill_max_runes": true,
}

// UnmarshalYAML names an unknown key in the error, so startup refuses with
// "skills.<key>" rather than a decoder type name.
func (s *fileSkills) UnmarshalYAML(node *yaml.Node) error {
	s.set = true
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("config: skills must be a mapping")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if key := node.Content[i].Value; !knownSkillsKeys[key] {
			return fmt.Errorf("config: skills.%s is not a known key (known: enabled, repo_dir, global_dir, context_max_runes, skill_max_runes)", key)
		}
	}
	type raw fileSkills
	var r raw
	if err := node.Decode(&r); err != nil {
		return fmt.Errorf("config: skills: %w", err)
	}
	*s = fileSkills(r)
	s.set = true
	return nil
}

func buildSkillsConfig(fs fileSkills) (SkillsConfig, error) {
	cfg := DefaultSkillsConfig()
	if !fs.set {
		return cfg, nil
	}
	if fs.Enabled != nil {
		cfg.Enabled = *fs.Enabled
	}
	if fs.RepoDir != nil {
		dir := strings.TrimSpace(*fs.RepoDir)
		if dir == "" {
			return SkillsConfig{}, fmt.Errorf("config: skills.repo_dir must not be empty")
		}
		if filepath.IsAbs(dir) || path.IsAbs(filepath.ToSlash(dir)) || strings.HasPrefix(dir, "~") {
			return SkillsConfig{}, fmt.Errorf("config: skills.repo_dir must be relative to the run's workdir, got %q", dir)
		}
		for _, seg := range strings.Split(filepath.ToSlash(dir), "/") {
			if seg == ".." {
				return SkillsConfig{}, fmt.Errorf("config: skills.repo_dir must not contain \"..\", got %q", dir)
			}
		}
		cfg.RepoDir = path.Clean(filepath.ToSlash(dir))
	}
	if fs.GlobalDir != nil {
		cfg.GlobalDir = strings.TrimSpace(*fs.GlobalDir)
	}
	if fs.ContextMaxRunes != nil {
		if *fs.ContextMaxRunes <= 0 {
			return SkillsConfig{}, fmt.Errorf("config: skills.context_max_runes must be > 0")
		}
		cfg.ContextMaxRunes = *fs.ContextMaxRunes
	}
	if fs.SkillMaxRunes != nil {
		if *fs.SkillMaxRunes <= 0 {
			return SkillsConfig{}, fmt.Errorf("config: skills.skill_max_runes must be > 0")
		}
		cfg.SkillMaxRunes = *fs.SkillMaxRunes
	}
	if cfg.ContextMaxRunes < cfg.SkillMaxRunes {
		return SkillsConfig{}, fmt.Errorf("config: skills.context_max_runes (%d) must be >= skills.skill_max_runes (%d)", cfg.ContextMaxRunes, cfg.SkillMaxRunes)
	}
	return cfg, nil
}
