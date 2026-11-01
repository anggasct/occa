package config

import (
	"strings"
	"testing"
)

// AC-14: an absent skills: block starts with the documented defaults.
func TestSkillsBlockAbsentUsesDefaults(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cfg, err := Load(writeConfig(t, t.TempDir(), ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Skills != DefaultSkillsConfig() {
		t.Fatalf("Skills = %+v, want defaults %+v", cfg.Skills, DefaultSkillsConfig())
	}
	if !cfg.Skills.Enabled || cfg.Skills.RepoDir != ".occa/skills" || cfg.Skills.GlobalDir != "~/.occa/skills" ||
		cfg.Skills.ContextMaxRunes != 8000 || cfg.Skills.SkillMaxRunes != 4000 {
		t.Fatalf("defaults drifted from the spec: %+v", cfg.Skills)
	}
}

func TestSkillsBlockOverrides(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cfg, err := Load(writeConfig(t, t.TempDir(), "skills:\n  enabled: false\n  repo_dir: ./docs/skills/\n  global_dir: \"\"\n  context_max_runes: 6000\n  skill_max_runes: 3000\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := SkillsConfig{Enabled: false, RepoDir: "docs/skills", GlobalDir: "", ContextMaxRunes: 6000, SkillMaxRunes: 3000}
	if cfg.Skills != want {
		t.Fatalf("Skills = %+v, want %+v", cfg.Skills, want)
	}
	opts := cfg.Skills.Options("/repo")
	if opts.Workdir != "/repo" || opts.RepoDir != "docs/skills" || opts.ContextMaxRunes != 6000 {
		t.Fatalf("Options = %+v", opts)
	}
}

// AC-14: a present block is validated strictly and names the bad key.
func TestSkillsBlockRefusesStartup(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"unknown key", "skills:\n  enabled: true\n  colour: blue\n", "skills.colour"},
		{"absolute repo_dir", "skills:\n  repo_dir: /etc/skills\n", "skills.repo_dir"},
		{"home repo_dir", "skills:\n  repo_dir: ~/skills\n", "skills.repo_dir"},
		{"dotdot repo_dir", "skills:\n  repo_dir: .occa/../../skills\n", "skills.repo_dir"},
		{"empty repo_dir", "skills:\n  repo_dir: \"\"\n", "skills.repo_dir"},
		{"context below skill", "skills:\n  context_max_runes: 3000\n", "skills.context_max_runes (3000) must be >= skills.skill_max_runes (4000)"},
		{"zero skill cap", "skills:\n  skill_max_runes: 0\n", "skills.skill_max_runes"},
		{"not a mapping", "skills: [a]\n", "skills must be a mapping"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OCCA_ADMIN_ID", "admin123")
			_, err := Load(writeConfig(t, t.TempDir(), tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load err = %v, want it to name %q", err, tc.want)
			}
		})
	}
}
