package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	skillsFixtureRepo   = filepath.Join("..", "..", "internal", "skills", "testdata", "skills", "repo")
	skillsFixtureGlobal = filepath.Join("..", "..", "internal", "skills", "testdata", "skills", "global")
)

func runSkills(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := runSkillsCommand(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// AC-09: lint exits 1 with path:line: reason per invalid skill.
func TestSkillsLintReportsInvalid(t *testing.T) {
	code, out, _ := runSkills("lint", "--workdir", skillsFixtureRepo)
	if code != 1 {
		t.Fatalf("exit = %d, want 1; out=%s", code, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 1 {
		t.Fatalf("lint lines = %q, want one", lines)
	}
	if !strings.Contains(lines[0], filepath.Join("broken-frontmatter", "SKILL.md")+":5: ") || !strings.Contains(lines[0], "colour") {
		t.Fatalf("lint line = %q, want path:line: reason", lines[0])
	}
}

// AC-09: a clean tree exits 0 with "N skills ok".
func TestSkillsLintCleanTree(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".occa", "skills", "good-one")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: good-one\ndescription: d\ntrigger: always\n---\nok\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, _ := runSkills("lint", "--workdir", root)
	if code != 0 || strings.TrimSpace(out) != "1 skills ok" {
		t.Fatalf("exit=%d out=%q", code, out)
	}
	code, out, _ = runSkills("lint", "--workdir", t.TempDir())
	if code != 0 || strings.TrimSpace(out) != "0 skills ok" {
		t.Fatalf("empty tree exit=%d out=%q", code, out)
	}
}

// AC-10: explain prints the selected set with the trigger that fired, the
// rune total and the dropped entries.
func TestSkillsExplain(t *testing.T) {
	code, out, _ := runSkills("explain", "--workdir", skillsFixtureRepo, "--global-dir", skillsFixtureGlobal,
		"--text", "please migrate and deploy", "--scope-file", "internal/store/sqlite.go")
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{
		"selected store-contract · repo · paths: internal/store/** · ",
		"selected api-style · repo · keyword: migrate · ",
		"selected always-on · repo · always · ",
		"selected occa-global · repo · always · ",
		"selected global-only · global · keyword: deploy · ",
		"shadowed occa-global · global",
		"dropped=0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("explain output missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "total ") || !strings.Contains(out, " runes of 8000") {
		t.Fatalf("explain output missing total:\n%s", out)
	}
}

func TestSkillsListAndUsage(t *testing.T) {
	code, out, _ := runSkills("list", "--workdir", skillsFixtureRepo, "--global-dir", skillsFixtureGlobal)
	if code != 0 || !strings.Contains(out, "occa-global · global · always · 15 runes · shadowed") || !strings.Contains(out, "skipped ") {
		t.Fatalf("list exit=%d out=\n%s", code, out)
	}
	code, out, _ = runSkills("list", "--workdir", t.TempDir(), "--global-dir", "")
	if code != 0 || strings.TrimSpace(out) != "no skills in this workdir" {
		t.Fatalf("empty list exit=%d out=%q", code, out)
	}
	if code, _, _ := runSkills("bogus"); code != 2 {
		t.Fatalf("unknown command exit = %d, want 2", code)
	}
	if code, _, _ := runSkills("lint"); code != 2 {
		t.Fatalf("missing --workdir exit = %d, want 2", code)
	}
}
