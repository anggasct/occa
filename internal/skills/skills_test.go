package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func fixtureOptions(t *testing.T) Options {
	t.Helper()
	opts := DefaultOptions(filepath.Join("testdata", "skills", "repo"))
	opts.GlobalDir = filepath.Join("testdata", "skills", "global")
	return opts
}

func names(skills []Skill) []string {
	out := make([]string, 0, len(skills))
	for _, sk := range skills {
		out = append(out, sk.Scope+":"+sk.Name)
	}
	return out
}

func writeSkill(t *testing.T, root, dir, content string) string {
	t.Helper()
	path := filepath.Join(root, DefaultRepoDir, dir, skillFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// AC-01 (valid side) + AC-02: fixtures load, the broken file is skipped with
// its absolute path, and the repo twin shadows the global skill.
func TestLoadFixtures(t *testing.T) {
	set := Load(fixtureOptions(t))
	got := strings.Join(names(set.Skills), ",")
	want := "repo:always-on,repo:api-style,repo:occa-global,repo:store-contract,global:global-only"
	if got != want {
		t.Fatalf("skills = %s\nwant      %s", got, want)
	}
	if len(set.Shadowed) != 1 || set.Shadowed[0].Name != "occa-global" || set.Shadowed[0].Scope != ScopeGlobal {
		t.Fatalf("shadowed = %+v", names(set.Shadowed))
	}
	for _, sk := range set.Skills {
		if sk.Name == "occa-global" && sk.Body != "Repo version." {
			t.Fatalf("repo twin must win, body = %q", sk.Body)
		}
		if sk.Name == "always-on" && (sk.Body != "Run make check before you finish." || sk.Priority != 50) {
			t.Fatalf("CRLF skill parsed wrong: %+v", sk)
		}
		if sk.Name == "store-contract" && strings.Join(sk.Paths, ",") != "internal/store/**,cmd/db/*.go" {
			t.Fatalf("paths not normalized: %v", sk.Paths)
		}
		if sk.Name == "api-style" && sk.Priority != DefaultPriority {
			t.Fatalf("default priority = %d", sk.Priority)
		}
		if len(sk.Digest) != 64 {
			t.Fatalf("digest = %q", sk.Digest)
		}
	}
	if len(set.Errors) != 1 || !filepath.IsAbs(set.Errors[0].Path) ||
		!strings.HasSuffix(set.Errors[0].Path, filepath.Join("broken-frontmatter", "SKILL.md")) ||
		!strings.Contains(set.Errors[0].Reason, "colour") {
		t.Fatalf("errors = %+v", set.Errors)
	}
	if set.Errors[0].Line != 5 {
		t.Fatalf("unknown key line = %d, want 5", set.Errors[0].Line)
	}
}

// AC-01 (reject side): every invalid shape is skipped with a reason.
func TestParseRejects(t *testing.T) {
	long := strings.Repeat("x", DefaultSkillMaxRunes+1)
	cases := map[string]struct {
		dir, content, reason string
	}{
		"missing name":        {"a-skill", "---\ndescription: d\ntrigger: always\n---\nb", "missing required key name"},
		"missing description": {"a-skill", "---\nname: a-skill\ntrigger: always\n---\nb", "missing required key description"},
		"missing trigger":     {"a-skill", "---\nname: a-skill\ndescription: d\n---\nb", "missing required key trigger"},
		"bad trigger":         {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: sometimes\n---\nb", "must be always, keyword or paths"},
		"keyword no keywords": {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: keyword\n---\nb", "requires keywords"},
		"paths no paths":      {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: paths\n---\nb", "requires paths"},
		"keywords on always":  {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: always\nkeywords: [x]\n---\nb", "only allowed with trigger keyword"},
		"name != dir":         {"other-dir", "---\nname: a-skill\ndescription: d\ntrigger: always\n---\nb", "must equal its directory"},
		"bad name":            {"A", "---\nname: A\ndescription: d\ntrigger: always\n---\nb", "must match"},
		"body too long":       {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: always\n---\n" + long, "over 4000"},
		"no frontmatter":      {"a-skill", "just text", "missing frontmatter"},
		"unterminated":        {"a-skill", "---\nname: a-skill\n", "unterminated frontmatter"},
		"priority range":      {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: always\npriority: 1001\n---\nb", "priority 1001"},
		"multiline desc":      {"a-skill", "---\nname: a-skill\ndescription: \"a\\nb\"\ntrigger: always\n---\nb", "single line"},
		"yaml syntax":         {"a-skill", "---\nname: [unclosed\ndescription: d\n---\nb", "frontmatter: "},
		"bad glob":            {"a-skill", "---\nname: a-skill\ndescription: d\ntrigger: paths\npaths: [\"a**b\"]\n---\nb", "whole path segment"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseSkill([]byte(tc.content), tc.dir, DefaultSkillMaxRunes)
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err = %v, want %q", err, tc.reason)
			}
		})
	}
	if _, err := parseSkill([]byte{0xff, 0xfe}, "a-skill", DefaultSkillMaxRunes); err == nil || err.Error() != "invalid utf-8" {
		t.Fatalf("non-utf8 err = %v", err)
	}
}

// AC-03: keyword triggers on token boundaries, case-insensitively.
func TestContainsToken(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"please migrate the store", true},
		{"Migrate", true},
		{"migrate.", true},
		{"migration", false},
		{"migrated", false},
		{"premigrate", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := containsToken(tc.text, "migrate"); got != tc.want {
			t.Errorf("containsToken(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
	if !containsToken("fix the API Style guide", "api style") {
		t.Error("multi-word keyword must match")
	}
}

// AC-04: paths fire only on the declared scope, never on a workdir scan.
func TestPathsTriggerUsesDeclaredScopeOnly(t *testing.T) {
	set := Load(fixtureOptions(t))
	sel := Select(set, Input{Text: "", Scope: nil}, DefaultContextMaxRunes)
	for _, a := range sel.Applied {
		if a.Skill.Name == "store-contract" {
			t.Fatal("paths skill fired with an empty declared scope")
		}
	}
	sel = Select(set, Input{Scope: []string{"./internal/store/sqlite.go"}}, DefaultContextMaxRunes)
	var reason string
	for _, a := range sel.Applied {
		if a.Skill.Name == "store-contract" {
			reason = a.Reason
		}
	}
	if reason != "paths: internal/store/**" {
		t.Fatalf("store-contract reason = %q", reason)
	}
	sel = Select(set, Input{Scope: []string{"internal/webhook/server.go"}}, DefaultContextMaxRunes)
	for _, a := range sel.Applied {
		if a.Skill.Name == "store-contract" {
			t.Fatal("non-matching scope fired the paths skill")
		}
	}
}

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"internal/store/**", "internal/store/sqlite.go", true},
		{"internal/store/**", "internal/store", true},
		{"internal/store/**", "internal/store/a/b.go", true},
		{"internal/*.go", "internal/a.go", true},
		{"internal/*.go", "internal/a/b.go", false},
		{"**/*_test.go", "a/b/c_test.go", true},
		{"**/*_test.go", "c_test.go", true},
		{"cmd/?.go", "cmd/a.go", true},
		{"cmd/?.go", "cmd/ab.go", false},
		{"[a-c]*.md", "beta.md", true},
		{"[a-c]*.md", "zeta.md", false},
	}
	for _, tc := range cases {
		if got := matchGlob(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

// AC-05: over budget, lowest priority goes first, ties by name ascending.
func TestSelectBudgetDropOrder(t *testing.T) {
	mk := func(name string, priority, runes int) Skill {
		return Skill{Name: name, Scope: ScopeRepo, Trigger: TriggerAlways, Priority: priority, Body: strings.Repeat("y", runes), Runes: runes}
	}
	set := Set{Skills: []Skill{mk("high", 500, 300), mk("b-low", 10, 300), mk("a-low", 10, 300), mk("mid", 100, 300)}}
	full := Select(set, Input{}, DefaultContextMaxRunes)
	if len(full.Applied) != 4 || full.Applied[0].Skill.Name != "high" || full.Applied[3].Skill.Name != "b-low" {
		t.Fatalf("render order = %v", appliedNames(full.Applied))
	}
	budget := utf8.RuneCountInString(Render(full.Applied)) - 1
	sel := Select(set, Input{}, budget)
	if len(sel.Dropped) != 1 || sel.Dropped[0].Skill.Name != "a-low" {
		t.Fatalf("first dropped = %v, want a-low", appliedNames(sel.Dropped))
	}
	twoTop := utf8.RuneCountInString(Render(full.Applied[:2]))
	sel = Select(set, Input{}, twoTop)
	if got := strings.Join(appliedNames(sel.Dropped), ","); got != "a-low,b-low" {
		t.Fatalf("dropped = %s, want a-low,b-low", got)
	}
	if sel.Runes > twoTop || sel.Runes != utf8.RuneCountInString(Render(sel.Applied)) {
		t.Fatalf("runes = %d", sel.Runes)
	}
}

func appliedNames(applied []Applied) []string {
	out := make([]string, 0, len(applied))
	for _, a := range applied {
		out = append(out, a.Skill.Name)
	}
	return out
}

func TestRenderFormat(t *testing.T) {
	if Render(nil) != "" {
		t.Fatal("no skills must render nothing")
	}
	got := Render([]Applied{{Skill: Skill{Name: "a-skill", Scope: ScopeRepo, Body: "Body."}, Reason: `keyword: "x"`}})
	want := "<occa-skills count=\"1\">\n<skill name=\"a-skill\" scope=\"repo\" trigger=\"keyword: 'x'\">\nBody.\n</skill>\n</occa-skills>"
	if got != want {
		t.Fatalf("render =\n%s\nwant\n%s", got, want)
	}
}

func TestLoadEdgeCases(t *testing.T) {
	root := t.TempDir()
	// Missing directories are empty scopes, not errors.
	set := Load(Options{Workdir: root, GlobalDir: filepath.Join(root, "nope")})
	if len(set.Skills) != 0 || len(set.Errors) != 0 || set.GlobalUnreadable != nil {
		t.Fatalf("empty workdir = %+v", set)
	}
	// An empty skill directory and a stray file are ignored.
	if err := os.MkdirAll(filepath.Join(root, DefaultRepoDir, "empty-dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, DefaultRepoDir, "README.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeSkill(t, root, "real-one", "---\nname: real-one\ndescription: d\ntrigger: always\n---\nok")
	// A symlinked skill directory is followed once.
	target := filepath.Join(t.TempDir(), "linked-one")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, skillFile), []byte("---\nname: linked-one\ndescription: d\ntrigger: always\n---\nlinked"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, DefaultRepoDir, "linked-one")); err != nil {
		t.Fatal(err)
	}
	set = Load(Options{Workdir: root})
	if got := strings.Join(names(set.Skills), ","); got != "repo:linked-one,repo:real-one" {
		t.Fatalf("skills = %s", got)
	}
}

// F-001: a repo skills directory that exists but cannot be listed is an
// error, not a clean empty tree. A regular file in its place gives ENOTDIR.
func TestRepoDirUnreadable(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".occa"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, DefaultRepoDir), []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	set := Load(Options{Workdir: root})
	if len(set.Skills) != 0 {
		t.Fatalf("skills = %v, want none", names(set.Skills))
	}
	if len(set.Errors) != 1 || !strings.HasSuffix(set.Errors[0].Path, filepath.FromSlash(DefaultRepoDir)) ||
		!strings.Contains(set.Errors[0].Reason, "unreadable") {
		t.Fatalf("errors = %+v", set.Errors)
	}
	if set.Errors[0].Line != 1 {
		t.Fatalf("line = %d, want 1", set.Errors[0].Line)
	}
	if _, errs := LoadRepo(Options{Workdir: root}); len(errs) != 1 || !strings.Contains(errs[0].Reason, "unreadable") {
		t.Fatalf("LoadRepo errors = %+v", errs)
	}
}

// Spec validation table: a global dir that cannot be listed contributes
// zero global skills and is reported once via GlobalUnreadable.
func TestGlobalDirUnreadable(t *testing.T) {
	root := t.TempDir()
	global := filepath.Join(root, "global-skills")
	if err := os.WriteFile(global, []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeSkill(t, root, "repo-only", "---\nname: repo-only\ndescription: d\ntrigger: always\n---\nok")
	set := Load(Options{Workdir: root, GlobalDir: global})
	if set.GlobalUnreadable == nil {
		t.Fatal("GlobalUnreadable must be set when the global dir cannot be listed")
	}
	if got := strings.Join(names(set.Skills), ","); got != "repo:repo-only" {
		t.Fatalf("skills = %s, want the repo scope only", got)
	}
}

// F-003: declared scope entries are workdir-relative; absolute or
// ..-escaping entries are rejected, not silently mangled into a match.
func TestNormalizePathRejectsEscape(t *testing.T) {
	for _, p := range []string{
		"/internal/store/x.go",
		"/home/op/repo/internal/store/x.go",
		"..",
		"../outside.go",
		"internal/../../escape.go",
	} {
		if n, ok := normalizePath(p); ok {
			t.Errorf("normalizePath(%q) = %q, want rejection", p, n)
		}
	}
	for _, p := range []string{"internal/store/x.go", "./internal/a.go", "a/../b.go"} {
		n, ok := normalizePath(p)
		if !ok {
			t.Errorf("normalizePath(%q) rejected, want accepted", p)
			continue
		}
		if n == "" || n == ".." || strings.HasPrefix(n, "/") || strings.HasPrefix(n, "../") {
			t.Errorf("normalizePath(%q) = %q, want workdir-relative", p, n)
		}
	}
}

// F-003: Select excludes rejected scope entries from matching and reports
// them, so an absolute --scope-file cannot half-match or silently miss.
func TestSelectRejectsInvalidScope(t *testing.T) {
	set := Load(fixtureOptions(t))
	sel := Select(set, Input{Scope: []string{"/internal/store/x.go", "internal/store/sqlite.go"}}, DefaultContextMaxRunes)
	if len(sel.InvalidScope) != 1 || sel.InvalidScope[0] != "/internal/store/x.go" {
		t.Fatalf("invalid scope = %v", sel.InvalidScope)
	}
	found := false
	for _, a := range sel.Applied {
		if a.Skill.Name == "store-contract" {
			found = true
		}
	}
	if !found {
		t.Fatal("valid scope entry must still fire store-contract")
	}
	sel = Select(set, Input{Scope: []string{"/home/op/repo/internal/store/x.go"}}, DefaultContextMaxRunes)
	if len(sel.InvalidScope) != 1 {
		t.Fatalf("invalid scope = %v", sel.InvalidScope)
	}
	for _, a := range sel.Applied {
		if a.Skill.Name == "store-contract" {
			t.Fatal("absolute scope entry must not fire the paths skill")
		}
	}
}
