package main

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/anggasct/occa/internal/skills"
)

func runSkillsCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		skillsUsage(stdout)
		return 0
	}
	switch args[0] {
	case "list":
		return runSkillsList(args[1:], stdout, stderr)
	case "lint":
		return runSkillsLint(args[1:], stdout, stderr)
	case "explain":
		return runSkillsExplain(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		skillsUsage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "occa skills: unknown command %q\n\n", args[0])
		skillsUsage(stderr)
		return 2
	}
}

func skillsUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  occa skills list    --workdir DIR [--global-dir DIR]")
	_, _ = fmt.Fprintln(w, "  occa skills lint    --workdir DIR")
	_, _ = fmt.Fprintln(w, "  occa skills explain --workdir DIR --text TASK [--scope-file PATH ...] [--global-dir DIR]")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintf(w, "Skills live in DIR/%s/<name>/SKILL.md and %s/<name>/SKILL.md.\n", skills.DefaultRepoDir, skills.DefaultGlobalDir)
	_, _ = fmt.Fprintln(w, "lint checks the repo scope only and exits 1 when any skill is invalid.")
}

type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, ",") }
func (r *repeated) Set(v string) error { *r = append(*r, v); return nil }

func skillsFlags(name string, stderr io.Writer) (*flag.FlagSet, *string, *string) {
	fs := flag.NewFlagSet("occa skills "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	workdir := fs.String("workdir", "", "repository checkout to read skills from")
	global := fs.String("global-dir", skills.DefaultGlobalDir, "global skills directory (empty disables)")
	return fs, workdir, global
}

func skillsOptions(workdir, global string) skills.Options {
	opts := skills.DefaultOptions(workdir)
	opts.GlobalDir = global
	return opts
}

func runSkillsList(args []string, stdout, stderr io.Writer) int {
	fs, workdir, global := skillsFlags("list", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*workdir) == "" {
		_, _ = fmt.Fprintln(stderr, "occa skills list: --workdir is required")
		return 2
	}
	set := skills.Load(skillsOptions(*workdir, *global))
	if len(set.Skills) == 0 && len(set.Shadowed) == 0 && len(set.Errors) == 0 {
		_, _ = fmt.Fprintln(stdout, "no skills in this workdir")
		return 0
	}
	for _, sk := range set.Skills {
		_, _ = fmt.Fprintf(stdout, "%s · %s · %s · %d runes\n", sk.Name, sk.Scope, sk.Trigger, sk.Runes)
	}
	for _, sk := range set.Shadowed {
		_, _ = fmt.Fprintf(stdout, "%s · %s · %s · %d runes · shadowed\n", sk.Name, sk.Scope, sk.Trigger, sk.Runes)
	}
	for _, e := range set.Errors {
		_, _ = fmt.Fprintf(stdout, "skipped %s:%d: %s\n", e.Path, e.Line, e.Reason)
	}
	if set.GlobalUnreadable != nil {
		_, _ = fmt.Fprintf(stderr, "occa skills list: global dir unreadable: %v\n", set.GlobalUnreadable)
	}
	return 0
}

func runSkillsLint(args []string, stdout, stderr io.Writer) int {
	fs, workdir, _ := skillsFlags("lint", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*workdir) == "" {
		_, _ = fmt.Fprintln(stderr, "occa skills lint: --workdir is required")
		return 2
	}
	ok, errs := skills.LoadRepo(skills.DefaultOptions(*workdir))
	if len(errs) > 0 {
		for _, e := range errs {
			_, _ = fmt.Fprintf(stdout, "%s:%d: %s\n", e.Path, e.Line, e.Reason)
		}
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "%d skills ok\n", len(ok))
	return 0
}

func runSkillsExplain(args []string, stdout, stderr io.Writer) int {
	fs, workdir, global := skillsFlags("explain", stderr)
	text := fs.String("text", "", "task text the keyword triggers match against")
	var scope repeated
	fs.Var(&scope, "scope-file", "a file in the run's declared scope (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*workdir) == "" {
		_, _ = fmt.Fprintln(stderr, "occa skills explain: --workdir is required")
		return 2
	}
	opts := skillsOptions(*workdir, *global)
	set := skills.Load(opts)
	sel := skills.Select(set, skills.Input{Text: *text, Scope: scope}, opts.ContextMaxRunes)
	for _, p := range sel.InvalidScope {
		_, _ = fmt.Fprintf(stderr, "occa skills explain: invalid --scope-file %q: must be relative to the workdir (no leading / or ..)\n", p)
	}
	if len(sel.InvalidScope) > 0 {
		return 2
	}
	if len(sel.Applied) == 0 && len(sel.Dropped) == 0 {
		_, _ = fmt.Fprintln(stdout, "selected: none")
	}
	for _, a := range sel.Applied {
		_, _ = fmt.Fprintf(stdout, "selected %s · %s · %s · %d runes\n", a.Skill.Name, a.Skill.Scope, a.Reason, a.Skill.Runes)
	}
	for _, a := range sel.Dropped {
		_, _ = fmt.Fprintf(stdout, "dropped  %s · %s · %s · priority %d (over budget)\n", a.Skill.Name, a.Skill.Scope, a.Reason, a.Skill.Priority)
	}
	for _, sk := range set.Shadowed {
		_, _ = fmt.Fprintf(stdout, "shadowed %s · global (repo skill of the same name wins)\n", sk.Name)
	}
	_, _ = fmt.Fprintf(stdout, "total %d runes of %d · dropped=%d\n", sel.Runes, opts.ContextMaxRunes, len(sel.Dropped))
	return 0
}
