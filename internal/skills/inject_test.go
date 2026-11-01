package skills

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

type memLedger struct {
	mu   sync.Mutex
	rows map[string][]Record
}

func newMemLedger() *memLedger { return &memLedger{rows: map[string][]Record{}} }

func (l *memLedger) SessionSkills(_ context.Context, sessionID string) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Record(nil), l.rows[sessionID]...), nil
}

func (l *memLedger) RecordSessionSkills(_ context.Context, sessionID string, records []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.rows[sessionID]) == 0 {
		l.rows[sessionID] = append([]Record(nil), records...)
	}
	return nil
}

func injectorFor(ledger Ledger, global string) *Injector {
	return NewInjector(true, func(workdir string) Options {
		o := DefaultOptions(workdir)
		o.GlobalDir = global
		return o
	}, ledger)
}

// chatSession simulates the router: one plan per message, committed after a
// successful send, isFirst true only before the first prompt.
type chatSession struct {
	in      *Injector
	id      string
	workdir string
	sent    []string
	notices []string
}

func (c *chatSession) send(t *testing.T, text string) {
	t.Helper()
	plan := c.in.ForSession(context.Background(), c.id, c.workdir, Input{Text: text}, func() bool { return len(c.sent) == 0 })
	if plan.Notice != "" {
		c.notices = append(c.notices, plan.Notice)
	}
	c.sent = append(c.sent, plan.Prefix(text))
	c.in.Commit(context.Background(), c.id, plan)
}

func countBlocks(msgs []string) int {
	n := 0
	for _, m := range msgs {
		n += strings.Count(m, "<occa-skills ")
	}
	return n
}

// AC-06: three messages in one session carry exactly one block, on the
// first; the ledger holds exactly the injected rows.
func TestInjectOncePerSession(t *testing.T) {
	ledger := newMemLedger()
	in := injectorFor(ledger, "")
	c := &chatSession{in: in, id: "sess-1", workdir: filepath.Join("testdata", "skills", "repo")}
	c.send(t, "please migrate the store")
	c.send(t, "now migrate again")
	c.send(t, "and once more")
	if got := countBlocks(c.sent); got != 1 || !strings.HasPrefix(c.sent[0], `<occa-skills count="3">`) {
		t.Fatalf("blocks = %d, first = %q", got, c.sent[0])
	}
	if c.sent[1] != "now migrate again" || c.sent[2] != "and once more" {
		t.Fatalf("later prompts changed: %q", c.sent[1:])
	}
	if !strings.HasSuffix(c.sent[0], "\n\nplease migrate the store") {
		t.Fatalf("first prompt does not end with the operator text: %q", c.sent[0])
	}
	rows, _ := ledger.SessionSkills(context.Background(), "sess-1")
	var got []string
	for _, r := range rows {
		got = append(got, r.Name+"/"+r.Scope+"/"+r.Trigger)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "always-on/repo/always,api-style/repo/keyword,occa-global/repo/always" {
		t.Fatalf("rows = %v", got)
	}

	// A second run in the same agent session (fresh injector = restart)
	// injects nothing: the rows are the guard.
	again := &chatSession{in: injectorFor(ledger, ""), id: "sess-1", workdir: c.workdir}
	again.send(t, "migrate")
	if countBlocks(again.sent) != 0 || again.sent[0] != "migrate" {
		t.Fatalf("second run re-injected: %q", again.sent)
	}
}

// AC-07: a new session (after /reset) gets its own block and rows.
func TestInjectAfterReset(t *testing.T) {
	ledger := newMemLedger()
	in := injectorFor(ledger, "")
	wd := filepath.Join("testdata", "skills", "repo")
	first := &chatSession{in: in, id: "sess-1", workdir: wd}
	first.send(t, "hello")
	reset := &chatSession{in: in, id: "sess-2", workdir: wd}
	reset.send(t, "hello again")
	if countBlocks(first.sent) != 1 || countBlocks(reset.sent) != 1 {
		t.Fatalf("blocks = %d / %d", countBlocks(first.sent), countBlocks(reset.sent))
	}
	rows, _ := ledger.SessionSkills(context.Background(), "sess-2")
	if len(rows) != 2 {
		t.Fatalf("new session rows = %+v", rows)
	}
}

// A session that already had prompts before skills existed (or before a
// restart lost the in-memory mark) never gets a mid-session block.
func TestInjectNeverMidSession(t *testing.T) {
	in := injectorFor(newMemLedger(), "")
	plan := in.ForSession(context.Background(), "sess-1", filepath.Join("testdata", "skills", "repo"), Input{Text: "x"}, func() bool { return false })
	if plan.Block != "" || plan.Notice != "" {
		t.Fatalf("plan = %+v", plan)
	}
	calls := 0
	plan = in.ForSession(context.Background(), "sess-1", filepath.Join("testdata", "skills", "repo"), Input{Text: "x"}, func() bool { calls++; return true })
	if plan.Block != "" || calls != 0 {
		t.Fatalf("started session asked again: plan=%+v calls=%d", plan, calls)
	}
}

// AC-08: a changed skill in a live session sends one notice and injects
// nothing; the notice is not repeated for the same change.
func TestInjectChangedNotice(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "rules", "---\nname: rules\ndescription: d\ntrigger: always\n---\nv1\n")
	ledger := newMemLedger()
	in := injectorFor(ledger, "")
	c := &chatSession{in: in, id: "sess-1", workdir: root}
	c.send(t, "start")
	c.send(t, "unchanged")
	if len(c.notices) != 0 {
		t.Fatalf("notice without a change: %v", c.notices)
	}
	writeSkill(t, root, "rules", "---\nname: rules\ndescription: d\ntrigger: always\n---\nv2\n")
	c.send(t, "after edit")
	c.send(t, "after edit again")
	if len(c.notices) != 1 || c.notices[0] != ChangedNotice {
		t.Fatalf("notices = %v", c.notices)
	}
	if countBlocks(c.sent) != 1 || c.sent[2] != "after edit" {
		t.Fatalf("changed session got a block: %q", c.sent)
	}
	writeSkill(t, root, "extra", "---\nname: extra\ndescription: d\ntrigger: always\n---\nnew\n")
	c.send(t, "after add")
	if len(c.notices) != 2 {
		t.Fatalf("a new always-on skill is a change: %v", c.notices)
	}
	writeSkill(t, root, "later-keyword", "---\nname: later-keyword\ndescription: d\ntrigger: keyword\nkeywords: [deploy]\n---\nk\n")
	c.send(t, "deploy now")
	if len(c.notices) != 2 {
		t.Fatalf("a keyword skill a later message triggers is not a change: %v", c.notices)
	}
}

// AC-12 at the injector seam: no skills, disabled, nil injector, or no
// workdir leave the prompt byte-identical.
func TestInjectNoSkillsIsIdentity(t *testing.T) {
	prompt := "  Review PR #1\n\n<context>\n"
	empty := t.TempDir()
	cases := map[string]Plan{
		"no dirs":     injectorFor(newMemLedger(), "").ForNewSession(empty, Input{Text: prompt}),
		"disabled":    NewInjector(false, nil, nil).ForNewSession(filepath.Join("testdata", "skills", "repo"), Input{Text: prompt}),
		"nil":         (*Injector)(nil).ForNewSession(filepath.Join("testdata", "skills", "repo"), Input{Text: prompt}),
		"no workdir":  injectorFor(newMemLedger(), "").ForNewSession("", Input{Text: prompt}),
		"chat no dir": injectorFor(newMemLedger(), "").ForSession(context.Background(), "s", empty, Input{Text: prompt}, func() bool { return true }),
	}
	for name, plan := range cases {
		if got := plan.Prefix(prompt); got != prompt {
			t.Fatalf("%s: prompt changed: %q", name, got)
		}
	}
}

func TestInjectDisabledReadsNothing(t *testing.T) {
	called := false
	in := NewInjector(false, func(string) Options { called = true; return Options{} }, nil)
	in.ForNewSession("/does/not/matter", Input{})
	in.ForSession(context.Background(), "s", "/x", Input{}, nil)
	if called {
		t.Fatal("disabled injector resolved options")
	}
}

// A webhook run: block planned before the session exists, recorded after.
func TestInjectNewSessionCommit(t *testing.T) {
	ledger := newMemLedger()
	in := injectorFor(ledger, filepath.Join("testdata", "skills", "global"))
	plan := in.ForNewSession(filepath.Join("testdata", "skills", "repo"), Input{Text: "deploy", Scope: SplitScope("internal/store/sqlite.go, cmd/occa/")})
	if !strings.Contains(plan.Block, `name="store-contract"`) || !strings.Contains(plan.Block, `name="global-only"`) {
		t.Fatalf("block = %s", plan.Block)
	}
	in.Commit(context.Background(), "wh-1", plan)
	rows, _ := ledger.SessionSkills(context.Background(), "wh-1")
	if len(rows) != 4 {
		t.Fatalf("rows = %+v", rows)
	}
	// A takeover of that session from chat injects nothing.
	again := in.ForSession(context.Background(), "wh-1", filepath.Join("testdata", "skills", "repo"), Input{Text: "deploy"}, func() bool { return true })
	if again.Block != "" {
		t.Fatalf("takeover re-injected")
	}
}

func TestSplitScope(t *testing.T) {
	got := SplitScope(" internal/store/*.go, cmd/occa/;`README.md`\n internal/store/*.go ")
	if strings.Join(got, "|") != "internal/store/*.go|cmd/occa/|README.md" {
		t.Fatalf("SplitScope = %q", got)
	}
	if len(SplitScope("")) != 0 {
		t.Fatal("empty scope")
	}
}

func captureSkillLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// Domain rule 3 / F-001: a workdir whose only skill is broken still logs the
// skip at ERROR naming the file, on a webhook run and on a chat session's
// first prompt — once per session, not on every later message.
func TestInjectLogsSkipsWhenNoSkillIsValid(t *testing.T) {
	root := t.TempDir()
	broken := writeSkill(t, root, "broken", "---\nname: broken\ndescription: d\ntrigger: sometimes\n---\nx\n")
	logs := captureSkillLogs(t)
	in := injectorFor(newMemLedger(), "")

	plan := in.ForNewSession(root, Input{Text: "x"})
	if plan.Prefix("p") != "p" {
		t.Fatalf("an all-invalid workdir changed the prompt: %q", plan.Prefix("p"))
	}
	if got := strings.Count(logs.String(), "level=ERROR msg=\"skill skipped\" path="+broken); got != 1 {
		t.Fatalf("webhook run skip lines = %d:\n%s", got, logs.String())
	}

	logs.Reset()
	c := &chatSession{in: in, id: "sess-1", workdir: root}
	c.send(t, "first")
	c.send(t, "second")
	c.send(t, "third")
	if c.sent[0] != "first" || countBlocks(c.sent) != 0 {
		t.Fatalf("prompts = %q", c.sent)
	}
	if got := strings.Count(logs.String(), "msg=\"skill skipped\""); got != 1 {
		t.Fatalf("chat skip lines = %d, want 1 for the session:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "reason=") {
		t.Fatalf("skip line without a reason:\n%s", logs.String())
	}
}

// An unreadable global dir is reported at WARN once, not per run.
func TestInjectWarnsUnreadableGlobalOnce(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads unreadable dirs")
	}
	global := t.TempDir()
	if err := os.Chmod(global, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(global, 0o755) })
	logs := captureSkillLogs(t)
	in := injectorFor(newMemLedger(), global)
	for i := 0; i < 3; i++ {
		if p := in.ForNewSession(t.TempDir(), Input{}); p.Block != "" {
			t.Fatalf("block = %q", p.Block)
		}
	}
	if got := strings.Count(logs.String(), "skills: global dir unreadable"); got != 1 {
		t.Fatalf("unreadable-global WARN lines = %d:\n%s", got, logs.String())
	}
}
