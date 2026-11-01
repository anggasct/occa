package router

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/skills"
)

type routerSkillLedger struct {
	mu   sync.Mutex
	rows map[string][]skills.Record
}

func (l *routerSkillLedger) SessionSkills(_ context.Context, id string) ([]skills.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]skills.Record(nil), l.rows[id]...), nil
}

func (l *routerSkillLedger) RecordSessionSkills(_ context.Context, id string, recs []skills.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.rows[id]) == 0 {
		l.rows[id] = append([]skills.Record(nil), recs...)
	}
	return nil
}

func writeRouterSkill(t *testing.T, root, name, body string) {
	t.Helper()
	dir := filepath.Join(root, ".occa", "skills", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: "+name+"\ndescription: d\ntrigger: always\n---\n"+body+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newSkillsRouter(t *testing.T, workdir string) (*Router, *fakeRelayClient, *fakeReplyCtx, *routerSkillLedger) {
	t.Helper()
	client := &fakeRelayClient{sessionID: "sess-new"}
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: newFakeOverrideRepo(),
		scheduleRepo: &fakeScheduleRepo{},
	}
	r := NewWithAllowlists(&fakeInstanceProvider{client: client}, st, workdir, "", []string{"user1"}, []string{"user1"}, testRouterConfig())
	ledger := &routerSkillLedger{rows: map[string][]skills.Record{}}
	r.SetSkills(skills.NewInjector(true, func(wd string) skills.Options {
		o := skills.DefaultOptions(wd)
		o.GlobalDir = ""
		return o
	}, ledger))
	return r, client, &fakeReplyCtx{}, ledger
}

func sendChat(t *testing.T, r *Router, client *fakeRelayClient, reply *fakeReplyCtx, text string) string {
	t.Helper()
	if err := r.Route(context.Background(), msg(text, reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.rawMsg
}

// AC-06 + AC-07 through the router: one block per agent session, on its
// first prompt; /reset starts a session that gets its own block.
func TestRouterSkillsOncePerSessionAndReset(t *testing.T) {
	wd := t.TempDir()
	writeRouterSkill(t, wd, "house-rules", "Use tabs.")
	r, client, reply, ledger := newSkillsRouter(t, wd)

	first := sendChat(t, r, client, reply, "hello")
	if !strings.HasPrefix(first, `<occa-skills count="1">`) || !strings.Contains(first, "Use tabs.") || !strings.HasSuffix(first, "\n\nhello") {
		t.Fatalf("first prompt = %q", first)
	}
	if got := sendChat(t, r, client, reply, "second"); got != "second" {
		t.Fatalf("second prompt = %q", got)
	}
	if got := sendChat(t, r, client, reply, "third"); got != "third" {
		t.Fatalf("third prompt = %q", got)
	}
	ledger.mu.Lock()
	sessions := len(ledger.rows)
	ledger.mu.Unlock()
	if sessions != 1 {
		t.Fatalf("recorded sessions = %d, want 1", sessions)
	}

	if err := r.Route(context.Background(), msg("/reset", reply)); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if got := sendChat(t, r, client, reply, "after reset"); !strings.HasPrefix(got, "<occa-skills ") {
		t.Fatalf("post-reset prompt = %q", got)
	}
	ledger.mu.Lock()
	sessions = len(ledger.rows)
	ledger.mu.Unlock()
	if sessions != 2 {
		t.Fatalf("recorded sessions after reset = %d, want 2", sessions)
	}
}

// AC-08 through the router: an edited skill in a live session gets the
// notice once and no block.
func TestRouterSkillsChangedNotice(t *testing.T) {
	wd := t.TempDir()
	writeRouterSkill(t, wd, "house-rules", "Use tabs.")
	r, client, reply, _ := newSkillsRouter(t, wd)
	sendChat(t, r, client, reply, "hello")
	writeRouterSkill(t, wd, "house-rules", "Use spaces.")
	if got := sendChat(t, r, client, reply, "next"); got != "next" {
		t.Fatalf("changed session got a block: %q", got)
	}
	sendChat(t, r, client, reply, "again")
	reply.mu.Lock()
	defer reply.mu.Unlock()
	notices := 0
	for _, s := range reply.sends {
		if s == skills.ChangedNotice {
			notices++
		}
	}
	if notices != 1 {
		t.Fatalf("notices = %d in %q", notices, reply.sends)
	}
}

// A resumed session that already holds prompts never gets a block.
func TestRouterSkillsSkipStartedSession(t *testing.T) {
	wd := t.TempDir()
	r, client, reply, _ := newSkillsRouter(t, wd)
	sendChat(t, r, client, reply, "before skills existed")
	writeRouterSkill(t, wd, "house-rules", "Use tabs.")
	client.mu.Lock()
	client.messages = []relay.MessageInfo{{ID: "m1", Role: "user"}}
	client.mu.Unlock()
	if got := sendChat(t, r, client, reply, "after"); got != "after" {
		t.Fatalf("mid-session block: %q", got)
	}
}

// AC-12 through the router: no skills, the prompt is byte-identical.
func TestRouterSkillsGoldenNoSkills(t *testing.T) {
	r, client, reply, _ := newSkillsRouter(t, t.TempDir())
	if got := sendChat(t, r, client, reply, "  hello\n\nworld "); got != "  hello\n\nworld " {
		t.Fatalf("prompt = %q", got)
	}
}

type taskSourceLifecycle struct {
	recordingDispatchLifecycle
	brief, scope string
}

func (l *taskSourceLifecycle) DispatchTaskContext(taskID string) (string, string, bool) {
	if taskID != "task-7" {
		return "", "", false
	}
	return l.brief, l.scope, true
}

// AC-04 for /dispatch: a dispatched run's declared scope is the task's scope
// field, split into files; its brief joins the keyword text.
func TestRouterSkillInputFromDispatch(t *testing.T) {
	r, _, reply, _ := newSkillsRouter(t, t.TempDir())
	r.SetDispatchLifecycle(&taskSourceLifecycle{brief: "Migrate the store", scope: "internal/store/sqlite.go, cmd/occa/"})
	m := msg("🚀 Dispatched acme/widget#feat/x · task-7", reply)
	m.IsSelfDispatch = true
	in := r.skillInput(m, m.Text)
	if strings.Join(in.Scope, "|") != "internal/store/sqlite.go|cmd/occa/" || !strings.Contains(in.Text, "Migrate the store") {
		t.Fatalf("input = %+v", in)
	}
	plain := r.skillInput(msg("internal/store/sqlite.go", reply), "internal/store/sqlite.go")
	if len(plain.Scope) != 0 {
		t.Fatalf("a chat message declares no file scope: %+v", plain)
	}
	r.SetDispatchLifecycle(&recordingDispatchLifecycle{})
	if in := r.skillInput(m, m.Text); len(in.Scope) != 0 || in.Text != m.Text {
		t.Fatalf("lifecycle without a task source: %+v", in)
	}
}

func routeSkills(t *testing.T, r *Router, reply *fakeReplyCtx) string {
	t.Helper()
	if err := r.Route(context.Background(), msg("/skills", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	reply.mu.Lock()
	defer reply.mu.Unlock()
	if len(reply.sends) == 0 {
		t.Fatal("no reply")
	}
	return reply.sends[len(reply.sends)-1]
}

// AC-11: /skills lists name · scope · trigger · runes, marks shadowed
// globals, lists skipped files, and answers the empty case.
func TestRouterSkillsCommand(t *testing.T) {
	r, _, reply, _ := newSkillsRouter(t, t.TempDir())
	if got := routeSkills(t, r, reply); got != "no skills in this workdir" {
		t.Fatalf("empty = %q", got)
	}

	wd := t.TempDir()
	global := t.TempDir()
	writeRouterSkill(t, wd, "house-rules", "Use tabs.")
	writeRouterSkill(t, wd, "shared", "Repo wins.")
	writeRouterSkill(t, global, "shared", "Global loses.")
	if err := os.MkdirAll(filepath.Join(wd, ".occa", "skills", "broken"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wd, ".occa", "skills", "broken", "SKILL.md"), []byte("---\nname: broken\ndescription: d\ntrigger: sometimes\n---\nx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, _, reply, _ = newSkillsRouter(t, wd)
	r.SetSkills(skills.NewInjector(true, func(w string) skills.Options {
		o := skills.DefaultOptions(w)
		o.GlobalDir = filepath.Join(global, ".occa", "skills")
		return o
	}, nil))
	got := routeSkills(t, r, reply)
	lines := strings.Split(got, "\n")
	if len(lines) != 4 ||
		lines[0] != "house-rules · repo · always · 9 runes" ||
		lines[1] != "shared · repo · always · 10 runes" ||
		lines[2] != "shared · global · always · 13 runes · shadowed" ||
		!strings.HasPrefix(lines[3], "skipped "+filepath.Join(".occa", "skills", "broken", "SKILL.md")+":") {
		t.Fatalf("/skills =\n%s", got)
	}

	r.SetSkills(skills.NewInjector(false, nil, nil))
	if got := routeSkills(t, r, reply); !strings.Contains(got, "disabled") {
		t.Fatalf("disabled = %q", got)
	}
}

// A long list stays one message: capped at 20 lines plus a pointer.
func TestFormatSkillsListCaps(t *testing.T) {
	var set skills.Set
	for i := 0; i < 30; i++ {
		set.Skills = append(set.Skills, skills.Skill{Name: "s" + strings.Repeat("x", i), Scope: "repo", Trigger: "always", Runes: 1})
	}
	for i := 0; i < 7; i++ {
		set.Errors = append(set.Errors, skills.LoadError{Path: "/w/.occa/skills/b/SKILL.md", Line: 2, Reason: "bad"})
	}
	lines := strings.Split(formatSkillsList(set, "/w"), "\n")
	if len(lines) != 20+1+5+1 || lines[20] != "… and 10 more — occa skills list" || lines[26] != "… and 2 more skipped — occa skills lint" {
		t.Fatalf("lines = %d\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if lines[21] != "skipped .occa/skills/b/SKILL.md:2: bad" {
		t.Fatalf("skipped line = %q", lines[21])
	}
}
