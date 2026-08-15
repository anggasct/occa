package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func tempStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSchemaAutoCreated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fresh.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("db file not created: %v", err)
	}
}

func TestSessionRoundTrip(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	id, pid, err := s.SessionRepo().Active(ctx, "telegram", "chat1", "", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "" {
		t.Fatalf("expected empty, got %q", id)
	}
	if pid != 0 {
		t.Fatalf("expected pid 0, got %d", pid)
	}

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-1", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	id, _, err = s.SessionRepo().Active(ctx, "telegram", "chat1", "", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "sess-1" {
		t.Fatalf("got %q, want sess-1", id)
	}
}

// TestSessionKeyIsolation covers the per-conversation key: two users in the
// same channel hold separate active sessions, thread participants share one,
// and switching one key never disturbs another.
func TestSessionKeyIsolation(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	for _, c := range []struct{ threadID, userID, sessionID string }{
		{"", "alice", "sess-alice"},
		{"", "bob", "sess-bob"},
		{"thread-1", "", "sess-thread"},
	} {
		if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", c.threadID, c.userID, c.sessionID, 100); err != nil {
			t.Fatalf("SetActive(%q,%q): %v", c.threadID, c.userID, err)
		}
	}

	got, _, err := s.SessionRepo().Active(ctx, "telegram", "chat1", "", "alice")
	if err != nil {
		t.Fatalf("Active alice: %v", err)
	}
	if got != "sess-alice" {
		t.Fatalf("alice = %q, want sess-alice", got)
	}
	got, _, err = s.SessionRepo().Active(ctx, "telegram", "chat1", "", "bob")
	if err != nil {
		t.Fatalf("Active bob: %v", err)
	}
	if got != "sess-bob" {
		t.Fatalf("bob = %q, want sess-bob", got)
	}
	got, _, err = s.SessionRepo().Active(ctx, "telegram", "chat1", "thread-1", "")
	if err != nil {
		t.Fatalf("Active thread: %v", err)
	}
	if got != "sess-thread" {
		t.Fatalf("thread = %q, want sess-thread", got)
	}

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "alice", "sess-alice-2", 100); err != nil {
		t.Fatalf("SetActive alice second: %v", err)
	}
	got, _, err = s.SessionRepo().Active(ctx, "telegram", "chat1", "", "bob")
	if err != nil {
		t.Fatalf("Active bob after alice switch: %v", err)
	}
	if got != "sess-bob" {
		t.Fatalf("bob's session must be untouched by alice's switch, got %q", got)
	}

	activeCount := 0
	sessions, err := s.SessionRepo().List(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, sess := range sessions {
		if sess.Active {
			activeCount++
		}
	}
	if activeCount != 3 {
		t.Fatalf("expected 3 active sessions (one per conversation key), got %d", activeCount)
	}
}

// TestSessionSetActiveReKeysAdoptedRow: activating a session created under a
// different key (e.g. before key granularity) re-keys it to the current
// conversation so /occa:session switch keeps old sessions reachable.
func TestSessionSetActiveReKeysAdoptedRow(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-old", 100); err != nil {
		t.Fatalf("SetActive old: %v", err)
	}
	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "alice", "sess-old", 100); err != nil {
		t.Fatalf("SetActive adopt: %v", err)
	}

	got, _, err := s.SessionRepo().Active(ctx, "telegram", "chat1", "", "alice")
	if err != nil {
		t.Fatalf("Active adopted: %v", err)
	}
	if got != "sess-old" {
		t.Fatalf("adopted session = %q, want sess-old", got)
	}
	if _, _, err := s.SessionRepo().Active(ctx, "telegram", "chat1", "", ""); err != nil {
		t.Fatalf("Active old key: %v", err)
	}
}

// TestUniqueActiveIndexEnforced: the partial unique index rejects a second
// active row for the same conversation key.
func TestUniqueActiveIndexEnforced(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "alice", "sess-1", 100); err != nil {
		t.Fatalf("SetActive first: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO session (channel_id, platform, agent_session_id, thread_id, user_id, active, created_at, updated_at)
		 VALUES ('chat1', 'telegram', 'sess-2', '', 'alice', 1, 1, 1)`); err == nil {
		t.Fatal("expected duplicate active key rejected by the partial unique index")
	}
}

func TestSessionRestartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restart.db")
	ctx := context.Background()

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.SessionRepo().SetActive(ctx, "discord", "ch1", "", "", "sess-abc", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	id, _, err := s2.SessionRepo().Active(ctx, "discord", "ch1", "", "")
	if err != nil {
		t.Fatalf("Active after restart: %v", err)
	}
	if id != "sess-abc" {
		t.Fatalf("got %q, want sess-abc", id)
	}
}

func TestSessionSetActiveAtomic(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-1", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-2", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	id, _, err := s.SessionRepo().Active(ctx, "telegram", "chat1", "", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "sess-2" {
		t.Fatalf("got %q, want sess-2", id)
	}

	sessions, err := s.SessionRepo().List(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	activeCount := 0
	for _, sess := range sessions {
		if sess.Active {
			activeCount++
		}
	}
	if activeCount != 1 {
		t.Fatalf("expected 1 active session, got %d", activeCount)
	}
}

func TestChannelFieldUpsertsPreserveOtherSettings(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.ChannelRepo().UpsertWorkdir(ctx, "telegram", "workdir-first", "/repo"); err != nil {
		t.Fatalf("UpsertWorkdir first: %v", err)
	}
	workdirFirst, err := s.ChannelRepo().Get(ctx, "telegram", "workdir-first")
	if err != nil {
		t.Fatalf("Get workdir-first channel: %v", err)
	}
	if workdirFirst == nil || workdirFirst.Model != "" || workdirFirst.ListenMode != "mention" || workdirFirst.Workdir != "/repo" {
		t.Fatalf("unexpected workdir-first channel: %+v", workdirFirst)
	}

	ch, err := s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ch != nil {
		t.Fatal("expected nil for missing channel")
	}

	if err := s.ChannelRepo().UpsertModel(ctx, "telegram", "chat1", "gpt-4"); err != nil {
		t.Fatalf("UpsertModel: %v", err)
	}

	ch, err = s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ch == nil {
		t.Fatal("expected channel")
	}
	if ch.Model != "gpt-4" || ch.ListenMode != "mention" || ch.Workdir != "" {
		t.Fatalf("unexpected channel: %+v", ch)
	}

	if err := s.ChannelRepo().UpsertListenMode(ctx, "telegram", "chat1", "all"); err != nil {
		t.Fatalf("UpsertListenMode: %v", err)
	}
	ch, err = s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Get after listen mode: %v", err)
	}
	if ch.Model != "gpt-4" || ch.ListenMode != "all" || ch.Workdir != "" {
		t.Fatalf("listen mode update changed another field: %+v", ch)
	}

	if err := s.ChannelRepo().UpsertWorkdir(ctx, "telegram", "chat1", "/repo/api"); err != nil {
		t.Fatalf("UpsertWorkdir: %v", err)
	}
	ch, err = s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Get after workdir: %v", err)
	}
	if ch.Model != "gpt-4" || ch.ListenMode != "all" || ch.Workdir != "/repo/api" {
		t.Fatalf("workdir update changed another field: %+v", ch)
	}

	if err := s.ChannelRepo().UpsertModel(ctx, "telegram", "chat1", "claude"); err != nil {
		t.Fatalf("UpsertModel update: %v", err)
	}
	ch, err = s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Get after model update: %v", err)
	}
	if ch.Model != "claude" || ch.ListenMode != "all" || ch.Workdir != "/repo/api" {
		t.Fatalf("model update changed another field: %+v", ch)
	}
}

func TestOverrideCRUD(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	o, err := s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if o != nil {
		t.Fatal("expected nil for missing override")
	}

	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "allow"); err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}
	if err := s.OverrideRepo().UpsertModel(ctx, "telegram", "chat1", "user1", "gpt-4"); err != nil {
		t.Fatalf("UpsertModel: %v", err)
	}

	o, err = s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if o == nil || o.Role != "allow" || o.Model != "gpt-4" {
		t.Fatalf("unexpected override: %+v", o)
	}

	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "admin"); err != nil {
		t.Fatalf("UpsertRole update: %v", err)
	}

	o, err = s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if o.Role != "admin" {
		t.Fatalf("expected admin, got %q", o.Role)
	}
	if o.Model != "gpt-4" {
		t.Fatalf("expected model untouched by UpsertRole, got %q", o.Model)
	}

	list, err := s.OverrideRepo().ListByChannel(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("ListByChannel: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 override, got %d", len(list))
	}

	if err := s.OverrideRepo().Delete(ctx, "telegram", "chat1", "user1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	o, err = s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if o != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestOverrideRoleOnlyRowRoundTrips(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "allow"); err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}

	o, err := s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get on a row with no model: %v", err)
	}
	if o == nil || o.Role != "allow" || o.Model != "" {
		t.Fatalf("unexpected override: %+v", o)
	}

	list, err := s.OverrideRepo().ListByChannel(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("ListByChannel on a row with no model: %v", err)
	}
	if len(list) != 1 || list[0].Role != "allow" || list[0].Model != "" {
		t.Fatalf("unexpected list: %+v", list)
	}

	if err := s.OverrideRepo().UpsertModel(ctx, "telegram", "chat1", "user1", "openai/gpt-4o"); err != nil {
		t.Fatalf("UpsertModel: %v", err)
	}
	o, err = s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get after UpsertModel: %v", err)
	}
	if o.Model != "openai/gpt-4o" || o.Role != "allow" {
		t.Fatalf("expected model set and role preserved, got %+v", o)
	}
}

func TestOverrideUpsertDoesNotClobberOtherField(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.OverrideRepo().UpsertModel(ctx, "telegram", "chat1", "user1", "gpt-4"); err != nil {
		t.Fatalf("UpsertModel: %v", err)
	}
	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "admin"); err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}

	o, err := s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if o.Role != "admin" || o.Model != "gpt-4" {
		t.Fatalf("expected role/model to coexist without clobbering, got: %+v", o)
	}
}

func TestOverrideUpsertModelDefaultsRoleToDeny(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.OverrideRepo().UpsertModel(ctx, "telegram", "chat1", "user1", "gpt-4"); err != nil {
		t.Fatalf("UpsertModel: %v", err)
	}

	o, err := s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if o.Role != "deny" {
		t.Fatalf("expected a model-only write to default role to deny, got %q", o.Role)
	}
}

func TestOverridePlatformScoping(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.OverrideRepo().UpsertRole(ctx, "telegram", "same-id", "user1", "admin"); err != nil {
		t.Fatalf("UpsertRole telegram: %v", err)
	}

	o, err := s.OverrideRepo().Get(ctx, "discord", "same-id", "user1")
	if err != nil {
		t.Fatalf("Get discord: %v", err)
	}
	if o != nil {
		t.Fatalf("expected no cross-platform leak, got: %+v", o)
	}
}

// TestSessionThreadChannel: ThreadChannel resolves the parent channel for an
// OCCA-created thread (channel_id != thread_id) and the thread itself for a
// self-scoped conversation.
func TestSessionThreadChannel(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.SessionRepo().SetActive(ctx, "discord", "parent-1", "thread-9", "", "sess-1", 100); err != nil {
		t.Fatalf("SetActive owned thread: %v", err)
	}
	if err := s.SessionRepo().SetActive(ctx, "discord", "user-thread", "user-thread", "", "sess-2", 100); err != nil {
		t.Fatalf("SetActive user thread: %v", err)
	}

	parent, err := s.SessionRepo().ThreadChannel(ctx, "discord", "thread-9")
	if err != nil {
		t.Fatalf("ThreadChannel owned: %v", err)
	}
	if parent != "parent-1" {
		t.Fatalf("owned thread parent = %q, want parent-1", parent)
	}

	self, err := s.SessionRepo().ThreadChannel(ctx, "discord", "user-thread")
	if err != nil {
		t.Fatalf("ThreadChannel user thread: %v", err)
	}
	if self != "user-thread" {
		t.Fatalf("user thread channel = %q, want itself", self)
	}

	none, err := s.SessionRepo().ThreadChannel(ctx, "discord", "no-such-thread")
	if err != nil {
		t.Fatalf("ThreadChannel missing: %v", err)
	}
	if none != "" {
		t.Fatalf("missing thread channel = %q, want empty", none)
	}
}

func TestSessionTitle(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-1", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	sessions, err := s.SessionRepo().List(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].Title != "" {
		t.Fatalf("expected empty title by default, got %q", sessions[0].Title)
	}

	if err := s.SessionRepo().SetTitle(ctx, sessions[0].ID, "Refactor backend API"); err != nil {
		t.Fatalf("SetTitle: %v", err)
	}

	sessions, err = s.SessionRepo().List(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("List after SetTitle: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].Title != "Refactor backend API" {
		t.Fatalf("expected title %q, got %q", "Refactor backend API", sessions[0].Title)
	}
}

func TestScheduleAttribute(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sched := Schedule{
		Platform:       "",
		ChannelID:      "",
		CronExpression: "0 9 * * 1-5",
		HumanSchedule:  "weekdays 9am",
		Prompt:         "test",
		Enabled:        false,
	}
	id, err := s.ScheduleRepo().Create(ctx, &sched)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.ScheduleRepo().Attribute(ctx, id, "telegram", "chat123"); err != nil {
		t.Fatalf("Attribute: %v", err)
	}

	list, err := s.ScheduleRepo().List(ctx, "telegram", "chat123")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != id || list[0].Platform != "telegram" || list[0].ChannelID != "chat123" || !list[0].Enabled {
		t.Fatalf("unexpected attributed schedule: %+v", list)
	}

	if err := s.ScheduleRepo().Attribute(ctx, 99999, "telegram", "chat123"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing id, got: %v", err)
	}
}

func TestScheduleSweepPending(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sched1 := Schedule{CronExpression: "0 9 * * 1-5", Prompt: "stray", Enabled: false}
	sched2 := Schedule{Platform: "telegram", ChannelID: "c1", CronExpression: "0 9 * * 1-5", Prompt: "active", Enabled: true}

	_, _ = s.ScheduleRepo().Create(ctx, &sched1)
	_, _ = s.ScheduleRepo().Create(ctx, &sched2)

	n, err := s.ScheduleRepo().SweepPending(ctx)
	if err != nil {
		t.Fatalf("SweepPending: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 swept row, got %d", n)
	}

	all, _ := s.ScheduleRepo().ListAll(ctx)
	if len(all) != 1 || all[0].Prompt != "active" {
		t.Fatalf("unexpected remaining schedules after sweep: %+v", all)
	}
}
