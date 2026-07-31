package store

import (
	"context"
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

	id, err := s.SessionRepo().Active(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "" {
		t.Fatalf("expected empty, got %q", id)
	}

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "sess-1"); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	id, err = s.SessionRepo().Active(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "sess-1" {
		t.Fatalf("got %q, want sess-1", id)
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
	if err := s1.SessionRepo().SetActive(ctx, "discord", "ch1", "sess-abc"); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	id, err := s2.SessionRepo().Active(ctx, "discord", "ch1")
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

	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "sess-1"); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := s.SessionRepo().SetActive(ctx, "telegram", "chat1", "sess-2"); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	id, err := s.SessionRepo().Active(ctx, "telegram", "chat1")
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
