package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestAgentDefaultRoundTrip(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if err := s.ChannelRepo().UpsertAgent(ctx, "telegram", "chat1", "reviewer"); err != nil {
		t.Fatalf("channel upsert agent: %v", err)
	}
	ch, err := s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("channel get: %v", err)
	}
	if ch == nil || ch.Agent != "reviewer" {
		t.Fatalf("channel agent = %+v, want reviewer", ch)
	}

	if err := s.OverrideRepo().UpsertAgent(ctx, "telegram", "chat1", "user1", "planner"); err != nil {
		t.Fatalf("override upsert agent: %v", err)
	}
	o, err := s.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("override get: %v", err)
	}
	if o == nil || o.Agent != "planner" {
		t.Fatalf("personal agent = %+v, want planner", o)
	}
	if o.Model != "" {
		t.Fatalf("agent upsert must not touch model, got %q", o.Model)
	}

	if err := s.ChannelRepo().UpsertAgent(ctx, "telegram", "chat1", ""); err != nil {
		t.Fatalf("channel clear agent: %v", err)
	}
	ch, err = s.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("channel get after clear: %v", err)
	}
	if ch == nil || ch.Agent != "" {
		t.Fatalf("channel agent after clear = %+v, want empty", ch)
	}

	listed, err := s.OverrideRepo().ListByChannel(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 || listed[0].Agent != "planner" {
		t.Fatalf("listed overrides = %+v, want one planner row", listed)
	}
}

func TestAgentMigrationFromV12(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "upgrade.db")
	ctx := context.Background()

	s1, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s1.db.Exec("ALTER TABLE channel DROP COLUMN agent"); err != nil {
		_ = s1.Close()
		t.Fatalf("drop channel agent: %v", err)
	}
	if _, err := s1.db.Exec("ALTER TABLE user_override DROP COLUMN agent"); err != nil {
		_ = s1.Close()
		t.Fatalf("drop override agent: %v", err)
	}
	if _, err := s1.db.Exec("DROP INDEX IF EXISTS idx_session_takeover"); err != nil {
		_ = s1.Close()
		t.Fatalf("drop takeover index: %v", err)
	}
	if _, err := s1.db.Exec("ALTER TABLE session DROP COLUMN continuable"); err != nil {
		_ = s1.Close()
		t.Fatalf("drop continuable: %v", err)
	}
	if _, err := s1.db.Exec("ALTER TABLE session DROP COLUMN takeover_seed"); err != nil {
		_ = s1.Close()
		t.Fatalf("drop takeover_seed: %v", err)
	}
	dropSessionRootCardColumns(t, s1)
	if _, err := s1.db.Exec("PRAGMA user_version=12"); err != nil {
		_ = s1.Close()
		t.Fatalf("stamp user_version=12: %v", err)
	}
	_ = s1.Close()

	s2, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	var version int
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", version, SchemaVersion)
	}

	if err := s2.ChannelRepo().UpsertAgent(ctx, "telegram", "chat1", "reviewer"); err != nil {
		t.Fatalf("channel upsert after migration: %v", err)
	}
	ch, err := s2.ChannelRepo().Get(ctx, "telegram", "chat1")
	if err != nil {
		t.Fatalf("channel get after migration: %v", err)
	}
	if ch == nil || ch.Agent != "reviewer" {
		t.Fatalf("channel agent after migration = %+v, want reviewer", ch)
	}
	if err := s2.OverrideRepo().UpsertAgent(ctx, "telegram", "chat1", "user1", "planner"); err != nil {
		t.Fatalf("override upsert after migration: %v", err)
	}
	o, err := s2.OverrideRepo().Get(ctx, "telegram", "chat1", "user1")
	if err != nil {
		t.Fatalf("override get after migration: %v", err)
	}
	if o == nil || o.Agent != "planner" {
		t.Fatalf("personal agent after migration = %+v, want planner", o)
	}
}
