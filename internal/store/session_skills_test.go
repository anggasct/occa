package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionSkillRecordAndList(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionSkillRepo()
	rows := []SessionSkill{
		{SkillName: "store-contract", Scope: "repo", TriggerKind: "paths", SkillDigest: "d1", Runes: 40},
		{SkillName: "always-on", Scope: "global", TriggerKind: "always", SkillDigest: "d2", Runes: 10, AppliedAt: 7},
	}
	if err := repo.Record(ctx, "sess-1", rows); err != nil {
		t.Fatalf("Record: %v", err)
	}
	// A second record for the same session keeps the first injection.
	if err := repo.Record(ctx, "sess-1", []SessionSkill{{SkillName: "always-on", Scope: "repo", TriggerKind: "always", SkillDigest: "other", Runes: 1}}); err != nil {
		t.Fatalf("Record again: %v", err)
	}
	got, err := repo.List(ctx, "sess-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].SkillName != "always-on" || got[0].SkillDigest != "d2" || got[0].AppliedAt != 7 ||
		got[1].SkillName != "store-contract" || got[1].AgentSessionID != "sess-1" || got[1].AppliedAt == 0 {
		t.Fatalf("List = %+v", got)
	}
	if other, err := repo.List(ctx, "sess-2"); err != nil || len(other) != 0 {
		t.Fatalf("List other = %+v, %v", other, err)
	}
	if err := repo.Record(ctx, "sess-3", []SessionSkill{{SkillName: "x", Scope: "elsewhere", TriggerKind: "always", SkillDigest: "d", Runes: 1}}); err == nil {
		t.Fatal("Record accepted a scope outside the CHECK constraint")
	}
}

func TestSessionSkillPrunedWithSession(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	if err := s.SessionRepo().SetActive(ctx, "discord", "chan", "", "u1", "sess-1", 1); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := s.SessionSkillRepo().Record(ctx, "sess-1", []SessionSkill{{SkillName: "a", Scope: "repo", TriggerKind: "always", SkillDigest: "d", Runes: 1}}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	sessions, err := s.SessionRepo().ListConversation(ctx, "discord", "chan", "", "u1")
	if err != nil || len(sessions) != 1 {
		t.Fatalf("ListConversation = %+v, %v", sessions, err)
	}
	if err := s.SessionRepo().Delete(ctx, sessions[0].ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got, err := s.SessionSkillRepo().List(ctx, "sess-1"); err != nil || len(got) != 0 {
		t.Fatalf("rows after session delete = %+v, %v", got, err)
	}
}

// v25 applied over an existing v24 database is additive and idempotent.
func TestSessionSkillMigrationIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	open := func() *SQLiteStore {
		s, err := OpenWithRetention(path, "", UsageRetention{Retention: time.Hour, MaxRows: 10}, time.Hour)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return s
	}
	s := open()
	if err := s.SessionSkillRepo().Record(context.Background(), "sess-1", []SessionSkill{{SkillName: "a", Scope: "repo", TriggerKind: "always", SkillDigest: "d", Runes: 1}}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if _, err := s.db.Exec("PRAGMA user_version=24"); err != nil {
		t.Fatalf("rewind version: %v", err)
	}
	_ = s.Close()
	s = open()
	defer func() { _ = s.Close() }()
	assertVersion(t, s, SchemaVersion)
	if got, err := s.SessionSkillRepo().List(context.Background(), "sess-1"); err != nil || len(got) != 1 {
		t.Fatalf("rows after re-migration = %+v, %v", got, err)
	}
}
