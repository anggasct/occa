package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryEventPutAndList(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithDefaultWorkdir(filepath.Join(dir, "test.db"), "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	repo := s.RecoveryEventRepo()
	base := time.Now().Unix()
	for i, outcome := range []RecoveryOutcome{RecoveryOutcomeResumed, RecoveryOutcomeRecreated, RecoveryOutcomeFailed} {
		ev := RecoveryEvent{
			Platform:      "telegram",
			ChannelID:     "chat1",
			ThreadID:      "",
			UserID:        "user1",
			Workdir:       "/repo",
			Trigger:       RecoveryTriggerProcessExit,
			Outcome:       outcome,
			CorrelationID: "rcv-test",
			Detail:        "detail",
			CreatedAt:     base + int64(i),
		}
		if err := repo.Put(context.Background(), ev); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	events, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	if events[0].Outcome != RecoveryOutcomeFailed {
		t.Errorf("newest event outcome = %q, want failed (newest first)", events[0].Outcome)
	}
	if events[2].Outcome != RecoveryOutcomeResumed || events[2].CorrelationID != "rcv-test" || events[2].Trigger != RecoveryTriggerProcessExit {
		t.Errorf("oldest event = %+v, want resumed rcv-test process_exit", events[2])
	}
}

func TestRecoveryEventPrunesOldRows(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithDefaultWorkdir(filepath.Join(dir, "test.db"), "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	repo := s.RecoveryEventRepo()
	old := RecoveryEvent{
		Platform: "telegram", ChannelID: "c", Workdir: "/w",
		Trigger: RecoveryTriggerProcessExit, Outcome: RecoveryOutcomeFailed,
		CreatedAt: time.Now().Add(-recoveryEventRetention - time.Hour).Unix(),
	}
	if err := repo.Put(context.Background(), old); err != nil {
		t.Fatalf("put old: %v", err)
	}
	fresh := RecoveryEvent{
		Platform: "telegram", ChannelID: "c", Workdir: "/w",
		Trigger: RecoveryTriggerSendTimeout, Outcome: RecoveryOutcomeResumed,
	}
	if err := repo.Put(context.Background(), fresh); err != nil {
		t.Fatalf("put fresh: %v", err)
	}

	events, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != 1 || events[0].Trigger != RecoveryTriggerSendTimeout {
		t.Fatalf("events = %+v, want only the fresh row", events)
	}
}

func TestRecoveryEventSchemaMigrates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	version, err := s.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", version, SchemaVersion)
	}
	_ = s.Close()

	s2, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if _, err := s2.RecoveryEventRepo().List(context.Background(), 5); err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
}
