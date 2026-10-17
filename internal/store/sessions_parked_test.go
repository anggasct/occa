package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSessionParkKeepsRowListed(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if version, err := s.SchemaVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("schema version = %d, %v; want %d", version, err, SchemaVersion)
	}

	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-parked", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "user1", "idle"); err != nil {
		t.Fatalf("MarkParked: %v", err)
	}

	id, _, err := repo.Active(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "ses-parked" {
		t.Fatalf("parked session must stay active, got %q", id)
	}

	listed, err := repo.ListConversation(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ListConversation: %v", err)
	}
	if len(listed) != 1 || listed[0].AgentSessionID != "ses-parked" {
		t.Fatalf("parked session must stay listed, got %+v", listed)
	}
	if !listed[0].Active || !listed[0].Parked {
		t.Fatalf("parked row must be active+parked, got %+v", listed[0])
	}
	if listed[0].ParkReason != "idle" || listed[0].ParkedAt <= 0 {
		t.Fatalf("parked row must carry reason and timestamp, got %+v", listed[0])
	}

	parked, reason, err := repo.ParkState(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if !parked || reason != "idle" {
		t.Fatalf("ParkState = (%v, %q), want (true, idle)", parked, reason)
	}
}

func TestSessionParkRequiresActiveRow(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "ghost", "idle"); err != ErrNotFound {
		t.Fatalf("MarkParked without active row = %v, want ErrNotFound", err)
	}
}

func TestSessionResumeClearsPark(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-parked", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "user1", "permission"); err != nil {
		t.Fatalf("MarkParked: %v", err)
	}
	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-parked", 100); err != nil {
		t.Fatalf("resume SetActive: %v", err)
	}

	parked, _, err := repo.ParkState(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if parked {
		t.Fatal("resuming the parked session must clear the park marker")
	}
}

func TestSessionFreshRowStartsUnparked(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-fresh", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	parked, _, err := repo.ParkState(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if parked {
		t.Fatal("fresh session must not be parked")
	}
}

func TestParkedColumnsRequiredAtV18(t *testing.T) {
	has := func(cols []string, want string) bool {
		for _, c := range cols {
			if c == want {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"parked", "parked_at", "park_reason"} {
		if !has(requiredColumnsForVersion(SchemaVersion)["session"], want) {
			t.Fatalf("v%d session reference missing column %s", SchemaVersion, want)
		}
		if has(requiredColumnsForVersion(17)["session"], want) {
			t.Fatalf("pre-v18 session reference must not require column %s", want)
		}
	}

	path := filepath.Join(t.TempDir(), "v18.db")
	s, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if version, err := validateDatabase(path); err != nil || version != SchemaVersion {
		t.Fatalf("validateDatabase = (%d, %v), want (%d, nil)", version, err, SchemaVersion)
	}
}
