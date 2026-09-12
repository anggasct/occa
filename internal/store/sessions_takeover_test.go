package store

import (
	"context"
	"testing"
)

func TestSessionTakeoverMarkAndCandidate(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if version, err := s.SchemaVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("schema version = %d, %v; want %d", version, err, SchemaVersion)
	}

	if err := repo.MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-sess", 4242, "seed-summary"); err != nil {
		t.Fatalf("MarkTakeoverEligible: %v", err)
	}
	candidate, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate == nil || candidate.SessionID != "failed-sess" || candidate.AgentPID != 4242 || candidate.Seed != "seed-summary" {
		t.Fatalf("candidate = %+v, want failed-sess/4242/seed-summary", candidate)
	}

	id, _, err := repo.Active(ctx, "discord", "chan-1", "thread-1", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "failed-sess" {
		t.Fatalf("direct lookup = %q, want failed-sess", id)
	}
}

func TestSessionTakeoverSkipsOperatorOwnedRow(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.SetActive(ctx, "discord", "chan-1", "thread-1", "", "operator-sess", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := repo.MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-sess", 200, "seed"); err != nil {
		t.Fatalf("MarkTakeoverEligible: %v", err)
	}

	candidate, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate != nil {
		t.Fatalf("operator-owned row must not become a candidate: %+v", candidate)
	}
	id, _, err := repo.Active(ctx, "discord", "chan-1", "thread-1", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "operator-sess" {
		t.Fatalf("operator row disturbed: %q", id)
	}
}

func TestSessionTakeoverReplaceOnRepeatFailure(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-attempt-1", 100, "seed-1"); err != nil {
		t.Fatalf("mark 1: %v", err)
	}
	if err := repo.MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-attempt-2", 200, "seed-2"); err != nil {
		t.Fatalf("mark 2: %v", err)
	}

	candidate, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate == nil || candidate.SessionID != "failed-attempt-2" || candidate.AgentPID != 200 || candidate.Seed != "seed-2" {
		t.Fatalf("candidate = %+v, want latest failed attempt", candidate)
	}
}

func TestSessionTakeoverClear(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-sess", 100, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := repo.ClearTakeoverEligible(ctx, "discord", "chan-1", "thread-1"); err != nil {
		t.Fatalf("clear: %v", err)
	}

	candidate, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate != nil {
		t.Fatalf("cleared row still a candidate: %+v", candidate)
	}
	id, _, err := repo.Active(ctx, "discord", "chan-1", "thread-1", "")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if id != "failed-sess" {
		t.Fatalf("cleared row should stay directly resolvable, got %q", id)
	}
}

func TestSessionTakeoverNoCandidateWhenUnmarked(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()

	if err := repo.SetActive(ctx, "discord", "chan-1", "thread-1", "", "own-sess", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	candidate, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate != nil {
		t.Fatalf("unmarked row must not be a candidate: %+v", candidate)
	}

	other, err := repo.TakeoverCandidate(ctx, "discord", "chan-1", "thread-2")
	if err != nil {
		t.Fatalf("TakeoverCandidate sibling: %v", err)
	}
	if other != nil {
		t.Fatalf("sibling thread must not see the mark: %+v", other)
	}
}
