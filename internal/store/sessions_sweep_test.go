package store

import (
	"context"
	"testing"
)

func TestSweepActiveClearsDeadSessionsAndKeepsLiveOnes(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()
	const alivePID = 4242
	const deadPID = 999999
	if err := repo.SetActive(ctx, "telegram", "chat-1", "thread-live", "user-live", "ses-live", alivePID); err != nil {
		t.Fatalf("set live session: %v", err)
	}
	if err := repo.SetActive(ctx, "telegram", "chat-1", "thread-dead", "user-dead", "ses-dead", deadPID); err != nil {
		t.Fatalf("set dead session: %v", err)
	}

	cleared, err := repo.SweepActive(ctx, func(pid int) bool { return pid == alivePID })
	if err != nil {
		t.Fatalf("sweep active: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("cleared = %d, want 1", cleared)
	}
	sessionID, _, err := repo.Active(ctx, "telegram", "chat-1", "thread-live", "user-live")
	if err != nil || sessionID != "ses-live" {
		t.Fatalf("live session = %q, %v, want ses-live", sessionID, err)
	}
	sessionID, _, err = repo.Active(ctx, "telegram", "chat-1", "thread-dead", "user-dead")
	if err != nil || sessionID != "" {
		t.Fatalf("dead session = %q, %v, want cleared", sessionID, err)
	}
}

func TestSweepActiveClearsZeroPID(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.SessionRepo()
	if err := repo.SetActive(ctx, "telegram", "chat-1", "thread-zero", "user-zero", "ses-zero", 0); err != nil {
		t.Fatalf("set zero-pid session: %v", err)
	}
	cleared, err := repo.SweepActive(ctx, func(int) bool { return true })
	if err != nil {
		t.Fatalf("sweep active: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("cleared = %d, want 1", cleared)
	}
}
