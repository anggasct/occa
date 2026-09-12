package store

import (
	"context"
	"path/filepath"
	"testing"
)

func dropSessionRootCardColumns(t *testing.T, s *SQLiteStore) {
	t.Helper()
	for _, col := range []string{"root_message_id", "root_channel", "root_card"} {
		if _, err := s.db.Exec("ALTER TABLE session DROP COLUMN " + col); err != nil {
			t.Fatalf("drop %s: %v", col, err)
		}
	}
}

func TestThreadRootLinkAndLookup(t *testing.T) {
	st, err := OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-sess", 100, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := st.SessionRepo().LinkThreadRoot(ctx, "discord", "chan-1", "thread-1", "root-1", "chan-1", "card v1"); err != nil {
		t.Fatalf("link: %v", err)
	}

	root, err := st.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if root == nil || root.MessageID != "root-1" || root.Channel != "chan-1" || root.Card != "card v1" {
		t.Fatalf("root = %+v, want root-1/chan-1/card v1", root)
	}

	if err := st.SessionRepo().ClearTakeoverEligible(ctx, "discord", "chan-1", "thread-1"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	root, err = st.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("lookup after clear: %v", err)
	}
	if root == nil || root.Card != "card v1" {
		t.Fatalf("root must survive takeover clearing, got %+v", root)
	}

	if err := st.SessionRepo().LinkThreadRoot(ctx, "discord", "chan-1", "thread-1", "root-2", "chan-1", "card v2"); err != nil {
		t.Fatalf("relink: %v", err)
	}
	root, err = st.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("lookup after relink: %v", err)
	}
	if root == nil || root.MessageID != "root-2" || root.Card != "card v2" {
		t.Fatalf("root must track the latest link, got %+v", root)
	}

	if root, _ := st.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-2"); root != nil {
		t.Fatalf("unlinked thread resolved a root: %+v", root)
	}
}

func TestThreadRootLinkWithoutPriorMarkInsertsRow(t *testing.T) {
	st, err := OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	if err := st.SessionRepo().LinkThreadRoot(ctx, "telegram", "chat-1", "topic-9", "root-9", "chat-1:topic-9", "card"); err != nil {
		t.Fatalf("link without mark: %v", err)
	}
	root, err := st.SessionRepo().ThreadRoot(ctx, "telegram", "chat-1", "topic-9")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if root == nil || root.MessageID != "root-9" || root.Channel != "chat-1:topic-9" {
		t.Fatalf("root = %+v, want root-9/chat-1:topic-9", root)
	}
}

func TestThreadRootSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook.db")
	st, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "sess", 1, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := st.SessionRepo().LinkThreadRoot(ctx, "discord", "chan-1", "thread-1", "root-1", "chan-1", "card"); err != nil {
		t.Fatalf("link: %v", err)
	}
	_ = st.Close()

	st2, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st2.Close() }()

	root, err := st2.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("lookup after restart: %v", err)
	}
	if root == nil || root.MessageID != "root-1" || root.Card != "card" {
		t.Fatalf("root must persist across restart, got %+v", root)
	}
}
