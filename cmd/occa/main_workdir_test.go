package main

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

// newChannelStore opens a temp SQLite store and seeds two channels: aura with
// its own repo workdir, proceed without any workdir set.
func newChannelStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook-workdir.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.ChannelRepo().UpsertWorkdir(context.Background(), "discord", "aura-channel", "/home/ubuntu/projects/aura"); err != nil {
		t.Fatalf("seed aura workdir: %v", err)
	}
	if err := st.ChannelRepo().UpsertListenMode(context.Background(), "discord", "proceed-channel", "mention"); err != nil {
		t.Fatalf("seed proceed channel: %v", err)
	}
	return st
}

func TestResolveWebhookWorkdirChannelRowWins(t *testing.T) {
	st := newChannelStore(t)

	got := resolveWebhookWorkdir(context.Background(), st.ChannelRepo(), "/home/ubuntu", "discord", "aura-channel", "")

	if got != "/home/ubuntu/projects/aura" {
		t.Fatalf("resolved workdir = %q, want the channel's own repo workdir", got)
	}
}

func TestResolveWebhookWorkdirFallsBackWhenChannelRowMissing(t *testing.T) {
	st := newChannelStore(t)

	got := resolveWebhookWorkdir(context.Background(), st.ChannelRepo(), "/home/ubuntu", "discord", "unknown-channel", "")

	if got != "/home/ubuntu" {
		t.Fatalf("resolved workdir = %q, want the default workdir", got)
	}
}

func TestResolveWebhookWorkdirFallsBackOnStoreError(t *testing.T) {
	st := newChannelStore(t)
	channels := st.ChannelRepo()

	// A closed store makes the lookup fail.
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	got := resolveWebhookWorkdir(context.Background(), channels, "/home/ubuntu", "discord", "aura-channel", "")

	if got != "/home/ubuntu" {
		t.Fatalf("resolved workdir = %q, want the default workdir after store error", got)
	}
}

func TestResolveWebhookWorkdirFallsBackWhenChannelWorkdirEmpty(t *testing.T) {
	st := newChannelStore(t)

	// Capture the default slog logger so the WARN on the empty-workdir
	// fallback can be asserted.
	var logBuf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	got := resolveWebhookWorkdir(context.Background(), st.ChannelRepo(), "/home/ubuntu", "discord", "proceed-channel", "")

	if got != "/home/ubuntu" {
		t.Fatalf("resolved workdir = %q, want the default workdir", got)
	}
	if !strings.Contains(logBuf.String(), "level=WARN") ||
		!strings.Contains(logBuf.String(), "webhook: channel workdir empty; using default workdir") {
		t.Fatalf("expected WARN log for empty channel workdir, got log output: %q", logBuf.String())
	}
}

func TestResolveWebhookWorkdirLeaseWins(t *testing.T) {
	st := newChannelStore(t)

	got := resolveWebhookWorkdir(context.Background(), st.ChannelRepo(), "/home/ubuntu", "discord", "aura-channel", "/tmp/worktree-lease")

	if got != "/tmp/worktree-lease" {
		t.Fatalf("resolved workdir = %q, want the lease workdir (lease path wins)", got)
	}
}
