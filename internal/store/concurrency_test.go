package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentWritersAllSucceed(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	const writers = 16
	const perWriter = 10

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter*3)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				platform := "telegram"
				channelID := fmt.Sprintf("chat-%d", w)
				userID := fmt.Sprintf("user-%d", i)

				err := s.SessionRepo().SetActive(ctx, platform, channelID, "", "", "agent-sess", 100)
				errs <- err
				err = s.ChannelRepo().UpsertModel(ctx, platform, channelID, "gpt-4o")
				errs <- err
				err = s.OverrideRepo().UpsertRole(ctx, platform, channelID, userID, "member")
				errs <- err
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}
}

func TestBusyTimeoutConfigured(t *testing.T) {
	s := tempStore(t)

	var ms int
	if err := s.db.QueryRow("PRAGMA busy_timeout").Scan(&ms); err != nil {
		t.Fatalf("busy_timeout: %v", err)
	}
	if ms <= 0 {
		t.Fatalf("busy_timeout = %d, want > 0", ms)
	}
}

func TestPoolExplicitlyConfigured(t *testing.T) {
	s := tempStore(t)

	stats := s.db.Stats()
	if stats.MaxOpenConnections != 1 {
		t.Fatalf("max open conns = %d, want 1", stats.MaxOpenConnections)
	}
}

func TestSchemaVersionAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	assertVersion(t, s, schemaVersion)
	_ = s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	assertVersion(t, s2, schemaVersion)
}

func TestAdoptsUnversionedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")

	legacy := `
CREATE TABLE session (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	agent_session_id TEXT NOT NULL,
	active INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE TABLE channel (
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	model TEXT,
	listen_mode TEXT NOT NULL DEFAULT 'mention',
	workdir TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY (channel_id, platform)
);
CREATE TABLE user_override (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	user_id TEXT NOT NULL,
	role TEXT NOT NULL,
	model TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	UNIQUE (platform, channel_id, user_id)
);
CREATE TABLE schedule (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	cron_expression TEXT NOT NULL,
	human_schedule TEXT,
	prompt TEXT NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
INSERT INTO session (channel_id, platform, agent_session_id, active, created_at, updated_at)
VALUES ('chat-legacy', 'telegram', 'agent-sess-1', 1, 1, 1);
`
	legacyDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("legacy open: %v", err)
	}
	if _, err := legacyDB.Exec(legacy); err != nil {
		t.Fatalf("legacy ddl: %v", err)
	}
	_ = legacyDB.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("adopt open: %v", err)
	}
	defer func() { _ = s.Close() }()
	assertVersion(t, s, schemaVersion)

	id, _, err := s.SessionRepo().Active(context.Background(), "telegram", "chat-legacy", "", "")
	if err != nil {
		t.Fatalf("adopted row lookup: %v", err)
	}
	if id != "agent-sess-1" {
		t.Fatalf("adopted session = %q, want agent-sess-1", id)
	}
}

func assertVersion(t *testing.T, s *SQLiteStore, want int) {
	t.Helper()
	var got int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&got); err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if got != want {
		t.Fatalf("user_version = %d, want %d", got, want)
	}
}

func TestQueriesUseIndexes(t *testing.T) {
	s := tempStore(t)

	statements := []struct {
		name string
		sql  string
		args []any
	}{
		{"session active", `SELECT agent_session_id FROM session WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`, []any{"telegram", "chat1", "", "user1"}},
		{"session list", `SELECT id, channel_id, platform, agent_session_id, thread_id, user_id, title, active, created_at, updated_at FROM session WHERE platform = ? AND channel_id = ? ORDER BY created_at DESC`, []any{"telegram", "chat1"}},
		{"session deactivate", `UPDATE session SET active = 0, updated_at = ? WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`, []any{int64(1), "telegram", "chat1", "", "user1"}},
		{"channel get", `SELECT channel_id, platform, model, listen_mode, workdir, auto_thread, created_at, updated_at FROM channel WHERE platform = ? AND channel_id = ?`, []any{"telegram", "chat1"}},
		{"override get", `SELECT id, channel_id, platform, user_id, role, model, created_at, updated_at FROM user_override WHERE platform = ? AND channel_id = ? AND user_id = ?`, []any{"telegram", "chat1", "user1"}},
		{"override list", `SELECT id, channel_id, platform, user_id, role, model, created_at, updated_at FROM user_override WHERE platform = ? AND channel_id = ? ORDER BY created_at`, []any{"telegram", "chat1"}},
		{"schedule list", `SELECT id, channel_id, platform, cron_expression, human_schedule, prompt, enabled, created_at, updated_at FROM schedule WHERE platform = ? AND channel_id = ? AND enabled = 1 ORDER BY id`, []any{"telegram", "chat1"}},
		{"schedule all", `SELECT id, channel_id, platform, cron_expression, human_schedule, prompt, enabled, created_at, updated_at FROM schedule WHERE enabled = 1 ORDER BY id`, nil},
	}

	for _, st := range statements {
		rows, err := s.db.Query("EXPLAIN QUERY PLAN "+st.sql, st.args...)
		if err != nil {
			t.Fatalf("%s: explain: %v", st.name, err)
		}
		defer func() { _ = rows.Close() }()
		var plan strings.Builder
		for rows.Next() {
			var id, parent int
			var notused, detail string
			if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
				_ = rows.Close()
				t.Fatalf("%s: scan plan: %v", st.name, err)
			}
			plan.WriteString(detail)
		}
		_ = rows.Close()
		if strings.Contains(plan.String(), "SCAN") {
			t.Fatalf("%s: query plan scans the table: %s", st.name, plan.String())
		}
	}
}
