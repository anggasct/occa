package store

import (
	"database/sql"
	"fmt"
	"log/slog"

	_ "modernc.org/sqlite"
)

const (
	schemaVersion    = 1
	busyTimeoutMilli = 5000
)

// migrations run in order inside a transaction that stamps user_version, so
// a step cannot be applied twice or out of order.
var migrations = []func(tx *sql.Tx) error{
	createInitialSchema,
}

func createInitialSchema(tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS session (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	agent_session_id TEXT NOT NULL,
	active INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS channel (
	channel_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	model TEXT,
	listen_mode TEXT NOT NULL DEFAULT 'mention',
	workdir TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY (channel_id, platform)
);

CREATE TABLE IF NOT EXISTS user_override (
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

CREATE TABLE IF NOT EXISTS schedule (
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
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}

	indexes := `
CREATE INDEX IF NOT EXISTS idx_session_lookup ON session (platform, channel_id, active);
CREATE INDEX IF NOT EXISTS idx_schedule_lookup ON schedule (platform, channel_id);
CREATE INDEX IF NOT EXISTS idx_schedule_enabled ON schedule (enabled);
`
	if _, err := tx.Exec(indexes); err != nil {
		return fmt.Errorf("store: index: %w", err)
	}
	return nil
}

type SQLiteStore struct {
	db        *sql.DB
	sessions  *sqliteSessionRepo
	channels  *sqliteChannelRepo
	overrides *sqliteOverrideRepo
	schedules *sqliteScheduleRepo
}

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: wal mode: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMilli)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: foreign keys: %w", err)
	}

	// SQLite allows a single writer; one connection removes lock contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &SQLiteStore{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}

	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		slog.Warn("store: read schema version", "error", err)
	}
	slog.Info("store opened", "path", path, "schema_version", version)

	s.sessions = &sqliteSessionRepo{db: db}
	s.channels = &sqliteChannelRepo{db: db}
	s.overrides = &sqliteOverrideRepo{db: db}
	s.schedules = &sqliteScheduleRepo{db: db}
	return s, nil
}

func (s *SQLiteStore) migrate() error {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	for i := version; i < len(migrations); i++ {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("store: begin migration %d: %w", i+1, err)
		}
		if err := migrations[i](tx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("store: migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", i+1)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("store: stamp version %d: %w", i+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: commit migration %d: %w", i+1, err)
		}
	}
	return nil
}

func (s *SQLiteStore) SessionRepo() SessionRepo   { return s.sessions }
func (s *SQLiteStore) ChannelRepo() ChannelRepo   { return s.channels }
func (s *SQLiteStore) OverrideRepo() OverrideRepo { return s.overrides }
func (s *SQLiteStore) ScheduleRepo() ScheduleRepo { return s.schedules }

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) DB() *sql.DB { return s.db }

var _ Store = (*SQLiteStore)(nil)
