package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db       *sql.DB
	sessions *sqliteSessionRepo
	channels *sqliteChannelRepo
	overrides *sqliteOverrideRepo
}

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: wal mode: %w", err)
	}

	s := &SQLiteStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}

	s.sessions = &sqliteSessionRepo{db: db}
	s.channels = &sqliteChannelRepo{db: db}
	s.overrides = &sqliteOverrideRepo{db: db}
	return s, nil
}

func (s *SQLiteStore) migrate() error {
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
`
	if _, err := s.db.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

func (s *SQLiteStore) SessionRepo() SessionRepo   { return s.sessions }
func (s *SQLiteStore) ChannelRepo() ChannelRepo    { return s.channels }
func (s *SQLiteStore) OverrideRepo() OverrideRepo  { return s.overrides }

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) DB() *sql.DB { return s.db }

var _ Store = (*SQLiteStore)(nil)
