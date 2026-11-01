package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SessionSkill is one skill injected into one agent session. A row's
// presence is what forbids injecting into that session again.
type SessionSkill struct {
	AgentSessionID string
	SkillName      string
	Scope          string
	TriggerKind    string
	SkillDigest    string
	Runes          int
	AppliedAt      int64
}

type SessionSkillRepo interface {
	List(ctx context.Context, agentSessionID string) ([]SessionSkill, error)
	Record(ctx context.Context, agentSessionID string, rows []SessionSkill) error
}

type sqliteSessionSkillRepo struct {
	db *sql.DB
}

func addSessionSkill(_ *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS session_skill (
	agent_session_id TEXT    NOT NULL,
	skill_name       TEXT    NOT NULL,
	scope            TEXT    NOT NULL CHECK (scope IN ('repo','global')),
	trigger_kind     TEXT    NOT NULL CHECK (trigger_kind IN ('always','keyword','paths')),
	skill_digest     TEXT    NOT NULL,
	runes            INTEGER NOT NULL,
	applied_at       INTEGER NOT NULL,
	PRIMARY KEY (agent_session_id, skill_name)
);
CREATE INDEX IF NOT EXISTS idx_session_skill_applied ON session_skill(applied_at);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate session skill: %w", err)
	}
	return nil
}

func (r *sqliteSessionSkillRepo) List(ctx context.Context, agentSessionID string) ([]SessionSkill, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT agent_session_id, skill_name, scope, trigger_kind, skill_digest, runes, applied_at
		 FROM session_skill WHERE agent_session_id = ? ORDER BY skill_name`, agentSessionID)
	if err != nil {
		return nil, fmt.Errorf("store: session skill list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SessionSkill
	for rows.Next() {
		var s SessionSkill
		if err := rows.Scan(&s.AgentSessionID, &s.SkillName, &s.Scope, &s.TriggerKind, &s.SkillDigest, &s.Runes, &s.AppliedAt); err != nil {
			return nil, fmt.Errorf("store: session skill scan: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: session skill rows: %w", err)
	}
	return out, nil
}

// Record writes the injected set of one session in one transaction. A row
// that already exists is kept as it was: the first injection is the record.
func (r *sqliteSessionSkillRepo) Record(ctx context.Context, agentSessionID string, rows []SessionSkill) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: session skill begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()
	for _, s := range rows {
		at := s.AppliedAt
		if at == 0 {
			at = now
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO session_skill (agent_session_id, skill_name, scope, trigger_kind, skill_digest, runes, applied_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (agent_session_id, skill_name) DO NOTHING`,
			agentSessionID, s.SkillName, s.Scope, s.TriggerKind, s.SkillDigest, s.Runes, at,
		); err != nil {
			return fmt.Errorf("store: session skill insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: session skill commit: %w", err)
	}
	return nil
}
