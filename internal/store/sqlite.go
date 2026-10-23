package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	schemaVersion    = 21
	SchemaVersion    = schemaVersion
	busyTimeoutMilli = 5000
)

func sqliteDSN(path string) string {
	if path == ":memory:" || strings.Contains(path, "_txlock=") {
		return path
	}
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + "_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
}

var migrations = []func(s *SQLiteStore, tx *sql.Tx) error{
	createInitialSchema,
	addConversationKeys,
	addSessionAgentPID,
	addSessionTitle,
	addProgressNotices,
	addSessionModel,
	addThreadConfig,
	addPermissionRules,
	ensureUsageAndWebhookTables,
	ensureUsageAndWebhookTables,
	cleanupLegacyPermissionRules,
	addRecoveryEvents,
	addAgentDefaults,
	addSessionTakeover,
	addSessionRootCard,
	addWebhookReviewKey,
	dropOverrideRoleColumn,
	addSessionParked,
	addPRQueue,
	addPRQueueRetryAt,
	addWebhookReplay,
}

func addWebhookReplay(_ *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS webhook_replay (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	endpoint         TEXT NOT NULL,
	delivery_id      TEXT NOT NULL,
	event_type       TEXT NOT NULL,
	pr_key           TEXT NOT NULL DEFAULT '',
	body             BLOB NOT NULL,
	reason           TEXT NOT NULL,
	attempts         INTEGER NOT NULL DEFAULT 0,
	next_attempt_at  INTEGER NOT NULL,
	expires_at       INTEGER NOT NULL,
	status           TEXT NOT NULL DEFAULT 'pending',
	created_at       INTEGER NOT NULL,
	updated_at       INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_replay_key ON webhook_replay (endpoint, delivery_id);
CREATE INDEX IF NOT EXISTS idx_webhook_replay_due ON webhook_replay (status, next_attempt_at);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate webhook replay: %w", err)
	}
	return nil
}

func addPRQueueRetryAt(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE pr_queue ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0;`); err != nil {
		return fmt.Errorf("store: migrate pr queue retry at: %w", err)
	}
	return nil
}

func addPRQueue(_ *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS pr_queue (
	pr_key         TEXT PRIMARY KEY,
	thread_id      TEXT NOT NULL,
	root_msg_id    TEXT NOT NULL,
	status         TEXT NOT NULL DEFAULT 'active',
	active_kind    TEXT NOT NULL DEFAULT '',
	active_session TEXT NOT NULL DEFAULT '',
	active_delivery INTEGER NOT NULL DEFAULT 0,
	active_since   INTEGER NOT NULL DEFAULT 0,
	retry_count    INTEGER NOT NULL DEFAULT 0,
	blocked_reason TEXT NOT NULL DEFAULT '',
	created_at     INTEGER NOT NULL,
	updated_at     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pr_queue_items (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	pr_key      TEXT NOT NULL,
	endpoint    TEXT NOT NULL,
	event_type  TEXT NOT NULL DEFAULT '',
	delivery_id INTEGER NOT NULL DEFAULT 0,
	priority    INTEGER NOT NULL DEFAULT 4,
	enqueued_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pr_items ON pr_queue_items (pr_key, priority, id);
ALTER TABLE webhook_delivery ADD COLUMN pr_key TEXT NOT NULL DEFAULT '';
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate pr queue: %w", err)
	}
	return nil
}

func addSessionParked(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN parked INTEGER NOT NULL DEFAULT 0;`); err != nil {
		return fmt.Errorf("store: migrate session parked: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN parked_at INTEGER NOT NULL DEFAULT 0;`); err != nil {
		return fmt.Errorf("store: migrate session parked at: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN park_reason TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate session park reason: %w", err)
	}
	return nil
}

func dropOverrideRoleColumn(_ *SQLiteStore, tx *sql.Tx) error {
	var hasRole bool
	rows, err := tx.Query(`PRAGMA table_info(user_override)`)
	if err != nil {
		return fmt.Errorf("store: inspect override columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("store: inspect override columns: %w", err)
		}
		if name == "role" {
			hasRole = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: inspect override columns: %w", err)
	}
	if !hasRole {
		return nil
	}
	var grants int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM user_override`).Scan(&grants); err != nil {
		return fmt.Errorf("store: count legacy grants: %w", err)
	}
	slog.Warn("store: legacy grant rows no longer grant access; move these sender IDs into the config allowlist", "rows", grants)
	if _, err := tx.Exec(`ALTER TABLE user_override DROP COLUMN role;`); err != nil {
		return fmt.Errorf("store: drop override role: %w", err)
	}
	return nil
}

func addWebhookReviewKey(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE webhook_delivery ADD COLUMN review_key TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate webhook review key: %w", err)
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_webhook_delivery_review ON webhook_delivery (endpoint, review_key, status);`); err != nil {
		return fmt.Errorf("store: migrate webhook review key index: %w", err)
	}
	return nil
}

func addSessionRootCard(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN root_message_id TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate session root message: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN root_channel TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate session root channel: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN root_card TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate session root card: %w", err)
	}
	return nil
}

func addSessionTakeover(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN continuable INTEGER NOT NULL DEFAULT 0;`); err != nil {
		return fmt.Errorf("store: migrate session takeover: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE session ADD COLUMN takeover_seed TEXT NOT NULL DEFAULT '';`); err != nil {
		return fmt.Errorf("store: migrate session takeover seed: %w", err)
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_session_takeover ON session (platform, channel_id, thread_id) WHERE continuable = 1 AND active = 1;`); err != nil {
		return fmt.Errorf("store: migrate session takeover index: %w", err)
	}
	return nil
}

func addAgentDefaults(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE channel ADD COLUMN agent TEXT;`); err != nil {
		return fmt.Errorf("store: migrate channel agent: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE user_override ADD COLUMN agent TEXT;`); err != nil {
		return fmt.Errorf("store: migrate override agent: %w", err)
	}
	return nil
}

func addRecoveryEvents(_ *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS recovery_event (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	platform       TEXT NOT NULL,
	channel_id     TEXT NOT NULL,
	thread_id      TEXT NOT NULL DEFAULT '',
	user_id        TEXT NOT NULL DEFAULT '',
	workdir        TEXT NOT NULL DEFAULT '',
	trigger_kind   TEXT NOT NULL,
	outcome        TEXT NOT NULL,
	correlation_id TEXT NOT NULL DEFAULT '',
	detail         TEXT NOT NULL DEFAULT '',
	created_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_event_created ON recovery_event (created_at);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate recovery events: %w", err)
	}
	return nil
}

func cleanupLegacyPermissionRules(_ *SQLiteStore, tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM permission_rule WHERE tool GLOB 'call_*'`); err != nil {
		return fmt.Errorf("store: clean up legacy permission rules: %w", err)
	}
	return nil
}

func ensureUsageAndWebhookTables(s *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS usage_projection (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		platform TEXT NOT NULL,
		channel_id TEXT NOT NULL,
		thread_id TEXT NOT NULL DEFAULT '',
		user_id TEXT NOT NULL DEFAULT '',
		session_id TEXT NOT NULL,
		model TEXT NOT NULL DEFAULT '',
		workdir TEXT NOT NULL DEFAULT '',
		input_tokens INTEGER NOT NULL,
		output_tokens INTEGER NOT NULL,
		reasoning_tokens INTEGER NOT NULL,
		cache_read_tokens INTEGER NOT NULL,
		cache_write_tokens INTEGER NOT NULL,
		input_total INTEGER NOT NULL,
		output_total INTEGER NOT NULL,
		reasoning_total INTEGER NOT NULL,
		cache_read_total INTEGER NOT NULL,
		cache_write_total INTEGER NOT NULL,
		cost REAL,
		cost_known INTEGER NOT NULL DEFAULT 0,
		recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_scope_time
	ON usage_projection (platform, channel_id, thread_id, user_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_usage_session_time
	ON usage_projection (session_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_usage_time
	ON usage_projection (recorded_at);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate usage projection: %w", err)
	}
	webhookDDL := `
CREATE TABLE IF NOT EXISTS webhook_delivery (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	endpoint      TEXT NOT NULL,
	delivery_id   TEXT NOT NULL,
	event_type    TEXT NOT NULL DEFAULT '',
	payload_hash  TEXT NOT NULL,
	status        TEXT NOT NULL,
	attempt       INTEGER NOT NULL DEFAULT 1,
	error_summary TEXT NOT NULL DEFAULT '',
	created_at    INTEGER NOT NULL,
	updated_at    INTEGER NOT NULL,
	started_at    INTEGER NOT NULL DEFAULT 0,
	completed_at  INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_delivery_key ON webhook_delivery (endpoint, delivery_id);
CREATE INDEX IF NOT EXISTS idx_webhook_delivery_created ON webhook_delivery (created_at);
`
	if _, err := tx.Exec(webhookDDL); err != nil {
		return fmt.Errorf("store: migrate webhook deliveries: %w", err)
	}
	return nil
}

func addPermissionRules(s *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS permission_rule (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	platform    TEXT NOT NULL,
	channel_id  TEXT NOT NULL,
	thread_id   TEXT NOT NULL DEFAULT '',
	user_id     TEXT NOT NULL DEFAULT '',
	tool        TEXT NOT NULL,
	patterns    TEXT NOT NULL,
	created_at  INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_permission_rule_owner_tool_patterns
	ON permission_rule (platform, channel_id, thread_id, user_id, tool, patterns);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate permission rules: %w", err)
	}
	return nil
}

func addSessionModel(s *SQLiteStore, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE session ADD COLUMN model TEXT NOT NULL DEFAULT '';`)
	return err
}

func addThreadConfig(s *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS thread_config (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	platform TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	workdir TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	listen_mode TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_thread_config_key ON thread_config (platform, channel_id, thread_id);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate thread config: %w", err)
	}

	now := time.Now().Unix()
	_, err := tx.Exec(`
INSERT OR IGNORE INTO thread_config (platform, channel_id, thread_id, workdir, model, listen_mode, created_at, updated_at)
SELECT s.platform, s.channel_id, s.thread_id,
	COALESCE((SELECT ch.workdir FROM channel ch WHERE ch.platform = s.platform AND ch.channel_id = s.channel_id), ?),
	COALESCE((SELECT ch.model FROM channel ch WHERE ch.platform = s.platform AND ch.channel_id = s.channel_id), ''),
	COALESCE((SELECT ch.listen_mode FROM channel ch WHERE ch.platform = s.platform AND ch.channel_id = s.channel_id), 'mention'),
	?, ?
FROM (SELECT DISTINCT platform, channel_id, thread_id FROM session WHERE thread_id != '' AND channel_id != thread_id) s`,
		s.defaultWorkdir, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: backfill thread config: %w", err)
	}
	return nil
}

func addProgressNotices(s *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS progress_notice (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	platform TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	thread_id TEXT NOT NULL DEFAULT '',
	message_id TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_progress_notice_lookup ON progress_notice (platform, channel_id, thread_id);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate progress notices: %w", err)
	}
	return nil
}

func addSessionTitle(s *SQLiteStore, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE session ADD COLUMN title TEXT NOT NULL DEFAULT '';`)
	return err
}

func addSessionAgentPID(s *SQLiteStore, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE session ADD COLUMN agent_pid INTEGER NOT NULL DEFAULT 0;`)
	return err
}

func addConversationKeys(s *SQLiteStore, tx *sql.Tx) error {
	ddl := `
ALTER TABLE session ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';
ALTER TABLE session ADD COLUMN user_id TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS idx_session_lookup;
CREATE UNIQUE INDEX IF NOT EXISTS idx_session_active_key
	ON session (platform, channel_id, thread_id, user_id) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_session_lookup
	ON session (platform, channel_id, thread_id, user_id, active);

ALTER TABLE channel ADD COLUMN auto_thread INTEGER NOT NULL DEFAULT 1;
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate conversation keys: %w", err)
	}
	return nil
}

func createInitialSchema(s *SQLiteStore, tx *sql.Tx) error {
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
	db                *sql.DB
	ingestDB          *sql.DB
	sessions          *sqliteSessionRepo
	channels          *sqliteChannelRepo
	overrides         *sqliteOverrideRepo
	schedules         *sqliteScheduleRepo
	progressNotices   *sqliteProgressNoticeRepo
	threadConfigs     *sqliteThreadConfigRepo
	permissionRules   *sqlitePermissionRuleRepo
	usage             *sqliteUsageRepo
	webhookDeliveries *sqliteWebhookDeliveryRepo
	recoveryEvents    *sqliteRecoveryEventRepo
	prQueues          *sqlitePRQueueRepo
	webhookReplays    *sqliteWebhookReplayRepo
	defaultWorkdir    string
}

func OpenWithDefaultWorkdir(path, defaultWorkdir string) (*SQLiteStore, error) {
	return OpenWithRetention(path, defaultWorkdir, UsageRetention{}, 0)
}

func OpenWithRetention(path, defaultWorkdir string, usage UsageRetention, recoveryRetention time.Duration) (*SQLiteStore, error) {
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
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

	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	ingestDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: open ingest: %w", err)
	}
	ingestDB.SetMaxOpenConns(2)
	ingestDB.SetMaxIdleConns(2)

	s := &SQLiteStore{db: db, ingestDB: ingestDB, defaultWorkdir: defaultWorkdir}
	if err := s.migrate(); err != nil {
		_ = ingestDB.Close()
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
	s.progressNotices = &sqliteProgressNoticeRepo{db: db}
	s.threadConfigs = &sqliteThreadConfigRepo{db: db}
	s.permissionRules = &sqlitePermissionRuleRepo{db: db}
	s.usage = &sqliteUsageRepo{db: db, retention: usage.Retention, maxRows: usage.MaxRows}
	s.webhookDeliveries = &sqliteWebhookDeliveryRepo{db: ingestDB}
	s.recoveryEvents = &sqliteRecoveryEventRepo{db: db, retention: recoveryRetention}
	s.prQueues = &sqlitePRQueueRepo{db: db}
	s.webhookReplays = &sqliteWebhookReplayRepo{db: db}
	if repaired, err := s.prQueues.RepairInvariants(context.Background(), ""); err != nil {
		slog.Warn("store: pr queue invariant check failed", "error", err)
	} else {
		for _, key := range repaired {
			slog.Warn("store: pr queue invariant repaired", "pr_key", key, "status", "drained")
		}
	}
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
		if err := migrations[i](s, tx); err != nil {
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

func (s *SQLiteStore) SessionRepo() SessionRepo                 { return s.sessions }
func (s *SQLiteStore) ChannelRepo() ChannelRepo                 { return s.channels }
func (s *SQLiteStore) OverrideRepo() OverrideRepo               { return s.overrides }
func (s *SQLiteStore) ScheduleRepo() ScheduleRepo               { return s.schedules }
func (s *SQLiteStore) ProgressNoticeRepo() ProgressNoticeRepo   { return s.progressNotices }
func (s *SQLiteStore) ThreadConfigRepo() ThreadConfigRepo       { return s.threadConfigs }
func (s *SQLiteStore) PermissionRuleRepo() PermissionRuleRepo   { return s.permissionRules }
func (s *SQLiteStore) UsageRepo() UsageRepo                     { return s.usage }
func (s *SQLiteStore) WebhookDeliveryRepo() WebhookDeliveryRepo { return s.webhookDeliveries }
func (s *SQLiteStore) RecoveryEventRepo() RecoveryEventRepo     { return s.recoveryEvents }
func (s *SQLiteStore) PRQueueRepo() PRQueueRepo                 { return s.prQueues }
func (s *SQLiteStore) WebhookReplayRepo() WebhookReplayRepo     { return s.webhookReplays }

func (s *SQLiteStore) Close() error {
	err := s.db.Close()
	if s.ingestDB != nil {
		if ingestErr := s.ingestDB.Close(); err == nil {
			err = ingestErr
		}
	}
	return err
}

func (s *SQLiteStore) DB() *sql.DB { return s.db }

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version)
	return version, err
}

var _ Store = (*SQLiteStore)(nil)
var _ UsageStore = (*SQLiteStore)(nil)
