package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// PermissionOwner is the conversation key a rule is scoped to, mirroring the
// session conversation key policy: DM -> threadID and userID empty; group
// without thread -> userID set; thread -> threadID set, userID empty.
type PermissionOwner struct {
	Platform  string
	ChannelID string
	ThreadID  string
	UserID    string
}

// CanonicalizePatterns sorts, dedupes, and joins permission patterns with "|"
// so equivalent asks match regardless of the order the agent sent them.
func CanonicalizePatterns(patterns []string) string {
	seen := make(map[string]struct{}, len(patterns))
	uniq := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		uniq = append(uniq, p)
	}
	sort.Strings(uniq)
	return strings.Join(uniq, "|")
}

type sqlitePermissionRuleRepo struct {
	db *sql.DB
}

func (r *sqlitePermissionRuleRepo) Add(ctx context.Context, owner PermissionOwner, tool string, patterns []string) (int64, error) {
	now := time.Now().Unix()
	canonical := CanonicalizePatterns(patterns)
	if _, err := r.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO permission_rule (platform, channel_id, thread_id, user_id, tool, patterns, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID, tool, canonical, now,
	); err != nil {
		return 0, fmt.Errorf("store: permission rule add: %w", err)
	}
	var id int64
	err := r.db.QueryRowContext(ctx,
		`SELECT id FROM permission_rule WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND tool = ? AND patterns = ?`,
		owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID, tool, canonical,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: permission rule add lookup: %w", err)
	}
	return id, nil
}

func (r *sqlitePermissionRuleRepo) ListByOwner(ctx context.Context, owner PermissionOwner) ([]PermissionRule, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, platform, channel_id, thread_id, user_id, tool, patterns, created_at
		 FROM permission_rule
		 WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ?
		 ORDER BY id DESC`,
		owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: permission rule list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var rules []PermissionRule
	for rows.Next() {
		var rule PermissionRule
		if err := rows.Scan(&rule.ID, &rule.Platform, &rule.ChannelID, &rule.ThreadID, &rule.UserID, &rule.Tool, &rule.Patterns, &rule.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: permission rule list: scan: %w", err)
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func (r *sqlitePermissionRuleRepo) DeleteByID(ctx context.Context, owner PermissionOwner, id int64) error {
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM permission_rule WHERE id = ? AND platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ?`,
		id, owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID,
	); err != nil {
		return fmt.Errorf("store: permission rule delete: %w", err)
	}
	return nil
}

func (r *sqlitePermissionRuleRepo) ClearByOwner(ctx context.Context, owner PermissionOwner) error {
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM permission_rule WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ?`,
		owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID,
	); err != nil {
		return fmt.Errorf("store: permission rule clear: %w", err)
	}
	return nil
}

func (r *sqlitePermissionRuleRepo) Match(ctx context.Context, owner PermissionOwner, tool string, patterns []string) (*PermissionRule, error) {
	canonical := CanonicalizePatterns(patterns)
	var rule PermissionRule
	err := r.db.QueryRowContext(ctx,
		`SELECT id, platform, channel_id, thread_id, user_id, tool, patterns, created_at
		 FROM permission_rule
		 WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND tool = ? AND patterns = ?`,
		owner.Platform, owner.ChannelID, owner.ThreadID, owner.UserID, tool, canonical,
	).Scan(&rule.ID, &rule.Platform, &rule.ChannelID, &rule.ThreadID, &rule.UserID, &rule.Tool, &rule.Patterns, &rule.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: permission rule match: %w", err)
	}
	return &rule, nil
}

var _ PermissionRuleRepo = (*sqlitePermissionRuleRepo)(nil)
