package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	usageRetention = 90 * 24 * time.Hour
	usageMaxRows   = 100000
)

type UsageSnapshot struct {
	Platform   string
	ChannelID  string
	ThreadID   string
	UserID     string
	SessionID  string
	Model      string
	Workdir    string
	Input      int64
	Output     int64
	Reasoning  int64
	CacheRead  int64
	CacheWrite int64
	Cost       float64
	CostKnown  bool
	RecordedAt int64
}

type UsageQuery struct {
	Platform    string
	ChannelID   string
	ThreadID    string
	UserID      string
	ChannelWide bool
	SessionID   string
	Since       int64
	Limit       int
	Offset      int
}

type UsageTotals struct {
	Input      int64
	Output     int64
	Reasoning  int64
	CacheRead  int64
	CacheWrite int64
	Cost       float64
	CostKnown  bool
}

type UsageBreakdown struct {
	Model      string
	Workdir    string
	ThreadID   string
	UserID     string
	Input      int64
	Output     int64
	Reasoning  int64
	CacheRead  int64
	CacheWrite int64
	Cost       float64
	CostKnown  bool
}

type UsageReport struct {
	Totals         UsageTotals
	Breakdowns     []UsageBreakdown
	BreakdownTotal int
}

type UsageRepo interface {
	RecordSnapshot(ctx context.Context, snapshot UsageSnapshot) error
	Query(ctx context.Context, query UsageQuery) (UsageReport, error)
}

type UsageStore interface {
	UsageRepo() UsageRepo
}

type sqliteUsageRepo struct {
	db *sql.DB
}

func (r *sqliteUsageRepo) RecordSnapshot(ctx context.Context, snapshot UsageSnapshot) error {
	if snapshot.SessionID == "" || snapshot.RecordedAt <= 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: usage record: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var previous UsageSnapshot
	var previousCost sql.NullFloat64
	var previousCostKnown int
	err = tx.QueryRowContext(ctx, `
		SELECT input_total, output_total, reasoning_total, cache_read_total, cache_write_total,
		       cost, cost_known
		FROM usage_projection
		WHERE session_id = ?
		ORDER BY id DESC LIMIT 1`, snapshot.SessionID).Scan(
		&previous.Input, &previous.Output, &previous.Reasoning, &previous.CacheRead,
		&previous.CacheWrite, &previousCost, &previousCostKnown,
	)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("store: usage record: previous: %w", err)
	}
	if err == nil {
		previous.Cost = previousCost.Float64
		previous.CostKnown = previousCostKnown == 1
	}

	input := usageDelta(snapshot.Input, previous.Input)
	output := usageDelta(snapshot.Output, previous.Output)
	reasoning := usageDelta(snapshot.Reasoning, previous.Reasoning)
	cacheRead := usageDelta(snapshot.CacheRead, previous.CacheRead)
	cacheWrite := usageDelta(snapshot.CacheWrite, previous.CacheWrite)
	cost := 0.0
	costKnown := snapshot.CostKnown
	if costKnown {
		cost = usageDeltaFloat(snapshot.Cost, previous.Cost)
	}
	if input == 0 && output == 0 && reasoning == 0 && cacheRead == 0 && cacheWrite == 0 && (!costKnown || cost == 0) {
		return tx.Commit()
	}

	var costValue any
	if costKnown {
		costValue = cost
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO usage_projection (
			platform, channel_id, thread_id, user_id, session_id, model, workdir,
			input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens,
			input_total, output_total, reasoning_total, cache_read_total, cache_write_total,
			cost, cost_known, recorded_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.Platform, snapshot.ChannelID, snapshot.ThreadID, snapshot.UserID, snapshot.SessionID,
		snapshot.Model, snapshot.Workdir, input, output, reasoning, cacheRead, cacheWrite,
		snapshot.Input, snapshot.Output, snapshot.Reasoning, snapshot.CacheRead, snapshot.CacheWrite,
		costValue, boolInt(costKnown), snapshot.RecordedAt,
	); err != nil {
		return fmt.Errorf("store: usage record: insert: %w", err)
	}

	cutoff := snapshot.RecordedAt - int64(usageRetention/time.Second)
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_projection WHERE recorded_at < ?`, cutoff); err != nil {
		return fmt.Errorf("store: usage record: retention: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_projection WHERE id NOT IN (SELECT id FROM usage_projection ORDER BY recorded_at DESC, id DESC LIMIT ?)`, usageMaxRows); err != nil {
		return fmt.Errorf("store: usage record: row cap: %w", err)
	}
	return tx.Commit()
}

func (r *sqliteUsageRepo) Query(ctx context.Context, query UsageQuery) (UsageReport, error) {
	where, args := usageWhere(query)

	var report UsageReport
	var cost sql.NullFloat64
	var unknown int
	if err := r.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0),
		       COALESCE(SUM(reasoning_tokens), 0), COALESCE(SUM(cache_read_tokens), 0),
		       COALESCE(SUM(cache_write_tokens), 0), SUM(cost),
		       COALESCE(MAX(CASE WHEN cost_known = 0 THEN 1 ELSE 0 END), 1)
		FROM usage_projection WHERE `+where, args...).Scan(
		&report.Totals.Input, &report.Totals.Output, &report.Totals.Reasoning,
		&report.Totals.CacheRead, &report.Totals.CacheWrite, &cost, &unknown,
	); err != nil {
		return UsageReport{}, fmt.Errorf("store: usage query totals: %w", err)
	}
	report.Totals.Cost = cost.Float64
	report.Totals.CostKnown = unknown == 0 && cost.Valid

	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
			SELECT model, workdir, thread_id, user_id
			FROM usage_projection WHERE `+where+`
			GROUP BY model, workdir, thread_id, user_id
		)`, args...).Scan(&report.BreakdownTotal); err != nil {
		return UsageReport{}, fmt.Errorf("store: usage query count: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT model, workdir, thread_id, user_id,
		       COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0),
		       COALESCE(SUM(reasoning_tokens), 0), COALESCE(SUM(cache_read_tokens), 0),
		       COALESCE(SUM(cache_write_tokens), 0), SUM(cost),
		       COALESCE(MAX(CASE WHEN cost_known = 0 THEN 1 ELSE 0 END), 1)
		FROM usage_projection WHERE `+where+`
		GROUP BY model, workdir, thread_id, user_id
		ORDER BY (COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) +
		          COALESCE(SUM(reasoning_tokens), 0) + COALESCE(SUM(cache_read_tokens), 0) +
		          COALESCE(SUM(cache_write_tokens), 0)) DESC, model, workdir, thread_id, user_id
		LIMIT ? OFFSET ?`, append(args, query.Limit, query.Offset)...)
	if err != nil {
		return UsageReport{}, fmt.Errorf("store: usage query breakdown: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var breakdown UsageBreakdown
		var breakdownCost sql.NullFloat64
		var breakdownUnknown int
		if err := rows.Scan(&breakdown.Model, &breakdown.Workdir, &breakdown.ThreadID, &breakdown.UserID,
			&breakdown.Input, &breakdown.Output, &breakdown.Reasoning, &breakdown.CacheRead,
			&breakdown.CacheWrite, &breakdownCost, &breakdownUnknown); err != nil {
			return UsageReport{}, fmt.Errorf("store: usage query breakdown scan: %w", err)
		}
		breakdown.Cost = breakdownCost.Float64
		breakdown.CostKnown = breakdownUnknown == 0 && breakdownCost.Valid
		report.Breakdowns = append(report.Breakdowns, breakdown)
	}
	if err := rows.Err(); err != nil {
		return UsageReport{}, fmt.Errorf("store: usage query breakdown rows: %w", err)
	}
	return report, nil
}

func usageWhere(query UsageQuery) (string, []any) {
	where := "platform = ? AND channel_id = ?"
	args := []any{query.Platform, query.ChannelID}
	if !query.ChannelWide {
		where += " AND thread_id = ? AND user_id = ?"
		args = append(args, query.ThreadID, query.UserID)
	}
	if query.SessionID != "" {
		where += " AND session_id = ?"
		args = append(args, query.SessionID)
	}
	if query.Since > 0 {
		where += " AND recorded_at >= ?"
		args = append(args, query.Since)
	}
	return where, args
}

func usageDelta(current, previous int64) int64 {
	if current < previous {
		return current
	}
	return current - previous
}

func usageDeltaFloat(current, previous float64) float64 {
	if current < previous {
		return current
	}
	return current - previous
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var _ UsageRepo = (*sqliteUsageRepo)(nil)
