package webhook

import (
	"context"
	"log/slog"
	"strings"
)

// hydrateTaskThreads fills the in-memory task registry from the PR queue at
// start, so thread commands, task buttons and short callback ids resolve for
// PRs that have not run since the restart. Records already known are kept.
func (s *Server) hydrateTaskThreads(ctx context.Context) int {
	if s == nil || s.tasks == nil || s.prQueues == nil {
		return 0
	}
	rows, err := s.prQueues.ListThreads(ctx)
	if err != nil {
		slog.Warn("webhook: task thread hydrate failed", "error", err)
		return 0
	}
	// One branch can carry several PRs over time; rows come oldest first, so
	// the newest row per repo+branch wins.
	latest := make(map[string]int)
	var order []string
	for i, row := range rows {
		repo, _, ok := splitPRKey(row.PRKey)
		if !ok || repo == "" || strings.TrimSpace(row.Branch) == "" || strings.TrimSpace(row.ThreadID) == "" {
			continue
		}
		key := TaskThreadKey(repo, row.Branch)
		if _, seen := latest[key]; !seen {
			order = append(order, key)
		}
		latest[key] = i
	}
	loaded := 0
	for _, key := range order {
		row := rows[latest[key]]
		repo, _, _ := splitPRKey(row.PRKey)
		branch := strings.TrimSpace(row.Branch)
		thread := strings.TrimSpace(row.ThreadID)
		if _, known := s.tasks.ResolvePRThread(row.PRKey); known {
			continue
		}
		rec, exists := s.tasks.Lookup(repo, branch)
		if !exists || strings.TrimSpace(rec.ThreadID) == "" {
			rec = s.tasks.Upsert(repo, branch, strings.TrimSpace(row.RootMsgID), thread, "")
		}
		s.tasks.LinkPRThread(row.PRKey, rec)
		loaded++
	}
	if loaded > 0 {
		slog.Info("webhook: task threads hydrated from pr queue", "loaded", loaded)
	}
	return loaded
}
