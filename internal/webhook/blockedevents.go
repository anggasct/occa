package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/store"
)

// unblockOnNewHead answers a push to a blocked PR: new commits are the
// operator's (or a person's) answer to the block, so the PR is unblocked
// like Cancel without dropping queued work. A storm-cap block is about how
// often occa pushed, which a push does not answer; it stays.
func (s *Server) unblockOnNewHead(item dispatchItem, row *store.PRRun, envelope WebhookEnvelope) bool {
	if row == nil || s.prQueues == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(item.eventType), "pull_request") ||
		!strings.EqualFold(strings.TrimSpace(stringValue(envelope["action"])), "synchronize") {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(row.BlockedReason), "auto-fix storm cap") {
		return false
	}
	if err := s.prQueues.Unblock(context.Background(), item.prKey); err != nil {
		// A sibling endpoint's copy of the same push may have unblocked it.
		if current, gerr := s.prQueues.Get(context.Background(), item.prKey); gerr == nil && current != nil && current.Status != store.PRStatusBlocked {
			return true
		}
		slog.Warn("webhook: new head unblock failed", "pr_key", item.prKey, "error", err)
		return false
	}
	s.reopenPRSurface(item.prKey)
	s.clearEscalated(item.prKey)
	headline := describeBlock(row.BlockedReason).Headline
	slog.Info("webhook: new head unblocked PR", "pr_key", item.prKey, "endpoint", item.ep.Name, "head", shortRevision(stringValue(envelope["head_sha"])), "blocked_reason", row.BlockedReason)
	s.postBlockedThreadLine(item, row, fmt.Sprintf("🔓 New commits on %s — unblocked (was: %s).", item.prKey, strings.TrimSuffix(headline, ".")))
	return true
}

// noteBlockedSkip tells the thread why a request was dropped: a person asked
// (a comment trigger matched) while the PR is blocked.
func (s *Server) noteBlockedSkip(item dispatchItem, row *store.PRRun, envelope WebhookEnvelope) {
	if row == nil || strings.TrimSpace(stringValue(envelope["comment_trigger"])) == "" {
		return
	}
	headline := describeBlock(row.BlockedReason).Headline
	s.postBlockedThreadLine(item, row, fmt.Sprintf("⏸ %s is blocked — %s Use Retry or Cancel on the blocked notice (or /retry, /cancel here), then ask again.", item.prKey, headline))
}

func (s *Server) postBlockedThreadLine(item dispatchItem, row *store.PRRun, text string) {
	threadID := strings.TrimSpace(row.ThreadID)
	if threadID == "" {
		if rec, ok := s.tasks.ResolvePRThread(item.prKey); ok {
			threadID = strings.TrimSpace(rec.ThreadID)
		}
	}
	if threadID == "" {
		return
	}
	target := threadTargetFor(item.ep.Platform, item.ep.ChannelID, threadID)
	if err := s.notifyThread(context.Background(), item.ep.Platform, target, text); err != nil {
		slog.Warn("webhook: blocked thread line failed", "pr_key", item.prKey, "error", err)
	}
}
