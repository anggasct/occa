package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

// shortTaskCallbackPrefix marks the compact task callbacks: ta:<action>:<id>.
// The id is a hash of the task key, so the callback fits Telegram's 64-byte
// limit whatever the branch name is.
const shortTaskCallbackPrefix = "ta:"

const shortTaskIDLen = 10

// Task actions a button can carry.
const (
	TaskActionReview = "review"
	TaskActionRetry  = "retry"
	TaskActionCancel = "cancel"
	TaskActionMerge  = "merge"
)

// ShortTaskID names a task in a compact callback.
func ShortTaskID(repo, branch string) string {
	sum := sha256.Sum256([]byte(TaskThreadKey(repo, branch)))
	return hex.EncodeToString(sum[:])[:shortTaskIDLen]
}

// ShortTaskCallback renders ta:<action>:<id> for a task.
func ShortTaskCallback(action, repo, branch string) string {
	return shortTaskCallbackPrefix + action + ":" + ShortTaskID(repo, branch)
}

// ParseShortTaskCallback splits ta:<action>:<id>.
func ParseShortTaskCallback(data string) (string, string, bool) {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(trimmed, shortTaskCallbackPrefix) {
		return "", "", false
	}
	action, id, ok := strings.Cut(strings.TrimPrefix(trimmed, shortTaskCallbackPrefix), ":")
	if !ok {
		return "", "", false
	}
	switch action {
	case TaskActionReview, TaskActionRetry, TaskActionCancel, TaskActionMerge:
	default:
		return "", "", false
	}
	id = strings.TrimSpace(id)
	if len(id) != shortTaskIDLen {
		return "", "", false
	}
	return action, id, true
}

// TaskForShortID resolves a compact callback id to its task.
func (t *TaskThreadRegistry) TaskForShortID(id string) (string, string, bool) {
	if t == nil {
		return "", "", false
	}
	id = strings.TrimSpace(id)
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, rec := range t.records {
		if ShortTaskID(rec.Repo, rec.Branch) == id {
			return rec.Repo, rec.Branch, true
		}
	}
	return "", "", false
}

// blockedActionButtons are the buttons of a blocked PR: Retry, Merge, Cancel.
// The suffix names the PR when several PRs share one message (/inbox).
func blockedActionButtons(repo, branch, suffix string, row int) []channel.Button {
	if strings.TrimSpace(repo) == "" || strings.TrimSpace(branch) == "" {
		return nil
	}
	label := func(name string) string {
		if suffix == "" {
			return name
		}
		return name + " " + suffix
	}
	return []channel.Button{
		{Label: label("Retry"), Value: ShortTaskCallback(TaskActionRetry, repo, branch), Row: row},
		{Label: label("Merge"), Value: ShortTaskCallback(TaskActionMerge, repo, branch), Row: row},
		{Label: label("Cancel"), Value: ShortTaskCallback(TaskActionCancel, repo, branch), Row: row},
	}
}

// HandleTaskAction runs a PR action pressed on a button: review, retry or
// cancel. The outcome is posted in the PR thread; the pressed message is not
// edited. Merge stays on the dispatch path the router owns.
func (s *Server) HandleTaskAction(ctx context.Context, msg channel.IncomingMessage, action, repo, branch string) error {
	if action == TaskActionReview {
		return s.RequestReview(ctx, msg, repo, branch)
	}
	prKey, known := "", false
	if s.tasks != nil {
		prKey, known = s.tasks.PRKeyForTask(TaskThreadKey(repo, branch))
	}
	outcome, text := s.taskAction(ctx, msg, action, prKey, known, repo, branch)
	slog.Info("webhook: task action button",
		"action", action,
		"pr_key", prKey,
		"repo", repo,
		"branch", branch,
		"user_id", msg.UserID,
		"outcome", outcome,
	)
	return s.replyTaskAction(ctx, msg, prKey, text)
}

func (s *Server) taskAction(ctx context.Context, msg channel.IncomingMessage, action, prKey string, known bool, repo, branch string) (string, string) {
	if !s.operatorAllowed(msg.Platform, msg.UserID) {
		return "not_allowed", "⛔ Not allowed."
	}
	if !known || strings.TrimSpace(prKey) == "" {
		return "no_pr", fmt.Sprintf("No open PR for %s#%s yet.", repo, branch)
	}
	var (
		reply string
		err   error
	)
	switch action {
	case TaskActionRetry:
		reply, _, err = s.operatorRetry(ctx, msg, prKey)
	case TaskActionCancel:
		reply, _, err = s.operatorCancel(ctx, msg, prKey)
	default:
		return "unknown", fmt.Sprintf("⚠️ Unknown action %q.", action)
	}
	if err != nil {
		slog.Warn("webhook: task action failed", "action", action, "pr_key", prKey, "error", err)
		return "failed", fmt.Sprintf("⚠️ %s failed for %s: %v", action, prKey, err)
	}
	return action, reply
}

// replyTaskAction posts a button outcome in the PR thread; without a known
// thread it goes to the thread or channel the press came from.
func (s *Server) replyTaskAction(ctx context.Context, msg channel.IncomingMessage, prKey, text string) error {
	target := ""
	if s.tasks != nil && prKey != "" {
		if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
			target = strings.TrimSpace(rec.ThreadID)
		}
	}
	if target == "" {
		target = strings.TrimSpace(msg.ThreadID)
	}
	if target == "" {
		target = strings.TrimSpace(msg.ChannelID)
	}
	if target == "" || strings.TrimSpace(text) == "" {
		return nil
	}
	return s.notifyThread(ctx, msg.Platform, target, text)
}
