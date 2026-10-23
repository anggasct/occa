package webhook

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/store"
)

const prBindingLookupTimeout = 15 * time.Second

type prBindingResolution struct {
	ThreadID      string
	RootMessageID string
	Repo          string
	Branch        string
	BranchSource  string
}

func (s *Server) resolvePRBinding(ctx context.Context, prKey, eventType string, envelope WebhookEnvelope) prBindingResolution {
	res := prBindingResolution{Repo: normalizeRepoKey(prKeyRepo(prKey, envelope))}
	if s == nil {
		return res
	}
	res.Branch = strings.TrimSpace(stringValue(envelope["head_branch"]))
	if res.Branch != "" {
		res.BranchSource = "payload"
	}
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil {
			if threadID := strings.TrimSpace(row.ThreadID); threadID != "" {
				res.ThreadID = threadID
				res.RootMessageID = strings.TrimSpace(row.RootMsgID)
				s.logPRBindingConflict(prKey, res)
				return res
			}
		}
	}
	if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
		res.ThreadID = strings.TrimSpace(rec.ThreadID)
		res.RootMessageID = strings.TrimSpace(rec.RootMessageID)
		if res.Branch == "" {
			res.Branch = strings.TrimSpace(rec.Branch)
		}
		if res.ThreadID != "" {
			s.persistPRBinding(ctx, prKey, rec)
			return res
		}
	}
	if res.Branch == "" {
		res.Branch, res.BranchSource = s.resolveHeadBranch(ctx, eventType, envelope)
	}
	if res.Branch == "" {
		return res
	}
	rec, ok := s.tasks.Lookup(res.Repo, res.Branch)
	if !ok {
		return res
	}
	res.ThreadID = strings.TrimSpace(rec.ThreadID)
	res.RootMessageID = strings.TrimSpace(rec.RootMessageID)
	if res.ThreadID == "" {
		return res
	}
	s.persistPRBinding(ctx, prKey, rec)
	return res
}

func prKeyRepo(prKey string, envelope WebhookEnvelope) string {
	if repo, _, ok := splitPRKey(prKey); ok {
		return repo
	}
	return stringValue(envelope["repository"])
}

func (s *Server) resolveHeadBranch(ctx context.Context, eventType string, envelope WebhookEnvelope) (string, string) {
	if branch := strings.TrimSpace(stringValue(envelope["head_branch"])); branch != "" {
		return branch, "payload"
	}
	if eventType != "issue_comment" {
		return "", "absent"
	}
	repo := normalizeRepoKey(stringValue(envelope["repository"]))
	number, err := strconv.Atoi(strings.TrimSpace(stringValue(envelope["pr_number"])))
	if repo == "" || err != nil || number <= 0 {
		return "", "absent"
	}
	resolver := s.prHeadResolver
	if resolver == nil {
		resolver = &ghPRHeadResolver{timeout: prBindingLookupTimeout}
	}
	lookupCtx, cancel := context.WithTimeout(ctx, prBindingLookupTimeout)
	defer cancel()
	head, err := resolver.ResolvePRHead(lookupCtx, repo, number)
	if err != nil {
		slog.Warn("webhook: pr binding branch lookup failed", "repo", repo, "pr_number", number, "error", err)
		return "", "gh-error"
	}
	branch := strings.TrimSpace(head.Branch)
	if branch == "" {
		return "", "gh-empty"
	}
	envelope["head_branch"] = branch
	return branch, "gh"
}

func (s *Server) logPRBindingConflict(prKey string, res prBindingResolution) {
	if s == nil || res.Branch == "" || res.ThreadID == "" {
		return
	}
	rec, ok := s.tasks.Lookup(res.Repo, res.Branch)
	if !ok {
		return
	}
	found := strings.TrimSpace(rec.ThreadID)
	if found == "" || found == res.ThreadID {
		return
	}
	slog.Warn("webhook: pr thread binding conflict",
		"pr_key", prKey,
		"existing_thread_id", res.ThreadID,
		"found_thread_id", found,
		"branch", res.Branch,
	)
}

func (s *Server) persistPRBinding(ctx context.Context, prKey string, rec TaskThreadRecord) {
	threadID := strings.TrimSpace(rec.ThreadID)
	if s == nil || threadID == "" {
		return
	}
	rootID := strings.TrimSpace(rec.RootMessageID)
	var row *store.PRRun
	if s.prQueues != nil {
		got, err := s.prQueues.Get(ctx, prKey)
		if err != nil {
			slog.Warn("webhook: pr binding read failed", "pr_key", prKey, "error", err)
			return
		}
		row = got
		if row != nil {
			if existing := strings.TrimSpace(row.ThreadID); existing != "" && existing != threadID {
				slog.Warn("webhook: pr thread binding conflict",
					"pr_key", prKey,
					"existing_thread_id", existing,
					"found_thread_id", threadID,
					"branch", strings.TrimSpace(rec.Branch),
				)
				return
			}
		}
	}
	s.tasks.LinkPRThread(prKey, rec)
	if s.prQueues == nil {
		return
	}
	if row == nil {
		if _, err := s.prQueues.EnsurePR(ctx, prKey, threadID, rootID); err != nil {
			slog.Warn("webhook: pr binding persist failed", "pr_key", prKey, "error", err)
		}
		return
	}
	if strings.TrimSpace(row.ThreadID) == "" {
		if err := s.prQueues.UpdateThread(ctx, prKey, threadID, rootID); err != nil {
			slog.Warn("webhook: pr binding persist failed", "pr_key", prKey, "error", err)
		}
	}
}

func isBindingRequiredWorkflow(workflow string) bool {
	switch strings.ToLower(strings.TrimSpace(workflow)) {
	case "fix", "merge":
		return true
	default:
		return false
	}
}

func prBindingRefusalSummary(prKey string, res prBindingResolution) string {
	repo := strings.TrimSpace(res.Repo)
	if repo == "" {
		repo = "unknown"
	}
	searched := "pr_key=" + strings.TrimSpace(prKey)
	if branch := strings.TrimSpace(res.Branch); branch != "" {
		searched += ", " + repo + "#" + branch + " (branch from " + res.BranchSource + ")"
	} else {
		searched += ", " + repo + "#<branch unresolved>"
	}
	return "run refused: PR thread binding unresolved (searched " + searched + "; re-run /retry or re-trigger)"
}
