package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

type ReviewObservation struct {
	LiveHead    string
	VerdictHead string
}

type ReviewObserver func(ctx context.Context, repo, number string) (ReviewObservation, error)

var defaultReviewObserver ReviewObserver = GHReviewObserver

func (s *Server) SetReviewObserver(fn ReviewObserver) {
	s.reviewObserver = fn
}

// GHReviewObserver resolves the live PR head and the commit the latest
// logical verdict review was posted against.
func GHReviewObserver(ctx context.Context, repo, number string) (ReviewObservation, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(lookupCtx, "gh", "pr", "view", strings.TrimSpace(number), "--repo", strings.TrimSpace(repo),
		"--json", "headRefOid,reviews")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return ReviewObservation{}, fmt.Errorf("gh pr view: %s", detail)
	}
	var raw struct {
		HeadRefOid string              `json:"headRefOid"`
		Reviews    []ghReconcileReview `json:"reviews"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return ReviewObservation{}, fmt.Errorf("gh pr view decode: %w", err)
	}
	return ReviewObservation{
		LiveHead:    normalizeRevision(raw.HeadRefOid),
		VerdictHead: latestVerdictReviewHead(raw.Reviews),
	}, nil
}

func latestVerdictReviewHead(reviews []ghReconcileReview) string {
	head := ""
	var latest time.Time
	for _, review := range reviews {
		if !reviewCountsAsVerdict(review) {
			continue
		}
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(review.SubmittedAt))
		if err != nil {
			head = normalizeRevision(review.Commit.OID)
			latest = time.Time{}
			continue
		}
		if latest.IsZero() || !at.Before(latest) {
			head = normalizeRevision(review.Commit.OID)
			latest = at
		}
	}
	return head
}

func reviewCountsAsVerdict(review ghReconcileReview) bool {
	switch reviewLogicalVerdict(review) {
	case "approved", "changes_requested", "spec_unclear", "blocked":
		return true
	}
	return false
}

// finalizeReviewHead re-keys a finished review on the commit its verdict was
// actually posted against. When the PR head has moved past that commit since,
// the delivery is reported as superseded so the normal re-review can cover it.
func (s *Server) finalizeReviewHead(ep config.EndpointConfig, id int64, prKey string, envelope WebhookEnvelope) string {
	if s == nil || s.deliveries == nil || s.reviewObserver == nil || id == 0 {
		return ""
	}
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		prefix := strings.TrimSuffix(reviewDedupePrefix(envelope), "@")
		repo, number, ok = splitPRKey(prefix)
	}
	if !ok {
		return ""
	}
	admission := reviewDedupeHead(envelope)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	obs, err := s.reviewObserver(ctx, repo, number)
	if err != nil {
		slog.Warn("webhook: review head observation failed", "endpoint", ep.Name, "pr_key", prKey, "error", err)
		return ""
	}
	live := normalizeRevision(obs.LiveHead)
	verdict := normalizeRevision(obs.VerdictHead)
	judged := ""
	switch {
	case verdict != "":
		if verdict == admission || verdict == live {
			judged = verdict
		} else {
			judged = admission
		}
	case live != "":
		judged = live
	default:
		judged = admission
	}
	if judged == "" {
		return ""
	}
	if err := s.deliveries.SetReviewKey(context.Background(), id, repo+"#"+number+"@"+judged); err != nil {
		slog.Warn("webhook: review key rewrite failed", "endpoint", ep.Name, "pr_key", prKey, "error", err)
		return ""
	}
	if live != "" && live != judged {
		return "superseded by head " + shortRevision(live)
	}
	return ""
}

func shortRevision(rev string) string {
	rev = normalizeRevision(rev)
	if len(rev) > 10 {
		return rev[:10]
	}
	return rev
}
