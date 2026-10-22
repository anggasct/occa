package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"

	"github.com/anggasct/occa/internal/config"
)

// internalReviewTrigger is the comment the internal re-review delivery
// carries when the review endpoint has no comment trigger of its own.
const internalReviewTrigger = "please re-review"

// enqueueInternalReview queues one review for a PR whose fix round pushed
// commits. The review is an internal delivery, not a webhook: a human push
// never triggers a review, and one fix round triggers exactly one review.
// The delivery id carries the head commit, so a repeated run on the same head
// is refused instead of starting a second review.
func (s *Server) enqueueInternalReview(prKey, headSHA string) {
	if s == nil {
		return
	}
	prKey = strings.TrimSpace(prKey)
	if prKey == "" {
		return
	}
	ep, ok := s.reviewEndpointForPR(prKey)
	if !ok {
		slog.Warn("webhook: re-review not queued, no review endpoint", "pr_key", prKey)
		return
	}
	trigger := s.reviewTrigger(ep)
	if trigger == "" {
		slog.Warn("webhook: re-review not queued, no review trigger", "pr_key", prKey, "endpoint", ep.Name)
		return
	}
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return
	}
	body := internalReviewCommentBody(repo, number, trigger, s.prURLForPR(prKey))
	deliveryID := internalReviewDeliveryID(prKey, headSHA)
	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  deliveryID,
		eventType:   "issue_comment",
		payloadHash: payloadHashOf(body),
	}
	key := WebhookExecutionKey{Repository: repo, Branch: s.branchForPR(prKey)}
	if _, ok, _ := s.enqueue(ep, key, item); !ok {
		slog.Warn("webhook: re-review not queued, queue full", "pr_key", prKey, "endpoint", ep.Name)
		return
	}
	slog.Info("webhook: re-review queued after fix",
		"pr_key", prKey,
		"endpoint", ep.Name,
		"delivery_id", deliveryID,
		"head_sha", headSHA,
	)
}

// reviewEndpointForPR returns the review endpoint of the repository a PR
// belongs to.
func (s *Server) reviewEndpointForPR(prKey string) (config.EndpointConfig, bool) {
	repo, _, _ := splitPRKey(prKey)
	want := normalizeRepoKey(repo)
	for _, ep := range s.endpoints {
		if !strings.EqualFold(strings.TrimSpace(ep.Workflow), "review") {
			continue
		}
		if want != "" && normalizeRepoKey(ep.Repository) != want {
			continue
		}
		return ep, true
	}
	return config.EndpointConfig{}, false
}

// reviewTrigger returns the comment trigger the review endpoint accepts.
func (s *Server) reviewTrigger(ep config.EndpointConfig) string {
	for _, trigger := range ep.CommentTrigger {
		if trimmed := strings.TrimSpace(trigger); trimmed != "" {
			return trimmed
		}
	}
	if len(ep.CommentTrigger) == 0 {
		return internalReviewTrigger
	}
	return ""
}

// internalReviewCommentBody renders the comment delivery of an internal
// re-review.
func internalReviewCommentBody(repo, number, trigger, prURL string) []byte {
	payload := map[string]any{
		"action": "created",
		"repository": map[string]any{
			"full_name": repo,
		},
		"issue": map[string]any{
			"number": number,
			"state":  "open",
			"pull_request": map[string]any{
				"html_url": prURL,
			},
		},
		"comment": map[string]any{
			"body": trigger,
			"user": map[string]any{"login": "occa"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return body
}

// internalReviewDeliveryID names the internal review delivery after the PR
// and the head commit it reviews.
func internalReviewDeliveryID(prKey, headSHA string) string {
	sum := sha256.Sum256([]byte(prKey + "@" + headSHA))
	return "internal-review-" + hex.EncodeToString(sum[:])[:16]
}

func payloadHashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// livePRHeadSHA resolves the live head commit of a PR. An empty result means
// the head could not be resolved.
func (s *Server) livePRHeadSHA(ctx context.Context, prKey string) string {
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return ""
	}
	prNumber, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil || prNumber <= 0 {
		return ""
	}
	resolver := s.prHeadResolver
	if resolver == nil {
		resolver = &ghPRHeadResolver{timeout: s.processingTimeout}
	}
	head, err := resolver.ResolvePRHead(ctx, repo, prNumber)
	if err != nil {
		return ""
	}
	return normalizeRevision(head.SHA)
}

// prURLForPR returns the URL of a PR surface, if it is known.
func (s *Server) prURLForPR(prKey string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
		if url := strings.TrimSpace(detail.PRURL); url != "" {
			return url
		}
	}
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return ""
	}
	return "https://github.com/" + repo + "/pull/" + number
}

// branchForPR returns the branch of a PR surface, if it is known.
func (s *Server) branchForPR(prKey string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
		return strings.TrimSpace(detail.Branch)
	}
	return ""
}

// queueReviewAfterFix queues a review when the fix round moved the PR head.
// A fix round that pushed commits closes the review loop; a fix round that
// changed nothing does not. The head the run started from is the head the
// triggering event carried, so the live head is read once, after the run.
func (s *Server) queueReviewAfterFix(prKey, headBefore string) {
	prKey = strings.TrimSpace(prKey)
	if prKey == "" {
		return
	}
	headAfter := s.livePRHeadSHA(context.Background(), prKey)
	if headAfter == "" || headAfter == headBefore {
		return
	}
	s.enqueueInternalReview(prKey, headAfter)
}

// isFixWorkflow reports whether an endpoint runs the fix lane.
func isFixWorkflow(workflow string) bool {
	switch strings.ToLower(strings.TrimSpace(workflow)) {
	case "fix", "pr-fix":
		return true
	}
	return false
}
