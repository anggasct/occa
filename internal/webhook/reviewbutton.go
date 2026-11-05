package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
)

// reviewButtonDeliveryPrefix marks a review queued by the Review button. It
// is an internal review delivery: commenter gates do not apply, the review
// dedupe and the trigger rate cap do.
const reviewButtonDeliveryPrefix = internalReviewDeliveryPrefix + "button-"

// discoverDeliveryPrefix marks a review queued for an open PR occa had no
// record of. Like the button it is internal: the review dedupe and the
// trigger rate cap apply, commenter gates do not.
const discoverDeliveryPrefix = "discover-"

func isDiscoverDelivery(deliveryID string) bool {
	return strings.HasPrefix(strings.TrimSpace(deliveryID), discoverDeliveryPrefix)
}

// RequestReview handles a Review press on a PR card. It queues at most one
// review: a review already running or queued, a head that was already
// reviewed, a terminal PR or an unreadable PR state each refuse with one
// line in the PR thread. A review queued behind a fix leaves its head open,
// so it reviews whatever the fix pushed and the fix's own re-review of that
// head is deduped against it.
func (s *Server) RequestReview(ctx context.Context, msg channel.IncomingMessage, repo, branch string) error {
	prKey, ok := "", false
	if s.tasks != nil {
		prKey, ok = s.tasks.PRKeyForTask(TaskThreadKey(repo, branch))
	}
	outcome, text := s.requestReview(ctx, msg, prKey, ok, repo, branch)
	slog.Info("webhook: review button",
		"pr_key", prKey,
		"repo", repo,
		"branch", branch,
		"user_id", msg.UserID,
		"outcome", outcome,
	)
	return s.replyTaskAction(ctx, msg, prKey, text)
}

func (s *Server) requestReview(ctx context.Context, msg channel.IncomingMessage, prKey string, known bool, repo, branch string) (string, string) {
	if !s.operatorAllowed(msg.Platform, msg.UserID) {
		return "not_allowed", "⛔ Not allowed."
	}
	if !known || strings.TrimSpace(prKey) == "" {
		return "no_pr", fmt.Sprintf("No open PR for %s#%s yet.", repo, branch)
	}
	state, err := s.newPRStateDecision(prKey).resolve(ctx)
	if err != nil || state == "" {
		return "state_unknown", fmt.Sprintf("⚠️ Could not read the live state of %s — try again.", prKey)
	}
	if state == PRStateMerged || state == PRStateClosed {
		return "terminal", fmt.Sprintf("⛔ %s is %s — nothing to review.", prKey, state)
	}
	ep, ok := s.reviewEndpointForPR(prKey)
	if !ok {
		return "refused", fmt.Sprintf("⚠️ Review not queued for %s: no review endpoint for this repository.", prKey)
	}

	// Check and enqueue under one lock, so two presses cannot both queue.
	s.reviewButtonMu.Lock()
	defer s.reviewButtonMu.Unlock()

	busy := s.prDispatchState(prKey)
	if busy.reviewActive {
		return "running", fmt.Sprintf("👀 A review of %s is already running.", prKey)
	}
	if busy.reviewQueued {
		return "already_queued", fmt.Sprintf("👀 A review of %s is already queued.", prKey)
	}
	if !busy.fixActive && !busy.fixQueued {
		if head := s.livePRHeadSHA(ctx, prKey); head != "" && s.headAlreadyReviewed(ctx, ep.Name, prKey, head) {
			return "already_reviewed", fmt.Sprintf("✅ %s@%s was already reviewed — push a commit, or use /retry in the thread to force a review.", prKey, shortRevision(head))
		}
	}

	trigger := s.reviewTrigger(ep)
	if trigger == "" {
		return "refused", fmt.Sprintf("⚠️ Review not queued for %s: the review endpoint has no comment trigger.", prKey)
	}
	prRepo, number, _ := splitPRKey(prKey)
	prBranch := s.branchForPR(prKey)
	if prBranch == "" {
		prBranch = strings.TrimSpace(branch)
	}
	body := internalReviewCommentBody(prRepo, number, trigger, s.prURLForPR(prKey), prBranch, "")
	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  fmt.Sprintf("%s%d", reviewButtonDeliveryPrefix, time.Now().UnixNano()),
		eventType:   "issue_comment",
		payloadHash: payloadHashOf(body),
	}
	key := WebhookExecutionKey{Repository: prRepo, Branch: prBranch}
	if _, admitted, why := s.enqueue(ep, key, item); !admitted {
		return "refused", fmt.Sprintf("⚠️ Review not queued for %s: %s.", prKey, why)
	}
	text := fmt.Sprintf("⏳ Review queued for %s.", prKey)
	switch {
	case busy.activeKind != "":
		text += fmt.Sprintf(" It runs after the current %s run.", busy.activeKind)
	case busy.queued > 0:
		text += " It runs after the queued work."
	}
	return "queued", text
}

// prDispatchBusy is what the PR's dispatcher is doing at the moment of a press.
type prDispatchBusy struct {
	activeKind   string
	reviewActive bool
	reviewQueued bool
	fixActive    bool
	fixQueued    bool
	queued       int
}

func (s *Server) prDispatchState(prKey string) prDispatchBusy {
	var busy prDispatchBusy
	s.dispatchMu.Lock()
	d, ok := s.dispatchers["pr:"+prKey]
	if ok && d.exiting {
		ok = false
	}
	s.dispatchMu.Unlock()
	if !ok {
		return busy
	}
	snap := d.snapshot()
	reviewLane := laneOfWorkflow("review")
	fixLane := laneOfWorkflow("fix")
	if snap.hasActive {
		kind := strings.ToLower(strings.TrimSpace(snap.active.ep.Workflow))
		if kind == "" {
			kind = "agent"
		}
		busy.activeKind = kind
		busy.reviewActive = laneOfWorkflow(kind) == reviewLane
		busy.fixActive = laneOfWorkflow(kind) == fixLane
	}
	busy.reviewQueued = d.laneLen(reviewLane) > 0
	busy.fixQueued = d.laneLen(fixLane) > 0
	busy.queued = snap.queueLen
	return busy
}

// headAlreadyReviewed reports a review delivery that already ran, or is
// running, on this head inside the dedupe window — the same lookup the
// review lane uses to skip a duplicate.
func (s *Server) headAlreadyReviewed(ctx context.Context, endpoint, prKey, head string) bool {
	if s.deliveries == nil {
		return false
	}
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return false
	}
	reviewKey := normalizeRepoKey(repo) + "#" + strings.TrimSpace(number) + "@" + normalizeRevision(head)
	prior, err := s.deliveries.FindReviewDuplicate(ctx, endpoint, reviewKey, 0, s.reviewDedupeCutoff())
	if err != nil {
		slog.Warn("webhook: review button duplicate lookup failed", "pr_key", prKey, "error", err)
		return false
	}
	return prior != nil
}
