package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

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
	s.reviewButtonMu.Lock()
	defer s.reviewButtonMu.Unlock()
	out, err := s.FireAction(ctx, msg.Platform+":"+msg.UserID, FireActionRequest{
		WorkItemID: prKey,
		Action:     ActionReReview,
	})
	if err != nil {
		slog.Warn("webhook: review action failed", "pr_key", prKey, "error", err)
		return "failed", fmt.Sprintf("⚠️ Review not queued for %s.", prKey)
	}
	if !out.Accepted {
		return out.Code, "⚠️ " + out.Error()
	}
	return "queued", "⏳ " + out.Sentence
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
