package webhook

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const (
	interruptedByRestart = "interrupted by restart"
	appRestartedDetail   = "app restarted"
	stageTriggerPrefix   = "delivery:"
)

// stageRecorder writes one stage run per executed lane attempt. It only
// records: every store error is logged and the delivery goes on unchanged.
type stageRecorder struct {
	repo store.StageRepo

	mu      sync.Mutex
	byRun   map[string]*stageAttempt
	byPRKey map[string]*stageAttempt
}

type stageAttempt struct {
	runID      int64
	prKey      string
	endpoint   string
	deliveryID string
	block      *stageBlock
}

type stageBlock struct {
	code   store.BlockerCode
	detail string
}

func newStageRecorder(repo store.StageRepo) *stageRecorder {
	return &stageRecorder{repo: repo, byRun: map[string]*stageAttempt{}, byPRKey: map[string]*stageAttempt{}}
}

func stageForWorkflow(workflow string) (store.Stage, bool) {
	switch {
	case isReviewWorkflow(workflow):
		return store.StageReview, true
	case isFixWorkflow(workflow):
		return store.StageFix, true
	case strings.EqualFold(strings.TrimSpace(workflow), "merge"):
		return store.StageMerge, true
	}
	return "", false
}

func stageRunKey(endpoint, deliveryID string) string {
	return endpoint + "/" + deliveryID
}

// blockerCodeFor maps a stored block reason to its blocker code. The checks
// follow describeBlock so the code and the operator copy never disagree.
func blockerCodeFor(reason string) (store.BlockerCode, bool) {
	reason = strings.TrimSpace(reason)
	switch {
	case strings.Contains(reason, " made no change"):
		return store.BlockerAgentNoCommit, true
	case strings.Contains(reason, " stopped on a provider limit"):
		return store.BlockerProviderLimit, true
	case strings.Contains(reason, " stopped on a denied permission"):
		return store.BlockerAuthDenied, true
	case strings.Contains(reason, " could not get its worktree"):
		return store.BlockerWorkspaceDirty, true
	case strings.HasPrefix(reason, "live PR state unresolvable"):
		return store.BlockerGitHubUnreachable, true
	case strings.HasPrefix(reason, "auto-fix storm cap"):
		return store.BlockerRetriesExhausted, true
	case strings.HasPrefix(reason, "merge starved"):
		return store.BlockerMergeStarved, true
	case reasonRetriesRe.MatchString(reason):
		return store.BlockerRetriesExhausted, true
	case reason == interruptedByRestart:
		return store.BlockerTimeout, true
	}
	return "", false
}

func (r *stageRecorder) warn(op, endpoint, prKey, deliveryID string, err error) {
	slog.Warn("stage: record failed", "op", op, "endpoint", endpoint, "pr_key", prKey, "delivery_id", deliveryID, "error", err)
}

func (r *stageRecorder) start(ctx context.Context, ep config.EndpointConfig, workCtx *WebhookWorkContext) {
	if r == nil || workCtx == nil {
		return
	}
	stage, ok := stageForWorkflow(ep.Workflow)
	prKey := strings.TrimSpace(workCtx.PRKey)
	if !ok || prKey == "" {
		return
	}
	repo, number, _ := splitPRKey(prKey)
	n, err := strconv.Atoi(number)
	if err != nil {
		r.warn("start", ep.Name, prKey, workCtx.DeliveryID, err)
		return
	}
	if err := r.repo.UpsertWorkItem(ctx, store.WorkItem{ID: prKey, Repo: repo, Number: n, Kind: store.WorkItemPullRequest}); err != nil {
		r.warn("start", ep.Name, prKey, workCtx.DeliveryID, err)
		return
	}
	runID, err := r.repo.StartRun(ctx, store.StageRunStart{
		WorkItemID:    prKey,
		Stage:         stage,
		TriggeredBy:   stageTriggerPrefix + stageRunKey(ep.Name, workCtx.DeliveryID),
		WorkspacePath: workCtx.Worktree,
	})
	if err != nil {
		r.warn("start", ep.Name, prKey, workCtx.DeliveryID, err)
		return
	}
	a := &stageAttempt{runID: runID, prKey: prKey, endpoint: ep.Name, deliveryID: workCtx.DeliveryID}
	r.mu.Lock()
	r.byRun[stageRunKey(ep.Name, workCtx.DeliveryID)] = a
	r.byPRKey[prKey] = a
	r.mu.Unlock()
}

func (r *stageRecorder) noteAgent(ctx context.Context, endpoint string, workCtx *WebhookWorkContext) {
	if r == nil || workCtx == nil || (workCtx.SessionID == "" && workCtx.AgentPID == 0) {
		return
	}
	r.mu.Lock()
	a := r.byRun[stageRunKey(endpoint, workCtx.DeliveryID)]
	r.mu.Unlock()
	if a == nil {
		return
	}
	if err := r.repo.SetRunAgent(ctx, a.runID, workCtx.SessionID, workCtx.AgentPID); err != nil {
		r.warn("agent", a.endpoint, a.prKey, a.deliveryID, err)
	}
}

// block notes the blocker of the PR's running attempt. It is written when the
// attempt ends as a failure, so an attempt that still completes stays done.
func (r *stageRecorder) block(prKey, reason string) {
	if r == nil {
		return
	}
	code, ok := blockerCodeFor(reason)
	r.mu.Lock()
	a := r.byPRKey[strings.TrimSpace(prKey)]
	if a != nil {
		a.block = &stageBlock{code: code, detail: reason}
	}
	r.mu.Unlock()
	if a != nil && !ok {
		slog.Warn("stage: block reason has no blocker code", "endpoint", a.endpoint, "pr_key", a.prKey, "delivery_id", a.deliveryID, "reason", reason)
	}
}

func (r *stageRecorder) finish(ctx context.Context, endpoint, deliveryID string, state store.StageState, detail string) {
	if r == nil {
		return
	}
	key := stageRunKey(endpoint, deliveryID)
	r.mu.Lock()
	a := r.byRun[key]
	if a != nil {
		delete(r.byRun, key)
		if r.byPRKey[a.prKey] == a {
			delete(r.byPRKey, a.prKey)
		}
	}
	r.mu.Unlock()
	if a == nil {
		return
	}
	var err error
	if state == store.StageStateFailed && a.block != nil {
		err = r.repo.MarkBlocked(ctx, a.runID, a.block.code, a.block.detail)
	} else {
		err = r.repo.EndRun(ctx, a.runID, state, detail)
	}
	if err != nil {
		r.warn("finish", a.endpoint, a.prKey, a.deliveryID, err)
	}
}

func (r *stageRecorder) closeWorkItem(ctx context.Context, prKey string, state store.WorkItemState) {
	if r == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	if err := r.repo.SetWorkItemState(ctx, strings.TrimSpace(prKey), state); err != nil && !errors.Is(err, store.ErrNotFound) {
		r.warn("work item", "", prKey, "", err)
	}
}

// recoverInterrupted blocks the runs whose delivery the restart recovery
// failed: their agent died with the previous process, so nothing re-runs them.
func (r *stageRecorder) recoverInterrupted(ctx context.Context, deliveries DeliveryStore) {
	if r == nil || deliveries == nil {
		return
	}
	runs, err := r.repo.ListRunning(ctx)
	if err != nil {
		r.warn("recover", "", "", "", err)
		return
	}
	for _, run := range runs {
		endpoint, deliveryID, ok := strings.Cut(strings.TrimPrefix(run.TriggeredBy, stageTriggerPrefix), "/")
		if !ok {
			continue
		}
		receipt, err := deliveries.Get(ctx, endpoint, deliveryID)
		if err != nil {
			r.warn("recover", endpoint, run.WorkItemID, deliveryID, err)
			continue
		}
		if receipt == nil || receipt.Status != store.WebhookStatusFailed || receipt.ErrorSummary != interruptedByRestart {
			continue
		}
		code, _ := blockerCodeFor(interruptedByRestart)
		if err := r.repo.MarkBlocked(ctx, run.ID, code, appRestartedDetail); err != nil {
			r.warn("recover", endpoint, run.WorkItemID, deliveryID, err)
		}
	}
}
