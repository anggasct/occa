package webhook

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/store"
)

const (
	keptWhileBlockedDetail = "kept while blocked"
	duplicateDetail        = "duplicate delivery"
	configuredSkipDetail   = "configured event skip"
	operatorCancelDetail   = "cancelled by operator"
	receiptGoneDetail      = "receipt gone"
)

func (r *stageRecorder) reserve(key string) *pendingSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.queued[key]; ok {
		return nil
	}
	slot := &pendingSlot{decided: make(chan struct{})}
	r.queued[key] = slot
	return slot
}

func (r *stageRecorder) unqueue(key string) *pendingSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := r.queued[key]
	delete(r.queued, key)
	return slot
}

func (r *stageRecorder) settleQueued(ctx context.Context, key string, slot *pendingSlot, admitted bool, start store.StageRunStart, endpoint, deliveryID string) {
	defer close(slot.decided)
	if !admitted {
		r.mu.Lock()
		if r.queued[key] == slot {
			delete(r.queued, key)
		}
		r.mu.Unlock()
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.done = true
	if !admitted {
		return
	}
	if err := r.ensureWorkItem(ctx, start.WorkItemID); err != nil {
		r.warn("queue", endpoint, start.WorkItemID, deliveryID, err)
		return
	}
	id, err := r.repo.QueueRun(ctx, start)
	if err != nil {
		r.warn("queue", endpoint, start.WorkItemID, deliveryID, err)
		return
	}
	if slot.outcome != nil {
		if err := r.repo.EndPending(ctx, id, slot.outcome.state, slot.outcome.detail); err != nil && !errors.Is(err, store.ErrStageRunNotPending) {
			r.warn("end pending", endpoint, start.WorkItemID, deliveryID, err)
		}
		return
	}
	slot.runID = id
}

// takePending hands the delivery's pending run to its start. The start waits
// for the admission decision, so a run that was about to be written pending
// is promoted instead of being followed by a second attempt.
func (r *stageRecorder) takePending(key string) int64 {
	slot := r.unqueue(key)
	if slot == nil {
		return 0
	}
	<-slot.decided
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.runID
}

func (r *stageRecorder) promoteOrStart(ctx context.Context, pendingID int64, start store.StageRunStart) (int64, error) {
	if pendingID != 0 {
		err := r.repo.PromotePending(ctx, pendingID, start.WorkspacePath)
		if err == nil {
			return pendingID, nil
		}
		if !errors.Is(err, store.ErrStageRunNotPending) && !errors.Is(err, store.ErrTerminalAlreadySet) {
			return 0, err
		}
	}
	return r.repo.StartRun(ctx, start)
}

// endPending ends the pending run of a queued delivery that will not run. It
// never waits: an end that arrives before the admission decision is applied
// by the writer.
func (r *stageRecorder) endPending(ctx context.Context, endpoint, deliveryID string, state store.StageState, detail string) {
	if r == nil {
		return
	}
	slot := r.unqueue(stageRunKey(endpoint, deliveryID))
	if slot == nil {
		return
	}
	slot.mu.Lock()
	if !slot.done {
		slot.outcome = &pendingOutcome{state: state, detail: detail}
		slot.mu.Unlock()
		return
	}
	id := slot.runID
	slot.runID = 0
	slot.mu.Unlock()
	if id == 0 {
		return
	}
	if err := r.repo.EndPending(ctx, id, state, detail); err != nil && !errors.Is(err, store.ErrStageRunNotPending) {
		r.warn("end pending", endpoint, "", deliveryID, err)
	}
}

func (r *stageRecorder) closePending(ctx context.Context, run store.StageRun, detail string) bool {
	if endpoint, deliveryID, ok := parseStageTrigger(run.TriggeredBy); ok {
		r.unqueue(stageRunKey(endpoint, deliveryID))
	}
	if err := r.repo.EndPending(ctx, run.ID, store.StageStateSkipped, detail); err != nil {
		if !errors.Is(err, store.ErrStageRunNotPending) && !errors.Is(err, store.ErrTerminalAlreadySet) {
			r.warn("close pending", "", run.WorkItemID, "", err)
		}
		return false
	}
	return true
}

func parseStageTrigger(triggeredBy string) (string, string, bool) {
	rest, ok := strings.CutPrefix(triggeredBy, stageTriggerPrefix)
	if !ok {
		return "", "", false
	}
	endpoint, deliveryID, ok := strings.Cut(rest, "/")
	if !ok || endpoint == "" || deliveryID == "" {
		return "", "", false
	}
	return endpoint, deliveryID, true
}

// queuePending records a stage request that just entered its PR queue. The
// admission check may ask GitHub for the PR author, so it runs off the
// request path; the caller holds dispatchMu, which orders the reservation
// before any start of the same delivery.
func (s *Server) queuePending(item dispatchItem) {
	rec := s.stages
	if rec == nil || strings.TrimSpace(item.prKey) == "" {
		return
	}
	stage, ok := stageForWorkflow(item.ep.Workflow)
	if !ok {
		return
	}
	key := stageRunKey(item.ep.Name, item.deliveryID)
	slot := rec.reserve(key)
	if slot == nil {
		return
	}
	start := store.StageRunStart{WorkItemID: item.prKey, Stage: stage, TriggeredBy: stageTriggerPrefix + key}
	s.dispatchWG.Add(1)
	go func() {
		defer s.dispatchWG.Done()
		envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
		rec.settleQueued(context.Background(), key, slot, s.wouldAdmit(item, envelope), start, item.ep.Name, item.deliveryID)
	}()
}

// sweepPendingRuns ends pending runs whose delivery is settled or gone, so a
// missed hook cannot leave a run pending forever.
func (s *Server) sweepPendingRuns(ctx context.Context) {
	if s.stages == nil || s.deliveries == nil {
		return
	}
	runs, err := s.stages.repo.ListPending(ctx, pendingSweepLimit)
	if err != nil {
		slog.Warn("webhook: pending run sweep failed", "error", err)
		return
	}
	if len(runs) == 0 {
		return
	}
	live := s.liveDeliveries()
	for _, run := range runs {
		if ctx.Err() != nil {
			return
		}
		detail := receiptGoneDetail
		if endpoint, deliveryID, ok := parseStageTrigger(run.TriggeredBy); ok {
			if live[deliveryRef{endpoint, deliveryID}] {
				continue
			}
			receipt, err := s.deliveries.Get(ctx, endpoint, deliveryID)
			if err != nil {
				slog.Warn("webhook: pending run receipt lookup failed", "stage_run_id", run.ID, "endpoint", endpoint, "delivery_id", deliveryID, "error", err)
				continue
			}
			if receipt != nil {
				if !receiptSettled(receipt.Status) {
					continue
				}
				detail = receipt.ErrorSummary
				if strings.TrimSpace(detail) == "" {
					detail = string(receipt.Status)
				}
			}
		}
		if s.stages.closePending(ctx, run, detail) {
			slog.Info("webhook: pending run closed", "stage_run_id", run.ID, "work_item_id", run.WorkItemID, "stage", string(run.Stage), "detail", detail)
		}
	}
}

func receiptSettled(status store.WebhookStatus) bool {
	switch status {
	case store.WebhookStatusCompleted, store.WebhookStatusSkipped, store.WebhookStatusFailed, store.WebhookStatusRejected:
		return true
	}
	return false
}
