package webhook

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/anggasct/occa/internal/store"
)

const (
	supersededWhileBlocked = "superseded by a newer request while blocked"
	expiredWhileBlocked    = "expired while blocked"
	blockedInputSweepLimit = 100
)

func blockedRunStage(activeKind, workflow string) store.Stage {
	if stage, ok := stageForWorkflow(activeKind); ok {
		return stage
	}
	stage, _ := stageForWorkflow(workflow)
	return stage
}

func (s *Server) wouldAdmit(item dispatchItem, envelope WebhookEnvelope) bool {
	if allowed, _ := admitDelivery(item.ep, s.policy, envelope); !allowed {
		return false
	}
	return s.automationAllows(item.ep, ExtractExecutionKey(item.body), envelope)
}

// attachBlockedInput keeps a stage request that arrived while the PR is
// blocked, so Retry can run it later. It reports false when the request must
// take the skip path instead; a store error fails closed to that path.
func (s *Server) attachBlockedInput(item dispatchItem, receipt *store.WebhookDelivery, row *store.PRRun, envelope WebhookEnvelope) bool {
	if s.stages == nil || receipt == nil {
		return false
	}
	if _, ok := stageForWorkflow(item.ep.Workflow); !ok || !s.wouldAdmit(item, envelope) {
		return false
	}
	ctx := context.Background()
	code, _ := blockerCodeFor(row.BlockedReason)
	in, added, err := s.stages.attach(ctx,
		store.StageRunBlock{WorkItemID: item.prKey, Stage: blockedRunStage(row.ActiveKind, item.ep.Workflow), Code: code, Detail: row.BlockedReason},
		store.StageInput{DeliveryID: item.deliveryID, Endpoint: item.ep.Name, EventType: item.eventType, Body: item.body})
	if err != nil {
		slog.Warn("webhook: blocked input attach failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "pr_key", item.prKey, "error", err)
		return false
	}
	if in.State != store.StageInputWaiting {
		slog.Warn("webhook: blocked input not kept", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "pr_key", item.prKey, "stored_state", string(in.State))
		return false
	}
	ok, err := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusReceived, "")
	if err != nil {
		slog.Warn("webhook: blocked input receipt release failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
	} else if !ok {
		slog.Warn("webhook: blocked input receipt release lost race", "endpoint", item.ep.Name, "delivery_id", item.deliveryID)
	}
	slog.Info("webhook: delivery kept while PR blocked",
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"pr_key", item.prKey,
		"stage_run_id", in.StageRunID,
		"stage_input_id", in.ID,
		"redelivery", !added,
	)
	s.stages.endPending(ctx, item.ep.Name, item.deliveryID, store.StageStateSkipped, keptWhileBlockedDetail)
	if added {
		s.noteBlockedKept(item, row, envelope)
	}
	return true
}

// runWaitingInputs re-dispatches the newest waiting input of each stage and
// supersedes the older ones, so a stale request never runs after a newer one.
func (s *Server) runWaitingInputs(ctx context.Context, prKey string) []store.Stage {
	if s.stages == nil {
		return nil
	}
	inputs, err := s.stages.repo.ListWaitingInputs(ctx, prKey)
	if err != nil {
		slog.Warn("webhook: blocked inputs lookup failed", "pr_key", prKey, "error", err)
		return nil
	}
	var ran []store.Stage
	seen := map[store.Stage]bool{}
	for _, in := range inputs {
		ep, found := s.endpointsByName[in.Endpoint]
		stage, isStage := stageForWorkflow(ep.Workflow)
		switch {
		case !found || !isStage:
			s.settleInput(ctx, in, store.StageInputSuperseded, "endpoint no longer configured")
			continue
		case seen[stage]:
			s.settleInput(ctx, in, store.StageInputSuperseded, supersededWhileBlocked)
			continue
		}
		seen[stage] = true
		if err := s.stages.repo.SetInputState(ctx, in.ID, store.StageInputConsumed); err != nil {
			if !errors.Is(err, store.ErrStageInputNotWaiting) {
				slog.Warn("webhook: blocked input consume failed", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "pr_key", prKey, "error", err)
			}
			continue
		}
		item := dispatchItem{
			ep:          ep,
			body:        in.Body,
			deliveryID:  in.DeliveryID,
			eventType:   in.EventType,
			prKey:       prKey,
			actionClass: stateActionClass(in.EventType, webhookAction(in.Body)),
			lane:        laneOfWorkflow(ep.Workflow),
		}
		if err := s.pushPRItem(item); err != nil {
			slog.Warn("webhook: blocked input dispatch deferred", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "pr_key", prKey, "error", err)
			s.recordRejection(ep, item, err.Error())
		}
		slog.Info("webhook: blocked input released", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "pr_key", prKey, "stage", string(stage))
		ran = append(ran, stage)
	}
	return ran
}

func (s *Server) supersedeWaitingInputs(ctx context.Context, prKey, reason string) {
	if s.stages == nil {
		return
	}
	inputs, err := s.stages.repo.ListWaitingInputs(ctx, prKey)
	if err != nil {
		slog.Warn("webhook: blocked inputs lookup failed", "pr_key", prKey, "error", err)
		return
	}
	for _, in := range inputs {
		s.settleInput(ctx, in, store.StageInputSuperseded, reason)
	}
}

// expireBlockedInputs drops requests kept longer than delivery retention: past
// that window the receipt behind them may already be pruned.
func (s *Server) expireBlockedInputs(ctx context.Context, now time.Time) {
	if s.stages == nil || s.retentionAge <= 0 {
		return
	}
	inputs, err := s.stages.repo.ListExpiredInputs(ctx, now.Add(-s.retentionAge).Unix(), blockedInputSweepLimit)
	if err != nil {
		slog.Warn("webhook: blocked input expiry failed", "error", err)
		return
	}
	for _, in := range inputs {
		if ctx.Err() != nil {
			return
		}
		if s.settleInput(ctx, in, store.StageInputExpired, expiredWhileBlocked) {
			slog.Info("webhook: blocked input expired", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "pr_key", in.WorkItemID)
		}
	}
}

func (s *Server) settleInput(ctx context.Context, in store.StageInput, state store.StageInputState, reason string) bool {
	if err := s.stages.repo.SetInputState(ctx, in.ID, state); err != nil {
		if !errors.Is(err, store.ErrStageInputNotWaiting) {
			slog.Warn("webhook: blocked input settle failed", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "pr_key", in.WorkItemID, "error", err)
		}
		return false
	}
	s.skipInputReceipt(ctx, in, reason)
	return true
}

func (s *Server) skipInputReceipt(ctx context.Context, in store.StageInput, reason string) {
	if s.deliveries == nil {
		return
	}
	receipt, err := s.deliveries.Get(ctx, in.Endpoint, in.DeliveryID)
	if err != nil {
		slog.Warn("webhook: blocked input receipt lookup failed", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "error", err)
		return
	}
	if receipt == nil {
		return
	}
	summary := reason
	if ep, ok := s.endpointsByName[in.Endpoint]; ok {
		envelope := normalizeWebhook(in.Body, in.EventType, in.DeliveryID, s.verdicts, false, "", ep.CommentTrigger)
		summary = redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "SKIP", reason), ep.Secret)
	}
	if _, err := s.deliveries.Transition(ctx, receipt.ID,
		[]store.WebhookStatus{store.WebhookStatusReceived},
		store.WebhookStatusSkipped, summary); err != nil {
		slog.Warn("webhook: blocked input receipt skip failed", "endpoint", in.Endpoint, "delivery_id", in.DeliveryID, "error", err)
	}
}
