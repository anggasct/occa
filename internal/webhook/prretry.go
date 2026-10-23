package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type failureClass int

const (
	failureModel failureClass = iota
	failureStall
	failureBuild
)

func (c failureClass) label() string {
	switch c {
	case failureStall:
		return "provider stall"
	case failureBuild:
		return "build/test failure"
	default:
		return "model error"
	}
}

func retryPolicyFor(class failureClass) (int, []time.Duration) {
	switch class {
	case failureStall:
		return 1, []time.Duration{0}
	case failureBuild:
		return 2, []time.Duration{60 * time.Second, 5 * time.Minute}
	default:
		return 3, []time.Duration{30 * time.Second, 2 * time.Minute, 5 * time.Minute}
	}
}

func (s *Server) retryPolicyFor(class failureClass) (int, []time.Duration) {
	if s.retryBackoffs != nil {
		if backoffs, ok := s.retryBackoffs[class]; ok {
			max, _ := retryPolicyFor(class)
			return max, backoffs
		}
	}
	return retryPolicyFor(class)
}

func isStallProgress(progress relay.TurnProgress, stallFreshness time.Duration) bool {
	if progress.DeltaCount == 0 {
		return true
	}
	if stallFreshness > 0 && !progress.LastDeltaAt.IsZero() && time.Since(progress.LastDeltaAt) >= stallFreshness {
		return true
	}
	return false
}

func classifyFailure(execErr error, workCtx *WebhookWorkContext, stallFreshness time.Duration, timedOut bool) failureClass {
	if timedOut {
		var progress relay.TurnProgress
		if workCtx != nil {
			progress = workCtx.Progress
		}
		if isStallProgress(progress, stallFreshness) {
			return failureStall
		}
		return failureBuild
	}
	if errors.Is(execErr, relay.ErrStreamFailed) ||
		errors.Is(execErr, relay.ErrStreamRead) ||
		errors.Is(execErr, relay.ErrWebhookEventStream) ||
		errors.Is(execErr, relay.ErrWebhookPrompt) ||
		errors.Is(execErr, relay.ErrWebhookAgentResponse) ||
		errors.Is(execErr, relay.ErrUnreachable) ||
		errors.Is(execErr, relay.ErrTimeout) {
		return failureModel
	}
	return failureBuild
}

var errRetryScheduled = errors.New("webhook: retry scheduled")
var errOpSkipped = errors.New("webhook: skipped by operator")
var errOpCancelled = errors.New("webhook: cancelled by operator")

func (s *Server) dispatcherFor(prKey string) *dispatcher {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	return s.dispatchers["pr:"+strings.TrimSpace(prKey)]
}

// blockedQueueRow returns the queue row of a PR whose queue is blocked. The
// row carries the block reason and the kind that failed, so a surface can
// name the PR and the reason instead of a generic phrase.
func (s *Server) blockedQueueRow(ctx context.Context, prKey string) (*store.PRRun, error) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return nil, nil
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil {
		return nil, err
	}
	if row.Status != store.PRStatusBlocked {
		return nil, nil
	}
	return row, nil
}

type retryWake int

const (
	wakeNone retryWake = iota
	wakeFire
	wakeSkip
	wakeCancel
	wakeShutdown
)

func (d *dispatcher) requestSkip() {
	d.cmdMu.Lock()
	d.skipRequested = true
	d.cmdMu.Unlock()
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *dispatcher) requestCancel() {
	d.cmdMu.Lock()
	d.cancelRequested = true
	d.cmdMu.Unlock()
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *dispatcher) requestRetryNow() {
	d.cmdMu.Lock()
	d.retryRequested = true
	d.cmdMu.Unlock()
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *dispatcher) isBackingOff() bool {
	d.cmdMu.Lock()
	defer d.cmdMu.Unlock()
	return d.backingOff
}

func (d *dispatcher) isActive() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hasActive
}

func (d *dispatcher) takeIntent() retryWake {
	d.cmdMu.Lock()
	defer d.cmdMu.Unlock()
	switch {
	case d.cancelRequested:
		d.cancelRequested = false
		d.skipRequested = false
		d.retryRequested = false
		return wakeCancel
	case d.skipRequested:
		d.skipRequested = false
		d.retryRequested = false
		return wakeSkip
	case d.retryRequested:
		d.retryRequested = false
		return wakeFire
	default:
		return wakeNone
	}
}

func (d *dispatcher) sleepForRetry(prKey string, shutdownCtx context.Context) retryWake {
	d.cmdMu.Lock()
	d.backingOff = true
	d.cmdMu.Unlock()
	defer func() {
		d.cmdMu.Lock()
		d.backingOff = false
		d.cmdMu.Unlock()
	}()
	for {
		if op := d.takeIntent(); op != wakeNone {
			return op
		}
		var wait time.Duration
		if d.server.prQueues != nil {
			if row, err := d.server.prQueues.Get(context.Background(), prKey); err == nil && row != nil {
				wait = time.Until(time.Unix(row.NextRetryAt, 0))
			}
		}
		if wait <= 0 {
			return wakeFire
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-d.notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-shutdownCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return wakeShutdown
		case <-d.server.shutdownCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return wakeShutdown
		}
	}
}

func escalateReason(kind string, class failureClass, used, max int) string {
	return fmt.Sprintf("%s exhausted after %d/%d retries (%s) — operator action required", kind, used, max, class.label())
}

func (s *Server) escalateBlocked(ep config.EndpointConfig, prKey string, row *store.PRRun, class failureClass, used, max int) {
	s.escalateBlockedReason(ep, prKey, row, class, used, max, "", "")
}

func (s *Server) escalateBlockedReason(ep config.EndpointConfig, prKey string, row *store.PRRun, class failureClass, used, max int, reason, why string) {
	kind := strings.TrimSpace(row.ActiveKind)
	if kind == "" {
		kind = "run"
	}
	if reason == "" {
		reason = escalateReason(kind, class, used, max)
	}
	if row.Status == store.PRStatusBlocked {
		return
	}
	if err := s.prQueues.SetBlocked(context.Background(), prKey, reason); err != nil {
		slog.Warn("webhook: block mark failed", "pr_key", prKey, "error", err)
		return
	}
	if !s.markEscalated(prKey) {
		return
	}
	// A new blocking episode freezes the parent again: the operator has to
	// act before the parent leaves the blocked phase.
	s.tasks.ClearReopen(prKey)
	threadID := strings.TrimSpace(row.ThreadID)
	repo, prNumber, _ := splitPRKey(prKey)
	if threadID == "" {
		if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
			threadID = strings.TrimSpace(rec.ThreadID)
		}
	}
	ctx := context.Background()
	what := fmt.Sprintf("%s run failed %d/%d times", kind, used, max)
	if why == "" {
		why = class.label()
	}
	envelope := WebhookEnvelope{"repository": repo, "pr_number": prNumber}
	if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
		if strings.TrimSpace(rec.Branch) != "" {
			envelope["head_branch"] = rec.Branch
		}
	}
	s.syncPhaseParent(ctx, ep, prKey, envelope, PhaseBlocked)
	if threadID != "" {
		s.ensureThreadStatus(ctx, ep.Platform, ep.ChannelID, threadID, repo, "", prKey, ThreadStatusUpdate{
			Phase:  PhaseBlocked,
			Action: blockedQueueText(prKey, reason, blockedQueueCommand),
		})
		threadLine := FormatEscalationCopy(repo, prNumber, PhaseBlocked, what, why, s.statusIcons)
		target := threadTargetFor(ep.Platform, ep.ChannelID, threadID)
		if err := s.notifyThread(ctx, ep.Platform, target, threadLine); err != nil {
			slog.Warn("webhook: escalation thread line failed", "endpoint", ep.Name, "pr_key", prKey, "error", err)
		}
	}
	s.escalateToChannel(ctx, ep, repo, prNumber, PhaseBlocked, what, why)
	slog.Error("webhook: PR blocked after retry exhaustion",
		"endpoint", ep.Name,
		"pr_key", prKey,
		"kind", kind,
		"retries", fmt.Sprintf("%d/%d", used, max),
		"class", class.label(),
	)
}

// planRetry classifies an executor failure for a PR run. It returns true when
// a backoff retry was persisted and the caller should re-execute; on retry
// exhaustion it escalates (blocked + mention) and returns false so the normal
// failure path records the terminal receipt. A run the watchdog already
// killed is never scheduled again: the live PR state is still resolved so a
// terminal PR is reported as refused. The live state is resolved for this
// decision only, so a later decision sees the PR as it is now.
func (s *Server) planRetry(ep config.EndpointConfig, prKey string, receiptID int64, execErr error, workCtx *WebhookWorkContext, timedOut, runDead bool) bool {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return false
	}
	if s.shutdownCtx != nil {
		select {
		case <-s.shutdownCtx.Done():
			return false
		default:
		}
	}
	class := classifyFailure(execErr, workCtx, s.stallFreshness, timedOut)
	max, backoffs := s.retryPolicyFor(class)
	ctx := context.Background()
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil {
		return false
	}
	state, stateErr := s.newPRStateDecision(prKey).resolve(ctx)
	switch {
	case stateErr != nil || state == "":
		slog.Error("webhook: retry refused, PR state unresolvable",
			"endpoint", ep.Name,
			"pr_key", prKey,
			"error", stateErr,
		)
		if runDead {
			return false
		}
		s.escalateBlockedReason(ep, prKey, row, class, row.RetryCount, max,
			"live PR state unresolvable — retry refused (fail-closed)", "live PR state unknown")
		return false
	case state == PRStateMerged || state == PRStateClosed:
		s.refuseRetryForTerminalPR(ep, prKey, receiptID, state)
		return false
	}
	if runDead {
		slog.Error("webhook: retry not scheduled, run aborted by watchdog",
			"endpoint", ep.Name,
			"pr_key", prKey,
			"pr_state", state,
		)
		return false
	}
	if row.RetryCount >= max {
		s.escalateBlocked(ep, prKey, row, class, row.RetryCount, max)
		return false
	}
	wait := backoffs[0]
	if row.RetryCount < len(backoffs) {
		wait = backoffs[row.RetryCount]
	}
	fireAt := time.Now().Add(wait).Unix()
	if err := s.prQueues.SetRetryState(ctx, prKey, row.RetryCount+1, fireAt); err != nil {
		slog.Warn("webhook: retry persist failed", "endpoint", ep.Name, "pr_key", prKey, "error", err)
		return false
	}
	if err := s.deliveries.BumpAttempt(ctx, receiptID); err != nil {
		slog.Warn("webhook: retry bump failed", "endpoint", ep.Name, "pr_key", prKey, "error", err)
	}
	slog.Info("webhook: retry scheduled",
		"endpoint", ep.Name,
		"pr_key", prKey,
		"class", class.label(),
		"retry", fmt.Sprintf("%d/%d", row.RetryCount+1, max),
		"backoff", wait.String(),
	)
	return true
}

// retryStillAllowed re-resolves the live PR state before a retry attempt
// starts: the PR may have reached a terminal state while the attempt waited
// for its backoff. An unresolvable state fails closed — the attempt is
// refused and the PR is escalated. A retry that cannot read the PR state may
// start a second agent turn on a merged PR, and that is the incident this
// guard exists to stop.
func (s *Server) retryStillAllowed(ep config.EndpointConfig, prKey string, receiptID int64) bool {
	state, err := s.newPRStateDecision(prKey).resolve(context.Background())
	if err != nil || state == "" {
		slog.Error("webhook: retry refused, PR state unresolvable",
			"endpoint", ep.Name,
			"pr_key", prKey,
			"error", err,
		)
		s.refuseRetryUnresolvableState(ep, prKey, receiptID)
		return false
	}
	if state == PRStateMerged || state == PRStateClosed {
		s.refuseRetryForTerminalPR(ep, prKey, receiptID, state)
		return false
	}
	return true
}

// refuseRetryUnresolvableState fails closed: the attempt is skipped and the
// PR is escalated, so an unreadable PR state never starts a second agent turn.
func (s *Server) refuseRetryUnresolvableState(ep config.EndpointConfig, prKey string, receiptID int64) {
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(context.Background(), prKey); err == nil && row != nil {
			max, _ := s.retryPolicyFor(failureModel)
			s.escalateBlockedReason(ep, prKey, row, failureModel, row.RetryCount, max,
				"live PR state unresolvable — retry refused (fail-closed)", "live PR state unknown")
		}
	}
	s.skipDeliveryQuiet(ep, receiptID, "PR state unresolvable — retry refused (fail-closed)")
}

// refuseRetryForTerminalPR stops a retry whose PR already reached a terminal
// state: the delivery is skipped and the queue is drained, so no second run
// can write to a merged or closed PR.
func (s *Server) refuseRetryForTerminalPR(ep config.EndpointConfig, prKey string, receiptID int64, state string) {
	slog.Error("webhook: retry refused, PR terminal",
		"endpoint", ep.Name,
		"pr_key", prKey,
		"pr_state", state,
	)
	if d := s.dispatcherFor(prKey); d != nil {
		s.drainPRQueue(d, prKey)
	}
	s.skipDeliveryQuiet(ep, receiptID, "PR already "+state)
}

func (s *Server) skipDeliveryQuiet(ep config.EndpointConfig, id int64, reason string) {
	if id == 0 {
		return
	}
	ok, err := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusSkipped, reason)
	if err != nil {
		slog.Error("webhook: skip transition failed", "endpoint", ep.Name, "delivery_id", id, "error", err)
		return
	}
	if !ok {
		slog.Info("webhook: skip transition skipped, delivery not processing", "endpoint", ep.Name, "delivery_id", id)
	}
}

// RecoverPRQueue reconciles persisted PR-queue state after a restart without
// re-executing anything: queued-but-unexecuted items cannot run because the
// delivery bodies live only in memory, so they are dropped with a count.
// Active rows become drained with a resume marker so /retry can rebuild them
// from live PR state instead of dead-ending on an empty active kind.
func (s *Server) RecoverPRQueue() string {
	if s.prQueues == nil {
		return "pr queue recovery skipped (no store)"
	}
	ctx := context.Background()
	runs, err := s.prQueues.ListUnresolved(ctx)
	if err != nil {
		slog.Error("webhook: pr queue recovery list failed", "error", err)
		return "pr queue recovery failed: " + err.Error()
	}
	var active, failed, dropped, blocked, resumed int
	for i := range runs {
		run := &runs[i]
		if run.Status == store.PRStatusBlocked {
			blocked++
			continue
		}
		active++
		droppedHere := 0
		if n, err := s.prQueues.ClearQueue(ctx, run.PRKey); err != nil {
			slog.Warn("webhook: pr queue recovery clear failed", "pr_key", run.PRKey, "error", err)
		} else {
			droppedHere = n
			dropped += n
		}
		kind := s.inferRetryKind(ctx, run)
		if run.ActiveDelivery != 0 {
			if rec, rerr := s.deliveries.GetByID(ctx, run.ActiveDelivery); rerr == nil && rec != nil && rec.Status == store.WebhookStatusProcessing {
				if _, terr := s.deliveries.Transition(ctx, rec.ID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, "interrupted by restart"); terr == nil {
					failed++
				}
			}
		}
		if serr := s.prQueues.SetResumePending(ctx, run.PRKey, kind, run.ActiveDelivery); serr != nil {
			slog.Warn("webhook: pr queue recovery resume mark failed", "pr_key", run.PRKey, "error", serr)
			continue
		}
		resumed++
		slog.Info("webhook: pr recovery resume pending",
			"pr_key", run.PRKey,
			"kind", kind,
			"delivery_id", run.ActiveDelivery,
			"dropped", droppedHere,
		)
	}
	result := fmt.Sprintf("recovered %d active (resumed %d, failed %d), dropped %d queued, blocked %d", active, resumed, failed, dropped, blocked)
	slog.Warn("webhook: pr queue recovery", "result", result)
	return result
}
