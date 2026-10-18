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

func (s *Server) prQueueBlocked(prKey string) (bool, error) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return false, nil
	}
	row, err := s.prQueues.Get(context.Background(), prKey)
	if err != nil || row == nil {
		return false, err
	}
	return row.Status == store.PRStatusBlocked, nil
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
	kind := strings.TrimSpace(row.ActiveKind)
	if kind == "" {
		kind = "run"
	}
	reason := escalateReason(kind, class, used, max)
	if row.Status == store.PRStatusBlocked {
		return
	}
	if err := s.prQueues.SetBlocked(context.Background(), prKey, reason); err != nil {
		slog.Warn("webhook: block mark failed", "pr_key", prKey, "error", err)
		return
	}
	threadID := strings.TrimSpace(row.ThreadID)
	if threadID == "" {
		if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
			threadID = strings.TrimSpace(rec.ThreadID)
		}
	}
	mention := strings.TrimSpace(s.escalationMention)
	statusLine := "🔴 BLOCKED — " + reason + fmt.Sprintf(" · retry %d/%d", used, max)
	if threadID != "" {
		repo, _, _ := splitPRKey(prKey)
		s.ensureThreadStatus(context.Background(), ep.Platform, ep.ChannelID, threadID, repo, "", row.ActiveKind, kind+" run", statusLine)
		target := threadTargetFor(ep.Platform, ep.ChannelID, threadID)
		if mention != "" {
			_ = s.notifyThread(context.Background(), ep.Platform, target, "@"+mention+" — action required in this thread: /retry, /skip or /cancel.")
		}
	}
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
// failure path records the terminal receipt.
func (s *Server) planRetry(ep config.EndpointConfig, prKey string, receiptID int64, execErr error, workCtx *WebhookWorkContext, timedOut bool) bool {
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

// RecoverPRQueue reconciles persisted PR-queue state after a restart without
// re-executing anything: queued-but-unexecuted items cannot run because the
// delivery bodies live only in memory, so they are dropped with a count.
// Active rows are marked failed and escalated to blocked — nothing is silently
// lost, and the operator re-drives via /retry or a fresh trigger.
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
	var active, failed, dropped, blocked int
	for i := range runs {
		run := &runs[i]
		if run.Status == store.PRStatusBlocked {
			blocked++
			continue
		}
		active++
		if n, err := s.prQueues.ClearQueue(ctx, run.PRKey); err != nil {
			slog.Warn("webhook: pr queue recovery clear failed", "pr_key", run.PRKey, "error", err)
		} else {
			dropped += n
		}
		if run.ActiveDelivery != 0 {
			if rec, rerr := s.deliveries.GetByID(ctx, run.ActiveDelivery); rerr == nil && rec != nil && rec.Status == store.WebhookStatusProcessing {
				_, _ = s.deliveries.Transition(ctx, rec.ID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, "interrupted by restart")
			}
		}
		reason := "interrupted by restart — re-run via /retry or re-trigger"
		if serr := s.prQueues.SetBlocked(ctx, run.PRKey, reason); serr != nil {
			slog.Warn("webhook: pr queue recovery block failed", "pr_key", run.PRKey, "error", serr)
			continue
		}
		failed++
		blocked++
	}
	result := fmt.Sprintf("recovered %d active (resumed 0, failed %d), dropped %d queued, blocked %d", active, failed, dropped, blocked)
	slog.Warn("webhook: pr queue recovery", "result", result)
	return result
}
