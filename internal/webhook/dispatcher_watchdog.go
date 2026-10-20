package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/store"
)

func (s *Server) SetDispatcherStuckAfter(d time.Duration) {
	if d > 0 {
		s.dispatcherStuckAfter = d
	}
}

func (s *Server) SetDispatcherWatchdogInterval(d time.Duration) {
	if d > 0 {
		s.watchdogInterval = d
	}
}

func (s *Server) stuckAfter() time.Duration {
	if s.dispatcherStuckAfter > 0 {
		return s.dispatcherStuckAfter
	}
	if s.processingTimeout > 0 {
		return s.processingTimeout / 6
	}
	return s.claimGrace / 6
}

func (s *Server) watchdogTick() time.Duration {
	if s.watchdogInterval > 0 {
		return s.watchdogInterval
	}
	return s.stuckAfter() / 5
}

func (s *Server) watchdogLoop() {
	ticker := time.NewTicker(s.watchdogTick())
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case now := <-ticker.C:
			s.sweepStuckDispatchers(now)
		}
	}
}

func (s *Server) sweepStuckDispatchers(now time.Time) int {
	after := s.stuckAfter()
	type candidate struct {
		d        *dispatcher
		snap     dispatcherSnapshot
		liveness sessionLivenessState
	}
	var stuck []candidate
	s.dispatchMu.Lock()
	for _, d := range s.dispatchers {
		if d.exiting {
			continue
		}
		snap := d.snapshot()
		if snap.stuckMarked {
			continue
		}
		if snap.hasActive {
			if d.isBackingOff() {
				// A retry backoff is a known wait, not a stuck run.
				continue
			}
			last := s.runHeartbeat(snap)
			if last.IsZero() {
				continue
			}
			if now.Sub(last) < after {
				continue
			}
			// The liveness probe is the primary signal, the elapsed time the
			// fallback: a run whose agent is gone and a run whose agent is
			// alive but silent past the threshold are both stuck. Only a run
			// with a fresh heartbeat is left alone.
			liveness := s.probeSessionLiveness(snap.active.prKey, snap.active.ep.Name, snap.active.deliveryID)
			stuck = append(stuck, candidate{d, snap, liveness})
			continue
		}
		if snap.queueLen > 0 && !snap.lastActive.IsZero() && now.Sub(snap.lastActive) >= after {
			stuck = append(stuck, candidate{d, snap, livenessUnknown})
		}
	}
	s.dispatchMu.Unlock()
	for _, c := range stuck {
		c.d.markStuckReported()
		s.alarmStuckDispatcher(c.d, c.snap, now, after, c.liveness)
		s.releaseStuckDispatcher(c.d)
	}
	return len(stuck)
}

func stuckRepoBranch(s *Server, item dispatchItem) (string, string) {
	key := ExtractExecutionKey(item.body)
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	repo, branch := TaskRepoBranch(key, envelope)
	if repo != "" && branch != "" {
		return repo, branch
	}
	return normalizeRepoKey(key.Repository), normalizeBranchKey(key.Branch)
}

// runHeartbeat returns the last moment the active run showed progress. The
// run reports it on every agent event, so it measures the agent itself; a
// run that has not reported yet, and a dispatcher without a run, fall back to
// the dispatcher activity.
func (s *Server) runHeartbeat(snap dispatcherSnapshot) time.Time {
	if run := s.activeRunFor(snap.active.prKey, snap.active.ep.Name, snap.active.deliveryID); run != nil {
		if at := run.progressAt(); !at.IsZero() {
			return at
		}
	}
	last := snap.lastActive
	if last.IsZero() {
		last = snap.startedAt
	}
	return last
}

func (s *Server) alarmStuckDispatcher(d *dispatcher, snap dispatcherSnapshot, now time.Time, after time.Duration, liveness sessionLivenessState) {
	item := snap.active
	if !snap.hasActive && snap.queueLen > 0 {
		s.dispatchMu.Lock()
		drained := d.drain()
		if len(drained) > 0 {
			item = drained[0]
			for _, q := range drained {
				d.push(q)
			}
		}
		s.dispatchMu.Unlock()
	}
	repo, branch := stuckRepoBranch(s, item)
	if repo == "" || branch == "" {
		if key := strings.TrimSpace(d.key); key != "" {
			repo, branch = key, "-"
		}
	}
	stuckSince := snap.lastActive
	if stuckSince.IsZero() {
		stuckSince = snap.startedAt
	}
	elapsed := now.Sub(stuckSince).Round(time.Second)
	sessionID, workdir := s.stuckSessionIdentity(item, repo)
	outcome := s.abortStuckSession(item, sessionID, workdir)
	abortClause := "watchdog released the key so the next item proceeds."
	switch outcome {
	case stuckAbortDone:
		abortClause = "watchdog aborted the agent session and released the key so the next item proceeds."
	case stuckAbortFailed:
		abortClause = "watchdog could not abort the agent session and released the key so the next item proceeds."
	case stuckAbortNotPossible:
		abortClause = "watchdog released the key without aborting the agent session, which is still running."
	}
	summary, logMessage := stuckSessionOutcome(outcome, liveness)
	card := fmt.Sprintf("🚨 STUCK dispatcher %s#%s · delivery %s · stuck since %s (%s, threshold %s) · queue %d/%d — %s Check delivery log and sessions.",
		repo, branch, strings.TrimSpace(item.deliveryID), stuckSince.UTC().Format(time.RFC3339), elapsed, after, snap.queueLen, snap.queueCap, abortClause)
	slog.Error("webhook: dispatcher stuck, key released",
		"lock", d.key,
		"repo", repo,
		"branch", branch,
		"delivery_id", strings.TrimSpace(item.deliveryID),
		"stuck_since", stuckSince.UTC().Format(time.RFC3339),
		"elapsed", elapsed.String(),
		"queue", fmt.Sprintf("%d/%d", snap.queueLen, snap.queueCap),
		"session_id", sessionID,
		"session_liveness", liveness.String(),
		"session_aborted", outcome == stuckAbortDone,
		"session_abort_ok", outcome == stuckAbortDone,
		"session_still_running", outcome == stuckAbortFailed || outcome == stuckAbortNotPossible,
	)
	slog.Error(logMessage,
		"delivery_id", strings.TrimSpace(item.deliveryID),
		"session_id", sessionID,
		"session_liveness", liveness.String(),
		"session_aborted", outcome == stuckAbortDone,
		"session_abort_ok", outcome == stuckAbortDone,
		"session_still_running", outcome == stuckAbortFailed || outcome == stuckAbortNotPossible,
	)
	// The watchdog owns the run it declares stuck: the delivery reaches a
	// terminal receipt whatever the session outcome, and the dispatcher must
	// not run it a second time.
	d.markWatchdogTookOver()
	s.markRunTakenOver(item)
	s.failStuckDelivery(item, sessionID, summary)
	if repo == "" || branch == "" || branch == "-" {
		return
	}
	threadID := ""
	if rec, ok := s.tasks.ResolveTaskThread(repo, branch, "watchdog"); ok {
		threadID = strings.TrimSpace(rec.ThreadID)
	}
	if threadID == "" || s.notifier == nil {
		return
	}
	target := threadID
	if strings.EqualFold(strings.TrimSpace(item.ep.Platform), "telegram") && !strings.Contains(strings.TrimSpace(item.ep.ChannelID), ":") && strings.TrimSpace(item.ep.ChannelID) != "" {
		target = strings.TrimSpace(item.ep.ChannelID) + ":" + threadID
	} else if !strings.EqualFold(strings.TrimSpace(item.ep.Platform), "discord") && strings.TrimSpace(item.ep.ChannelID) != "" {
		target = auditTargetChannel(item.ep.Platform, item.ep.ChannelID, threadID)
		if strings.TrimSpace(target) == "" {
			target = threadID
		}
	}
	if err := s.notifier(context.Background(), item.ep.Platform, target, card); err != nil {
		slog.Warn("webhook: stuck alarm card failed", "repo", repo, "branch", branch, "delivery_id", strings.TrimSpace(item.deliveryID), "error", err)
	}
}

// stuckAbortOutcome is what the watchdog managed to do with the session of a
// run it declared stuck.
type stuckAbortOutcome int

const (
	// stuckAbortNone means there was nothing to abort: no session, or the
	// agent behind it is gone.
	stuckAbortNone stuckAbortOutcome = iota
	// stuckAbortNotPossible means a session exists but cannot be reached, so
	// it may still be running.
	stuckAbortNotPossible
	// stuckAbortFailed means the abort ran and failed.
	stuckAbortFailed
	// stuckAbortDone means the abort ran and the session was stopped.
	stuckAbortDone
)

// abortStuckSession kills the agent session of a run the watchdog declared
// stuck. Releasing the key without killing the run leaves two writers on the
// same PR surface, so the abort happens first and the key is released even
// when the abort fails. The abort never starts an agent: a workdir without a
// running agent has nothing to abort, and a session that cannot be reached is
// left running and reported as such.
func (s *Server) abortStuckSession(item dispatchItem, sessionID, workdir string) stuckAbortOutcome {
	if sessionID == "" {
		return stuckAbortNone
	}
	if s.sessionAborter == nil || workdir == "" {
		slog.Warn("webhook: dispatcher stuck, session abort not possible",
			"delivery_id", strings.TrimSpace(item.deliveryID),
			"session_id", sessionID,
			"reason", "no abort capability",
		)
		return stuckAbortNotPossible
	}
	if err := s.sessionAborter(context.Background(), sessionID, workdir); err != nil {
		if errors.Is(err, ErrNoLiveAgent) {
			slog.Info("webhook: dispatcher stuck, no live agent to abort",
				"delivery_id", strings.TrimSpace(item.deliveryID),
				"session_id", sessionID,
			)
			s.markRunAborted(item, false)
			return stuckAbortNone
		}
		slog.Error("webhook: dispatcher stuck, session abort failed",
			"delivery_id", strings.TrimSpace(item.deliveryID),
			"session_id", sessionID,
			"error", err,
		)
		s.markRunAborted(item, false)
		return stuckAbortFailed
	}
	s.markRunAborted(item, true)
	return stuckAbortDone
}

func (s *Server) markRunAborted(item dispatchItem, ok bool) {
	if run := s.activeRunFor(item.prKey, item.ep.Name, item.deliveryID); run != nil {
		run.markAborted(ok)
	}
}

func (s *Server) markRunTakenOver(item dispatchItem) {
	if run := s.activeRunFor(item.prKey, item.ep.Name, item.deliveryID); run != nil {
		run.markTakenOver()
	}
}

func (s *Server) stuckSessionIdentity(item dispatchItem, repo string) (string, string) {
	if run := s.activeRunFor(item.prKey, item.ep.Name, item.deliveryID); run != nil {
		sessionID, workdir := run.identity()
		if sessionID != "" {
			return sessionID, workdir
		}
	}
	if s.prQueues == nil || strings.TrimSpace(item.prKey) == "" {
		return "", ""
	}
	row, err := s.prQueues.Get(context.Background(), item.prKey)
	if err != nil || row == nil {
		return "", ""
	}
	sessionID := strings.TrimSpace(row.ActiveSession)
	if sessionID == "" {
		return "", ""
	}
	return sessionID, s.checkoutForRepo(repo)
}

// failStuckDelivery records the stuck delivery with the summary of what
// happened to the agent session: only an abort that ran may be reported as an
// aborted session.
func (s *Server) failStuckDelivery(item dispatchItem, sessionID, summary string) {
	deliveryID := strings.TrimSpace(item.deliveryID)
	if deliveryID == "" {
		return
	}
	receipt, err := s.deliveries.Get(context.Background(), item.ep.Name, deliveryID)
	if err != nil || receipt == nil {
		return
	}
	ok, tErr := s.deliveries.Transition(context.Background(), receipt.ID,
		[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
		store.WebhookStatusFailed, summary)
	if tErr != nil {
		slog.Error("webhook: stuck delivery transition failed", "delivery_id", deliveryID, "error", tErr)
		return
	}
	if !ok {
		slog.Warn("webhook: stuck delivery transition lost race", "delivery_id", deliveryID, "session_id", sessionID)
	}
}

// stuckSessionOutcome words the session outcome of a stuck run. Only an abort
// that ran may be reported as an aborted session; a run whose agent is gone
// has nothing left to abort.
func stuckSessionOutcome(outcome stuckAbortOutcome, liveness sessionLivenessState) (string, string) {
	switch outcome {
	case stuckAbortDone:
		return "stuck — session aborted", "webhook: dispatcher stuck, session aborted"
	case stuckAbortFailed:
		return "stuck — session abort failed, still running", "webhook: dispatcher stuck, session abort failed, still running"
	case stuckAbortNotPossible:
		if liveness == livenessDead {
			return "stuck — no session to abort", "webhook: dispatcher stuck, no session to abort"
		}
		return "stuck — session not aborted, still running", "webhook: dispatcher stuck, session not aborted, still running"
	default:
		return "stuck — no session to abort", "webhook: dispatcher stuck, no session to abort"
	}
}

func (s *Server) releaseStuckDispatcher(old *dispatcher) {
	s.dispatchMu.Lock()
	current, ok := s.dispatchers[old.key]
	if !ok || current != old {
		s.dispatchMu.Unlock()
		return
	}
	old.exiting = true
	delete(s.dispatchers, old.key)
	fresh := newDispatcher(s, old.key)
	s.dispatchers[old.key] = fresh
	s.dispatchWG.Add(1)
	go fresh.run(s.shutdownCtx)
	queued := old.drain()
	s.dispatchMu.Unlock()
	moved := 0
	for _, item := range queued {
		if fresh.push(item) {
			moved++
		} else {
			s.abandon(item, "watchdog queue overflow after key release")
		}
	}
	slog.Error("webhook: dispatcher key released after stuck", "lock", old.key, "moved", moved, "queued", len(queued))
}

func (s *Server) handleDispatcherPanic(d *dispatcher, item dispatchItem, r any) {
	stack := strings.TrimSpace(string(debug.Stack()))
	summary := redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes)
	slog.Error("webhook: dispatcher panic, rebuilding",
		"lock", d.key,
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"panic", fmt.Sprint(r),
		"stack", clipRunes(stack, 2000),
	)
	if receipt, err := s.deliveries.Get(context.Background(), item.ep.Name, item.deliveryID); err == nil && receipt != nil {
		_, _ = s.deliveries.Transition(context.Background(), receipt.ID,
			[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
			store.WebhookStatusFailed, summary)
	} else if err != nil {
		slog.Error("webhook: panic receipt lookup failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
	}
	s.releaseStuckDispatcher(d)
}

func (s *Server) rebuildAfterPanic(d *dispatcher, item dispatchItem, r any) {
	stack := strings.TrimSpace(string(debug.Stack()))
	slog.Error("webhook: dispatcher goroutine panic, rebuilding",
		"lock", d.key,
		"panic", fmt.Sprint(r),
		"stack", clipRunes(stack, 2000),
	)
	if strings.TrimSpace(item.deliveryID) != "" {
		if receipt, err := s.deliveries.Get(context.Background(), item.ep.Name, item.deliveryID); err == nil && receipt != nil {
			_, _ = s.deliveries.Transition(context.Background(), receipt.ID,
				[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
				store.WebhookStatusFailed, redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes))
		}
	}
	s.dispatchMu.Lock()
	if current, ok := s.dispatchers[d.key]; ok && current == d {
		d.exiting = true
		delete(s.dispatchers, d.key)
	}
	queued := d.drain()
	freshNeeded := len(queued) > 0
	var fresh *dispatcher
	if freshNeeded {
		fresh = newDispatcher(s, d.key)
		s.dispatchers[d.key] = fresh
		s.dispatchWG.Add(1)
		go fresh.run(s.shutdownCtx)
	}
	s.dispatchMu.Unlock()
	if fresh == nil {
		return
	}
	for _, q := range queued {
		if !fresh.push(q) {
			s.abandon(q, "panic rebuild queue overflow")
		}
	}
}
