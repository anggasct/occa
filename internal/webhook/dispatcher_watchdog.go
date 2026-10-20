package webhook

import (
	"context"
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
			last := snap.lastActive
			if last.IsZero() {
				last = snap.startedAt
			}
			if last.IsZero() {
				continue
			}
			if now.Sub(last) < after {
				continue
			}
			liveness := s.probeSessionLiveness(snap.active.prKey, snap.active.ep.Name, snap.active.deliveryID)
			if liveness == livenessAlive {
				continue
			}
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
	abortOK, aborted := false, false
	if snap.hasActive && liveness == livenessDead {
		abortOK, aborted = s.abortStuckSession(item, sessionID, workdir)
	}
	abortClause := "watchdog released the key so the next item proceeds."
	switch {
	case aborted && abortOK:
		abortClause = "watchdog aborted the agent session and released the key so the next item proceeds."
	case aborted:
		abortClause = "watchdog could not abort the agent session and released the key so the next item proceeds."
	case sessionID != "":
		abortClause = "watchdog released the key without aborting the agent session, which is still running."
	}
	summary, logMessage := stuckSessionOutcome(abortOK, aborted)
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
		"session_aborted", aborted,
		"session_abort_ok", abortOK,
		"session_still_running", !aborted || !abortOK,
	)
	slog.Error(logMessage,
		"delivery_id", strings.TrimSpace(item.deliveryID),
		"session_id", sessionID,
		"session_aborted", aborted,
		"session_abort_ok", abortOK,
		"session_still_running", !aborted || !abortOK,
	)
	if liveness == livenessDead {
		d.markWatchdogTookOver()
		s.markRunTakenOver(item)
		s.failStuckDelivery(item, sessionID, summary)
	}
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

// abortStuckSession kills the agent session of a run the watchdog declared
// stuck. Releasing the key without killing the run leaves two writers on the
// same PR surface, so the abort happens first and the key is released even
// when the abort fails. A session that cannot be reached is left running and
// reported as such: an abort that never ran must not be reported as one.
func (s *Server) abortStuckSession(item dispatchItem, sessionID, workdir string) (bool, bool) {
	if sessionID == "" {
		return false, false
	}
	if s.sessionAborter == nil || workdir == "" {
		slog.Warn("webhook: dispatcher stuck, session abort not possible",
			"delivery_id", strings.TrimSpace(item.deliveryID),
			"session_id", sessionID,
		)
		return false, false
	}
	if err := s.sessionAborter(context.Background(), sessionID, workdir); err != nil {
		slog.Error("webhook: dispatcher stuck, session abort failed",
			"delivery_id", strings.TrimSpace(item.deliveryID),
			"session_id", sessionID,
			"error", err,
		)
		s.markRunAborted(item, false)
		return false, true
	}
	s.markRunAborted(item, true)
	return true, true
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
// that ran may be reported as an aborted session.
func stuckSessionOutcome(abortOK, aborted bool) (string, string) {
	switch {
	case aborted && abortOK:
		return "stuck — session aborted", "webhook: dispatcher stuck, session aborted"
	case aborted:
		return "stuck — session abort failed, still running", "webhook: dispatcher stuck, session abort failed, still running"
	default:
		return "stuck — session not aborted, still running", "webhook: dispatcher stuck, session not aborted, still running"
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
