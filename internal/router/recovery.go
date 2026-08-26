package router

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/process"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const (
	recoveryBudget         = 60 * time.Second
	recoveryBaseBackoff    = 10 * time.Second
	recoveryMaxBackoff     = 40 * time.Second
	recoveryDetailMaxRunes = 200
)

const (
	recoveryResumedMessage      = "🔄 The agent was restarted and your session resumed — context kept. Your last message may not have completed; send it again if needed. See /status."
	recoveryStreamIntactMessage = "🔄 The response stream ended early, but the agent and your session are intact. The task may still be running — check /status and resend if the answer is missing."
	recoveryRecreatedMessage    = "🆕 The agent was restarted, but its previous session was lost — a new session was started and earlier context is gone. Send your message again to continue. See /status."
	recoveryFreshMessage        = "🆕 The agent was restarted with a fresh session. Send your message again to continue. See /status."
	recoveryFailedMessage       = "⚠️ Agent recovery failed — it did not come back up. Try again in a moment; see /status."
	recoverySuppressedMessage   = "⚠️ Agent recovery was skipped — another recovery is in progress or retry cooling down. Try again in a moment; see /status."
	agentStartFailedMessage     = "⚠️ The agent failed to start (not ready in time). Try again shortly; see /status."
)

type recoveryState string

const (
	recoveryStateHealthy     recoveryState = "healthy"
	recoveryStateUnavailable recoveryState = "unavailable"
	recoveryStateRestarting  recoveryState = "restarting"
)

type workdirRecovery struct {
	state       recoveryState
	inflight    bool
	lastAttempt time.Time
	failures    int
}

type recoveryCoordinator struct {
	mu        sync.Mutex
	states    map[string]*workdirRecovery
	now       func() time.Time
	baseDelay time.Duration
	maxDelay  time.Duration
}

func newRecoveryCoordinator() *recoveryCoordinator {
	return &recoveryCoordinator{
		states:    make(map[string]*workdirRecovery),
		now:       time.Now,
		baseDelay: recoveryBaseBackoff,
		maxDelay:  recoveryMaxBackoff,
	}
}

// beginAttempt claims the single recovery slot for a workdir. A second
// concurrent recovery and any attempt inside the backoff window are refused,
// which bounds restart churn to one attempt per window per agent instance.
func (c *recoveryCoordinator) beginAttempt(workdir string) (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[workdir]
	if !ok {
		st = &workdirRecovery{state: recoveryStateHealthy}
		c.states[workdir] = st
	}
	if st.inflight {
		return false, "in_flight"
	}
	if !st.lastAttempt.IsZero() && c.now().Sub(st.lastAttempt) < c.backoffLocked(st) {
		return false, "backoff"
	}
	st.inflight = true
	st.state = recoveryStateUnavailable
	st.lastAttempt = c.now()
	return true, ""
}

func (c *recoveryCoordinator) backoffLocked(st *workdirRecovery) time.Duration {
	delay := c.baseDelay
	for i := 1; i < st.failures && delay < c.maxDelay; i++ {
		delay *= 2
	}
	if delay > c.maxDelay {
		delay = c.maxDelay
	}
	return delay
}

func (c *recoveryCoordinator) markRestarting(workdir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.states[workdir]; ok {
		st.state = recoveryStateRestarting
	}
}

func (c *recoveryCoordinator) finishAttempt(workdir string, outcome store.RecoveryOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[workdir]
	if !ok {
		return
	}
	st.inflight = false
	st.state = recoveryState(outcome)
	switch outcome {
	case store.RecoveryOutcomeResumed, store.RecoveryOutcomeRecreated:
		st.failures = 0
		st.lastAttempt = time.Time{}
	default:
		st.failures++
	}
}

func newRecoveryID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("rcv-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("rcv-%x", b[:])
}

func classifyDispatchFailure(err error) (store.RecoveryTrigger, bool) {
	switch {
	case errors.Is(err, relay.ErrTimeout):
		return store.RecoveryTriggerSendTimeout, true
	case errors.Is(err, relay.ErrUnreachable):
		return store.RecoveryTriggerProcessExit, true
	}
	return "", false
}

func truncateDetail(s string) string {
	runes := []rune(s)
	if len(runes) <= recoveryDetailMaxRunes {
		return s
	}
	return string(runes[:recoveryDetailMaxRunes]) + "…"
}

func (r *Router) recordRecoveryEvent(ctx context.Context, e store.RecoveryEvent) {
	if r.store == nil {
		return
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.store.RecoveryEventRepo().Put(pctx, e); err != nil {
		slog.Warn("agent recovery event persist failed", "correlation_id", e.CorrelationID, "error", err)
	}
}

// recoverAfterFailure runs the bounded recovery state machine for one failed
// response: healthy → unavailable → restarting → resumed | recreated | failed.
// It never replays the user's prompt — the uncertain send is reported, not
// retried.
func (r *Router) recoverAfterFailure(ctx context.Context, msg channel.IncomingMessage, key responseKey, inst AgentInstance, trigger store.RecoveryTrigger, cause error) {
	workdir := inst.Workdir()
	corrID := newRecoveryID()
	budget := r.recoveryBudget
	if budget <= 0 {
		budget = recoveryBudget
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	started := time.Now()
	slog.Info("agent recovery started", "correlation_id", corrID, "trigger", string(trigger), "workdir", workdir, "platform", msg.Platform, "channel_id", msg.ChannelID, "thread_id", key.threadID, "user_id", key.userID)

	if trigger == store.RecoveryTriggerStreamEnded && r.streamSessionIntact(rctx, msg, key, inst) {
		r.recordRecoveryEvent(rctx, store.RecoveryEvent{
			Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: key.threadID, UserID: key.userID,
			Workdir: workdir, Trigger: trigger, Outcome: store.RecoveryOutcomeResumed,
			CorrelationID: corrID, Detail: "session intact after stream termination",
		})
		r.reply(msg, recoveryStreamIntactMessage)
		slog.Info("agent recovery finished", "correlation_id", corrID, "trigger", string(trigger), "outcome", string(store.RecoveryOutcomeResumed), "workdir", workdir, "elapsed", time.Since(started).Truncate(time.Millisecond))
		return
	}

	// Only the goroutine that claimed the recovery slot may finish it: a
	// refused trigger must leave the in-flight attempt and the backoff
	// window untouched, or a later trigger could start a second concurrent
	// restart and suppression would artificially escalate the backoff.
	if ok, reason := r.recovery.beginAttempt(workdir); !ok {
		r.recordRecoveryEvent(rctx, store.RecoveryEvent{
			Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: key.threadID, UserID: key.userID,
			Workdir: workdir, Trigger: trigger, Outcome: store.RecoveryOutcomeSuppressed,
			CorrelationID: corrID, Detail: "recovery refused: " + reason,
		})
		r.reply(msg, recoverySuppressedMessage)
		slog.Warn("agent recovery suppressed", "correlation_id", corrID, "trigger", string(trigger), "workdir", workdir, "reason", reason)
		return
	}

	r.instances.ForceStop(workdir)
	r.recovery.markRestarting(workdir)

	replacement, err := r.instances.Instance(rctx, workdir)
	if err != nil {
		r.recovery.finishAttempt(workdir, store.RecoveryOutcomeFailed)
		detail := truncateDetail(err.Error())
		if errors.Is(err, process.ErrReadinessTimeout) {
			detail = "restart not ready: " + detail
		}
		r.recordRecoveryEvent(rctx, store.RecoveryEvent{
			Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: key.threadID, UserID: key.userID,
			Workdir: workdir, Trigger: trigger, Outcome: store.RecoveryOutcomeFailed,
			CorrelationID: corrID, Detail: detail,
		})
		r.recordHealthError("agent recovery failed")
		r.reply(msg, recoveryFailedMessage)
		slog.Warn("agent recovery finished", "correlation_id", corrID, "trigger", string(trigger), "outcome", string(store.RecoveryOutcomeFailed), "workdir", workdir, "elapsed", time.Since(started).Truncate(time.Millisecond), "error", err)
		return
	}

	resolver := relay.NewSessionResolver(r.store.SessionRepo(), replacement.Client())
	resolution, err := resolver.ResolveDetailed(rctx, msg.Platform, msg.ChannelID, key.threadID, key.userID, replacement.PID())
	replacement.End()
	if err != nil {
		r.recovery.finishAttempt(workdir, store.RecoveryOutcomeFailed)
		r.recordRecoveryEvent(rctx, store.RecoveryEvent{
			Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: key.threadID, UserID: key.userID,
			Workdir: workdir, Trigger: trigger, Outcome: store.RecoveryOutcomeFailed,
			CorrelationID: corrID, Detail: truncateDetail(err.Error()),
		})
		r.recordHealthError("agent recovery failed")
		r.reply(msg, recoveryFailedMessage)
		slog.Warn("agent recovery finished", "correlation_id", corrID, "trigger", string(trigger), "outcome", string(store.RecoveryOutcomeFailed), "workdir", workdir, "elapsed", time.Since(started).Truncate(time.Millisecond), "error", err)
		return
	}

	outcome := store.RecoveryOutcomeRecreated
	message := recoveryFreshMessage
	if resolution.Resumed {
		outcome = store.RecoveryOutcomeResumed
		message = recoveryResumedMessage
	} else if resolution.HadStored {
		message = recoveryRecreatedMessage
	}
	r.recovery.finishAttempt(workdir, outcome)
	r.recordRecoveryEvent(rctx, store.RecoveryEvent{
		Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: key.threadID, UserID: key.userID,
		Workdir: workdir, Trigger: trigger, Outcome: outcome,
		CorrelationID: corrID, Detail: fmt.Sprintf("session %s", resolution.SessionID),
	})
	r.reply(msg, message)
	slog.Info("agent recovery finished", "correlation_id", corrID, "trigger", string(trigger), "outcome", string(outcome), "workdir", workdir, "elapsed", time.Since(started).Truncate(time.Millisecond))
}

// streamSessionIntact reports whether a stream termination left the stored
// session verifiable on the still-running agent — when true, no restart is
// needed and the response state can be reported without recovery churn.
func (r *Router) streamSessionIntact(ctx context.Context, msg channel.IncomingMessage, key responseKey, inst AgentInstance) bool {
	storedID, _, err := r.store.SessionRepo().Active(ctx, msg.Platform, msg.ChannelID, key.threadID, key.userID)
	if err != nil || storedID == "" {
		return false
	}
	exists, err := inst.Client().SessionExists(ctx, storedID)
	if err != nil {
		return false
	}
	return exists
}
