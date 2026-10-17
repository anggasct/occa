package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/attribution"
	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/store"
)

const busyResponseMessage = "⚠️ A response is already running in this conversation. Wait for it to finish or check /status."

const defaultProgressStalledAfter = 5 * time.Minute

type turnTracker struct {
	mu                sync.Mutex
	lastProductive    time.Time
	outputs           int64
	permissionPending func() bool
}

func newTurnTracker(now time.Time, permissionPending func() bool) *turnTracker {
	return &turnTracker{lastProductive: now, permissionPending: permissionPending}
}

func (t *turnTracker) markProductive(now time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.lastProductive = now
	t.outputs++
	t.mu.Unlock()
}

func (t *turnTracker) stallState(now time.Time, stalledAfter time.Duration) (time.Duration, string) {
	if t == nil || stalledAfter <= 0 {
		return 0, ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	quiet := now.Sub(t.lastProductive)
	if quiet < stalledAfter {
		return quiet, ""
	}
	if t.permissionPending != nil && t.permissionPending() {
		return quiet, "permission"
	}
	if t.outputs == 0 {
		return quiet, "provider"
	}
	return quiet, "idle"
}

func isProductiveEvent(ev relay.Event) bool {
	switch ev.Type {
	case relay.EventDelta, relay.EventTool, relay.EventReasoning, relay.EventSegment:
		return true
	default:
		return false
	}
}

type queuedMessage struct {
	ctx context.Context
	msg channel.IncomingMessage
}

// responseKey is the full conversation key: one active task per
// (platform, channelID, threadID, userID), so different users or threads in
// the same channel run concurrently while the same conversation stays
// single-flight.
type responseKey struct {
	platform  string
	channelID string
	threadID  string
	userID    string
}

type responseCoordinator struct {
	mu        sync.Mutex
	active    map[responseKey]context.CancelFunc
	queues    map[responseKey][]queuedMessage
	maxQueued int
}

func newResponseCoordinator(maxQueued int) *responseCoordinator {
	return &responseCoordinator{
		active:    make(map[responseKey]context.CancelFunc),
		queues:    make(map[responseKey][]queuedMessage),
		maxQueued: maxQueued,
	}
}

func (c *responseCoordinator) acquire(key responseKey, cancel context.CancelFunc) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.active[key]; ok {
		return false
	}
	c.active[key] = cancel
	return true
}

func (c *responseCoordinator) enqueue(key responseKey, ctx context.Context, msg channel.IncomingMessage) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, active := c.active[key]; !active {
		return 0, false
	}
	q := c.queues[key]
	if len(q) >= c.maxQueued {
		return 0, false
	}
	q = append(q, queuedMessage{ctx: ctx, msg: msg})
	c.queues[key] = q
	return len(q), true
}

func (c *responseCoordinator) drain(key responseKey) []queuedMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.queues[key]
	delete(c.queues, key)
	return q
}

func (c *responseCoordinator) queueDepth(key responseKey) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queues[key])
}

func (c *responseCoordinator) requeuePrefix(key responseKey, msgs []queuedMessage) {
	if len(msgs) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[key] = append(msgs, c.queues[key]...)
}

func (c *responseCoordinator) release(key responseKey) {
	c.mu.Lock()
	delete(c.active, key)
	c.mu.Unlock()
}

// cancelResponse cancels the in-flight response for a conversation key,
// used by /reset and /session new to stop a running response.
func (c *responseCoordinator) cancelResponse(key responseKey) {
	c.mu.Lock()
	cancel := c.active[key]
	delete(c.active, key)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *responseCoordinator) cancelMatching(platform, channelID, threadID, userID string) {
	c.mu.Lock()
	var toCancel []context.CancelFunc
	for k, cancel := range c.active {
		if k.platform != platform || k.channelID != channelID {
			continue
		}
		if threadID != "" {
			if k.threadID == threadID {
				toCancel = append(toCancel, cancel)
				delete(c.active, k)
			}
		} else {
			if k.threadID == "" && k.userID == userID {
				toCancel = append(toCancel, cancel)
				delete(c.active, k)
			}
		}
	}
	c.mu.Unlock()
	for _, cancel := range toCancel {
		if cancel != nil {
			cancel()
		}
	}
}

func (r *Router) stalledAfter() time.Duration {
	if r.progressStalledAfter > 0 {
		return r.progressStalledAfter
	}
	return defaultProgressStalledAfter
}

func (r *Router) runResponse(
	ctx context.Context,
	cancel context.CancelFunc,
	key responseKey,
	msg channel.IncomingMessage,
	inst AgentInstance,
	sessionID string,
	events <-chan relay.Event,
	dispatch func(context.Context) error,
) {
	started := time.Now()
	outcome := "complete"
	root := r.threadRoot(key)
	if root != nil {
		r.editRootCard(key.platform, root, followupRunningLine)
	}
	owner := &permissionOwner{}
	permissionHandler := &permissionPromptHandler{
		broker:    r.permissions,
		encode:    func(text string) string { return r.inline(msg.Platform, text) },
		owner:     owner,
		client:    inst.Client(),
		platform:  key.platform,
		channelID: key.channelID,
		threadID:  key.threadID,
		userID:    key.userID,
		sessionID: sessionID,
		reply:     msg.ReplyCtx,
	}
	questionHandler := &questionPromptHandler{
		broker:    r.questions,
		split:     func(text string) []string { return r.outbound(msg.Platform, text) },
		client:    inst.Client(),
		platform:  key.platform,
		channelID: key.channelID,
		sessionID: sessionID,
		reply:     msg.ReplyCtx,
	}
	slog.Info("response started", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID)
	if r.agentTracker != nil {
		r.agentTracker.snapshotWorkdir(inst.Workdir())
	}
	defer func() {
		r.recordUsage(msg, inst, sessionID)
		r.permissions.expireOwner(owner)
		cancel()
		inst.End()
		r.responses.release(key)
		slog.Info("response finished", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "outcome", outcome, "elapsed", time.Since(started).Truncate(time.Millisecond))
		drained := r.responses.drain(key)
		r.dispatchDrained(key, drained)
	}()

	progressStopCh := make(chan struct{})
	var progressStopOnce sync.Once
	stopProgress := func() {
		progressStopOnce.Do(func() {
			close(progressStopCh)
		})
	}
	defer stopProgress()

	progressActivityCh := make(chan struct{}, 1)

	var notices store.ProgressNoticeRepo
	if r.store != nil {
		notices = r.store.ProgressNoticeRepo()
	}

	tracker := newTurnTracker(time.Now(), func() bool {
		return r.permissions.HasPendingFor(owner)
	})

	go startProgressTicker(ctx, msg.ReplyCtx, progressActivityCh, progressStopCh, progressTickerInterval, r.progressQuietThreshold, notices, key.platform, key.channelID, key.threadID, nil, r.stalledAfter(), tracker)

	observedEvents := make(chan relay.Event, 64)
	go func() {
		for ev := range events {
			select {
			case progressActivityCh <- struct{}{}:
			default:
			}
			if isProductiveEvent(ev) {
				tracker.markProductive(time.Now())
			}
			select {
			case observedEvents <- ev:
			case <-ctx.Done():
				return
			}
		}
		close(observedEvents)
	}()

	dispatchDone := make(chan error, 1)
	streamDone := make(chan error, 1)

	go func() {
		dispatchDone <- dispatch(ctx)
	}()
	go func() {
		streamer := relay.NewStreamer(msg.ReplyCtx, r.renderer, render.PlatformFor(msg.Platform), r.streamerNoEventTimeout)
		streamer.SetPermissionPromptHandler(permissionHandler)
		streamer.SetQuestionPromptHandler(questionHandler)
		streamer.SetPermissionPendingFunc(func() bool {
			return r.permissions.HasPendingFor(owner)
		})
		if r.attrib != nil {
			streamer.SetScheduleAttributionHandler(func(input map[string]any) error {
				cronExpr, _ := input["cron_expression"].(string)
				prompt, _ := input["prompt"].(string)
				humanSched, _ := input["human_schedule"].(string)
				fp := attribution.Fingerprint(cronExpr, prompt, humanSched)
				r.attrib.Put(fp, msg.Platform, msg.ChannelID)
				return nil
			})
		}
		if setter, ok := msg.ReplyCtx.(channel.ReactionSetter); ok {
			streamer.SetReactionSetter(setter)
		}
		if msg.SourceRef != nil {
			streamer.SetReactionTarget(msg.SourceRef)
		}
		if sessionID != "" {
			streamer.SetStopCallbackData("stop:" + sessionID)
		} else {
			streamer.SetStopCallbackData("stop")
		}
		streamDone <- streamer.Run(ctx, observedEvents)
	}()

	var dispatchErr, streamErr error
	dispatchPending := true
	streamPending := true
	for dispatchPending || streamPending {
		select {
		case dispatchErr = <-dispatchDone:
			dispatchPending = false
			if dispatchErr != nil {
				cancel()
			}
		case streamErr = <-streamDone:
			streamPending = false
			cancel()
		}
	}
	if errors.Is(dispatchErr, context.Canceled) && errors.Is(streamErr, context.Canceled) {
		outcome = "cancelled"
	}
	if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) {
		outcome = "dispatch_error"
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		if errors.Is(streamErr, relay.ErrIncompleteStream) {
			outcome = "incomplete"
		} else {
			outcome = "stream_error"
		}
	}

	// One recovery decision per response: when dispatch and stream both
	// terminate with real errors, the dispatch-side failure is the root
	// cause and the stream error only its consequence — recovering for
	// each independently would restart the agent twice and notify twice.
	var recoveryTrigger store.RecoveryTrigger
	var recoveryCause error
	if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) {
		slog.Warn("response dispatch failed", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "error", dispatchErr)
		if errors.Is(dispatchErr, relay.ErrNotFound) {
			r.replaceDeadSession(msg, key, inst, sessionID)
		} else if errors.Is(dispatchErr, relay.ErrAttachmentTooLarge) {
			r.reply(msg, "⚠️ "+dispatchErr.Error())
		} else if trigger, classified := classifyDispatchFailure(dispatchErr); classified {
			recoveryTrigger, recoveryCause = trigger, dispatchErr
		} else {
			r.reply(msg, "⚠️ Agent unreachable")
		}
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		slog.Warn("response stream ended", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "error", streamErr)
		if errors.Is(streamErr, relay.ErrTimeout) {
			r.parkTimedOutSession(key, sessionID, r.permissions.HasPendingFor(owner))
		}
		if recoveryTrigger == "" && errors.Is(streamErr, relay.ErrIncompleteStream) {
			recoveryTrigger, recoveryCause = store.RecoveryTriggerStreamEnded, streamErr
		}
	}
	if recoveryTrigger != "" {
		r.recoverAfterFailure(ctx, msg, key, inst, recoveryTrigger, recoveryCause)
	}
	if outcome == "complete" {
		r.detectNewAgents(ctx, msg, inst)
	}
	if root != nil {
		r.editRootCard(key.platform, root, followupTerminalLine(outcome, time.Since(started)))
	}
}

func (r *Router) parkTimedOutSession(key responseKey, sessionID string, permissionPending bool) {
	reason := "idle"
	if permissionPending {
		reason = "permission"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.store.SessionRepo().MarkParked(ctx, key.platform, key.channelID, key.threadID, key.userID, reason); err != nil {
		slog.Warn("response timeout park failed", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "session_id", sessionID, "error", err)
		return
	}
	slog.Info("response timed out, session parked", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "session_id", sessionID, "reason", reason)
}

func (r *Router) replaceDeadSession(msg channel.IncomingMessage, key responseKey, inst AgentInstance, deadSessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	freshID, err := inst.Client().CreateSession(ctx)
	if err != nil {
		r.reply(msg, "⚠️ Agent unreachable")
		return
	}
	if err := r.store.SessionRepo().SetActive(ctx, key.platform, key.channelID, key.threadID, key.userID, freshID, inst.PID()); err != nil {
		r.reply(msg, "⚠️ Agent unreachable")
		return
	}
	slog.Info("dead session replaced", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "dead_session_id", deadSessionID, "session_id", freshID)
	r.reply(msg, fmt.Sprintf("⚠️ Session %s is gone on the agent — fresh session %s is ready. Send your message again to continue.", deadSessionID, freshID))
}

const followupRunningLine = "Follow-up: 🔄 running in thread"

func followupTerminalLine(outcome string, elapsed time.Duration) string {
	switch outcome {
	case "complete":
		return "Follow-up: ✅ completed (" + formatFollowupDuration(elapsed) + ")"
	case "cancelled":
		return "Follow-up: ⏹️ cancelled"
	default:
		return "Follow-up: ⚠️ failed"
	}
}

func formatFollowupDuration(d time.Duration) string {
	d = d.Round(time.Second)
	m := int(d / time.Minute)
	s := int((d % time.Minute) / time.Second)
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func (r *Router) threadRoot(key responseKey) *store.ThreadRootCard {
	if r.rootCardEditor == nil || r.store == nil || key.threadID == "" {
		return nil
	}
	root, err := r.store.SessionRepo().ThreadRoot(context.Background(), key.platform, key.channelID, key.threadID)
	if err != nil {
		slog.Warn("router: thread root card lookup failed", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "error", err)
		return nil
	}
	return root
}

func (r *Router) editRootCard(platform string, root *store.ThreadRootCard, line string) {
	if r.rootCardEditor == nil || root == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.rootCardEditor(ctx, platform, root.Channel, root.MessageID, root.Card+"\n"+line); err != nil {
		slog.Warn("router: root card follow-up edit failed", "platform", platform, "channel_id", root.Channel, "message_id", root.MessageID, "error", err)
	}
}

func progressNotice(seconds int64) string {
	minutes := seconds / 60
	return fmt.Sprintf("⏳ Still working... (%dm)", minutes)
}

func stalledNotice(quiet time.Duration, gate string) string {
	minutes := int64(quiet / time.Minute)
	if minutes < 1 {
		minutes = 1
	}
	return fmt.Sprintf("⚠️ Stalled — no agent output for %dm (suspected gate: %s)", minutes, gate)
}

var progressTickerInterval = 15 * time.Second

func startProgressTicker(
	ctx context.Context,
	reply channel.ReplyContext,
	activityCh <-chan struct{},
	stopCh <-chan struct{},
	interval, quietThreshold time.Duration,
	notices store.ProgressNoticeRepo,
	platform, channelID, threadID string,
	now func() time.Time,
	stalledAfter time.Duration,
	tracker *turnTracker,
) {
	if now == nil {
		now = time.Now
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastActivity := now()
	var noticeRef channel.MessageRef
	var lastText string

	removeNotice := func() {
		if noticeRef == nil {
			return
		}
		if remover, ok := reply.(channel.MessageRemover); ok {
			if err := remover.Delete(noticeRef); err != nil {
				slog.Warn("progress notice delete failed", "error", err)
			}
		}
		if notices != nil {
			if err := notices.Delete(ctx, platform, channelID, threadID, noticeRef.ID()); err != nil {
				slog.Warn("progress notice persist delete failed", "error", err)
			}
		}
		noticeRef = nil
		lastText = ""
	}
	defer removeNotice()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-activityCh:
			lastActivity = now()
			removeNotice()
		case <-ticker.C:
			nowTime := now()
			quiet := nowTime.Sub(lastActivity)
			if quiet < quietThreshold {
				if noticeRef != nil {
					removeNotice()
				}
				continue
			}
			minutes := quiet / time.Minute
			if minutes < 1 {
				minutes = 1
			}
			text := progressNotice(int64(minutes) * 60)
			if tracker != nil {
				if stallQuiet, gate := tracker.stallState(nowTime, stalledAfter); gate != "" {
					text = stalledNotice(stallQuiet, gate)
				}
			}
			if noticeRef == nil {
				ref, err := reply.Send(text)
				if err != nil {
					slog.Warn("progress notice send failed", "error", err)
					continue
				}
				noticeRef = ref
				lastText = text
				if notices != nil {
					if err := notices.Put(ctx, platform, channelID, threadID, ref.ID()); err != nil {
						slog.Warn("progress notice persist failed", "error", err)
					}
				}
			} else {
				if text == lastText {
					// Telegram rejects identical edits with 400 "message is
					// not modified"; skip until the minute band changes.
					continue
				}
				if err := reply.Edit(noticeRef, text); err != nil {
					slog.Warn("progress notice edit failed", "error", err)
					continue
				}
				lastText = text
			}
		}
	}
}
