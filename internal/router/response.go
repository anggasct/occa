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

const busyResponseMessage = "⚠️ A response is already running in this conversation. Wait for it to finish or check /occa:status."

const maxQueuedMessages = 5

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
	mu     sync.Mutex
	active map[responseKey]context.CancelFunc
	queues map[responseKey][]queuedMessage
}

func newResponseCoordinator() *responseCoordinator {
	return &responseCoordinator{
		active: make(map[responseKey]context.CancelFunc),
		queues: make(map[responseKey][]queuedMessage),
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
	if len(q) >= maxQueuedMessages {
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
// used by /occa:reset and /occa:session new to stop a running response.
func (c *responseCoordinator) cancelResponse(key responseKey) {
	c.mu.Lock()
	cancel := c.active[key]
	delete(c.active, key)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
		encode:    func(text string) string { return r.inline(msg.Platform, text) },
		client:    inst.Client(),
		platform:  key.platform,
		channelID: key.channelID,
		sessionID: sessionID,
		reply:     msg.ReplyCtx,
	}
	slog.Info("response started", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID)
	defer func() {
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

	go startProgressTicker(ctx, msg.ReplyCtx, progressActivityCh, progressStopCh, progressTickerInterval, progressQuietThreshold, notices, key.platform, key.channelID, key.threadID, nil)

	observedEvents := make(chan relay.Event, 64)
	go func() {
		for ev := range events {
			select {
			case progressActivityCh <- struct{}{}:
			default:
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
		streamer := relay.NewStreamer(msg.ReplyCtx, r.renderer, render.PlatformFor(msg.Platform))
		streamer.SetPermissionPromptHandler(permissionHandler)
		streamer.SetQuestionPromptHandler(questionHandler)
		streamer.SetPermissionPendingFunc(func() bool {
			return r.permissions.HasPendingFor(owner)
		})
		if r.streamerNoEventTimeout > 0 {
			streamer.SetNoEventTimeout(r.streamerNoEventTimeout)
		}
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

	if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) {
		slog.Warn("response dispatch failed", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "error", dispatchErr)
		if errors.Is(dispatchErr, relay.ErrAttachmentTooLarge) {
			r.reply(msg, "⚠️ "+dispatchErr.Error())
		} else if errors.Is(dispatchErr, relay.ErrTimeout) {
			r.instances.ForceStop(inst.Workdir())
			r.reply(msg, "⚠️ The agent stopped responding after 3 minutes and was restarted automatically. Please send your message again.")
		} else {
			r.reply(msg, "⚠️ Agent unreachable")
		}
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		slog.Warn("response stream ended", "platform", key.platform, "channel_id", key.channelID, "thread_id", key.threadID, "user_id", key.userID, "error", streamErr)
	}
}

func progressNotice(seconds int64) string {
	minutes := seconds / 60
	return fmt.Sprintf("⏳ Still working... (%dm)", minutes)
}

var (
	progressQuietThreshold = 90 * time.Second
	progressTickerInterval = 15 * time.Second
)

func startProgressTicker(
	ctx context.Context,
	reply channel.ReplyContext,
	activityCh <-chan struct{},
	stopCh <-chan struct{},
	interval, quietThreshold time.Duration,
	notices store.ProgressNoticeRepo,
	platform, channelID, threadID string,
	now func() time.Time,
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
			quiet := now().Sub(lastActivity)
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
