package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
)

const busyResponseMessage = "⚠️ A response is already running in this conversation. Wait for it to finish or check /occa:status."

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
}

func newResponseCoordinator() *responseCoordinator {
	return &responseCoordinator{active: make(map[responseKey]context.CancelFunc)}
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
	}()

	progressStopCh := make(chan struct{})
	var progressStopOnce sync.Once
	stopProgress := func() {
		progressStopOnce.Do(func() {
			close(progressStopCh)
		})
	}
	defer stopProgress()

	go startProgressTicker(ctx, msg.ReplyCtx, progressStopCh)

	observedEvents := make(chan relay.Event, 64)
	go func() {
		defer close(observedEvents)
		for ev := range events {
			stopProgress()
			select {
			case observedEvents <- ev:
			case <-ctx.Done():
				return
			}
		}
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
		if setter, ok := msg.ReplyCtx.(channel.ReactionSetter); ok {
			streamer.SetReactionSetter(setter)
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
			r.reply(msg, "⚠️ Agent-nya macet (nggak ngerespon 3 menit). Gw udah restart — coba kirim ulang pesan lo ya.")
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
	return fmt.Sprintf("⏳ masih ngerjain... (%dm)", minutes)
}

var progressTickerInterval = 60 * time.Second

func startProgressTicker(ctx context.Context, reply channel.ReplyContext, stopCh <-chan struct{}) {
	ticker := time.NewTicker(progressTickerInterval)
	defer ticker.Stop()

	var elapsed int64
	var noticeRef channel.MessageRef
	var removed bool

	removeNotice := func() {
		if !removed && noticeRef != nil {
			removed = true
			if remover, ok := reply.(channel.MessageRemover); ok {
				_ = remover.Delete(noticeRef)
			}
		}
	}
	defer removeNotice()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			elapsed += 60
			text := progressNotice(elapsed)
			if noticeRef == nil {
				ref, err := reply.Send(text)
				if err == nil {
					noticeRef = ref
				}
			} else {
				_ = reply.Edit(noticeRef, text)
			}
		}
	}
}
