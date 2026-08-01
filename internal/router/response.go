package router

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
)

const busyResponseMessage = "⚠️ A response is already running in this channel. Wait for it to finish or check /occa:status."

type responseKey struct {
	platform  string
	channelID string
}

type responseCoordinator struct {
	mu     sync.Mutex
	active map[responseKey]struct{}
}

func newResponseCoordinator() *responseCoordinator {
	return &responseCoordinator{active: make(map[responseKey]struct{})}
}

func (c *responseCoordinator) acquire(key responseKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.active[key]; ok {
		return false
	}
	c.active[key] = struct{}{}
	return true
}

func (c *responseCoordinator) release(key responseKey) {
	c.mu.Lock()
	delete(c.active, key)
	c.mu.Unlock()
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
	slog.Info("response started", "platform", key.platform, "channel_id", key.channelID)
	defer func() {
		r.permissions.expireOwner(owner)
		cancel()
		inst.End()
		r.responses.release(key)
		slog.Info("response finished", "platform", key.platform, "channel_id", key.channelID, "outcome", outcome, "elapsed", time.Since(started).Truncate(time.Millisecond))
	}()

	dispatchDone := make(chan error, 1)
	streamDone := make(chan error, 1)

	go func() {
		dispatchDone <- dispatch(ctx)
	}()
	go func() {
		streamer := relay.NewStreamer(msg.ReplyCtx, r.renderer, render.PlatformFor(msg.Platform))
		streamer.SetPermissionPromptHandler(permissionHandler)
		streamDone <- streamer.Run(ctx, events)
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
		slog.Warn("response dispatch failed", "platform", key.platform, "channel_id", key.channelID, "error", dispatchErr)
		if errors.Is(dispatchErr, relay.ErrAttachmentTooLarge) {
			r.reply(msg, "⚠️ "+dispatchErr.Error())
		} else {
			r.reply(msg, "⚠️ Agent unreachable")
		}
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		slog.Warn("response stream ended", "platform", key.platform, "channel_id", key.channelID, "error", streamErr)
	}
}
