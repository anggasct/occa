package router

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func isOwnedThreadMessage(msg channel.IncomingMessage) bool {
	return msg.IsThread && msg.ThreadID != "" && msg.ChannelID != msg.ThreadID
}

// threadScopeChannelID returns the parent chat/channel identity that scopes a
// thread. Telegram forum topic ids and Discord thread ids are only unique
// within their parent channel, so the thread-config key must include it.
func threadScopeChannelID(msg channel.IncomingMessage) string {
	if msg.ParentChannelID != "" {
		return msg.ParentChannelID
	}
	return msg.ChannelID
}

func (r *Router) ensureThreadConfig(ctx context.Context, msg channel.IncomingMessage) error {
	if !isOwnedThreadMessage(msg) {
		return nil
	}
	return r.store.ThreadConfigRepo().SnapshotFromChannel(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, r.defaultWorkdir)
}

// threadRow returns the thread snapshot for an owned thread message, or nil
// when the message is not thread-scoped. A repository read error is propagated
// so callers fail closed instead of treating the failure as "no snapshot" and
// falling back to the parent channel (which would cross the isolation
// boundary: once a thread_config row exists, the channel is never consulted).
func (r *Router) threadRow(ctx context.Context, msg channel.IncomingMessage) (*store.ThreadConfig, error) {
	if !isOwnedThreadMessage(msg) {
		return nil, nil
	}
	channelID := threadScopeChannelID(msg)
	row, err := r.store.ThreadConfigRepo().Get(ctx, msg.Platform, channelID, msg.ThreadID)
	if err != nil {
		slog.Warn("router: read thread config failed", "platform", msg.Platform, "channel_id", channelID, "thread_id", msg.ThreadID, "error", err)
		return nil, fmt.Errorf("router: read thread config: %w", err)
	}
	return row, nil
}

// effectiveWorkdir resolves the working directory for a message. Channel
// fallback happens only after a successful read that returned no thread row;
// a thread-config read failure is propagated so the thread is never routed to
// the channel's agent instance.
func (r *Router) effectiveWorkdir(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	row, err := r.threadRow(ctx, msg)
	if err != nil {
		return "", err
	}
	if row != nil {
		if row.Workdir != "" {
			return row.Workdir, nil
		}
		return r.defaultWorkdir, nil
	}
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err == nil && ch != nil && ch.Workdir != "" {
		return ch.Workdir, nil
	}
	return r.defaultWorkdir, nil
}

// effectiveListenMode resolves the listen mode for a message. Channel
// fallback happens only after a successful read that returned no thread row;
// a thread-config read failure is propagated so the caller can fail closed.
func (r *Router) effectiveListenMode(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	row, err := r.threadRow(ctx, msg)
	if err != nil {
		return "", err
	}
	if row != nil {
		if row.ListenMode != "" {
			return row.ListenMode, nil
		}
		return "mention", nil
	}
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err == nil && ch != nil && ch.ListenMode != "" {
		return ch.ListenMode, nil
	}
	return "mention", nil
}
