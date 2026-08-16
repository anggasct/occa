package router

import (
	"context"
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

func (r *Router) threadRow(ctx context.Context, msg channel.IncomingMessage) *store.ThreadConfig {
	if !isOwnedThreadMessage(msg) {
		return nil
	}
	channelID := threadScopeChannelID(msg)
	row, err := r.store.ThreadConfigRepo().Get(ctx, msg.Platform, channelID, msg.ThreadID)
	if err != nil {
		slog.Warn("router: read thread config failed", "platform", msg.Platform, "channel_id", channelID, "thread_id", msg.ThreadID, "error", err)
		return nil
	}
	return row
}

func (r *Router) effectiveWorkdir(ctx context.Context, msg channel.IncomingMessage) string {
	if row := r.threadRow(ctx, msg); row != nil {
		if row.Workdir != "" {
			return row.Workdir
		}
		return r.defaultWorkdir
	}
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err == nil && ch != nil && ch.Workdir != "" {
		return ch.Workdir
	}
	return r.defaultWorkdir
}

func (r *Router) effectiveListenMode(ctx context.Context, msg channel.IncomingMessage) string {
	if row := r.threadRow(ctx, msg); row != nil {
		if row.ListenMode != "" {
			return row.ListenMode
		}
		return "mention"
	}
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err == nil && ch != nil && ch.ListenMode != "" {
		return ch.ListenMode
	}
	return "mention"
}
