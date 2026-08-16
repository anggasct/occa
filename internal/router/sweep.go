package router

import (
	"context"
	"errors"
	"log/slog"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func SweepStaleProgressNotices(
	ctx context.Context,
	repo store.ProgressNoticeRepo,
	deleter func(platform string) channel.MessageDeleter,
) (int, error) {
	notices, err := repo.List(ctx)
	if err != nil {
		return 0, err
	}

	swept := 0
	for _, n := range notices {
		d := deleter(n.Platform)
		if d == nil {
			continue
		}
		target := n.ChannelID
		if n.Platform == "discord" && n.ThreadID != "" {
			target = n.ThreadID
		}
		err := d.DeleteMessage(target, n.MessageID)
		if err != nil && !errors.Is(err, channel.ErrMessageNotFound) {
			slog.Warn("progress notice sweep delete failed", "platform", n.Platform, "channel_id", n.ChannelID, "thread_id", n.ThreadID, "message_id", n.MessageID, "error", err)
			continue
		}
		if err := repo.Delete(ctx, n.Platform, n.ChannelID, n.ThreadID, n.MessageID); err != nil {
			slog.Warn("progress notice sweep row delete failed", "platform", n.Platform, "channel_id", n.ChannelID, "thread_id", n.ThreadID, "message_id", n.MessageID, "error", err)
			continue
		}
		swept++
	}
	return swept, nil
}
