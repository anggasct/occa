package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

var validListenModes = map[string]bool{"mention": true, "all": true, "thread": true}

func (r *Router) handleChannel(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	mode := strings.TrimSpace(args)
	if mode == "" {
		return r.viewChannel(ctx, msg)
	}
	return r.setChannel(ctx, msg, mode)
}

func (r *Router) viewChannel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	mode, err := r.effectiveListenMode(ctx, msg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("📡 Listen mode: %s", mode), nil
}

func (r *Router) setChannel(ctx context.Context, msg channel.IncomingMessage, mode string) (string, error) {
	if !validListenModes[mode] {
		return "Usage: /channel [mention|all|thread]", nil
	}

	if isOwnedThreadMessage(msg) {
		if err := r.store.ThreadConfigRepo().UpsertListenMode(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, mode); err != nil {
			return "", fmt.Errorf("channel: %w", err)
		}
	} else {
		if err := r.store.ChannelRepo().UpsertListenMode(ctx, msg.Platform, msg.ChannelID, mode); err != nil {
			return "", fmt.Errorf("channel: %w", err)
		}
	}
	return fmt.Sprintf("✅ Listen mode set: %s", mode), nil
}
