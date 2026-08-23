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
	return r.listenModeView(ctx, msg)
}

func (r *Router) listenModeView(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	mode, err := r.effectiveListenMode(ctx, msg)
	if err != nil {
		return "", err
	}
	location := listenModeLocation(msg)
	return fmt.Sprintf("📡 Listen mode: %s · %s\n%s", location, mode, listenModeNextAction(location, mode)), nil
}

func listenModeLocation(msg channel.IncomingMessage) string {
	if isOwnedThreadMessage(msg) {
		return "this thread"
	}
	return "channel"
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
		channelID, err := modelScopeChannelID(msg)
		if err != nil {
			return "", safeReplyError("Channel information unavailable. Please try again.", err)
		}
		if err := r.store.ChannelRepo().UpsertListenMode(ctx, msg.Platform, channelID, mode); err != nil {
			return "", fmt.Errorf("channel: %w", err)
		}
	}
	view, err := r.listenModeView(ctx, msg)
	if err != nil {
		return fmt.Sprintf("✅ Listen mode set: %s", mode), nil
	}
	return fmt.Sprintf("✅ Listen mode set: %s\n%s", mode, view), nil
}

func listenModeNextAction(location, mode string) string {
	switch mode {
	case "all":
		return "Ordinary messages are forwarded."
	case "thread":
		if location == "this thread" {
			return "Thread messages and mentions are accepted."
		}
		return "Owned-thread messages and mentions are forwarded."
	default:
		if location == "this thread" {
			return "Thread messages are accepted; parent-channel policy remains isolated."
		}
		return "Plain channel messages are ignored; mention OCCA or use /channel all."
	}
}
