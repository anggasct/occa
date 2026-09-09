package router

import (
	"context"
	"errors"
	"fmt"

	"github.com/anggasct/occa/internal/channel"
)

func (r *Router) SetThreadParentResolver(fn func(string) (string, error)) {
	r.threadParentOf = fn
}

func (r *Router) resolveThreadParent(ctx context.Context, msg channel.IncomingMessage) (string, string, error) {
	if msg.Platform != "discord" {
		return "", "", nil
	}
	if !msg.IsThread {
		return "", "", nil
	}
	threadID := msg.ThreadID
	if threadID == "" {
		return "", "", nil
	}
	if msg.ParentChannelID != "" {
		return msg.ParentChannelID, "message", nil
	}
	parent, err := r.store.SessionRepo().ThreadChannel(ctx, msg.Platform, threadID)
	if err != nil {
		return "", "", fmt.Errorf("thread parent lookup: %w", err)
	}
	if parent != "" && parent != threadID {
		return parent, "session", nil
	}
	if r.threadParentOf == nil {
		return "", "", nil
	}
	parent, err = r.threadParentOf(threadID)
	if err != nil {
		if errors.Is(err, channel.ErrThreadNotFound) || errors.Is(err, channel.ErrParentResolutionUnsupported) {
			return "", "", ErrDenied
		}
		return "", "", fmt.Errorf("thread parent lookup: %w", err)
	}
	if parent == "" || parent == threadID {
		return "", "", nil
	}
	return parent, "api", nil
}
