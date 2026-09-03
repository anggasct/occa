package router

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/loop"
)

func (r *Router) SetLooper(l *loop.Looper) {
	r.loops = l
}

func (r *Router) LoopBusy(conv loop.Conversation) bool {
	return r.responses.busy(responseKey{
		platform:  conv.Platform,
		channelID: conv.ChannelID,
		threadID:  conv.ThreadID,
		userID:    conv.UserID,
	})
}

func (c *responseCoordinator) busy(key responseKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.active[key]
	return ok
}

func loopConv(msg channel.IncomingMessage) loop.Conversation {
	threadID, userID := conversationKey(msg)
	return loop.Conversation{
		Platform:  msg.Platform,
		ChannelID: msg.ChannelID,
		ThreadID:  threadID,
		UserID:    userID,
	}
}

func (r *Router) handleLoop(_ context.Context, msg channel.IncomingMessage, args string) (string, error) {
	if r.loops == nil {
		return "⚠️ Loops not available", nil
	}
	trimmed := strings.TrimSpace(args)
	switch {
	case trimmed == "":
		return loop.Usage, nil
	case trimmed == "list":
		return r.listLoops(msg), nil
	case trimmed == "stop" || strings.HasPrefix(trimmed, "stop "):
		return r.stopLoop(msg, strings.TrimSpace(strings.TrimPrefix(trimmed, "stop")))
	case strings.HasPrefix(trimmed, "every"):
		return r.createLoop(msg, trimmed), nil
	default:
		return loop.Usage, nil
	}
}

func (r *Router) handleLoops(_ context.Context, msg channel.IncomingMessage, args string) (string, error) {
	if r.loops == nil {
		return "⚠️ Loops not available", nil
	}
	if strings.TrimSpace(args) != "" {
		return "Usage: /loops", nil
	}
	return r.listLoops(msg), nil
}

func (r *Router) createLoop(msg channel.IncomingMessage, args string) string {
	req, err := loop.ParseRequest(args)
	if err != nil {
		return loop.Usage
	}
	info, err := r.loops.Create(loopConv(msg), req)
	if err != nil {
		var exists *loop.ExistsError
		switch {
		case errors.As(err, &exists):
			return fmt.Sprintf("⚠️ This conversation already has an active loop (%d). Stop it first with /loop stop %d.", exists.ID, exists.ID)
		case errors.Is(err, loop.ErrGlobalLimit):
			return "⚠️ Too many active loops. Try again later."
		default:
			return loop.Usage
		}
	}
	end := fmt.Sprintf("%d runs", req.Count)
	if req.Count == 0 {
		end = "for " + loop.FormatInterval(req.Length)
	}
	return fmt.Sprintf("🔁 Loop %d started: every %s, %s — %s\nStop with /loop stop %d.", info.ID, loop.FormatInterval(req.Interval), end, req.Prompt, info.ID)
}

func (r *Router) stopLoop(msg channel.IncomingMessage, arg string) (string, error) {
	id, err := strconv.Atoi(arg)
	if err != nil || id <= 0 {
		return "⚠️ Invalid loop ID. Usage: /loop stop <id>", nil
	}
	if !r.loops.Stop(loopConv(msg), int64(id)) {
		return fmt.Sprintf("⚠️ Unknown loop %d in this conversation. List with /loops.", id), nil
	}
	return "", errReplied
}

func (r *Router) listLoops(msg channel.IncomingMessage) string {
	infos := r.loops.List(loopConv(msg))
	if len(infos) == 0 {
		return "No active loops in this conversation."
	}
	now := time.Now()
	var sb strings.Builder
	sb.WriteString("Active loops:\n")
	for _, info := range infos {
		left := fmt.Sprintf("%d runs left", info.Total-info.Executed)
		if info.Total == 0 {
			left = loop.FormatLeft(info.Deadline.Sub(now))
		}
		fmt.Fprintf(&sb, "• [%d] every %s, %s — %s\n", info.ID, loop.FormatInterval(info.Interval), left, loop.TruncateRunes(info.Prompt, 40))
	}
	return strings.TrimRight(sb.String(), "\n")
}

func (r *Router) cancelLoops(msg channel.IncomingMessage) {
	if r.loops == nil {
		return
	}
	r.loops.CancelConversation(loopConv(msg))
}
