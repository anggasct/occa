package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

func noSessionAgentGuidance(admin bool) string {
	base := "No active session here. Sessions live per thread (auto-thread is on for this channel). /agent <name> now sets the default agent for NEW sessions."
	if admin {
		base += " Channel admins can also set it channel-wide with /agent <name>."
	}
	return base
}

func (r *Router) resolveAgentDefault(ctx context.Context, msg channel.IncomingMessage) (string, string, error) {
	channelID, err := modelScopeChannelID(msg)
	if err != nil {
		return "", "", safeReplyError("Channel information unavailable. Please try again.", err)
	}
	override, err := r.store.OverrideRepo().Get(ctx, msg.Platform, channelID, msg.UserID)
	if err != nil {
		return "", "", fmt.Errorf("agent: get personal override: %w", err)
	}
	if override != nil && override.Agent != "" {
		return override.Agent, "personal override", nil
	}
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, channelID)
	if err != nil {
		return "", "", fmt.Errorf("agent: get channel: %w", err)
	}
	if ch != nil && ch.Agent != "" {
		return ch.Agent, "channel default", nil
	}
	return "", "opencode default", nil
}

func (r *Router) agentDefaultView(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	agent, source, err := r.resolveAgentDefault(ctx, msg)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if agent != "" {
		fmt.Fprintf(&sb, "🤖 Agent: %s\nSource: %s\n\n", agent, source)
	} else {
		sb.WriteString("🤖 Agent: opencode default\nSource: opencode default\n\n")
	}
	sb.WriteString(noSessionAgentGuidance(r.isAdmin(ctx, msg)))
	return sb.String(), nil
}

func (r *Router) applyAgentDefault(ctx context.Context, msg channel.IncomingMessage, inst AgentInstance, sessionID string) {
	agent, source, err := r.resolveAgentDefault(ctx, msg)
	if err != nil {
		slog.Warn("agent default resolution failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "error", err)
		return
	}
	if agent == "" {
		return
	}
	if err := inst.Client().SwitchAgent(ctx, sessionID, agent); err != nil {
		slog.Warn("agent default switch failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "session_id", sessionID, "agent", agent, "error", err)
		return
	}
	slog.Debug("agent default resolved", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "session_id", sessionID, "agent", agent, "tier", source)
}
