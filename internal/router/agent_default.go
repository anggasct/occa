package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
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

func matchAgentName(switchable []relay.AgentInfo, target string) *relay.AgentInfo {
	if num, err := strconv.Atoi(target); err == nil && num >= 1 && num <= len(switchable) {
		return &switchable[num-1]
	}
	for i := range switchable {
		if strings.EqualFold(switchable[i].Name, target) {
			return &switchable[i]
		}
	}
	lowerTarget := strings.ToLower(target)
	var single *relay.AgentInfo
	count := 0
	for i := range switchable {
		if strings.Contains(strings.ToLower(switchable[i].Name), lowerTarget) {
			single = &switchable[i]
			count++
			if count > 1 {
				return nil
			}
		}
	}
	return single
}

func (r *Router) handleAgentNoSession(ctx context.Context, msg channel.IncomingMessage, target string, inst AgentInstance) (string, error) {
	channelID, err := modelScopeChannelID(msg)
	if err != nil {
		return "", safeReplyError("Channel information unavailable. Please try again.", err)
	}
	if target == "default" {
		if r.isAdmin(ctx, msg) {
			if err := r.store.ChannelRepo().UpsertAgent(ctx, msg.Platform, channelID, ""); err != nil {
				return "", fmt.Errorf("agent: clear channel: %w", err)
			}
			slog.Info("agent default cleared", "platform", msg.Platform, "channel_id", channelID, "user_id", msg.UserID, "scope", "channel")
			return "✅ Channel agent cleared.", nil
		}
		if err := r.store.OverrideRepo().UpsertAgent(ctx, msg.Platform, channelID, msg.UserID, ""); err != nil {
			return "", fmt.Errorf("agent: clear personal: %w", err)
		}
		slog.Info("agent default cleared", "platform", msg.Platform, "channel_id", channelID, "user_id", msg.UserID, "scope", "personal")
		return "✅ Personal agent cleared.", nil
	}
	allAgents, err := inst.Client().ListAgents(ctx)
	if err != nil {
		if errors.Is(err, relay.ErrUnsupported) {
			return "⚠️ Agent switching is not supported by the current agent backend.", nil
		}
		return "⚠️ Agents unavailable — agent server not responding", nil
	}
	switchable, _ := filterAgents(allAgents)
	matched := matchAgentName(switchable, target)
	if matched == nil {
		return "Agent not found — refresh with /agent", nil
	}
	if r.isAdmin(ctx, msg) {
		if err := r.store.ChannelRepo().UpsertAgent(ctx, msg.Platform, channelID, matched.Name); err != nil {
			return "", fmt.Errorf("agent: set channel: %w", err)
		}
		slog.Info("agent default set", "platform", msg.Platform, "channel_id", channelID, "user_id", msg.UserID, "scope", "channel", "agent", matched.Name)
		return fmt.Sprintf("✅ Channel agent set: %s\nScope: this channel — new sessions start on this agent.", matched.Name), nil
	}
	if err := r.store.OverrideRepo().UpsertAgent(ctx, msg.Platform, channelID, msg.UserID, matched.Name); err != nil {
		return "", fmt.Errorf("agent: set personal: %w", err)
	}
	slog.Info("agent default set", "platform", msg.Platform, "channel_id", channelID, "user_id", msg.UserID, "scope", "personal", "agent", matched.Name)
	return fmt.Sprintf("✅ Personal agent set: %s\nScope: personal — your new sessions start on this agent.", matched.Name), nil
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
