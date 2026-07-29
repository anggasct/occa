package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type Command struct {
	Name    string
	Admin   bool
	Handler func(ctx context.Context, msg channel.IncomingMessage, args string) (string, error)
}

type Router struct {
	commands map[string]Command
	relay    relay.Client
	store    store.Store
	resolver *relay.SessionResolver
}

func New(relayClient relay.Client, st store.Store, resolver *relay.SessionResolver) *Router {
	r := &Router{
		commands: make(map[string]Command),
		relay:    relayClient,
		store:    st,
		resolver: resolver,
	}
	r.registerDefaults()
	return r
}

func (r *Router) Route(ctx context.Context, msg channel.IncomingMessage) error {
	if strings.HasPrefix(msg.Text, "/occa:") {
		return r.handleCommand(ctx, msg)
	}
	return r.passthrough(ctx, msg)
}

func (r *Router) handleCommand(ctx context.Context, msg channel.IncomingMessage) error {
	trimmed := strings.TrimPrefix(msg.Text, "/occa:")
	parts := strings.SplitN(trimmed, " ", 2)
	name := parts[0]
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}

	cmd, ok := r.commands[name]
	if !ok {
		reply := r.helpText()
		msg.ReplyCtx.Send(reply)
		return nil
	}

	reply, err := cmd.Handler(ctx, msg, args)
	if err != nil {
		msg.ReplyCtx.Send("⚠️ " + err.Error())
		return nil
	}
	msg.ReplyCtx.Send(reply)
	return nil
}

func (r *Router) passthrough(ctx context.Context, msg channel.IncomingMessage) error {
	sessionID, err := r.resolver.Resolve(ctx, msg.Platform, msg.ChannelID)
	if err != nil {
		msg.ReplyCtx.Send("⚠️ OpenCode unreachable")
		return nil
	}

	msg.ReplyCtx.SendTyping()

	if strings.HasPrefix(msg.Text, "/") {
		err = r.relay.RunCommand(ctx, sessionID, msg.Text)
	} else {
		err = r.relay.SendMessage(ctx, sessionID, msg.Text)
	}
	if err != nil {
		msg.ReplyCtx.Send("⚠️ OpenCode unreachable")
	}
	return nil
}

func (r *Router) registerDefaults() {
	r.commands["help"] = Command{
		Name:    "help",
		Handler: func(_ context.Context, _ channel.IncomingMessage, _ string) (string, error) { return r.helpText(), nil },
	}
	r.commands["status"] = Command{
		Name:    "status",
		Handler: r.handleStatus,
	}
	r.commands["session"] = Command{
		Name:    "session",
		Handler: r.handleSession,
	}
	r.commands["reset"] = Command{
		Name:    "reset",
		Handler: r.handleReset,
	}
}

func (r *Router) helpText() string {
	return "OCCA commands:\n" +
		"• /occa:help — show this message\n" +
		"• /occa:status — OpenCode health + session info\n" +
		"• /occa:session [list|new|switch <id>|delete <id>] — manage sessions\n" +
		"• /occa:reset — clear current session and start fresh\n\n" +
		"All other messages and /commands are forwarded to OpenCode."
}

func (r *Router) handleStatus(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	sessionID, err := r.resolver.Resolve(ctx, msg.Platform, msg.ChannelID)
	if err != nil {
		return "⚠️ OpenCode unreachable", nil
	}
	return fmt.Sprintf("✅ OpenCode connected\nSession: %s", sessionID), nil
}

func (r *Router) handleSession(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	sub := "list"
	if len(parts) > 0 {
		sub = parts[0]
	}

	switch sub {
	case "list":
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session list: %w", err)
		}
		if len(sessions) == 0 {
			return "No sessions yet.", nil
		}
		var sb strings.Builder
		sb.WriteString("Sessions:\n")
		for _, s := range sessions {
			marker := "  "
			if s.Active {
				marker = "→ "
			}
			sb.WriteString(fmt.Sprintf("%s%s (created %d)\n", marker, s.OpenCodeSessionID, s.CreatedAt))
		}
		return strings.TrimRight(sb.String(), "\n"), nil

	case "new":
		sessionID, err := r.relay.CreateSession(ctx)
		if err != nil {
			return "⚠️ OpenCode unreachable", nil
		}
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, sessionID); err != nil {
			return "", fmt.Errorf("session new: %w", err)
		}
		return fmt.Sprintf("✅ New session: %s", sessionID), nil

	case "switch":
		if len(parts) < 2 {
			return "Usage: /occa:session switch <id>", nil
		}
		target := parts[1]
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}
		found := false
		for _, s := range sessions {
			if s.OpenCodeSessionID == target {
				found = true
				break
			}
		}
		if !found {
			return "Session not found.", nil
		}
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, target); err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}
		return fmt.Sprintf("✅ Switched to: %s", target), nil

	case "delete":
		if len(parts) < 2 {
			return "Usage: /occa:session delete <id>", nil
		}
		target := parts[1]
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session delete: %w", err)
		}
		for _, s := range sessions {
			if s.OpenCodeSessionID == target {
				if err := r.store.SessionRepo().Delete(ctx, s.ID); err != nil {
					return "", fmt.Errorf("session delete: %w", err)
				}
				return fmt.Sprintf("✅ Deleted: %s", target), nil
			}
		}
		return "Session not found.", nil

	default:
		return "Usage: /occa:session [list|new|switch <id>|delete <id>]", nil
	}
}

func (r *Router) handleReset(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	sessionID, err := r.relay.CreateSession(ctx)
	if err != nil {
		return "⚠️ OpenCode unreachable", nil
	}
	if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, sessionID); err != nil {
		return "", fmt.Errorf("reset: %w", err)
	}
	return fmt.Sprintf("✅ Session reset. New session: %s", sessionID), nil
}
