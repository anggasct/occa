package router

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/process"
)

func (r *Router) handleDir(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	path := strings.TrimSpace(args)
	if path == "" {
		return r.viewDir(ctx, msg)
	}
	return r.setDir(ctx, msg, path)
}

func (r *Router) viewDir(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	wd, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return "", err
	}
	status := "(exists)"
	if fi, err := os.Stat(wd); err != nil || !fi.IsDir() {
		status = "(missing on disk)"
	}
	return fmt.Sprintf("📂 Workdir: %s %s", wd, status), nil
}

func (r *Router) setDir(ctx context.Context, msg channel.IncomingMessage, path string) (string, error) {
	dir := process.NormalizeWorkdir(path)

	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Sprintf("⚠️ Directory not found: %s", dir), nil
	}
	if !fi.IsDir() {
		return fmt.Sprintf("⚠️ Not a directory: %s", dir), nil
	}

	if isOwnedThreadMessage(msg) {
		if err := r.store.ThreadConfigRepo().UpsertWorkdir(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, dir); err != nil {
			return "", fmt.Errorf("dir: %w", err)
		}
	} else {
		if err := r.store.ChannelRepo().UpsertWorkdir(ctx, msg.Platform, msg.ChannelID, dir); err != nil {
			return "", fmt.Errorf("dir: %w", err)
		}
	}

	threadID, userID := conversationKey(msg)
	if err := r.store.SessionRepo().Deactivate(ctx, msg.Platform, msg.ChannelID, threadID, userID); err != nil {
		return "", fmt.Errorf("dir: reset session: %w", err)
	}

	if setter, ok := msg.ReplyCtx.(channel.ChatCommandSetter); ok {
		go r.updateChatCommands(context.Background(), msg, setter, dir)
	}

	return fmt.Sprintf("✅ Workdir set: %s", dir), nil
}

func (r *Router) updateChatCommands(ctx context.Context, msg channel.IncomingMessage, setter channel.ChatCommandSetter, workdir string) {
	commands := r.MenuCommands()

	inst, err := r.instances.Instance(ctx, workdir)
	if err != nil {
		slog.Warn("dir: resolve agent instance for command menu failed", "workdir", workdir, "error", err)
	} else {
		defer inst.End()
		agentCommands, err := inst.Client().ListCommands(ctx)
		if err != nil {
			slog.Warn("dir: list agent commands failed", "workdir", workdir, "error", err)
		} else {
			for _, c := range agentCommands {
				commands = append(commands, channel.MenuCommand{Alias: c.Name, Description: c.Description})
			}
		}
	}

	if err := setter.SetChatCommands(commands); err != nil {
		slog.Warn("dir: set chat commands failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
	}
}
