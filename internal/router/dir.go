package router

import (
	"context"
	"fmt"
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

// effectiveWorkdir resolves the working directory for a channel: the channel's
// stored override, else the application default.
func (r *Router) effectiveWorkdir(ctx context.Context, platform, channelID string) string {
	ch, err := r.store.ChannelRepo().Get(ctx, platform, channelID)
	if err == nil && ch != nil && ch.Workdir != "" {
		return ch.Workdir
	}
	return r.defaultWorkdir
}

func (r *Router) viewDir(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	wd := r.effectiveWorkdir(ctx, msg.Platform, msg.ChannelID)
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

	// A Discord thread has its own channel_id, so writing under msg.ChannelID
	// isolates the override to that thread and leaves the parent unchanged.
	if err := r.store.ChannelRepo().UpsertWorkdir(ctx, msg.Platform, msg.ChannelID, dir); err != nil {
		return "", fmt.Errorf("dir: %w", err)
	}

	// The active session belongs to the old project; clear it so the next
	// message starts a fresh session in the new working directory.
	if err := r.store.SessionRepo().Deactivate(ctx, msg.Platform, msg.ChannelID); err != nil {
		return "", fmt.Errorf("dir: reset session: %w", err)
	}
	return fmt.Sprintf("✅ Workdir set: %s", dir), nil
}
