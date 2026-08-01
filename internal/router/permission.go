package router

import (
	"context"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

func (r *Router) handleCallback(ctx context.Context, msg channel.IncomingMessage) error {
	if !strings.HasPrefix(msg.CallbackData, "permission:") {
		return nil
	}
	return r.permissions.handle(ctx, msg)
}
