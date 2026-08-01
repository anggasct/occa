package router

import (
	"context"

	"github.com/anggasct/occa/internal/channel"
)

func (r *Router) handleCallback(ctx context.Context, msg channel.IncomingMessage) error {
	return r.permissions.handle(ctx, msg)
}
