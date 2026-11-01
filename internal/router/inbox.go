package router

import (
	"context"

	"github.com/anggasct/occa/internal/channel"
)

// handleInbox lists the PRs that wait on the operator, with action buttons.
func (r *Router) handleInbox(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	if r.inboxHook == nil {
		return "", safeReplyError("/inbox needs the webhook relay — start occa with webhooks configured.", nil)
	}
	text, buttons, err := r.inboxHook(ctx, msg)
	if err != nil {
		return "", err
	}
	if len(buttons) > 0 && msg.ReplyCtx != nil {
		if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
			return "", err
		}
		return "", errReplied
	}
	return text, nil
}

// handleDigest posts the daily digest on demand.
func (r *Router) handleDigest(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	if r.digestHook == nil {
		return "", safeReplyError("/digest needs the webhook relay — start occa with webhooks configured.", nil)
	}
	return r.digestHook(ctx, msg)
}
