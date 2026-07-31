package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

func (r *Router) handleCallback(ctx context.Context, msg channel.IncomingMessage) error {
	if !strings.HasPrefix(msg.CallbackData, "permission:") {
		return nil
	}

	parts := strings.SplitN(msg.CallbackData, ":", 3)
	if len(parts) != 3 {
		return nil
	}
	requestID := parts[1]
	replyStr := parts[2]

	var reply relay.PermissionReply
	var confirmation string
	switch replyStr {
	case "once":
		reply = relay.PermissionOnce
		confirmation = "✅ Allowed once"
	case "always":
		reply = relay.PermissionAlways
		confirmation = "✅ Always allowed"
	case "reject":
		reply = relay.PermissionReject
		confirmation = "❌ Denied"
	default:
		return nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		msg.ReplyCtx.Send("⚠️ Agent unreachable")
		return nil
	}
	defer inst.End()

	httpClient, ok := inst.Client().(*relay.HTTPClient)
	if !ok {
		msg.ReplyCtx.Send("⚠️ Agent unreachable")
		return nil
	}

	err = httpClient.ReplyPermission(ctx, requestID, reply)
	if err != nil {
		msg.ReplyCtx.Send(fmt.Sprintf("⚠️ Permission reply failed: %v", err))
		return nil
	}

	msg.ReplyCtx.Send(confirmation)
	return nil
}
