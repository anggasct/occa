package router

import (
	"context"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

func (r *Router) processQuestionCallback(ctx context.Context, msg channel.IncomingMessage) error {
	return r.questions.HandleQuestionCallback(ctx, msg)
}

func (r *Router) handleCallback(ctx context.Context, msg channel.IncomingMessage) error {
	if strings.HasPrefix(msg.CallbackData, usageCallbackPrefix) {
		return r.handleUsageCallback(ctx, msg)
	}
	if strings.HasPrefix(msg.CallbackData, modelCallbackPrefix) {
		return r.handleModelCallback(ctx, msg)
	}
	if strings.HasPrefix(msg.CallbackData, "question:") {
		return r.processQuestionCallback(ctx, msg)
	}
	if strings.HasPrefix(msg.CallbackData, "switch:") {
		return r.handleSwitchCallback(ctx, msg)
	}
	if strings.HasPrefix(msg.CallbackData, "spage:") {
		return r.handleSessionPageCallback(ctx, msg)
	}
	if strings.HasPrefix(msg.CallbackData, permCallbackPrefix) {
		return r.handlePermissionCallback(ctx, msg)
	}
	if !strings.HasPrefix(msg.CallbackData, "permission:") {
		return nil
	}
	return r.permissions.handle(ctx, msg)
}
