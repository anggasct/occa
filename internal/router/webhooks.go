package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

const maxWebhookDiagnostics = 8

var webhookStatusEmoji = map[store.WebhookStatus]string{
	store.WebhookStatusReceived:   "📥",
	store.WebhookStatusAccepted:   "📥",
	store.WebhookStatusProcessing: "⏳",
	store.WebhookStatusCompleted:  "✅",
	store.WebhookStatusSkipped:    "⏭️",
	store.WebhookStatusFailed:     "❌",
}

func (r *Router) handleWebhooks(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	if r.store.WebhookDeliveryRepo() == nil {
		return "⚠️ Webhook diagnostics unavailable.", nil
	}
	deliveries, err := r.store.WebhookDeliveryRepo().List(ctx, maxWebhookDiagnostics)
	if err != nil {
		return "", fmt.Errorf("webhooks list: %w", err)
	}
	if len(deliveries) == 0 {
		return "📨 No webhook deliveries recorded.", nil
	}

	lines := make([]string, 0, len(deliveries)+1)
	lines = append(lines, "📨 Recent webhook deliveries:")
	for _, d := range deliveries {
		emoji := webhookStatusEmoji[d.Status]
		if emoji == "" {
			emoji = "•"
		}
		eventType := d.EventType
		if eventType == "" {
			eventType = "unknown"
		}
		line := fmt.Sprintf("%s #%d %s | %s | %s | %s", emoji, d.ID, d.Endpoint, relativeAge(d.CreatedAt), eventType, d.Status)
		if d.Status == store.WebhookStatusFailed && d.ErrorSummary != "" {
			line += " — " + d.ErrorSummary
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}
