package webhook

import (
	"strings"
	"unicode"
)

const webhookMessageSeparator = "━━━━━━━━━━━━━━━━━━━━━━━━"

func FormatWebhookMessage(text string) string {
	text = strings.TrimRightFunc(text, unicode.IsSpace)
	for strings.HasSuffix(text, webhookMessageSeparator) {
		text = strings.TrimRightFunc(strings.TrimSuffix(text, webhookMessageSeparator), unicode.IsSpace)
	}
	if text == "" {
		return webhookMessageSeparator
	}
	return text + "\n" + webhookMessageSeparator
}

func formatWebhookMessage(text string) string {
	return FormatWebhookMessage(text)
}
