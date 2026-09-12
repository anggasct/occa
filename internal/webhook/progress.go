package webhook

import (
	"time"
)

// FormatTerminalCard builds the terminal audit status card (COMPLETED or FAILED)
// posted as the final message of a webhook turn.
func FormatTerminalCard(envelope WebhookEnvelope, workflow, status, reason, threadID, platform string, elapsed time.Duration) string {
	card := terminalRootCard(envelope, workflow, status, reason, threadID, platform, elapsed)
	if status == "COMPLETED" {
		card += "\n💬 Continue in this thread to keep full context."
	}
	return card
}
