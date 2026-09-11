package webhook

import (
	"fmt"
	"time"
)

// FormatTerminalCard builds the terminal audit status card (COMPLETED or FAILED)
// posted as the final message of a webhook turn.
func FormatTerminalCard(envelope WebhookEnvelope, workflow, status, reason, threadID, platform string, elapsed time.Duration) string {
	statusText := status
	switch status {
	case "COMPLETED":
		if elapsed > 0 {
			statusText = fmt.Sprintf("✅ COMPLETED (%s)", formatDuration(elapsed))
		} else {
			statusText = "✅ COMPLETED"
		}
	case "FAILED":
		if elapsed > 0 {
			statusText = fmt.Sprintf("⚠️ FAILED (%s)", formatDuration(elapsed))
		} else {
			statusText = "⚠️ FAILED"
		}
	}
	return FormatRootCard(envelope, workflow, statusText, reason, threadID, platform)
}
