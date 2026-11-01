package relay

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// EventRetry reports that the agent is retrying the model call itself
// (opencode session.status type "retry").
const EventRetry = "retry"

// retryMessageRunes bounds the provider message carried on a retry event.
const retryMessageRunes = 300

// RetryStatus is the agent's own retry state for a session.
type RetryStatus struct {
	Attempt  int
	Message  string
	Reason   string
	Provider string
	// Next is when the agent tries again; zero when not reported.
	Next time.Time
}

// String renders the retry for an operator: the provider's message, the
// provider and the next attempt.
func (r RetryStatus) String() string {
	text := r.Message
	if text == "" {
		text = "the model provider asked to retry"
	}
	var meta []string
	if r.Provider != "" {
		meta = append(meta, "provider "+r.Provider)
	}
	if !r.Next.IsZero() {
		meta = append(meta, "next attempt "+r.Next.UTC().Format(time.RFC3339))
	}
	if len(meta) > 0 {
		text += " (" + strings.Join(meta, ", ") + ")"
	}
	return text
}

// parseStatusEvent maps a session.status payload to a retry event. Busy and
// idle statuses carry nothing a turn acts on (session.idle ends the turn).
func parseStatusEvent(data string) (Event, bool) {
	type statusInfo struct {
		Type    string `json:"type"`
		Attempt int    `json:"attempt"`
		Message string `json:"message"`
		Next    int64  `json:"next"`
		Action  *struct {
			Reason   string `json:"reason"`
			Provider string `json:"provider"`
			Message  string `json:"message"`
		} `json:"action"`
	}
	var payload struct {
		Properties struct {
			Status statusInfo `json:"status"`
		} `json:"properties"`
		Status statusInfo `json:"status"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return Event{}, false
	}
	status := payload.Properties.Status
	if status.Type == "" {
		status = payload.Status
	}
	if status.Type != "retry" {
		return Event{}, false
	}
	retry := &RetryStatus{Attempt: status.Attempt, Message: strings.TrimSpace(status.Message)}
	if status.Action != nil {
		retry.Reason = strings.TrimSpace(status.Action.Reason)
		retry.Provider = strings.TrimSpace(status.Action.Provider)
		// The action message is the same text without the account link.
		if msg := strings.TrimSpace(status.Action.Message); msg != "" {
			retry.Message = msg
		}
	}
	retry.Message = clampRunes(strings.Join(strings.Fields(retry.Message), " "), retryMessageRunes)
	if status.Next > 0 {
		retry.Next = time.UnixMilli(status.Next)
	}
	return Event{Type: EventRetry, Retry: retry}, true
}

func clampRunes(text string, max int) string {
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	return string(runes[:max-1]) + "…"
}

// retryOutlivesTurn reports whether the agent's next attempt falls after the
// turn's deadline: waiting cannot produce a result, so the turn stops now.
func retryOutlivesTurn(retry *RetryStatus, deadline time.Time, hasDeadline bool) bool {
	return retry != nil && hasDeadline && !retry.Next.IsZero() && retry.Next.After(deadline)
}

func providerLimitError(retry *RetryStatus) error {
	return fmt.Errorf("%w: %s", ErrWebhookProviderLimit, retry.String())
}
