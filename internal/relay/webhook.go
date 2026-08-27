package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const defaultWebhookAbortTimeout = 5 * time.Second

var (
	ErrWebhookSessionCreate      = errors.New("webhook session create failed")
	ErrWebhookEventStream        = errors.New("webhook session event stream failed")
	ErrWebhookPrompt             = errors.New("webhook session prompt failed")
	ErrWebhookAgentResponse      = errors.New("webhook agent response failed")
	ErrWebhookResponseIncomplete = errors.New("webhook response incomplete")
)

// WebhookTurn owns the agent-session lifecycle of one webhook delivery
// attempt. It creates a fresh session, subscribes to that session's event
// stream before the prompt is sent, and consumes only that stream, so no
// other session's events can complete the turn. The session is never looked
// up, resumed, or persisted; every failed, cancelled, or panicked exit aborts
// it exactly once under a bounded cleanup deadline.
type WebhookTurn struct {
	Client       Client
	Prompt       string
	Model        *ModelRef
	Platform     string
	ChannelID    string
	DeliveryID   string
	ExecutionKey string
	Attempt      int
	AbortTimeout time.Duration
}

type WebhookTurnResult struct {
	SessionID string
	Output    string
	Aborted   bool
	AbortOK   bool
}

func (t WebhookTurn) Run(ctx context.Context) (res WebhookTurnResult, err error) {
	if t.Client == nil {
		return WebhookTurnResult{}, errors.New("relay: webhook turn: nil client")
	}
	if t.AbortTimeout <= 0 {
		t.AbortTimeout = defaultWebhookAbortTimeout
	}

	sessionID, err := t.Client.CreateSession(ctx)
	if err != nil {
		return WebhookTurnResult{}, fmt.Errorf("%w: %w", ErrWebhookSessionCreate, err)
	}
	res.SessionID = sessionID
	slog.Info("relay: webhook session created", t.attrs(sessionID)...)

	completed := false
	defer func() {
		if !completed {
			res.Aborted = true
			res.AbortOK = t.abort(sessionID)
		}
	}()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, err := t.Client.Events(streamCtx, sessionID)
	if err != nil {
		return res, fmt.Errorf("%w: %w", ErrWebhookEventStream, err)
	}

	if err := t.Client.SendMessage(ctx, sessionID, t.Prompt, t.Model, nil); err != nil {
		return res, fmt.Errorf("%w: %w", ErrWebhookPrompt, err)
	}

	var buf strings.Builder
	for {
		select {
		case <-ctx.Done():
			return res, fmt.Errorf("relay: webhook turn: %w", ctx.Err())
		case ev, ok := <-events:
			if !ok {
				return res, fmt.Errorf("%w: event stream ended", ErrWebhookResponseIncomplete)
			}
			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
			case "done":
				completed = true
				res.Output = buf.String()
				return res, nil
			case "stream_error":
				return res, fmt.Errorf("%w: %v", ErrWebhookEventStream, ev.Err)
			case "error":
				return res, ErrWebhookAgentResponse
			}
		}
	}
}

func (t WebhookTurn) attrs(sessionID string, extra ...any) []any {
	attrs := []any{
		"platform", t.Platform,
		"channel", t.ChannelID,
		"delivery_id", t.DeliveryID,
		"execution_key", t.ExecutionKey,
		"attempt", t.Attempt,
		"session_id", sessionID,
	}
	return append(attrs, extra...)
}

func (t WebhookTurn) abort(sessionID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), t.AbortTimeout)
	defer cancel()
	if err := t.Client.AbortSession(ctx, sessionID); err != nil {
		slog.Warn("relay: webhook session abort failed", t.attrs(sessionID, "error", err)...)
		return false
	}
	slog.Info("relay: webhook session aborted", t.attrs(sessionID)...)
	return true
}
