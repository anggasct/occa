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
	Scope        string
	AbortTimeout time.Duration
}

type WebhookTurnResult struct {
	SessionID string
	Output    string
}

func (t WebhookTurn) Run(ctx context.Context) (WebhookTurnResult, error) {
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
	t.log(sessionID, "relay: webhook session created")

	completed := false
	defer func() {
		if !completed {
			t.abort(sessionID)
		}
	}()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, err := t.Client.Events(streamCtx, sessionID)
	if err != nil {
		return WebhookTurnResult{SessionID: sessionID}, fmt.Errorf("%w: %w", ErrWebhookEventStream, err)
	}

	if err := t.Client.SendMessage(ctx, sessionID, t.Prompt, t.Model, nil); err != nil {
		return WebhookTurnResult{SessionID: sessionID}, fmt.Errorf("%w: %w", ErrWebhookPrompt, err)
	}

	var buf strings.Builder
	for {
		select {
		case <-ctx.Done():
			return WebhookTurnResult{SessionID: sessionID}, fmt.Errorf("relay: webhook turn: %w", ctx.Err())
		case ev, ok := <-events:
			if !ok {
				return WebhookTurnResult{SessionID: sessionID}, fmt.Errorf("%w: event stream ended", ErrWebhookResponseIncomplete)
			}
			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
			case "done":
				completed = true
				return WebhookTurnResult{SessionID: sessionID, Output: buf.String()}, nil
			case "stream_error":
				return WebhookTurnResult{SessionID: sessionID}, fmt.Errorf("%w: %v", ErrWebhookEventStream, ev.Err)
			case "error":
				return WebhookTurnResult{SessionID: sessionID}, ErrWebhookAgentResponse
			}
		}
	}
}

func (t WebhookTurn) abort(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), t.AbortTimeout)
	defer cancel()
	if err := t.Client.AbortSession(ctx, sessionID); err != nil {
		slog.Warn("relay: webhook session abort failed", "scope", t.Scope, "session_id", sessionID, "error", err)
		return
	}
	slog.Info("relay: webhook session aborted", "scope", t.Scope, "session_id", sessionID)
}

func (t WebhookTurn) log(sessionID, msg string) {
	slog.Info(msg, "scope", t.Scope, "session_id", sessionID)
}
