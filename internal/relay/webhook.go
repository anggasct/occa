package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var (
	ErrWebhookSessionCreate      = errors.New("webhook session create failed")
	ErrWebhookEventStream        = errors.New("webhook session event stream failed")
	ErrWebhookPrompt             = errors.New("webhook session prompt failed")
	ErrWebhookAgentResponse      = errors.New("webhook agent response failed")
	ErrWebhookResponseIncomplete = errors.New("webhook response incomplete")
)

type WebhookTurn struct {
	Client        Client
	Prompt        string
	Model         *ModelRef
	Platform      string
	ChannelID     string
	DeliveryID    string
	ExecutionKey  string
	Attempt       int
	AbortTimeout  time.Duration
	VerifyTimeout time.Duration
	OnEvent       func(ev Event)
	OnSession     func(sessionID string)
	Streamer      *Streamer
}

type WebhookTurnResult struct {
	SessionID string
	Output    string
	Aborted   bool
	AbortOK   bool
	Progress  TurnProgress
}

func (t WebhookTurn) Run(ctx context.Context) (res WebhookTurnResult, err error) {
	if t.Client == nil {
		return WebhookTurnResult{}, errors.New("relay: webhook turn: nil client")
	}

	sessionID, err := t.Client.CreateSession(ctx)
	if err != nil {
		return WebhookTurnResult{}, fmt.Errorf("%w: %w", ErrWebhookSessionCreate, err)
	}
	res.SessionID = sessionID
	slog.Info("relay: webhook session created", t.attrs(sessionID)...)
	if t.OnSession != nil {
		t.OnSession(sessionID)
	}

	completed := false
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("relay: webhook turn: panic: %v", r)
		}
		if !completed && res.SessionID != "" {
			res.Aborted = true
			res.AbortOK = t.abort(res.SessionID)
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
	res.Progress.PromptSentAt = time.Now()

	var buf strings.Builder
	var (
		streamerEvents chan Event
		streamerErrCh  chan error
		streamerOnce   sync.Once
	)
	if t.Streamer != nil {
		streamerEvents = make(chan Event, 64)
		streamerErrCh = make(chan error, 1)
		go func() {
			streamerErrCh <- t.Streamer.Run(ctx, streamerEvents)
		}()
	}
	closeStreamer := func() {
		if streamerEvents != nil {
			streamerOnce.Do(func() {
				close(streamerEvents)
				<-streamerErrCh
			})
		}
	}
	defer closeStreamer()

	for {
		select {
		case <-ctx.Done():
			closeStreamer()
			return res, fmt.Errorf("relay: webhook turn: %w", ctx.Err())
		case ev, ok := <-events:
			if !ok {
				closeStreamer()
				return res, fmt.Errorf("%w: event stream ended", ErrWebhookResponseIncomplete)
			}
			if t.OnEvent != nil {
				t.OnEvent(ev)
			}
			if streamerEvents != nil {
				select {
				case streamerEvents <- ev:
				case <-ctx.Done():
					closeStreamer()
					return res, fmt.Errorf("relay: webhook turn: %w", ctx.Err())
				}
			}
			switch ev.Type {
			case "delta":
				now := time.Now()
				if res.Progress.DeltaCount == 0 {
					res.Progress.FirstDeltaAt = now
				}
				res.Progress.LastDeltaAt = now
				res.Progress.DeltaCount++
				buf.WriteString(ev.Delta)
			case "permission_asked":
				t.denyUnhandledPermission(ctx, ev)
			case "question_asked":
				t.rejectUnhandledQuestion(ctx, ev)
			case "done":
				res.Output = buf.String()
				closeStreamer()
				if err := t.verify(res.SessionID, res.Output); err != nil {
					// completed stays false: the failed gate is not a
					// finished turn, so the deferred cleanup aborts the
					// session exactly once.
					return res, err
				}
				completed = true
				return res, nil
			case "stream_error":
				closeStreamer()
				return res, fmt.Errorf("%w: %v", ErrWebhookEventStream, ev.Err)
			case "error":
				closeStreamer()
				return res, ErrWebhookAgentResponse
			}
		}
	}
}

// verify is the sanity gate before a turn reports success: a terminal event
// alone is not proof of a result. (a) the buffered output must be non-empty,
// and (b) the agent must show at least one assistant message with
// time.completed set (the same message-tail API the /status context read
// uses). A stream that ended with an empty buffer or a still-in-flight
// assistant message means the turn produced no verified result — surfaced as
// ErrWebhookResponseIncomplete so the dispatcher can self-heal with one
// retry. The deferred cleanup aborts the session exactly once as before.
func (t WebhookTurn) verify(sessionID, output string) error {
	if strings.TrimSpace(output) == "" {
		return fmt.Errorf("%w: empty output buffer at terminal event", ErrWebhookResponseIncomplete)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.VerifyTimeout)
	defer cancel()
	messages, err := t.Client.ListMessages(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("%w: list messages: %v", ErrWebhookResponseIncomplete, err)
	}
	for _, msg := range messages {
		if msg.Role == "assistant" && msg.Completed > 0 {
			return nil
		}
	}
	return fmt.Errorf("%w: no completed assistant message for session", ErrWebhookResponseIncomplete)
}

// denyUnhandledPermission rejects a permission request this lane cannot
// answer. The webhook lane has no permission-prompt handler, so an unanswered
// request leaves the tool call running forever: the agent freezes, the
// delivery stays in processing and the PR lock stays held. A denial is a
// result the agent reports and the run ends.
func (t WebhookTurn) denyUnhandledPermission(ctx context.Context, ev Event) {
	if ev.Permission == nil {
		return
	}
	requestID := strings.TrimSpace(ev.Permission.ID)
	tool := strings.TrimSpace(ev.Permission.Tool)
	if requestID == "" {
		slog.Warn("webhook: permission denied, no handler", "tool", tool, "reason", "no request id")
		return
	}
	if err := replyPermission(ctx, t.Client, requestID, PermissionReject); err != nil {
		slog.Error("webhook: permission denied, no handler", "tool", tool, "request_id", requestID, "error", err)
		return
	}
	slog.Warn("webhook: permission denied, no handler", "tool", tool, "request_id", requestID)
}

// replyPermission answers a permission request without taking the turn down
// when the client misbehaves: a failed denial must not end the run silently.
func replyPermission(ctx context.Context, client Client, requestID string, reply PermissionReply) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("relay: reply permission: %v", r)
		}
	}()
	if client == nil {
		return errors.New("relay: reply permission: nil client")
	}
	return client.ReplyPermission(ctx, requestID, reply)
}

// rejectUnhandledQuestion rejects a question this lane cannot answer, for the
// same reason a permission request is denied: an unanswered question leaves
// the tool call running until the run times out.
func (t WebhookTurn) rejectUnhandledQuestion(ctx context.Context, ev Event) {
	if ev.Question == nil {
		return
	}
	requestID := strings.TrimSpace(ev.Question.ID)
	if requestID == "" {
		slog.Warn("webhook: question denied, no handler", "reason", "no request id")
		return
	}
	if err := rejectQuestion(ctx, t.Client, requestID); err != nil {
		slog.Error("webhook: question denied, no handler", "request_id", requestID, "error", err)
		return
	}
	slog.Warn("webhook: question denied, no handler", "request_id", requestID)
}

func rejectQuestion(ctx context.Context, client Client, requestID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("relay: reject question: %v", r)
		}
	}()
	if client == nil {
		return errors.New("relay: reject question: nil client")
	}
	return client.RejectQuestion(ctx, requestID)
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
		// An unreachable agent or a session that is already gone means the
		// abort's cleanup goal is already achieved — there is nothing left
		// to stop. This is the normal cascade when occa restarts/shuts down
		// mid-delivery: the agent dies with occa, the SSE stream breaks, and
		// the deferred cleanup finds a dead agent. Treating that as a WARN +
		// failed abort is misleading alert noise and reports a wrong
		// session_abort_ok in delivery logs.
		if errors.Is(err, ErrUnreachable) || errors.Is(err, ErrNotFound) {
			slog.Info("relay: webhook session abort skipped", t.attrs(sessionID, "reason", err.Error())...)
			return true
		}
		slog.Warn("relay: webhook session abort failed", t.attrs(sessionID, "error", err)...)
		return false
	}
	slog.Info("relay: webhook session aborted", t.attrs(sessionID)...)
	return true
}
