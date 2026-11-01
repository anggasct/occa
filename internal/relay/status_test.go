package relay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const limitStatusJSON = `{"type":"session.status","properties":{"sessionID":"ses-1","status":{"type":"retry","attempt":1,"message":"monthly usage limit reached. It will reset in 16 days - https://example.test/workspace/org_1/go","action":{"reason":"account_rate_limit","provider":"opencode-go","title":"Go limit reached","message":"monthly usage limit reached. It will reset in 16 days","label":"open settings","link":"https://example.test/workspace/org_1/go"},"next":%d}}}`

func limitStatus(next time.Time) string {
	return fmt.Sprintf(limitStatusJSON, next.UnixMilli())
}

// IMP-115 AC-01: a session.status retry line becomes a retry event, JSON-typed
// or SSE-typed; busy/idle statuses and foreign sessions yield nothing.
func TestSessionStatusRetryEvent(t *testing.T) {
	next := time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	for _, eventType := range []string{"", "session.status"} {
		ev, ok := parseSSEEvent(newEventDecoderFor("ses-1"), eventType, limitStatus(next))
		if !ok || ev.Type != EventRetry || ev.Retry == nil {
			t.Fatalf("event type %q: got %+v %v, want a retry event", eventType, ev, ok)
		}
		r := ev.Retry
		if r.Attempt != 1 || r.Reason != "account_rate_limit" || r.Provider != "opencode-go" || !r.Next.Equal(next) {
			t.Fatalf("retry = %+v", r)
		}
		if strings.Contains(r.Message, "https://") || !strings.HasPrefix(r.Message, "monthly usage limit reached") {
			t.Fatalf("message must be the action text without the account link: %q", r.Message)
		}
	}
	for _, line := range []string{
		`{"type":"session.status","properties":{"sessionID":"ses-1","status":{"type":"busy"}}}`,
		`{"type":"session.status","properties":{"sessionID":"ses-1","status":{"type":"idle"}}}`,
	} {
		if ev, ok := parseSSEEvent(newEventDecoderFor("ses-1"), "", line); ok {
			t.Fatalf("%s: got %+v, want nothing", line, ev)
		}
	}
	if ev, ok := parseSSEEvent(newEventDecoderFor("ses-other"), "", limitStatus(next)); ok {
		t.Fatalf("a foreign session's status must be dropped, got %+v", ev)
	}
	transient := `{"type":"session.status","properties":{"sessionID":"ses-1","status":{"type":"retry","attempt":2,"message":"overloaded","next":0}}}`
	ev, ok := parseSSEEvent(newEventDecoderFor("ses-1"), "", transient)
	if !ok || ev.Retry == nil || !ev.Retry.Next.IsZero() || ev.Retry.Message != "overloaded" {
		t.Fatalf("transient retry = %+v %v", ev, ok)
	}
}

// IMP-115 AC-02: a retry past the deadline stops the turn with the provider
// message and aborts the session.
func TestWebhookTurnStopsOnRetryPastDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	next := time.Now().Add(16 * 24 * time.Hour)
	retry := &RetryStatus{Attempt: 1, Message: "monthly usage limit reached", Reason: "account_rate_limit", Provider: "opencode-go", Next: next}
	client := newWebhookTurnClient(eventsWith(Event{Type: EventRetry, Retry: retry}))
	res, err := runWebhookTurn(t, client, ctx)
	if !errors.Is(err, ErrWebhookProviderLimit) {
		t.Fatalf("err = %v, want ErrWebhookProviderLimit", err)
	}
	if !strings.Contains(err.Error(), "monthly usage limit reached (provider opencode-go, next attempt "+next.UTC().Format(time.RFC3339)+")") {
		t.Fatalf("err = %v, want the provider message", err)
	}
	if !res.Aborted {
		t.Fatal("the session must be aborted")
	}
	if res.Progress.Retry != retry {
		t.Fatalf("progress retry = %+v", res.Progress.Retry)
	}
}

// IMP-115 AC-02: a retry inside the deadline keeps waiting and the turn
// completes; output after it clears the recorded retry.
func TestWebhookTurnWaitsOnRetryInsideDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	retry := &RetryStatus{Attempt: 1, Message: "overloaded", Next: time.Now().Add(time.Second)}
	client := newWebhookTurnClient(eventsWith(
		Event{Type: EventRetry, Retry: retry},
		Event{Type: "delta", Delta: "done it"},
		Event{Type: "done"},
	))
	res, err := runWebhookTurn(t, client, ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Output != "done it" || res.Progress.Retry != nil {
		t.Fatalf("result = %q retry=%+v", res.Output, res.Progress.Retry)
	}
}

// IMP-115 AC-03: a timed-out turn that saw a retry names it.
func TestClassifyTimeoutAfterRetry(t *testing.T) {
	progress := TurnProgress{PromptSentAt: time.Now().Add(-time.Hour), Retry: &RetryStatus{Attempt: 3, Message: "rate limited", Provider: "acme"}}
	got := ClassifyTimeoutFailure(progress, time.Hour, "acme/m1", time.Minute)
	if got != "provider kept retrying (attempt 3): rate limited (provider acme)" {
		t.Fatalf("classification = %q", got)
	}
}
