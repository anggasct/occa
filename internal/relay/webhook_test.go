package relay

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeWebhookTurnClient struct {
	Client

	mu           sync.Mutex
	sessionSeq   int
	order        []string
	eventsCalls  []string
	sendCalls    []string
	abortCalls   []string
	sessionCalls []string

	newEvents func() <-chan Event
	eventsErr error
	sendErr   error
	sendPanic bool
	abortErr  error
	sendGate  chan struct{}
}

func (f *fakeWebhookTurnClient) record(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, op)
}

func (f *fakeWebhookTurnClient) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func (f *fakeWebhookTurnClient) CreateSession(_ context.Context) (string, error) {
	f.mu.Lock()
	f.sessionSeq++
	id := "sess-" + string(rune('0'+f.sessionSeq))
	f.sessionCalls = append(f.sessionCalls, id)
	f.mu.Unlock()
	f.record("create:" + id)
	return id, nil
}

func (f *fakeWebhookTurnClient) Events(_ context.Context, sessionID string) (<-chan Event, error) {
	f.mu.Lock()
	f.eventsCalls = append(f.eventsCalls, sessionID)
	f.mu.Unlock()
	f.record("events:" + sessionID)
	if f.eventsErr != nil {
		return nil, f.eventsErr
	}
	return f.newEvents(), nil
}

func (f *fakeWebhookTurnClient) SendMessage(_ context.Context, sessionID, _ string, _ *ModelRef, _ []Attachment) error {
	if f.sendGate != nil {
		<-f.sendGate
	}
	if f.sendPanic {
		panic("client exploded during prompt dispatch")
	}
	f.mu.Lock()
	f.sendCalls = append(f.sendCalls, sessionID)
	f.mu.Unlock()
	f.record("send:" + sessionID)
	return f.sendErr
}

func (f *fakeWebhookTurnClient) AbortSession(_ context.Context, sessionID string) error {
	f.mu.Lock()
	f.abortCalls = append(f.abortCalls, sessionID)
	f.mu.Unlock()
	f.record("abort:" + sessionID)
	return f.abortErr
}

func (f *fakeWebhookTurnClient) calls(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.order {
		if strings.HasPrefix(e, op+":") {
			n++
		}
	}
	return n
}

func newWebhookTurnClient(newEvents func() <-chan Event) *fakeWebhookTurnClient {
	return &fakeWebhookTurnClient{newEvents: newEvents}
}

func eventsWith(evs ...Event) func() <-chan Event {
	return func() <-chan Event {
		ch := make(chan Event, len(evs)+1)
		for _, ev := range evs {
			ch <- ev
		}
		return ch
	}
}

func emptyEvents() func() <-chan Event {
	return func() <-chan Event { return make(chan Event) }
}

func runWebhookTurn(t *testing.T, client *fakeWebhookTurnClient, ctx context.Context) (WebhookTurnResult, error) {
	t.Helper()
	turn := WebhookTurn{
		Client:       client,
		Prompt:       "analyze",
		Platform:     "telegram",
		ChannelID:    "chat-1",
		DeliveryID:   "delivery-9",
		ExecutionKey: "owner/repo:main",
		Attempt:      1,
		AbortTimeout: 50 * time.Millisecond,
	}
	return turn.Run(ctx)
}

type capturedRecord struct {
	msg   string
	attrs map[string]any
	level slog.Level
}

type captureHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := capturedRecord{msg: r.Message, level: r.Level, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, rec)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(name string) slog.Handler { return h }

func (h *captureHandler) snapshot() []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]capturedRecord(nil), h.records...)
}

func captureWebhookLogs(t *testing.T) *captureHandler {
	t.Helper()
	previous := slog.Default()
	handler := &captureHandler{}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func TestWebhookTurnSubscribesBeforeSend(t *testing.T) {
	shared := make(chan Event, 4)
	client := newWebhookTurnClient(func() <-chan Event { return shared })
	client.sendGate = make(chan struct{})

	done := make(chan WebhookTurnResult, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := runWebhookTurn(t, client, context.Background())
		done <- res
		errCh <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		order := client.snapshot()
		if len(order) >= 2 {
			if order[0] != "create:sess-1" || order[1] != "events:sess-1" {
				t.Fatalf("expected create then events, got %v", order)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("turn never subscribed, order=%v", order)
		}
		time.Sleep(time.Millisecond)
	}

	if order := client.snapshot(); len(order) != 2 {
		t.Fatalf("prompt sent before unblocked, order=%v", order)
	}
	close(client.sendGate)

	shared <- Event{Type: "done"}
	res, err := <-done, <-errCh
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "sess-1" {
		t.Fatalf("session id = %q", res.SessionID)
	}
	if client.calls("abort") != 0 {
		t.Fatalf("successful turn must not abort, calls=%v", client.snapshot())
	}
}

func TestWebhookTurnCompletesOnOwnedDoneEvent(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(
		Event{Type: "delta", Delta: "hello "},
		Event{Type: "delta", Delta: "world"},
		Event{Type: "done"},
	))

	res, err := runWebhookTurn(t, client, context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Output != "hello world" {
		t.Fatalf("output = %q", res.Output)
	}
	if res.SessionID != "sess-1" {
		t.Fatalf("session id = %q", res.SessionID)
	}
	if len(client.abortCalls) != 0 {
		t.Fatalf("completed turn aborted the session: %v", client.abortCalls)
	}
}

func TestWebhookTurnFreshSessionPerRun(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(Event{Type: "done"}))

	ctx := context.Background()
	first, err := runWebhookTurn(t, client, ctx)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	second, err := runWebhookTurn(t, client, ctx)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}

	if first.SessionID == "" || first.SessionID == second.SessionID {
		t.Fatalf("expected distinct fresh sessions, got %q and %q", first.SessionID, second.SessionID)
	}
	if len(client.sessionCalls) != 2 || len(client.sendCalls) != 2 {
		t.Fatalf("expected 2 create/send calls, got %d/%d", len(client.sessionCalls), len(client.sendCalls))
	}
	if len(client.abortCalls) != 0 {
		t.Fatalf("completed turns must not abort: %v", client.abortCalls)
	}
}

func TestWebhookTurnNeverResumesOrLooksUpSessions(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(Event{Type: "done"}))

	if _, err := runWebhookTurn(t, client, context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, op := range client.snapshot() {
		if strings.HasPrefix(op, "get:") || strings.HasPrefix(op, "exists:") {
			t.Fatalf("turn must not resolve existing sessions: %v", client.snapshot())
		}
	}
	if len(client.sessionCalls) != 1 {
		t.Fatalf("turn must always create a fresh session, calls=%v", client.sessionCalls)
	}
}

func TestWebhookTurnAbortsOnAgentError(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(
		Event{Type: "error", Delta: "model exploded"},
		Event{Type: "done"},
	))

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookAgentResponse) {
		t.Fatalf("expected ErrWebhookAgentResponse, got %v", err)
	}
	if len(client.abortCalls) != 1 || client.abortCalls[0] != "sess-1" {
		t.Fatalf("expected exactly one abort of the owned session, got %v", client.abortCalls)
	}
}

func TestWebhookTurnAbortsOnStreamClose(t *testing.T) {
	closed := make(chan Event, 2)
	close(closed)
	client := newWebhookTurnClient(func() <-chan Event { return closed })

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookResponseIncomplete) {
		t.Fatalf("expected ErrWebhookResponseIncomplete, got %v", err)
	}
	if len(client.abortCalls) != 1 {
		t.Fatalf("expected one abort, got %v", client.abortCalls)
	}
}

func TestWebhookTurnAbortsOnStreamError(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(Event{Type: "stream_error", Err: errors.New("connection reset")}))

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookEventStream) {
		t.Fatalf("expected ErrWebhookEventStream, got %v", err)
	}
	if len(client.abortCalls) != 1 {
		t.Fatalf("expected one abort, got %v", client.abortCalls)
	}
}

func TestWebhookTurnAbortsOnContextTimeout(t *testing.T) {
	client := newWebhookTurnClient(emptyEvents())
	client.sendGate = make(chan struct{})
	close(client.sendGate)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := runWebhookTurn(t, client, ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if len(client.abortCalls) != 1 || client.abortCalls[0] != "sess-1" {
		t.Fatalf("expected exactly one abort of the owned session, got %v", client.abortCalls)
	}
}

func TestWebhookTurnAbortsOnPromptFailure(t *testing.T) {
	client := newWebhookTurnClient(emptyEvents())
	client.sendErr = errors.New("400 bad request")

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookPrompt) {
		t.Fatalf("expected ErrWebhookPrompt, got %v", err)
	}
	if len(client.abortCalls) != 1 {
		t.Fatalf("expected one abort, got %v", client.abortCalls)
	}
}

func TestWebhookTurnAbortsOnEventsFailure(t *testing.T) {
	client := newWebhookTurnClient(emptyEvents())
	client.eventsErr = errors.New("503 unavailable")

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookEventStream) {
		t.Fatalf("expected ErrWebhookEventStream, got %v", err)
	}
	if len(client.abortCalls) != 1 {
		t.Fatalf("expected one abort, got %v", client.abortCalls)
	}
	if len(client.sendCalls) != 0 {
		t.Fatalf("prompt must not be sent when subscription fails, sends=%v", client.sendCalls)
	}
}

func TestWebhookTurnAbortFailureDoesNotChangeOutcome(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(Event{Type: "error", Delta: "boom"}))
	client.abortErr = errors.New("abort endpoint down")

	_, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookAgentResponse) {
		t.Fatalf("abort failure must not mask the turn error, got %v", err)
	}
	if len(client.abortCalls) != 1 {
		t.Fatalf("abort must be attempted exactly once, got %v", client.abortCalls)
	}
}

func findRecord(t *testing.T, handler *captureHandler, msg string) capturedRecord {
	t.Helper()
	for _, rec := range handler.snapshot() {
		if rec.msg == msg {
			return rec
		}
	}
	t.Fatalf("no captured record %q; captured=%v", msg, handler.snapshot())
	return capturedRecord{}
}

func requireTurnAttrs(t *testing.T, rec capturedRecord) {
	t.Helper()
	want := map[string]any{
		"platform":      "telegram",
		"channel":       "chat-1",
		"delivery_id":   "delivery-9",
		"execution_key": "owner/repo:main",
		"attempt":       int64(1),
		"session_id":    "sess-1",
	}
	for key, value := range want {
		got, ok := rec.attrs[key]
		if !ok {
			t.Fatalf("record %q missing attribute %q; attrs=%v", rec.msg, key, rec.attrs)
		}
		if got != value {
			t.Fatalf("record %q attribute %q = %v, want %v", rec.msg, key, got, value)
		}
	}
}

func TestWebhookTurnSuccessLogCarriesDeliveryAndSession(t *testing.T) {
	handler := captureWebhookLogs(t)
	client := newWebhookTurnClient(eventsWith(Event{Type: "done"}))

	res, err := runWebhookTurn(t, client, context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Aborted || res.AbortOK {
		t.Fatalf("successful turn must not report an abort: %+v", res)
	}

	rec := findRecord(t, handler, "relay: webhook session created")
	requireTurnAttrs(t, rec)
	for _, captured := range handler.snapshot() {
		if captured.msg == "relay: webhook session aborted" || captured.msg == "relay: webhook session abort failed" {
			t.Fatalf("successful turn must not emit abort records: %v", handler.snapshot())
		}
	}
}

func TestWebhookTurnAbortSuccessLogCarriesDeliveryAndSession(t *testing.T) {
	handler := captureWebhookLogs(t)
	client := newWebhookTurnClient(eventsWith(Event{Type: "error", Delta: "boom"}))

	res, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookAgentResponse) {
		t.Fatalf("expected ErrWebhookAgentResponse, got %v", err)
	}
	if !res.Aborted || !res.AbortOK {
		t.Fatalf("expected recorded successful abort, got %+v", res)
	}

	rec := findRecord(t, handler, "relay: webhook session aborted")
	requireTurnAttrs(t, rec)
}

func TestWebhookTurnAbortFailureLogCarriesDeliverySessionAndError(t *testing.T) {
	handler := captureWebhookLogs(t)
	client := newWebhookTurnClient(eventsWith(Event{Type: "error", Delta: "boom"}))
	client.abortErr = errors.New("abort endpoint down")

	res, err := runWebhookTurn(t, client, context.Background())
	if !errors.Is(err, ErrWebhookAgentResponse) {
		t.Fatalf("expected ErrWebhookAgentResponse, got %v", err)
	}
	if !res.Aborted || res.AbortOK {
		t.Fatalf("expected recorded failed abort, got %+v", res)
	}

	rec := findRecord(t, handler, "relay: webhook session abort failed")
	requireTurnAttrs(t, rec)
	if rec.attrs["error"] == nil {
		t.Fatalf("abort failure record must carry the error; attrs=%v", rec.attrs)
	}
}

func TestWebhookTurnRecoversPanicAfterSessionCreation(t *testing.T) {
	handler := captureWebhookLogs(t)
	client := newWebhookTurnClient(eventsWith(Event{Type: "done"}))
	client.sendPanic = true

	res, err := runWebhookTurn(t, client, context.Background())
	if err == nil || !strings.Contains(err.Error(), "panic: client exploded") {
		t.Fatalf("expected wrapped panic error, got %v", err)
	}
	if res.SessionID != "sess-1" {
		t.Fatalf("panic path must preserve the owned session id, got %q", res.SessionID)
	}
	if !res.Aborted || !res.AbortOK {
		t.Fatalf("panic path must record the abort outcome, got %+v", res)
	}
	if len(client.abortCalls) != 1 || client.abortCalls[0] != "sess-1" {
		t.Fatalf("expected exactly one abort of the owned session, got %v", client.abortCalls)
	}

	rec := findRecord(t, handler, "relay: webhook session aborted")
	requireTurnAttrs(t, rec)
}
