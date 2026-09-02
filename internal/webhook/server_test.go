package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func newTestServer(t *testing.T, endpoints []config.EndpointConfig) (*Server, *fakeExecutor) {
	t.Helper()
	exec := &fakeExecutor{}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Endpoints: endpoints,
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec.exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	return srv, exec
}

type fakeExecutor struct {
	mu    sync.Mutex
	calls []execCall
	err   error
	block chan struct{}
	// errOnAttempt, when non-nil, overrides err per attempt (1-based). Used
	// for the incomplete-response retry tests: first attempt fails with
	// ErrWebhookResponseIncomplete, second succeeds.
	errOnAttempt func(attempt int) error
}

type execCall struct {
	platform  string
	channelID string
	prompt    string
	workCtx   *WebhookWorkContext
}

func (f *fakeExecutor) exec(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.errOnAttempt != nil {
		err := f.errOnAttempt(workCtx.Attempt)
		f.mu.Lock()
		f.calls = append(f.calls, execCall{platform, channelID, prompt, workCtx})
		f.mu.Unlock()
		return err
	}
	if f.err != nil {
		f.mu.Lock()
		f.calls = append(f.calls, execCall{platform, channelID, prompt, workCtx})
		f.mu.Unlock()
		return f.err
	}
	f.mu.Lock()
	f.calls = append(f.calls, execCall{platform, channelID, prompt, workCtx})
	f.mu.Unlock()
	return nil
}

func (f *fakeExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeExecutor) getCalls() []execCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := make([]execCall, len(f.calls))
	copy(res, f.calls)
	return res
}

type failingTransitionStore struct {
	DeliveryStore
	to  store.WebhookStatus
	err error
}

func (s *failingTransitionStore) Transition(ctx context.Context, id int64, from []store.WebhookStatus, to store.WebhookStatus, summary string) (bool, error) {
	if to == s.to {
		return false, s.err
	}
	return s.DeliveryStore.Transition(ctx, id, from, to, summary)
}

type failOnceTransitionStore struct {
	DeliveryStore
	to     store.WebhookStatus
	err    error
	failed atomic.Bool
}

func (s *failOnceTransitionStore) Disarm() {
	s.failed.Store(true)
}

func (s *failOnceTransitionStore) Transition(ctx context.Context, id int64, from []store.WebhookStatus, to store.WebhookStatus, summary string) (bool, error) {
	if to == s.to && !s.failed.Swap(true) {
		return false, s.err
	}
	return s.DeliveryStore.Transition(ctx, id, from, to, summary)
}

func TestWebhookValidSecret(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze this"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","number":42}`
	resp, err := http.Post(ts.URL+"/github?secret=s3cret", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestWebhookRejectsOversizedBody(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := strings.Repeat("x", int(maxWebhookBodySize)+1)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/github?secret=s3cret", io.NopCloser(strings.NewReader(body)))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

func TestWebhookInvalidSecret(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/github?secret=wrong", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebhookMissingSecret(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/github", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebhookEmptySecret(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	for _, url := range []string{ts.URL + "/github", ts.URL + "/github?secret=anything"} {
		resp, err := http.Post(url, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 for empty endpoint secret, got %d", resp.StatusCode)
		}
	}
}

func TestWebhookRejectsNonPost(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/github", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Allow") != http.MethodPost {
		t.Fatalf("Allow = %q, want POST", resp.Header.Get("Allow"))
	}
}

func TestWebhookHeaderSecret(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	req, _ := http.NewRequest("POST", ts.URL+"/github", strings.NewReader(`{}`))
	req.Header.Set("X-Webhook-Secret", "s3cret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with header secret, got %d", resp.StatusCode)
	}
}

func TestWebhookTemplateRendering(t *testing.T) {
	srv, exec := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1",
			Prompt: `Analyze PR #{{.payload.number}} action={{.payload.action}}`},
	})

	_ = srv.executeDelivery(config.EndpointConfig{
		Name: "github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1",
		Prompt: `Analyze PR #{{.payload.number}} action={{.payload.action}}`,
	}, []byte(`{"action":"opened","number":42}`), 0, "delivery-1", "pull_request", 1, nil, nil)
	if len(exec.calls) != 1 {
		t.Fatalf("expected 1 executor call, got %d", len(exec.calls))
	}
	prompt := exec.calls[0].prompt
	if !strings.Contains(prompt, "PR #42") {
		t.Fatalf("expected rendered PR #42, got: %s", prompt)
	}
	if !strings.Contains(prompt, "action=opened") {
		t.Fatalf("expected rendered action=opened, got: %s", prompt)
	}
	if !strings.Contains(prompt, "<untrusted_payload>") {
		t.Fatalf("expected untrusted_payload wrapper, got: %s", prompt)
	}
}

func TestWebhookUntrustedPayloadWrapper(t *testing.T) {
	srv, exec := newTestServer(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: "static prompt"},
	})

	_ = srv.executeDelivery(config.EndpointConfig{
		Prompt: "static prompt", Platform: "telegram", ChannelID: "c1",
	}, []byte(`{"foo":"bar"}`), 0, "delivery-1", "", 1, nil, nil)

	if len(exec.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(exec.calls))
	}
	if !strings.HasPrefix(exec.calls[0].prompt, "<untrusted_payload>") {
		t.Fatalf("expected untrusted_payload prefix, got: %s", exec.calls[0].prompt)
	}
	if !strings.HasSuffix(exec.calls[0].prompt, "</untrusted_payload>") {
		t.Fatalf("expected untrusted_payload suffix, got: %s", exec.calls[0].prompt)
	}
}

func TestWebhookEscapesClosingPayloadWrapper(t *testing.T) {
	srv, exec := newTestServer(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: `{{.json}}`},
	})

	_ = srv.executeDelivery(config.EndpointConfig{Prompt: `{{.json}}`}, []byte(`{"payload":"</untrusted_payload>"}`), 0, "delivery-1", "", 1, nil, nil)

	if len(exec.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(exec.calls))
	}
	if strings.Count(exec.calls[0].prompt, "</untrusted_payload>") != 1 {
		t.Fatalf("expected only the wrapper closing tag, got: %s", exec.calls[0].prompt)
	}
	if !strings.Contains(exec.calls[0].prompt, "&lt;/untrusted_payload&gt;") {
		t.Fatalf("expected payload closing tag to be escaped, got: %s", exec.calls[0].prompt)
	}
}

func TestWebhookTemplateErrorFailsClosed(t *testing.T) {
	srv, exec := newTestServer(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: "broken {{"},
	})
	body := []byte(`{"foo":"bar"}`)

	_ = srv.executeDelivery(config.EndpointConfig{Prompt: "broken {{"}, body, 0, "delivery-1", "", 1, nil, nil)

	if len(exec.calls) != 0 {
		t.Fatalf("template failure must not invoke executor, got %d calls", len(exec.calls))
	}
}

func TestWebhookConcurrencyLimit(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: "Analyze"},
	})

	for i := 0; i < maxConcurrentWebhookEvents; i++ {
		if !srv.tryAcquireEvent() {
			t.Fatalf("failed to acquire event slot %d", i)
		}
	}
	if srv.tryAcquireEvent() {
		t.Fatal("expected concurrency limit to reject an extra event")
	}
	for i := 0; i < maxConcurrentWebhookEvents; i++ {
		srv.releaseEvent()
	}
}

func TestWebhookStopWithoutStart(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestWebhookUnknownPath(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/unknown", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestRenderTemplateNoBraces(t *testing.T) {
	result, err := renderTemplate("plain text", nil)
	if err != nil {
		t.Fatalf("renderTemplate: %v", err)
	}
	if result != "plain text" {
		t.Fatalf("expected 'plain text', got %q", result)
	}
}

func TestRenderTemplateJSONField(t *testing.T) {
	data := map[string]any{
		"payload": map[string]any{"key": "val"},
		"json":    `{"key":"val"}`,
	}
	result, err := renderTemplate("json={{.json}}", data)
	if err != nil {
		t.Fatalf("renderTemplate: %v", err)
	}
	if !strings.Contains(result, `"key":"val"`) {
		t.Fatalf("expected json in output, got: %s", result)
	}
}

func TestStartBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	srv := New(config.WebhookConfig{Bind: ln.Addr().String()}, nil, st.WebhookDeliveryRepo())
	if err := srv.Start(context.Background()); err == nil {
		t.Fatal("expected error when bind address is taken")
	}
}

func TestStartServesImmediately(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	resp, err := http.Get("http://" + srv.Addr() + "/unknown")
	if err != nil {
		t.Fatalf("request after Start returned: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown path, got %d", resp.StatusCode)
	}
}

func TestUnexpectedServeFailureClearsHealth(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	if err := srv.listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for srv.Healthy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.Healthy() {
		t.Fatal("Healthy() = true after Serve returned from an unexpected listener failure")
	}
}

func TestServerTimeoutsSet(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	s := srv.httpSrv
	if s.ReadHeaderTimeout <= 0 || s.ReadTimeout <= 0 || s.WriteTimeout <= 0 || s.IdleTimeout <= 0 {
		t.Fatalf("server timeouts not set: %+v", s)
	}
}

func TestReadHeaderTimeoutClosesSilentConnection(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	srv.readHeaderTimeout = 200 * time.Millisecond

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("server held a silent connection open past the header timeout")
	}
}

func newTestServerFull(t *testing.T, endpoints []config.EndpointConfig) (*Server, *fakeExecutor, *store.SQLiteStore) {
	t.Helper()
	exec := &fakeExecutor{}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Endpoints: endpoints,
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec.exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	return srv, exec, st
}

func post(t *testing.T, url, deliveryID, eventType, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if deliveryID != "" {
		req.Header.Set("X-GitHub-Delivery", deliveryID)
	}
	if eventType != "" {
		req.Header.Set("X-GitHub-Event", eventType)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func waitForReceipt(t *testing.T, st *store.SQLiteStore, status store.WebhookStatus) *store.WebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		deliveries, err := st.WebhookDeliveryRepo().List(context.Background(), 20)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(deliveries) > 0 && deliveries[0].Status == status {
			return &deliveries[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("receipt never reached %s: %+v", status, mustList(t, st))
	return nil
}

func mustList(t *testing.T, st *store.SQLiteStore) []store.WebhookDelivery {
	t.Helper()
	deliveries, err := st.WebhookDeliveryRepo().List(context.Background(), 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return deliveries
}

func waitForCompletedCount(t *testing.T, st *store.SQLiteStore, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		deliveries, err := st.WebhookDeliveryRepo().List(context.Background(), 50)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		completed := 0
		for _, d := range deliveries {
			if isTerminal(d.Status) {
				completed++
			}
		}
		if completed >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not reach %d completed deliveries: %+v", want, mustList(t, st))
}

func waitForAttempt(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string, want int) *store.WebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if receipt != nil && receipt.Attempt >= want {
			return receipt
		}
		time.Sleep(10 * time.Millisecond)
	}
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
	t.Fatalf("receipt %s/%s never reached attempt %d: %+v", endpoint, deliveryID, want, receipt)
	return nil
}

func TestWebhookReceiptLifecycleAndReplay(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened","number":42}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusCompleted)
	if receipt.DeliveryID != "delivery-1" || receipt.EventType != "pull_request" || receipt.Endpoint != "github" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if receipt.Status != store.WebhookStatusCompleted || receipt.CompletedAt == 0 || receipt.StartedAt == 0 {
		t.Fatalf("receipt lifecycle incomplete: %+v", receipt)
	}
	if receipt.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", receipt.Attempt)
	}
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}

	resp = post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened","number":42}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay expected 200, got %d", resp.StatusCode)
	}
	if exec.callCount() != 1 {
		t.Fatalf("replay started a second session: %d executor calls", exec.callCount())
	}
	receipt = waitForAttempt(t, st, "github", "delivery-1", 2)
	if receipt.Status != store.WebhookStatusCompleted {
		t.Fatalf("replay receipt: attempt=%d status=%s", receipt.Attempt, receipt.Status)
	}
	if len(mustList(t, st)) != 1 {
		t.Fatalf("expected exactly one receipt row, got %d", len(mustList(t, st)))
	}
}

func TestWebhookReplayWithinGraceDoesNotRestartProcessing(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "fresh", EventType: "pull_request", PayloadHash: "abc", Attempt: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	fresh, err := repo.Get(ctx, "github", "fresh")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok, err := repo.Transition(ctx, fresh.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	before, err := repo.Get(ctx, "github", "fresh")
	if err != nil {
		t.Fatalf("Get before replay: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	resp := post(t, ts.URL+"/github?secret=s3cret", "fresh", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay expected 200, got %d", resp.StatusCode)
	}
	if exec.callCount() != 0 {
		t.Fatalf("fresh replay started %d sessions", exec.callCount())
	}
	after := waitForAttempt(t, st, "github", "fresh", 2)
	if after.Status != store.WebhookStatusProcessing || after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("fresh replay changed receipt unexpectedly: before=%+v after=%+v", before, after)
	}
}

func TestWebhookReplayAfterGraceRecoversStaleProcessing(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "stale", EventType: "pull_request", PayloadHash: "abc", Attempt: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	stale, err := repo.Get(ctx, "github", "stale")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok, err := repo.Transition(ctx, stale.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = ? WHERE id = ?`, time.Now().Add(-claimGrace-time.Second).Unix(), stale.ID); err != nil {
		t.Fatalf("backdate stale receipt: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	resp := post(t, ts.URL+"/github?secret=s3cret", "stale", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stale replay expected 200, got %d", resp.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("stale replay started %d sessions, want 1", exec.callCount())
	}
	recovered, err := repo.Get(ctx, "github", "stale")
	if err != nil {
		t.Fatalf("Get recovered receipt: %v", err)
	}
	if recovered.Attempt != 2 || recovered.Status != store.WebhookStatusCompleted {
		t.Fatalf("stale replay receipt: %+v", recovered)
	}
}

func TestWebhookSerializesSameSessionDeliveries(t *testing.T) {
	var mu sync.Mutex
	starts := 0
	firstStarted := make(chan struct{})
	firstReleased := make(chan struct{})
	secondStarted := make(chan struct{})

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		starts++
		call := starts
		mu.Unlock()

		switch call {
		case 1:
			close(firstStarted)
			select {
			case <-firstReleased:
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			close(secondStarted)
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery-1 executor never started")
	}

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-2", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-secondStarted:
		t.Fatal("delivery-2 started before delivery-1 returned")
	case <-time.After(150 * time.Millisecond):
	}

	close(firstReleased)
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery-2 executor never started after delivery-1 returned")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		deliveries := mustList(t, st)
		completed := 0
		for _, delivery := range deliveries {
			if delivery.Status == store.WebhookStatusCompleted {
				completed++
			}
		}
		if completed == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("serialized deliveries did not both complete: %+v", mustList(t, st))
}

func TestWebhookProcessingTimeoutStartsAtExecution(t *testing.T) {
	var mu sync.Mutex
	remaining := []time.Duration{}
	firstStarted := make(chan struct{})
	firstReleased := make(chan struct{})

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("executor context has no deadline")
			return nil
		}
		mu.Lock()
		remaining = append(remaining, time.Until(deadline))
		call := len(remaining)
		mu.Unlock()

		if call == 1 {
			close(firstStarted)
			select {
			case <-firstReleased:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: "Analyze"},
	})
	srv.executor = exec
	srv.processingTimeout = 200 * time.Millisecond
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/test?secret=s", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery-1 executor never started")
	}

	if resp := post(t, ts.URL+"/test?secret=s", "delivery-2", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}
	time.Sleep(150 * time.Millisecond)

	close(firstReleased)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		calls := len(remaining)
		mu.Unlock()
		if calls >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(remaining) != 2 {
		t.Fatalf("executor calls = %d, want 2", len(remaining))
	}
	if remaining[1] < 150*time.Millisecond {
		t.Fatalf("queued delivery burned its processing timeout in the queue: %s remaining of 200ms", remaining[1])
	}
}

func TestWebhookConcurrentDuplicateSuppression(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const workers = 8
	start := make(chan struct{})
	statuses := make(chan int, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/github?secret=s3cret", strings.NewReader(`{"action":"opened","number":42}`))
			if err != nil {
				t.Errorf("NewRequest: %v", err)
				return
			}
			req.Header.Set("X-GitHub-Delivery", "same-delivery")
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("POST: %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			statuses <- resp.StatusCode
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("concurrent duplicate returned %d, want 200", code)
		}
	}
	waitForAttempt(t, st, "github", "same-delivery", workers)
	if exec.callCount() != 1 {
		t.Fatalf("concurrent duplicates started %d sessions, want exactly 1", exec.callCount())
	}
	deliveries := mustList(t, st)
	if len(deliveries) != 1 {
		t.Fatalf("expected exactly one receipt, got %d", len(deliveries))
	}
}

func TestWebhookConcurrentStaleRecoveryClaimsOnce(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "stale", EventType: "pull_request", PayloadHash: "abc", Attempt: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	stale, err := repo.Get(ctx, "github", "stale")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok, err := repo.Transition(ctx, stale.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim stale receipt: ok=%v err=%v", ok, err)
	}
	if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = ? WHERE id = ?`, time.Now().Add(-claimGrace-time.Second).Unix(), stale.ID); err != nil {
		t.Fatalf("backdate stale receipt: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const workers = 2
	start := make(chan struct{})
	statuses := make(chan int, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := post(t, ts.URL+"/github?secret=s3cret", "stale", "pull_request", `{}`)
			statuses <- resp.StatusCode
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("stale recovery returned %d, want 200", code)
		}
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)
	recovered := waitForAttempt(t, st, "github", "stale", 3)
	if exec.callCount() != 1 {
		t.Fatalf("stale recovery started %d sessions, want exactly 1", exec.callCount())
	}
	if recovered.Status != store.WebhookStatusCompleted {
		t.Fatalf("recovered receipt: %+v", recovered)
	}
}

func TestWebhookInvalidSecretCreatesNoReceipt(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=wrong", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	if len(mustList(t, st)) != 0 {
		t.Fatal("invalid signature must not create a receipt")
	}
}

func TestWebhookUnknownPathCreatesNoReceipt(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/unknown", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	if len(mustList(t, st)) != 0 {
		t.Fatal("unknown endpoint must not create a receipt")
	}
}

func TestWebhookSkipEventNotProcessed(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", SkipEvents: []string{"ping"}},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-ping", "ping", `{"zen":"keep it simple"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for skipped event, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusSkipped)
	if receipt.Status != store.WebhookStatusSkipped || receipt.CompletedAt == 0 {
		t.Fatalf("skipped receipt: %+v", receipt)
	}
	if exec.callCount() != 0 {
		t.Fatalf("skipped event started a session: %d executor calls", exec.callCount())
	}

	resp = post(t, ts.URL+"/github?secret=s3cret", "delivery-pr", "pull_request", `{"action":"opened","number":7}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("non-skipped event did not process: %d executor calls", exec.callCount())
	}
}

func TestWebhookSkipTransitionFailureDoesNotAck(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", SkipEvents: []string{"ping"}},
	})
	base := srv.deliveries
	srv.deliveries = &failingTransitionStore{DeliveryStore: base, to: store.WebhookStatusSkipped, err: errors.New("skip transition unavailable")}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-ping", "ping", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("skip transition failure returned %d, want accepted 200", resp.StatusCode)
	}
	waitForAttempt(t, st, "github", "delivery-ping", 1)
	time.Sleep(100 * time.Millisecond)
	if exec.callCount() != 0 {
		t.Fatalf("skip transition failure started %d sessions", exec.callCount())
	}
	receipt, err := base.Get(context.Background(), "github", "delivery-ping")
	if err != nil {
		t.Fatalf("Get receipt: %v", err)
	}
	if receipt.Status != store.WebhookStatusReceived {
		t.Fatalf("failed skip transition changed receipt: %+v", receipt)
	}
}

func TestWebhookProcessingClaimFailureDoesNotAck(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	base := srv.deliveries
	srv.deliveries = &failingTransitionStore{DeliveryStore: base, to: store.WebhookStatusProcessing, err: errors.New("processing claim unavailable")}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("processing claim failure returned %d, want accepted 200", resp.StatusCode)
	}
	waitForAttempt(t, st, "github", "delivery-1", 1)
	time.Sleep(100 * time.Millisecond)
	if exec.callCount() != 0 {
		t.Fatalf("processing claim failure started %d sessions", exec.callCount())
	}
	receipt, err := base.Get(context.Background(), "github", "delivery-1")
	if err != nil {
		t.Fatalf("Get receipt: %v", err)
	}
	if receipt.Status != store.WebhookStatusReceived {
		t.Fatalf("failed processing claim changed receipt unexpectedly: %+v", receipt)
	}
}

func TestWebhookSkipTransitionFailureIsRetryable(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", SkipEvents: []string{"ping"}},
	})
	flaky := &failOnceTransitionStore{DeliveryStore: srv.deliveries, to: store.WebhookStatusSkipped, err: errors.New("skip transition unavailable")}
	base := srv.deliveries
	srv.deliveries = flaky
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-ping", "ping", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial skip transition failure returned %d, want accepted 200", resp.StatusCode)
	}
	waitForAttempt(t, st, "github", "delivery-ping", 1)
	receipt, err := base.Get(context.Background(), "github", "delivery-ping")
	if err != nil {
		t.Fatalf("Get after failed skip transition: %v", err)
	}
	if receipt.Status != store.WebhookStatusReceived {
		t.Fatalf("failed skip transition changed receipt: %+v", receipt)
	}

	flaky.Disarm()
	resp = post(t, ts.URL+"/github?secret=s3cret", "delivery-ping", "ping", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry after skip transition failure returned %d, want 200", resp.StatusCode)
	}
	receipt = waitForReceipt(t, st, store.WebhookStatusSkipped)
	if receipt.Attempt != 2 || receipt.Status != store.WebhookStatusSkipped {
		t.Fatalf("retry did not complete skip: %+v", receipt)
	}
	if exec.callCount() != 0 {
		t.Fatalf("skipped retry started %d sessions", exec.callCount())
	}
}

func TestWebhookProcessingClaimFailureIsRetryable(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	flaky := &failOnceTransitionStore{DeliveryStore: srv.deliveries, to: store.WebhookStatusProcessing, err: errors.New("processing claim unavailable")}
	base := srv.deliveries
	srv.deliveries = flaky
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial processing claim failure returned %d, want accepted 200", resp.StatusCode)
	}
	waitForAttempt(t, st, "github", "delivery-1", 1)
	receipt, err := base.Get(context.Background(), "github", "delivery-1")
	if err != nil {
		t.Fatalf("Get after failed processing claim: %v", err)
	}
	if receipt.Status != store.WebhookStatusReceived {
		t.Fatalf("failed processing claim changed receipt: %+v", receipt)
	}

	flaky.Disarm()
	resp = post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry after processing claim failure returned %d, want 200", resp.StatusCode)
	}
	receipt = waitForReceipt(t, st, store.WebhookStatusCompleted)
	if receipt.Attempt != 2 || receipt.Status != store.WebhookStatusCompleted {
		t.Fatalf("retry did not complete processing: %+v", receipt)
	}
	if exec.callCount() != 1 {
		t.Fatalf("processing retry started %d sessions, want 1", exec.callCount())
	}
}

func TestWebhookExecutorFailureRecordsRedactedSummary(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "hunter2", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.err = errors.New("agent unreachable: dial timeout hunter2")
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=hunter2", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt.Status != store.WebhookStatusFailed || receipt.CompletedAt == 0 {
		t.Fatalf("failed receipt: %+v", receipt)
	}
	if strings.Contains(receipt.ErrorSummary, "hunter2") {
		t.Fatalf("secret leaked into diagnostics: %q", receipt.ErrorSummary)
	}
	if !strings.Contains(receipt.ErrorSummary, "[redacted]") {
		t.Fatalf("expected redacted marker in summary, got %q", receipt.ErrorSummary)
	}
}

func TestWebhookExecutorTimeoutRecordsTimedOut(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.err = context.DeadlineExceeded
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if !strings.Contains(receipt.ErrorSummary, "timed out") {
		t.Fatalf("timed-out delivery not distinguishable: %q", receipt.ErrorSummary)
	}
}

func TestWebhookSyntheticDeliveryID(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/github?secret=s3cret", "", "", `{"action":"opened"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusCompleted)
	if !strings.HasPrefix(receipt.DeliveryID, "gen-") {
		t.Fatalf("delivery without provider id should get a synthetic id, got %q", receipt.DeliveryID)
	}
	if len(mustList(t, st)) != 1 {
		t.Fatalf("expected exactly one receipt, got %d", len(mustList(t, st)))
	}
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
}

func TestWebhookStartRecoversStaleInFlight(t *testing.T) {
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	repo := st.WebhookDeliveryRepo()
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "stuck", EventType: "pull_request", PayloadHash: "abc", Attempt: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	stuck, _ := repo.Get(ctx, "github", "stuck")
	if ok, err := repo.Transition(ctx, stuck.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted}, store.WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim stuck: ok=%v err=%v", ok, err)
	}
	if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = ? WHERE id = ?`, time.Now().Add(-claimGrace).Unix(), stuck.ID); err != nil {
		t.Fatalf("backdate stuck receipt: %v", err)
	}
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "done", EventType: "pull_request", PayloadHash: "def", Attempt: 1}); err != nil {
		t.Fatalf("Create done: %v", err)
	}
	done, _ := repo.Get(ctx, "github", "done")
	if ok, err := repo.Transition(ctx, done.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted}, store.WebhookStatusCompleted, ""); err != nil || !ok {
		t.Fatalf("complete done: ok=%v err=%v", ok, err)
	}

	srv := New(config.WebhookConfig{Bind: "127.0.0.1:0"}, nil, repo)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	recovered, _ := repo.Get(ctx, "github", "stuck")
	if recovered.Status != store.WebhookStatusFailed || recovered.ErrorSummary != "interrupted by restart" {
		t.Fatalf("stale in-flight receipt not recovered: %+v", recovered)
	}
	terminal, _ := repo.Get(ctx, "github", "done")
	if terminal.Status != store.WebhookStatusCompleted {
		t.Fatalf("terminal receipt disturbed by recovery: %+v", terminal)
	}
}

func TestWebhookStartPrunesOldDeliveries(t *testing.T) {
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()

	old := time.Now().Add(-40 * 24 * time.Hour).Unix()
	for i := 0; i < retentionKeep+1; i++ {
		d := store.WebhookDelivery{
			Endpoint:    "github",
			DeliveryID:  fmt.Sprintf("old-%d", i),
			EventType:   "pull_request",
			PayloadHash: "abc",
			Attempt:     1,
			CreatedAt:   old,
		}
		if _, err := repo.Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	srv := New(config.WebhookConfig{Bind: "127.0.0.1:0"}, nil, repo)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	deliveries, err := st.WebhookDeliveryRepo().List(context.Background(), retentionKeep+10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("retention after prune = %d rows, want 0 rows older than retention age", len(deliveries))
	}
}

func TestWebhookProjectAwareSerializationSameKey(t *testing.T) {
	var mu sync.Mutex
	starts := 0
	firstStarted := make(chan struct{})
	firstReleased := make(chan struct{})
	secondStarted := make(chan struct{})

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		starts++
		call := starts
		mu.Unlock()

		switch call {
		case 1:
			close(firstStarted)
			select {
			case <-firstReleased:
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			close(secondStarted)
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/feat-branch-1"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payloadSameKey := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/branch-1"}}}`

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payloadSameKey); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery-1 never started")
	}

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-2", "pull_request", payloadSameKey); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-secondStarted:
		t.Fatal("delivery-2 started before delivery-1 released lock")
	case <-time.After(150 * time.Millisecond):
	}

	close(firstReleased)
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery-2 never started after delivery-1 completed")
	}

	waitForCompletedCount(t, st, 2)
}

func TestWebhookProjectAwareParallelismDifferentBranches(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	bothRunning := make(chan struct{})
	release := make(chan struct{})

	var deliveryIDs sync.Map

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		deliveryIDs.Store(workCtx.Key.Branch, workCtx.DeliveryID)
		switch workCtx.Key.Branch {
		case "feat/branch-1":
			close(firstStarted)
		case "feat/branch-2":
			close(secondStarted)
		}

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/branch"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload1 := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/branch-1"}}}`
	payload2 := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/branch-2"}}}`

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payload1); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-2", "pull_request", payload2); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}

	go func() {
		<-firstStarted
		<-secondStarted
		close(bothRunning)
	}()

	select {
	case <-bothRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("both branches did not execute concurrently in parallel")
	}

	close(release)
	waitForCompletedCount(t, st, 2)

	id1, _ := deliveryIDs.Load("feat/branch-1")
	id2, _ := deliveryIDs.Load("feat/branch-2")
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("expected distinct delivery identities per branch: %v vs %v", id1, id2)
	}
}

func TestWebhookProjectAwareParallelismDifferentRepos(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	bothRunning := make(chan struct{})
	release := make(chan struct{})

	var deliveryIDs sync.Map

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		deliveryIDs.Store(workCtx.Key.Repository, workCtx.DeliveryID)
		switch workCtx.Key.Repository {
		case "anggasct/occa":
			close(firstStarted)
		case "anggasct/dispatch":
			close(secondStarted)
		}

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/repo/.worktree/main"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload1 := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"main"}}}`
	payload2 := `{"repository":{"full_name":"anggasct/dispatch"},"pull_request":{"base":{"repo":{"full_name":"anggasct/dispatch"}},"head":{"repo":{"full_name":"anggasct/dispatch"},"ref":"main"}}}`

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payload1); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-2", "pull_request", payload2); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}

	go func() {
		<-firstStarted
		<-secondStarted
		close(bothRunning)
	}()

	select {
	case <-bothRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("both repositories did not execute concurrently in parallel")
	}

	close(release)
	waitForCompletedCount(t, st, 2)

	id1, _ := deliveryIDs.Load("anggasct/occa")
	id2, _ := deliveryIDs.Load("anggasct/dispatch")
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("expected distinct delivery identities per repo: %v vs %v", id1, id2)
	}
}

type fakeWorkspaceResolver struct {
	path     string
	err      error
	failures int
	mu       sync.Mutex
	calls    int
}

func (f *fakeWorkspaceResolver) ResolveWorkspace(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
	f.mu.Lock()
	f.calls++
	failures := f.failures
	f.failures--
	if failures > 0 {
		f.mu.Unlock()
		return nil, ErrWorkspaceLeased
	}
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &WorkspaceLease{Path: f.path, Mode: req.Mode}, nil
}

func (f *fakeWorkspaceResolver) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestWebhookWorktreeResolutionIntegration(t *testing.T) {
	var observedWorktree string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		observedWorktree = workCtx.Worktree
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/my-fix"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"my-fix"}}}`
	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusCompleted)
	if observedWorktree != "/projects/occa/.worktree/my-fix" {
		t.Fatalf("observed worktree = %q, want /projects/occa/.worktree/my-fix", observedWorktree)
	}
}

func TestWebhookWorktreeConflictFailsDelivery(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{err: ErrWorktreeConflict})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"my-fix"}}}`
	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt.Status != store.WebhookStatusFailed {
		t.Fatalf("expected failed status, got %s", receipt.Status)
	}
	if !strings.Contains(receipt.ErrorSummary, "worktree conflict") {
		t.Fatalf("expected error summary to contain 'worktree conflict', got %q", receipt.ErrorSummary)
	}
}

func TestWebhookGitEndpointWithoutResolverFailsClosed(t *testing.T) {
	exec := &fakeExecutor{}
	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze",
				Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec.exec, st.WebhookDeliveryRepo())
	srv.workspaceResolver = nil
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/no-resolver"}}}`
	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}

	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if !strings.Contains(receipt.ErrorSummary, "workspace resolver unavailable") {
		t.Fatalf("expected 'workspace resolver unavailable', got %q", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor should not have been called without resolver, called %d times", exec.callCount())
	}
}

func TestWebhookExecutorPanicRecoversAndFailsDelivery(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		calls++
		c := calls
		mu.Unlock()
		if c == 1 {
			panic("simulated fatal executor crash")
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/fix"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"fix"}}}`

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-panic", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-panic expected 200, got %d", resp.StatusCode)
	}

	receipt1 := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt1.Status != store.WebhookStatusFailed {
		t.Fatalf("expected failed status on panic, got %s", receipt1.Status)
	}
	if !strings.Contains(receipt1.ErrorSummary, "panic: simulated fatal executor crash") {
		t.Fatalf("expected panic in summary, got %q", receipt1.ErrorSummary)
	}

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-recovery", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-recovery expected 200, got %d", resp.StatusCode)
	}

	waitForCompletedCount(t, st, 2)
}

func TestWebhookGitHubHMACSuccess(t *testing.T) {
	executed := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		close(executed)
		return nil
	}

	secret := "github-hmac-secret-123"
	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{
				Name:      "github-review",
				Path:      "/webhooks/github-review",
				Auth:      "github_hmac_sha256",
				Secret:    secret,
				Platform:  "telegram",
				ChannelID: "chat1",
				Prompt:    "Analyze review",
				Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated},
			},
		},
	}

	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/hmac"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := []byte(`{"action":"submitted","repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"main"}}}`)
	sig := computeTestSignature(payload, secret)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/webhooks/github-review", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Delivery", "delivery-hmac-1")
	req.Header.Set("X-GitHub-Event", "pull_request_review")
	req.Header.Set("X-Hub-Signature-256", sig)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	select {
	case <-executed:
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not called for valid HMAC delivery")
	}

	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-review", "delivery-hmac-1")
	if err != nil {
		t.Fatalf("get receipt: %v", err)
	}
	if receipt == nil {
		t.Fatal("expected delivery receipt in store, got nil")
	}
}

func TestWebhookGitHubHMACUnauthorizedNegativeCases(t *testing.T) {
	var execCalled bool
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		execCalled = true
		return nil
	}

	secret := "github-hmac-secret-123"
	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{
				Name:      "github-review",
				Path:      "/webhooks/github-review",
				Auth:      "github_hmac_sha256",
				Secret:    secret,
				Platform:  "telegram",
				ChannelID: "chat1",
				Prompt:    "Analyze review",
				Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated},
			},
		},
	}

	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/hmac"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := []byte(`{"action":"submitted","repository":{"full_name":"anggasct/occa"}}`)
	validSig := computeTestSignature(payload, secret)

	tests := []struct {
		name      string
		urlSuffix string
		headers   map[string]string
		body      []byte
	}{
		{
			name:      "missing signature header",
			urlSuffix: "/webhooks/github-review",
			headers:   map[string]string{"X-GitHub-Delivery": "d-missing-sig"},
			body:      payload,
		},
		{
			name:      "wrong secret signature",
			urlSuffix: "/webhooks/github-review",
			headers: map[string]string{
				"X-GitHub-Delivery":   "d-wrong-sig",
				"X-Hub-Signature-256": computeTestSignature(payload, "incorrect-secret"),
			},
			body: payload,
		},
		{
			name:      "malformed prefix",
			urlSuffix: "/webhooks/github-review",
			headers: map[string]string{
				"X-GitHub-Delivery":   "d-malformed-prefix",
				"X-Hub-Signature-256": "sha1=47eef3f9704f2c07c5fed441603d472cb05b741d",
			},
			body: payload,
		},
		{
			name:      "query param cannot authenticate HMAC endpoint",
			urlSuffix: "/webhooks/github-review?secret=" + secret,
			headers: map[string]string{
				"X-GitHub-Delivery": "d-query-only",
			},
			body: payload,
		},
		{
			name:      "legacy header cannot authenticate HMAC endpoint",
			urlSuffix: "/webhooks/github-review",
			headers: map[string]string{
				"X-GitHub-Delivery": "d-legacy-header-only",
				"X-Webhook-Secret":  secret,
			},
			body: payload,
		},
		{
			name:      "tampered body with valid signature for different body",
			urlSuffix: "/webhooks/github-review",
			headers: map[string]string{
				"X-GitHub-Delivery":   "d-tampered",
				"X-Hub-Signature-256": validSig,
			},
			body: []byte(`{"action":"submitted","repository":{"full_name":"anggasct/occa"}, "extra": 1}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCalled = false
			req, err := http.NewRequest(http.MethodPost, ts.URL+tt.urlSuffix, bytes.NewReader(tt.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("expected status 401 Unauthorized, got %d", resp.StatusCode)
			}

			if execCalled {
				t.Fatal("executor was erroneously called on unauthorized delivery")
			}

			if delID := tt.headers["X-GitHub-Delivery"]; delID != "" {
				r, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github-review", delID)
				if r != nil {
					t.Fatalf("receipt was erroneously created on 401 for %s", delID)
				}
			}
		})
	}
}

func TestWebhookWhitespacePaddedHMACModeRequiresSignatureAndRejectsLegacy(t *testing.T) {
	executed := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		close(executed)
		return nil
	}

	secret := "github-padded-secret"
	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{
				Name:      "github-review",
				Path:      "/webhooks/github-review",
				Auth:      "  github_hmac_sha256  ",
				Secret:    secret,
				Platform:  "telegram",
				ChannelID: "chat1",
				Prompt:    "Analyze review",
				Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated},
			},
		},
	}

	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/hmac"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := []byte(`{"action":"submitted","repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"main"}}}`)

	reqLegacy, err := http.NewRequest(http.MethodPost, ts.URL+"/webhooks/github-review?secret="+secret, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new legacy request: %v", err)
	}
	reqLegacy.Header.Set("Content-Type", "application/json")
	respLegacy, err := http.DefaultClient.Do(reqLegacy)
	if err != nil {
		t.Fatalf("do legacy request: %v", err)
	}
	defer func() { _ = respLegacy.Body.Close() }()
	if respLegacy.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for legacy query on whitespace-padded HMAC endpoint, got %d", respLegacy.StatusCode)
	}

	sig := computeTestSignature(payload, secret)
	reqHMAC, err := http.NewRequest(http.MethodPost, ts.URL+"/webhooks/github-review", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new HMAC request: %v", err)
	}
	reqHMAC.Header.Set("Content-Type", "application/json")
	reqHMAC.Header.Set("X-GitHub-Delivery", "delivery-padded-1")
	reqHMAC.Header.Set("X-GitHub-Event", "pull_request_review")
	reqHMAC.Header.Set("X-Hub-Signature-256", sig)

	respHMAC, err := http.DefaultClient.Do(reqHMAC)
	if err != nil {
		t.Fatalf("do HMAC request: %v", err)
	}
	defer func() { _ = respHMAC.Body.Close() }()
	if respHMAC.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for valid HMAC signature, got %d", respHMAC.StatusCode)
	}

	select {
	case <-executed:
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not called for valid HMAC on normalized endpoint")
	}
}

func TestWebhookModelResolution_ExplicitChannelModel(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-model-1",
		Prompt:    "analyze",
	}})

	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat-model-1", "alibaba-token-plan/deepseek-v4-flash-0731@max"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":121,"title":"Agent Workshop"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-model-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusCompleted)

	calls := exec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 executor call, got %d", len(calls))
	}
	if calls[0].workCtx.Model == nil {
		t.Fatal("expected non-nil workCtx.Model")
	}
	wantModel := relay.ModelRef{ProviderID: "alibaba-token-plan", ID: "deepseek-v4-flash-0731", Variant: "max"}
	if *calls[0].workCtx.Model != wantModel {
		t.Fatalf("got model %+v, want %+v", *calls[0].workCtx.Model, wantModel)
	}
	if calls[0].workCtx.ModelSource != "channel" {
		t.Fatalf("got ModelSource %q, want channel", calls[0].workCtx.ModelSource)
	}

	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Model: alibaba-token-plan/deepseek-v4-flash-0731@max") {
			t.Fatalf("audit summary missing Model line: %q", summary)
		}
		if !strings.Contains(summary, "Model source: channel") {
			t.Fatalf("audit summary missing Model source line: %q", summary)
		}
		if !strings.Contains(summary, "Status: COMPLETED") {
			t.Fatalf("audit summary missing Status COMPLETED: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected audit summary notification")
	}
}

func TestWebhookModelResolution_SlashInModelID(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-model-slash",
		Prompt:    "analyze",
	}})

	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat-model-slash", "openrouter/anthropic/claude-3-5-sonnet@high"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":121,"title":"Agent Workshop"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-slash-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusCompleted)

	calls := exec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 executor call, got %d", len(calls))
	}
	if calls[0].workCtx.Model == nil {
		t.Fatal("expected non-nil workCtx.Model")
	}
	wantModel := relay.ModelRef{ProviderID: "openrouter", ID: "anthropic/claude-3-5-sonnet", Variant: "high"}
	if *calls[0].workCtx.Model != wantModel {
		t.Fatalf("got model %+v, want %+v", *calls[0].workCtx.Model, wantModel)
	}
	if calls[0].workCtx.ModelSource != "channel" {
		t.Fatalf("got ModelSource %q, want channel", calls[0].workCtx.ModelSource)
	}
}

func TestWebhookModelResolution_FallbackWhenUnset(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-model-unset",
		Prompt:    "analyze",
	}})

	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":122,"title":"Retire Legacy Aliases"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-fallback-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusCompleted)

	calls := exec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 executor call, got %d", len(calls))
	}
	if calls[0].workCtx.Model != nil {
		t.Fatalf("expected nil workCtx.Model for fallback, got %+v", calls[0].workCtx.Model)
	}
	if calls[0].workCtx.ModelSource != "fallback" {
		t.Fatalf("got ModelSource %q, want fallback", calls[0].workCtx.ModelSource)
	}

	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Model: agent/session default") {
			t.Fatalf("audit summary missing fallback Model line: %q", summary)
		}
		if !strings.Contains(summary, "Model source: fallback") {
			t.Fatalf("audit summary missing fallback Model source line: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected audit summary notification")
	}
}

func TestWebhookModelResolution_DynamicChannelModelChange(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-dynamic-model",
		Prompt:    "analyze",
	}})

	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat-dynamic-model", "openai/gpt-4o"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":100,"title":"Test PR"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-dyn-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)

	calls := exec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 executor call, got %d", len(calls))
	}
	wantModel1 := relay.ModelRef{ProviderID: "openai", ID: "gpt-4o"}
	if *calls[0].workCtx.Model != wantModel1 {
		t.Fatalf("delivery 1 model = %+v, want %+v", *calls[0].workCtx.Model, wantModel1)
	}

	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat-dynamic-model", "anthropic/claude-3-5-sonnet@max"); err != nil {
		t.Fatalf("update channel model: %v", err)
	}

	if response := post(t, ts.URL+"/github?secret=secret", "delivery-dyn-2", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)

	calls = exec.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 executor calls, got %d", len(calls))
	}
	wantModel2 := relay.ModelRef{ProviderID: "anthropic", ID: "claude-3-5-sonnet", Variant: "max"}
	if *calls[1].workCtx.Model != wantModel2 {
		t.Fatalf("delivery 2 model = %+v, want %+v", *calls[1].workCtx.Model, wantModel2)
	}
}

type errorChannelStore struct {
	err error
}

func (e *errorChannelStore) Get(ctx context.Context, platform, channelID string) (*store.Channel, error) {
	return nil, e.err
}

func TestWebhookModelResolution_ChannelRepoErrorFailsClosed(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-err",
		Prompt:    "analyze",
	}})

	srv.SetChannelStore(&errorChannelStore{err: errors.New("database locked")})

	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":101,"title":"Error PR"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-err-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusFailed)

	if exec.callCount() != 0 {
		t.Fatalf("executor was called %d times on channel repo error, expected 0", exec.callCount())
	}

	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Status: FAILED") {
			t.Fatalf("audit summary missing Status FAILED: %q", summary)
		}
		if !strings.Contains(summary, "channel configuration error") {
			t.Fatalf("audit summary missing channel configuration error: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected audit summary notification")
	}
}

func TestWebhookModelResolution_MalformedChannelModelFailsClosed(t *testing.T) {
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat-malformed",
		Prompt:    "analyze",
	}})

	malformedCredentialValue := "provider_with_api_key_sk_test_12345_no_slash"
	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat-malformed", malformedCredentialValue); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":102,"title":"Malformed Model PR"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "delivery-malformed-1", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}

	waitForReceipt(t, st, store.WebhookStatusFailed)

	if exec.callCount() != 0 {
		t.Fatalf("executor was called %d times on malformed channel model, expected 0", exec.callCount())
	}

	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Status: FAILED") {
			t.Fatalf("audit summary missing Status FAILED: %q", summary)
		}
		if !strings.Contains(summary, "invalid channel model") {
			t.Fatalf("audit summary missing 'invalid channel model': %q", summary)
		}
		if strings.Contains(summary, malformedCredentialValue) {
			t.Fatalf("audit summary leaked raw malformed model string: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected audit summary notification")
	}

	if strings.Contains(logBuf.String(), malformedCredentialValue) {
		t.Fatalf("captured logs leaked raw malformed model credential string: %q", logBuf.String())
	}
}

func waitForRowCount(t *testing.T, st *store.SQLiteStore, want int) []store.WebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		deliveries := mustList(t, st)
		if len(deliveries) == want {
			return deliveries
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("receipt row count never reached %d: %+v", want, mustList(t, st))
	return nil
}

func TestWebhookFIFOOrderSameKey(t *testing.T) {
	var mu sync.Mutex
	var completions []string
	var execCount int

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		execCount++
		completions = append(completions, fmt.Sprintf("call-%d", execCount))
		mu.Unlock()
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	for i := 1; i <= 5; i++ {
		if resp := post(t, ts.URL+"/github?secret=s3cret", fmt.Sprintf("delivery-%d", i), "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery-%d expected 200, got %d", i, resp.StatusCode)
		}
	}
	waitForCompletedCount(t, st, 5)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"call-1", "call-2", "call-3", "call-4", "call-5"}
	if !reflect.DeepEqual(completions, want) {
		t.Fatalf("FIFO order violated: got %v, want %v", completions, want)
	}
}

func TestWebhookQueuedDeliveryHasNoRow(t *testing.T) {
	var startedOnce sync.Once
	started := make(chan struct{})
	released := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		startedOnce.Do(func() { close(started) })
		select {
		case <-released:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	var rows []store.WebhookDelivery
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows = mustList(t, st)
		if len(rows) == 1 && rows[0].Status == store.WebhookStatusProcessing && rows[0].DeliveryID == "delivery-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executing delivery row unexpected: %+v", rows)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-2", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 expected 200, got %d", resp.StatusCode)
	}
	time.Sleep(100 * time.Millisecond)
	rows = mustList(t, st)
	if len(rows) != 1 {
		t.Fatalf("queued delivery created a row while waiting: %+v", rows)
	}

	close(released)
	waitForCompletedCount(t, st, 2)
}

func TestWebhookQueueFullReturns429WithoutRow(t *testing.T) {
	release := make(chan struct{})
	blocked := make(chan struct{}, maxConcurrentWebhookEvents+maxQueuedPerKey+2)
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		blocked <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	url := ts.URL + "/github?secret=s3cret"
	if resp := post(t, url, "delivery-exec", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("executing delivery expected 200, got %d", resp.StatusCode)
	}
	waitForRowCount(t, st, 1)

	for i := 0; i < maxQueuedPerKey; i++ {
		resp := post(t, url, fmt.Sprintf("delivery-q%d", i), "pull_request", `{}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("queued delivery %d expected 200, got %d", i, resp.StatusCode)
		}
	}

	resp := post(t, url, "delivery-overflow", "pull_request", `{}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("overflow delivery expected 429, got %d", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Fatal("429 response missing Retry-After header")
	}
	rows := mustList(t, st)
	if len(rows) != 1 {
		t.Fatalf("overflow delivery wrote a receipt row: %+v", rows)
	}

	close(release)
	waitForCompletedCount(t, st, maxQueuedPerKey+1)
}

func TestWebhookSlotCapBoundedAcrossKeys(t *testing.T) {
	var mu sync.Mutex
	running, peak := 0, 0
	release := make(chan struct{})

	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		running++
		if running > peak {
			peak = running
		}
		mu.Unlock()

		select {
		case <-release:
		case <-ctx.Done():
			mu.Lock()
			running--
			mu.Unlock()
			return ctx.Err()
		}

		mu.Lock()
		running--
		mu.Unlock()
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/projects/occa/.worktree/branch"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const total = maxConcurrentWebhookEvents + 4
	for i := 0; i < total; i++ {
		payload := fmt.Sprintf(`{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/branch-%d"}}}`, i)
		if resp := post(t, ts.URL+"/github?secret=s3cret", fmt.Sprintf("delivery-%d", i), "pull_request", payload); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery-%d expected accepted 200, got %d", i, resp.StatusCode)
		}
	}
	peakSnapshot := func() int {
		mu.Lock()
		defer mu.Unlock()
		return peak
	}

	deadline := time.Now().Add(10 * time.Second)
	for peakSnapshot() < maxConcurrentWebhookEvents {
		if time.Now().After(deadline) {
			t.Fatalf("concurrent executions never reached the cap %d under full queue admission, peak = %d", maxConcurrentWebhookEvents, peakSnapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}

	if observedPeak := peakSnapshot(); observedPeak > maxConcurrentWebhookEvents {
		t.Fatalf("concurrent executions peaked at %d, cap is %d", observedPeak, maxConcurrentWebhookEvents)
	}
	if observedPeak := peakSnapshot(); observedPeak < 2 {
		t.Fatalf("distinct keys did not execute in parallel, peak = %d", observedPeak)
	}

	close(release)
	waitForCompletedCount(t, st, total)
}

func TestWebhookShutdownDrainsQueue(t *testing.T) {
	var startedOnce sync.Once
	started := make(chan struct{})
	released := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		startedOnce.Do(func() { close(started) })
		select {
		case <-released:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-inflight", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("in-flight delivery expected 200, got %d", resp.StatusCode)
	}
	<-started

	for _, id := range []string{"delivery-q1", "delivery-q2"} {
		if resp := post(t, ts.URL+"/github?secret=s3cret", id, "pull_request", `{}`); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s expected 200, got %d", id, resp.StatusCode)
		}
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- srv.Stop(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	for srv.shutdownCtx.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("shutdown context was never cancelled")
		}
		time.Sleep(time.Millisecond)
	}

	close(released)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	ctx := context.Background()
	for _, id := range []string{"delivery-q1", "delivery-q2"} {
		receipt, err := st.WebhookDeliveryRepo().Get(ctx, "github", id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if receipt == nil || receipt.Status != store.WebhookStatusFailed || receipt.ErrorSummary != "shutting down" {
			t.Fatalf("%s not drained as failed on shutdown: %+v", id, receipt)
		}
	}
	inflight, err := st.WebhookDeliveryRepo().Get(ctx, "github", "delivery-inflight")
	if err != nil {
		t.Fatalf("Get in-flight: %v", err)
	}
	if inflight.Status != store.WebhookStatusCompleted {
		t.Fatalf("in-flight delivery did not finish normally: %+v", inflight)
	}
	if srv.dispatcherCount() != 0 {
		t.Fatalf("dispatchers left registered after Stop: %d", srv.dispatcherCount())
	}
}

func TestWebhookDispatcherIdleEviction(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "test", Path: "/test", Secret: "s", Platform: "telegram", ChannelID: "c1", Prompt: "Analyze"},
	})
	srv.dispatcherIdleTTL = 80 * time.Millisecond
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/test?secret=s", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-1 expected 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 1)

	deadline := time.Now().Add(2 * time.Second)
	for srv.dispatcherCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.dispatcherCount() != 0 {
		t.Fatal("dispatcher was not evicted after idle TTL")
	}

	if resp := post(t, ts.URL+"/test?secret=s", "delivery-2", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery-2 after eviction expected 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 2)
}

func TestWebhookEnqueueRacingIdleEvictionNeverLosesDelivery(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.dispatcherIdleTTL = time.Millisecond
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const total = 25
	url := ts.URL + "/github?secret=s3cret"
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("delivery-race-%d", i)
		resp := post(t, url, id, "pull_request", `{}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s expected accepted 200, got %d", id, resp.StatusCode)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			receipt, err := repo.Get(ctx, "github", id)
			if err != nil {
				t.Fatalf("Get %s: %v", id, err)
			}
			if receipt != nil && isTerminal(receipt.Status) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("accepted delivery %s was neither executed nor recorded: %+v", id, receipt)
			}
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(2 * srv.dispatcherIdleTTL)
	}
}

func TestWebhookEnqueueRacingShutdownNeverLosesDelivery(t *testing.T) {
	var startedOnce sync.Once
	started := make(chan struct{})
	released := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		startedOnce.Do(func() { close(started) })
		select {
		case <-released:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	url := ts.URL + "/github?secret=s3cret"
	if resp := post(t, url, "delivery-inflight", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("in-flight delivery expected 200, got %d", resp.StatusCode)
	}
	<-started

	stopDone := make(chan error, 1)
	go func() { stopDone <- srv.Stop(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	for srv.shutdownCtx.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("shutdown context was never cancelled")
		}
		time.Sleep(time.Millisecond)
	}

	accepted := []string{}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("delivery-late-%d", i)
		resp := post(t, url, id, "pull_request", `{}`)
		switch resp.StatusCode {
		case http.StatusOK:
			accepted = append(accepted, id)
		case http.StatusTooManyRequests:
		default:
			t.Fatalf("%s unexpected status %d", id, resp.StatusCode)
		}
	}

	close(released)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	expected := append([]string{"delivery-inflight"}, accepted...)
	for _, id := range expected {
		receipt, err := repo.Get(ctx, "github", id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if receipt == nil || !isTerminal(receipt.Status) {
			t.Fatalf("accepted delivery %s was neither executed nor recorded on shutdown: %+v", id, receipt)
		}
	}
	inflight, _ := repo.Get(ctx, "github", "delivery-inflight")
	if inflight.Status != store.WebhookStatusCompleted {
		t.Fatalf("in-flight delivery did not finish normally: %+v", inflight)
	}
	if srv.dispatcherCount() != 0 {
		t.Fatalf("dispatchers left registered after Stop: %d", srv.dispatcherCount())
	}
}

func TestWebhookIngressPrefixAcceptedOnce(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github/review", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github/review?secret=s3cret", "delivery-direct", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("direct suffix path expected 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 1)

	if resp := post(t, ts.URL+"/occa/github/review?secret=s3cret", "delivery-prefixed", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("ingress-prefixed path expected 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 2)

	if resp := post(t, ts.URL+"/occa/occa/github/review?secret=s3cret", "delivery-double", "pull_request", `{}`); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("duplicated ingress prefix must not match twice, got %d", resp.StatusCode)
	}
}

func TestWebhookNoneWorkspaceNeverInvokesResolver(t *testing.T) {
	var worktrees []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		worktrees = append(worktrees, workCtx.Worktree)
		return nil
	}
	cfg := config.WebhookConfig{
		Bind: "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{
			{Name: "sentry", Path: "/sentry/aura", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze",
				Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeNone}},
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	resolver := &fakeWorkspaceResolver{path: "/should/not/be/used"}
	srv.SetWorkspaceResolver(resolver)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	payload := `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"main","sha":"0123456789abcdef0123456789abcdef01234567"}}}`
	if resp := post(t, ts.URL+"/sentry/aura?secret=s3cret", "delivery-1", "pull_request", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("repo-shaped payload on none endpoint expected 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 1)

	if resolver.callCount() != 0 {
		t.Fatalf("none endpoint must never invoke the workspace resolver, got %d calls", resolver.callCount())
	}
	if len(worktrees) != 1 || worktrees[0] != "" {
		t.Fatalf("none endpoint must execute with an empty worktree, got %v", worktrees)
	}
}

func TestWebhookRevisionRequiredFailsClosedWithoutExecutor(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze",
			Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated}},
	})
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{err: ErrRevisionRequired})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected accepted 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if !strings.Contains(receipt.ErrorSummary, "workspace revision required") {
		t.Fatalf("expected 'workspace revision required' in summary, got %q", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("revision-less isolated delivery must not execute, got %d calls", exec.callCount())
	}
}

func TestWebhookMutableConflictRetriesThenSucceeds(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze",
			Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable}},
	})
	resolver := &fakeWorkspaceResolver{path: "/projects/occa/.worktree/fix", failures: 2}
	srv.SetWorkspaceResolver(resolver)
	srv.workspaceRetryBackoff = []time.Duration{0, 0, 0}
	srv.workspaceRetrySleep = func(ctx context.Context, d time.Duration) bool { return true }
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected accepted 200, got %d", resp.StatusCode)
	}
	waitForCompletedCount(t, st, 1)
	if resolver.callCount() != 3 {
		t.Fatalf("expected 3 resolution attempts (2 busy + 1 success), got %d", resolver.callCount())
	}
	if exec.callCount() != 1 {
		t.Fatalf("expected exactly one execution, got %d", exec.callCount())
	}
	if calls := exec.getCalls(); len(calls) == 1 && calls[0].workCtx.Worktree != "/projects/occa/.worktree/fix" {
		t.Fatalf("expected execution inside leased worktree, got %q", calls[0].workCtx.Worktree)
	}
}

func TestWebhookMutableConflictExhaustsRetriesAndFails(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze",
			Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable}},
	})
	resolver := &fakeWorkspaceResolver{failures: 1000}
	srv.SetWorkspaceResolver(resolver)
	srv.workspaceRetryBackoff = []time.Duration{0, 0, 0}
	srv.workspaceRetrySleep = func(ctx context.Context, d time.Duration) bool { return true }
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected accepted 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if !strings.Contains(receipt.ErrorSummary, "workspace leased") {
		t.Fatalf("expected 'workspace leased' in terminal summary, got %q", receipt.ErrorSummary)
	}
	if resolver.callCount() != 4 {
		t.Fatalf("expected exactly 4 attempts (initial + 3 retries), got %d", resolver.callCount())
	}
	if exec.callCount() != 0 {
		t.Fatalf("busy workspace must never execute, got %d calls", exec.callCount())
	}
}

type capturedLogRecord struct {
	msg   string
	attrs map[string]any
}

type logCapture struct {
	mu      sync.Mutex
	records []capturedLogRecord
}

func (h *logCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	rec := capturedLogRecord{msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, rec)
	return nil
}

func (h *logCapture) WithAttrs(attrs []slog.Attr) slog.Handler { return h }

func (h *logCapture) WithGroup(name string) slog.Handler { return h }

func (h *logCapture) find(t *testing.T, msg string) capturedLogRecord {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rec := range h.records {
		if rec.msg == msg {
			return rec
		}
	}
	t.Fatalf("no captured log record %q; captured=%d records", msg, len(h.records))
	return capturedLogRecord{}
}

func captureServerLogs(t *testing.T) *logCapture {
	t.Helper()
	previous := slog.Default()
	handler := &logCapture{}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

func TestWebhookCompletedRecordCarriesSessionAndAttempt(t *testing.T) {
	handler := captureServerLogs(t)
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p"},
	})

	executor := srv.executor
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		workCtx.SessionID = "sess-done"
		return executor(ctx, platform, channelID, prompt, workCtx)
	}

	_ = srv.executeDelivery(config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Prompt: "p"}, []byte(`{"repository":{"full_name":"testowner/myrepo"}}`), 0, "delivery-1", "pull_request", 2, nil, nil)

	rec := handler.find(t, "webhook: delivery completed")
	if rec.attrs["attempt"] != int64(2) {
		t.Fatalf("completed record attempt = %v, want 2", rec.attrs["attempt"])
	}
	if rec.attrs["session_id"] != "sess-done" {
		t.Fatalf("completed record session_id = %v", rec.attrs["session_id"])
	}
	if rec.attrs["session_aborted"] != false || rec.attrs["session_abort_ok"] != false {
		t.Fatalf("completed record must carry explicit no-abort outcome, attrs=%v", rec.attrs)
	}
}

func TestWebhookFailedRecordCarriesSessionAndAttempt(t *testing.T) {
	handler := captureServerLogs(t)
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p"},
	})

	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		workCtx.SessionID = "sess-fail"
		workCtx.SessionAborted = true
		workCtx.SessionAbortOK = true
		return errors.New("agent exploded")
	}

	_ = srv.executeDelivery(config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Prompt: "p"}, []byte(`{"repository":{"full_name":"testowner/myrepo"}}`), 0, "delivery-1", "pull_request", 1, nil, nil)

	rec := handler.find(t, "webhook: delivery failed")
	if rec.attrs["attempt"] != int64(1) {
		t.Fatalf("failed record attempt = %v, want 1", rec.attrs["attempt"])
	}
	if rec.attrs["session_id"] != "sess-fail" {
		t.Fatalf("failed record session_id = %v", rec.attrs["session_id"])
	}
	if rec.attrs["session_aborted"] != true || rec.attrs["session_abort_ok"] != true {
		t.Fatalf("failed record must carry abort outcome, attrs=%v", rec.attrs)
	}
}

func TestWebhookWorkspaceFailureRecordCarriesAttempt(t *testing.T) {
	handler := captureServerLogs(t)
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p"},
	})

	body := []byte(`{"repository":{"full_name":"testowner/myrepo"}}`)
	srv.failWorkspace(dispatchItem{ep: config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1"}, body: body, deliveryID: "delivery-9", eventType: "pull_request"}, &store.WebhookDelivery{ID: 7, Attempt: 3}, errors.New("worktree dirty"))

	rec := handler.find(t, "webhook: delivery failed")
	if rec.attrs["attempt"] != int64(3) {
		t.Fatalf("workspace failure record attempt = %v, want 3", rec.attrs["attempt"])
	}
	if rec.attrs["delivery_id"] != "delivery-9" {
		t.Fatalf("workspace failure record delivery_id = %v", rec.attrs["delivery_id"])
	}
	if _, ok := rec.attrs["session_id"]; ok {
		t.Fatalf("workspace failure has no owned session, attrs=%v", rec.attrs)
	}
}

func TestWebhookPanicRecoveredRecordCarriesSession(t *testing.T) {
	handler := captureServerLogs(t)
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p"},
	})

	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		workCtx.SessionID = "sess-panic"
		workCtx.SessionAborted = true
		panic("executor exploded")
	}

	_ = srv.executeDelivery(config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Prompt: "p"}, []byte(`{"repository":{"full_name":"testowner/myrepo"}}`), 0, "delivery-1", "pull_request", 1, nil, nil)

	rec := handler.find(t, "webhook: panic recovered in delivery processing")
	if rec.attrs["session_id"] != "sess-panic" {
		t.Fatalf("panic record session_id = %v", rec.attrs["session_id"])
	}
	if rec.attrs["session_aborted"] != true {
		t.Fatalf("panic record must carry the abort attempt, attrs=%v", rec.attrs)
	}
	failed := handler.find(t, "webhook: delivery failed")
	if failed.attrs["session_id"] != "sess-panic" {
		t.Fatalf("terminal failure record session_id = %v", failed.attrs["session_id"])
	}
}

// IMP-050 AC-05: a delivery whose turn fails with ErrWebhookResponseIncomplete
// is re-executed once; the successful second attempt completes the delivery
// with exactly one COMPLETED audit.
func TestWebhookIncompleteResponseRetriesOnceThenCompletes(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	audit := make(chan string, 4)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})
	exec.errOnAttempt = func(attempt int) error {
		if attempt == 1 {
			return fmt.Errorf("relay: %w: no completed assistant message", relay.ErrWebhookResponseIncomplete)
		}
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusCompleted)
	if receipt.Attempt != 2 {
		t.Fatalf("receipt attempt = %d, want 2 after one self-heal retry", receipt.Attempt)
	}
	if exec.callCount() != 2 {
		t.Fatalf("executor calls = %d, want exactly 2 (no retry beyond the first)", exec.callCount())
	}
	calls := exec.getCalls()
	if calls[0].workCtx.Attempt != 1 || calls[1].workCtx.Attempt != 2 {
		t.Fatalf("attempts = %d/%d, want 1 then 2", calls[0].workCtx.Attempt, calls[1].workCtx.Attempt)
	}

	var completed int
	var failedSummaries []string
	deadline := time.After(5 * time.Second)
	for completed == 0 {
		select {
		case summary := <-audit:
			switch {
			case strings.Contains(summary, "Status: COMPLETED"):
				completed++
			case strings.Contains(summary, "Status: FAILED"):
				failedSummaries = append(failedSummaries, summary)
			}
		case <-deadline:
			t.Fatalf("audits: completed=%d failed=%v", completed, failedSummaries)
		}
	}
	if completed != 1 || len(failedSummaries) != 0 {
		t.Fatalf("want exactly one COMPLETED and no FAILED audit (attempt-1 failure is retried, not audited), got completed=%d failed=%v", completed, failedSummaries)
	}
}

// IMP-050 AC-05: when attempt 2 also fails the delivery is FAILED with the
// final error summary — no third attempt.
func TestWebhookIncompleteResponseSecondFailureIsTerminal(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.errOnAttempt = func(int) error {
		return fmt.Errorf("relay: %w: empty output buffer at terminal event", relay.ErrWebhookResponseIncomplete)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt.Attempt != 2 {
		t.Fatalf("receipt attempt = %d, want 2", receipt.Attempt)
	}
	if !strings.Contains(receipt.ErrorSummary, "webhook response incomplete") {
		t.Fatalf("final summary must carry the incomplete error, got %q", receipt.ErrorSummary)
	}
	if exec.callCount() != 2 {
		t.Fatalf("executor calls = %d, want exactly 2, got %+v", exec.callCount(), exec.getCalls())
	}
}

// IMP-050 AC-05: other error classes get no incomplete-response retry — a
// plain executor failure is terminal FAILED on attempt 1.
func TestWebhookOtherErrorsDoNotSelfHealRetry(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.err = errors.New("webhook agent unavailable")
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt.Attempt != 1 {
		t.Fatalf("receipt attempt = %d, want 1 (no retry for other error classes)", receipt.Attempt)
	}
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
}
