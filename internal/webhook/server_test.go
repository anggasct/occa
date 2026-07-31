package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func newTestServer(t *testing.T, endpoints []config.EndpointConfig) (*Server, *fakeExecutor) {
	t.Helper()
	exec := &fakeExecutor{}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Endpoints: endpoints,
	}
	srv := New(cfg, exec.exec)
	return srv, exec
}

type fakeExecutor struct {
	calls []execCall
}

type execCall struct {
	platform  string
	channelID string
	prompt    string
}

func (f *fakeExecutor) exec(_ context.Context, platform, channelID, prompt string) {
	f.calls = append(f.calls, execCall{platform, channelID, prompt})
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

	srv.processAsync(config.EndpointConfig{
		Name: "github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1",
		Prompt: `Analyze PR #{{.payload.number}} action={{.payload.action}}`,
	}, []byte(`{"action":"opened","number":42}`))

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

	srv.processAsync(config.EndpointConfig{
		Prompt: "static prompt", Platform: "telegram", ChannelID: "c1",
	}, []byte(`{"foo":"bar"}`))

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
