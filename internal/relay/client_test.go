package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateSession(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/session" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":"sess-123"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	id, err := c.CreateSession(context.Background())
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if id != "sess-123" {
		t.Fatalf("got id %q, want %q", id, "sess-123")
	}
}

func TestSendMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/session/s1/message" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.SendMessage(context.Background(), "s1", "hello", nil, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
}

func TestSendMessageWithModel(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.SendMessage(context.Background(), "s1", "hello", &ModelRef{ProviderID: "openai", ID: "gpt-4o"}, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	model, ok := gotBody["model"].(map[string]any)
	if !ok {
		t.Fatalf("expected model object, got: %v", gotBody["model"])
	}
	if model["providerID"] != "openai" || model["modelID"] != "gpt-4o" {
		t.Fatalf("unexpected model: %v", model)
	}
}

func TestProviders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/provider" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"all":[{"id":"openai","models":{"gpt-4o":{"id":"gpt-4o"}}}]}`))
	}))
	defer srv.Close()

	providers, err := NewHTTPClient(srv.URL).Providers(context.Background())
	if err != nil {
		t.Fatalf("Providers: %v", err)
	}
	if !providers.HasProvider("openai") {
		t.Fatal("expected openai provider")
	}
	if !providers.HasModel(ModelRef{ProviderID: "openai", ID: "gpt-4o"}) {
		t.Fatal("expected openai/gpt-4o model")
	}
	if providers.HasModel(ModelRef{ProviderID: "openai", ID: "missing"}) {
		t.Fatal("did not expect missing model")
	}
}

func TestProvidersPreservesSentinelCauses(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		_, err := NewHTTPClient(srv.URL).Providers(context.Background())
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		_, err := NewHTTPClient("http://127.0.0.1:1").Providers(context.Background())
		if !errors.Is(err, ErrUnreachable) {
			t.Fatalf("expected ErrUnreachable, got: %v", err)
		}
	})
}

func TestListCommands(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/command" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"plan","description":"Create a plan","source":"command","template":"...","hints":["$ARGUMENTS"]}]`))
	}))
	defer srv.Close()

	commands, err := NewHTTPClient(srv.URL).ListCommands(context.Background())
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}
	if commands[0] != (CommandInfo{Name: "plan", Description: "Create a plan", Source: "command"}) {
		t.Fatalf("unexpected command: %+v", commands[0])
	}
}

func TestListCommandsUnreachable(t *testing.T) {
	_, err := NewHTTPClient("http://127.0.0.1:1").ListCommands(context.Background())
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable, got %v", err)
	}
}

func TestRunCommand(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/session/s1/command" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.RunCommand(context.Background(), "s1", "/plan build a thing")
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	if gotBody != `{"arguments":"build a thing","command":"plan"}` {
		t.Fatalf("payload = %s, want command/arguments split", gotBody)
	}
}

func TestRunCommandNoArgs(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	if err := c.RunCommand(context.Background(), "s1", "/review"); err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	if gotBody != `{"arguments":"","command":"review"}` {
		t.Fatalf("payload = %s, want empty arguments", gotBody)
	}
}

func TestUnreachable(t *testing.T) {
	c := NewHTTPClient("http://127.0.0.1:1")
	_, err := c.CreateSession(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable, got: %v", err)
	}
}

func TestNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.SendMessage(context.Background(), "bad", "hello", nil, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		w.Write([]byte("event: message.part.delta\ndata: hello\n\n"))
		fl.Flush()
		w.Write([]byte("event: done\ndata: \n\n"))
		fl.Flush()
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := c.Events(ctx, "s1")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	ev := <-ch
	if ev.Type != "delta" || ev.Delta != "hello" {
		t.Fatalf("got event %+v, want delta/hello", ev)
	}
	ev = <-ch
	if ev.Type != "done" {
		t.Fatalf("got event %+v, want done", ev)
	}
}
