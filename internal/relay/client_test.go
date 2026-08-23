package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
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
		if r.Method != http.MethodPost || r.URL.Path != "/session/s1/prompt_async" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
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
		w.WriteHeader(http.StatusNoContent)
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

// TestWrapTransportErrPreservesCancel: a canceled request keeps the
// context.Canceled sentinel instead of being mislabeled unreachable.
func TestWrapTransportErrPreservesCancel(t *testing.T) {
	c := NewHTTPClient("http://127.0.0.1:1")
	err := c.wrapTransportErr(context.Canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled preserved, got %v", err)
	}
	if errors.Is(err, ErrUnreachable) {
		t.Fatalf("canceled request must not be ErrUnreachable")
	}
}

// TestAnswerQuestionPostsPayload verifies the reply endpoint and payload.
func TestAnswerQuestionPostsPayload(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/question/que_1/reply" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	if err := c.AnswerQuestion(context.Background(), "que_1", [][]string{{"A"}, {}}); err != nil {
		t.Fatalf("AnswerQuestion: %v", err)
	}
	var decoded struct {
		Answers [][]string `json:"answers"`
	}
	if err := json.Unmarshal([]byte(gotBody), &decoded); err != nil {
		t.Fatalf("bad payload %q: %v", gotBody, err)
	}
	if len(decoded.Answers) != 2 || decoded.Answers[0][0] != "A" || len(decoded.Answers[1]) != 0 {
		t.Fatalf("answers = %v", decoded.Answers)
	}
}

func TestNewHTTPClientTimeout(t *testing.T) {
	c := NewHTTPClient("http://localhost:8080")
	if c.http.Timeout != 3*time.Minute {
		t.Fatalf("timeout = %v, want 3m", c.http.Timeout)
	}
}

func TestAnswerQuestionErrorBodyReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Expected a string starting with que, got x"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.AnswerQuestion(context.Background(), "que_1", [][]string{{"A"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "Expected a string starting with que") {
		t.Fatalf("expected error to contain body details, got %q", err.Error())
	}
}

func TestRejectQuestionErrorBodyReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Question already resolved"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.RejectQuestion(context.Background(), "que_1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "Question already resolved") {
		t.Fatalf("expected error to contain body details, got %q", err.Error())
	}
}

func TestTruncateBodyRuneSafe(t *testing.T) {
	longBody := []byte(strings.Repeat("🌍", 600))
	truncated := truncateBody(longBody)
	if !utf8.ValidString(truncated) {
		t.Fatalf("truncated string is invalid UTF-8")
	}
	if !strings.Contains(truncated, "… (2400 bytes total)") {
		t.Fatalf("expected byte count note in truncated string, got %q", truncated)
	}
}

func TestSessionExists(t *testing.T) {
	t.Run("found 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/session/sess-123" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		exists, err := c.SessionExists(context.Background(), "sess-123")
		if err != nil {
			t.Fatalf("SessionExists: %v", err)
		}
		if !exists {
			t.Fatalf("got exists = false, want true")
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/session/sess-404" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		exists, err := c.SessionExists(context.Background(), "sess-404")
		if err != nil {
			t.Fatalf("SessionExists: %v", err)
		}
		if exists {
			t.Fatalf("got exists = true, want false")
		}
	})

	t.Run("unexpected status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		_, err := c.SessionExists(context.Background(), "sess-500")
		if err == nil {
			t.Fatal("expected error for status 500, got nil")
		}
	})
}

func TestBuildMessagePayloadModelVariant(t *testing.T) {
	t.Run("with variant", func(t *testing.T) {
		payload := buildMessagePayload("hello", &ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max"}, nil)
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var decoded struct {
			Model struct {
				ProviderID string `json:"providerID"`
				ModelID    string `json:"modelID"`
			} `json:"model"`
			Variant string `json:"variant"`
		}
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if decoded.Model.ProviderID != "zai-coding-plan" {
			t.Fatalf("providerID = %q, want zai-coding-plan", decoded.Model.ProviderID)
		}
		if decoded.Model.ModelID != "glm-5.2" {
			t.Fatalf("modelID = %q, want glm-5.2", decoded.Model.ModelID)
		}
		if decoded.Variant != "max" {
			t.Fatalf("top-level variant = %q, want max", decoded.Variant)
		}
		// variant must NOT be inside model
		var rawMap map[string]any
		if err := json.Unmarshal(data, &rawMap); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		modelMap := rawMap["model"].(map[string]any)
		if _, ok := modelMap["variant"]; ok {
			t.Fatalf("expected no variant key inside model, got %v", modelMap["variant"])
		}
	})

	t.Run("without variant", func(t *testing.T) {
		payload := buildMessagePayload("hello", &ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2"}, nil)
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var rawMap map[string]any
		if err := json.Unmarshal(data, &rawMap); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if _, ok := rawMap["variant"]; ok {
			t.Fatalf("expected no top-level variant key, got %v", rawMap["variant"])
		}
		modelMap := rawMap["model"].(map[string]any)
		if _, ok := modelMap["variant"]; ok {
			t.Fatalf("expected variant key to be omitted inside model, got %v", modelMap["variant"])
		}
	})

	t.Run("no model", func(t *testing.T) {
		payload := buildMessagePayload("hello", nil, nil)
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var rawMap map[string]any
		if err := json.Unmarshal(data, &rawMap); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if _, ok := rawMap["model"]; ok {
			t.Fatalf("expected no model key when model is nil, got %v", rawMap["model"])
		}
		if _, ok := rawMap["variant"]; ok {
			t.Fatalf("expected no variant key when model is nil, got %v", rawMap["variant"])
		}
	})
}

func TestProvidersHasVariant(t *testing.T) {
	providers := Providers{All: []Provider{
		{
			ID: "zai-coding-plan",
			Models: map[string]json.RawMessage{
				"glm-5.2":  json.RawMessage(`{"variants":{"high":{},"max":{},"low":{}}}`),
				"no-var":   json.RawMessage(`{"name":"no-var"}`),
				"bad-json": json.RawMessage(`invalid`),
			},
		},
	}}

	t.Run("variant exists", func(t *testing.T) {
		ref := ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max"}
		if !providers.HasVariant(ref) {
			t.Fatal("expected HasVariant to return true for existing variant")
		}
	})

	t.Run("variant missing from variants map", func(t *testing.T) {
		ref := ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "unknown"}
		if providers.HasVariant(ref) {
			t.Fatal("expected HasVariant to return false for missing variant")
		}
	})

	t.Run("variants field missing", func(t *testing.T) {
		ref := ModelRef{ProviderID: "zai-coding-plan", ID: "no-var", Variant: "any"}
		if !providers.HasVariant(ref) {
			t.Fatal("expected HasVariant to return true when variants field is missing")
		}
	})

	t.Run("variants field unparseable", func(t *testing.T) {
		ref := ModelRef{ProviderID: "zai-coding-plan", ID: "bad-json", Variant: "any"}
		if !providers.HasVariant(ref) {
			t.Fatal("expected HasVariant to return true when JSON is unparseable")
		}
	})
}

func TestAbortSession(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/session/ses_x/abort" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		if err := c.AbortSession(context.Background(), "ses_x"); err != nil {
			t.Fatalf("AbortSession: %v", err)
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.AbortSession(context.Background(), "ses_x")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("error 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.AbortSession(context.Background(), "ses_x")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGetSession(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/session/ses-123":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"cost": 0.05,
					"model": {"providerID": "anthropic", "id": "claude-3-5-sonnet-20241022", "variant": "max"},
					"tokens": {
						"input": 12000,
						"output": 3000,
						"reasoning": 500,
						"cache": {"read": 1000, "write": 200}
					}
				}`))
			case "/session/ses-123/message":
				if r.URL.RawQuery != "limit=20" {
					t.Errorf("unexpected message query: %q", r.URL.RawQuery)
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[
					{"info":{"role":"assistant","tokens":{"input":0,"cache":{"read":0}}}},
					{"info":{"role":"assistant","tokens":{"input":4829,"cache":{"read":236032}},"time":{"created":1787478451877,"completed":1787478457781}}},
					{"info":{"role":"user","tokens":{}}}
				]`))
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		info, err := c.GetSession(context.Background(), "ses-123")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if info.Cost != 0.05 {
			t.Fatalf("cost = %v, want 0.05", info.Cost)
		}
		if info.Model.ProviderID != "anthropic" || info.Model.ID != "claude-3-5-sonnet-20241022" || info.Model.Variant != "max" {
			t.Fatalf("unexpected model: %+v", info.Model)
		}
		if info.Tokens.Input != 12000 || info.Tokens.Output != 3000 || info.Tokens.Reasoning != 500 || info.Tokens.CacheRead != 1000 || info.Tokens.CacheWrite != 200 {
			t.Fatalf("unexpected tokens: %+v", info.Tokens)
		}
		if info.ContextTokens != 4829+236032 {
			t.Fatalf("context tokens = %d, want %d (last assistant input + cache read; in-flight zero skipped)", info.ContextTokens, 4829+236032)
		}
		if info.ContextSource != ContextSourceMessageTail {
			t.Fatalf("context source = %q, want %q", info.ContextSource, ContextSourceMessageTail)
		}
		if want := time.UnixMilli(1787478457781); !info.ContextUpdatedAt.Equal(want) {
			t.Fatalf("context updated at = %v, want %v (completion time)", info.ContextUpdatedAt, want)
		}
	})

	t.Run("in-flight message is skipped for context", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/session/ses-123/message" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"cost":0.05,"model":{"providerID":"anthropic","id":"claude-3-5-sonnet-20241022","variant":"max"},"tokens":{"input":12000,"output":3000,"reasoning":500,"cache":{"read":1000,"write":200}}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"info":{"role":"assistant","tokens":{"input":4829,"cache":{"read":236032}},"time":{"created":1787478451877,"completed":1787478457781}}},
				{"info":{"role":"assistant","tokens":{"input":9000,"cache":{"read":1000}},"time":{"created":1787478460000}}}
			]`))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		info, err := c.GetSession(context.Background(), "ses-123")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		// The newest assistant message has no completion time, so it cannot be
		// presented as live; the scan falls back to the older completed message.
		if info.ContextTokens != 4829+236032 {
			t.Fatalf("context tokens = %d, want %d (first timestamped assistant)", info.ContextTokens, 4829+236032)
		}
		if info.ContextSource != ContextSourceMessageTail {
			t.Fatalf("context source = %q, want %q", info.ContextSource, ContextSourceMessageTail)
		}
		if want := time.UnixMilli(1787478457781); !info.ContextUpdatedAt.Equal(want) {
			t.Fatalf("context updated at = %v, want %v", info.ContextUpdatedAt, want)
		}
	})

	t.Run("occupancy without any timestamp renders context unavailable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/session/ses-123/message" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"cost":0.05,"model":{"providerID":"anthropic","id":"claude-3-5-sonnet-20241022","variant":"max"},"tokens":{"input":12000,"output":3000,"reasoning":500,"cache":{"read":1000,"write":200}}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"info":{"role":"assistant","tokens":{"input":9000,"cache":{"read":1000}}}}
			]`))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		info, err := c.GetSession(context.Background(), "ses-123")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if info.ContextTokens != 0 {
			t.Fatalf("context tokens = %d, want 0 when no timestamp is available", info.ContextTokens)
		}
		if info.ContextSource != "" {
			t.Fatalf("context source = %q, want empty when unavailable", info.ContextSource)
		}
		if !info.ContextUpdatedAt.IsZero() {
			t.Fatalf("context updated at = %v, want zero when unavailable", info.ContextUpdatedAt)
		}
	})

	t.Run("message tail unavailable keeps context zero", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/session/ses-123" {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"cost": 0.05,
				"model": {"providerID": "anthropic", "id": "claude-3-5-sonnet-20241022", "variant": "max"},
				"tokens": {"input": 12000, "output": 3000, "reasoning": 500, "cache": {"read": 1000, "write": 200}}
			}`))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		info, err := c.GetSession(context.Background(), "ses-123")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if info.ContextTokens != 0 {
			t.Fatalf("context tokens = %d, want 0 when the message tail fails", info.ContextTokens)
		}
		if info.ContextSource != "" {
			t.Fatalf("context source = %q, want empty when the message tail fails", info.ContextSource)
		}
		if !info.ContextUpdatedAt.IsZero() {
			t.Fatalf("context updated at = %v, want zero when the message tail fails", info.ContextUpdatedAt)
		}
		if info.Tokens.Input != 12000 {
			t.Fatalf("cumulative input = %d, want 12000 even when the message tail fails", info.Tokens.Input)
		}
	})

	t.Run("non-200 message tail with valid-looking array keeps context zero", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/session/ses-123" {
				// Error body that still contains a valid-looking message array:
				// it must NOT be decoded into ContextTokens.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`[
					{"info":{"role":"assistant","tokens":{"input":4829,"cache":{"read":236032}}}}
				]`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"cost": 0.05,
				"model": {"providerID": "anthropic", "id": "claude-3-5-sonnet-20241022", "variant": "max"},
				"tokens": {"input": 12000, "output": 3000, "reasoning": 500, "cache": {"read": 1000, "write": 200}}
			}`))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		info, err := c.GetSession(context.Background(), "ses-123")
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if info.ContextTokens != 0 {
			t.Fatalf("context tokens = %d, want 0 when the message tail returns non-200 with a valid-looking array", info.ContextTokens)
		}
		if info.ContextSource != "" {
			t.Fatalf("context source = %q, want empty when the message tail returns non-200", info.ContextSource)
		}
		if !info.ContextUpdatedAt.IsZero() {
			t.Fatalf("context updated at = %v, want zero when the message tail returns non-200", info.ContextUpdatedAt)
		}
		if info.Tokens.Input != 12000 || info.Tokens.CacheRead != 1000 {
			t.Fatalf("cumulative tokens = %+v, want 12000/1000 to survive a failed message tail", info.Tokens)
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		_, err := c.GetSession(context.Background(), "ses-404")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestProvidersContextLimit(t *testing.T) {
	providers := Providers{All: []Provider{
		{
			ID: "openai",
			Models: map[string]json.RawMessage{
				"gpt-4o":   json.RawMessage(`{"limit":{"context":128000}}`),
				"no-limit": json.RawMessage(`{"name":"no-limit"}`),
			},
		},
	}}

	t.Run("limit present", func(t *testing.T) {
		limit, ok := providers.ContextLimit("openai", "gpt-4o")
		if !ok || limit != 128000 {
			t.Fatalf("ContextLimit = (%d, %v), want (128000, true)", limit, ok)
		}
	})

	t.Run("limit absent", func(t *testing.T) {
		limit, ok := providers.ContextLimit("openai", "no-limit")
		if ok || limit != 0 {
			t.Fatalf("ContextLimit = (%d, %v), want (0, false)", limit, ok)
		}
	})

	t.Run("provider absent", func(t *testing.T) {
		limit, ok := providers.ContextLimit("missing", "gpt-4o")
		if ok || limit != 0 {
			t.Fatalf("ContextLimit = (%d, %v), want (0, false)", limit, ok)
		}
	})
}

func TestSummarizeSession(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		var gotBody map[string]string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/session/ses_x/summarize" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		if err := c.SummarizeSession(context.Background(), "ses_x", "openai", "gpt-4o"); err != nil {
			t.Fatalf("SummarizeSession: %v", err)
		}
		if gotBody["providerID"] != "openai" || gotBody["modelID"] != "gpt-4o" {
			t.Fatalf("unexpected body: %+v", gotBody)
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.SummarizeSession(context.Background(), "ses_404", "openai", "gpt-4o")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("error 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("unconnected provider"))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.SummarizeSession(context.Background(), "ses_500", "openai", "gpt-4o")
		if err == nil || !strings.Contains(err.Error(), "unconnected provider") {
			t.Fatalf("expected error containing unconnected provider, got %v", err)
		}
	})
}

func TestRevertMessage(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		var gotBody map[string]string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/session/ses_x/revert" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		if err := c.RevertMessage(context.Background(), "ses_x", "msg-123"); err != nil {
			t.Fatalf("RevertMessage: %v", err)
		}
		if gotBody["messageID"] != "msg-123" {
			t.Fatalf("messageID = %q, want msg-123", gotBody["messageID"])
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.RevertMessage(context.Background(), "ses_404", "msg-123")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("error 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("revert failed"))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.RevertMessage(context.Background(), "ses_500", "msg-123")
		if err == nil || !strings.Contains(err.Error(), "revert failed") {
			t.Fatalf("expected error containing revert failed, got %v", err)
		}
	})
}

func TestUnrevertSession(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/session/ses_x/unrevert" {
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		if err := c.UnrevertSession(context.Background(), "ses_x"); err != nil {
			t.Fatalf("UnrevertSession: %v", err)
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.UnrevertSession(context.Background(), "ses_404")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("error 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("unrevert failed"))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		err := c.UnrevertSession(context.Background(), "ses_500")
		if err == nil || !strings.Contains(err.Error(), "unrevert failed") {
			t.Fatalf("expected error containing unrevert failed, got %v", err)
		}
	})
}

func TestListMessages(t *testing.T) {
	t.Run("success 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/session/ses_x/message" || r.URL.RawQuery != "limit=50" {
				t.Errorf("unexpected request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"info":{"id":"msg-1","role":"user","time":{"created":1000}}},
				{"info":{"id":"msg-2","role":"assistant","time":{"created":1001}}}
			]`))
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		msgs, err := c.ListMessages(context.Background(), "ses_x")
		if err != nil {
			t.Fatalf("ListMessages: %v", err)
		}
		if len(msgs) != 2 {
			t.Fatalf("len = %d, want 2", len(msgs))
		}
		if msgs[0].ID != "msg-1" || msgs[0].Role != "user" || msgs[0].Created != 1000 {
			t.Fatalf("unexpected message 0: %+v", msgs[0])
		}
	})

	t.Run("not found 404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		_, err := c.ListMessages(context.Background(), "ses_404")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("error 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		c := NewHTTPClient(srv.URL)
		_, err := c.ListMessages(context.Background(), "ses_500")
		if err == nil || !strings.Contains(err.Error(), "unexpected status 500") {
			t.Fatalf("expected error containing unexpected status 500, got %v", err)
		}
	})
}
