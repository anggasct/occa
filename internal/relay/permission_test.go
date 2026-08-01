package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type testPermissionPromptHandler struct {
	reply *fakeReplyContext
}

func (h testPermissionPromptHandler) Prompt(_ context.Context, request PermissionRequest) error {
	text := "🔐 Permission requested: " + request.Permission
	if request.Tool != "" {
		text += " (" + request.Tool + ")"
	}
	_, err := h.reply.SendWithButtons(text, []channel.Button{{Label: "Allow", Value: "permission:opaque:once"}})
	return err
}

func TestParsePermissionAskedEvent(t *testing.T) {
	payload := `{"properties":{"id":"per-123","sessionID":"sess-1","permission":"external_directory","tool":"bash"}}`
	ev, _ := parseSSEEvent("permission.asked", payload)

	if ev.Type != "permission_asked" {
		t.Fatalf("expected permission_asked, got %q", ev.Type)
	}
	if ev.Permission == nil {
		t.Fatal("expected Permission to be set")
	}
	if ev.Permission.ID != "per-123" {
		t.Fatalf("ID = %q, want per-123", ev.Permission.ID)
	}
	if ev.Permission.Permission != "external_directory" {
		t.Fatalf("Permission = %q, want external_directory", ev.Permission.Permission)
	}
	if ev.Permission.Tool != "bash" {
		t.Fatalf("Tool = %q, want bash", ev.Permission.Tool)
	}
}

func TestParsePermissionAskedInvalidJSON(t *testing.T) {
	ev, _ := parseSSEEvent("permission.asked", "not json")
	if ev.Type != "delta" {
		t.Fatalf("expected fallback to delta for invalid JSON, got %q", ev.Type)
	}
}

func TestReplyPermission(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.ReplyPermission(context.Background(), "per-456", PermissionOnce)
	if err != nil {
		t.Fatalf("ReplyPermission: %v", err)
	}
	if gotPath != "/permission/per-456/reply" {
		t.Fatalf("path = %q, want /permission/per-456/reply", gotPath)
	}
	if gotBody["reply"] != "once" {
		t.Fatalf("reply = %q, want once", gotBody["reply"])
	}
}

func TestReplyPermissionAlways(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.ReplyPermission(context.Background(), "per-789", PermissionAlways)
	if err != nil {
		t.Fatalf("ReplyPermission: %v", err)
	}
	if gotBody["reply"] != "always" {
		t.Fatalf("reply = %q, want always", gotBody["reply"])
	}
}

func TestReplyPermissionReject(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.ReplyPermission(context.Background(), "per-000", PermissionReject)
	if err != nil {
		t.Fatalf("ReplyPermission: %v", err)
	}
	if gotBody["reply"] != "reject" {
		t.Fatalf("reply = %q, want reject", gotBody["reply"])
	}
}

func TestStreamerPermissionPrompt(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)
	s.SetPermissionPromptHandler(testPermissionPromptHandler{reply: reply})

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: "Working..."}
	events <- Event{
		Type: "permission_asked",
		Permission: &PermissionRequest{
			ID:         "per-abc",
			SessionID:  "sess-1",
			Permission: "bash",
			Tool:       "terminal",
		},
	}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	foundPermission := false
	for _, s := range reply.sends {
		if strings.Contains(s, "Permission requested") {
			foundPermission = true
		}
	}
	if !foundPermission {
		t.Fatalf("expected permission prompt message in sends, got: %v", reply.sends)
	}
}
