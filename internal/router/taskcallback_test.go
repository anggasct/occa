package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

func TestParseTaskCallback(t *testing.T) {
	action, repo, branch, ok := parseTaskCallback("task:merge:acme/widget#feat/x")
	if !ok || action != "merge" || repo != "acme/widget" || branch != "feat/x" {
		t.Errorf("merge parse = %q %q %q %v", action, repo, branch, ok)
	}
	action, repo, branch, ok = parseTaskCallback("task:redispatch:acme/widget#feat/y")
	if !ok || action != "redispatch" || repo != "acme/widget" || branch != "feat/y" {
		t.Errorf("redispatch parse = %q %q %q %v", action, repo, branch, ok)
	}
	if _, _, _, ok := parseTaskCallback("task:bogus:acme/widget#feat/x"); ok {
		t.Error("bogus action must not parse")
	}
	if _, _, _, ok := parseTaskCallback("model:abc"); ok {
		t.Error("non-task callback must not parse")
	}
}

type taskEditReply struct {
	edits []string
}

func (r *taskEditReply) SendTyping() error { return nil }
func (r *taskEditReply) Send(text string) (channel.MessageRef, error) {
	return nil, nil
}
func (r *taskEditReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	return nil, nil
}
func (r *taskEditReply) Edit(ref channel.MessageRef, text string) error {
	r.edits = append(r.edits, text)
	return nil
}
func (r *taskEditReply) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	r.edits = append(r.edits, text)
	return nil
}

type taskRef struct{}

func (taskRef) ID() string { return "parent-1" }

func TestHandleTaskCallbackPostsDispatchWithToken(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"task_id": "task-123"})
	}))
	defer srv.Close()

	r := &Router{commands: make(map[string]Command)}
	r.SetDispatchConfig("dispatch-token", srv.URL)
	reply := &taskEditReply{}
	msg := channel.IncomingMessage{
		Platform:     "discord",
		ChannelID:    "chan-1",
		IsCallback:   true,
		CallbackData: "task:redispatch:acme/widget#feat/x",
		CallbackRef:  taskRef{},
		ReplyCtx:     reply,
	}
	if err := r.handleTaskCallback(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if gotAuth != "Bearer dispatch-token" {
		t.Errorf("auth = %q, want Bearer dispatch-token", gotAuth)
	}
	for _, want := range []string{"acme/widget", "feat/x", "spec_path"} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("dispatch body %q must contain %q", gotBody, want)
		}
	}
	if len(reply.edits) != 1 || !strings.Contains(reply.edits[0], "task-123") {
		t.Errorf("edits = %v, want Dispatched with task-123", reply.edits)
	}
}

func TestHandleTaskCallbackMissingConfig(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	reply := &taskEditReply{}
	msg := channel.IncomingMessage{
		Platform:     "discord",
		ChannelID:    "chan-1",
		IsCallback:   true,
		CallbackData: "task:merge:acme/widget#feat/x",
		CallbackRef:  taskRef{},
		ReplyCtx:     reply,
	}
	if err := r.handleTaskCallback(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if len(reply.edits) != 1 || !strings.Contains(reply.edits[0], "not configured") {
		t.Errorf("edits = %v, want not-configured warning", reply.edits)
	}
}
