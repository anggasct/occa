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

func TestHandleTaskCallbackRedispatchRefusedInPlace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "task already merged for acme/widget#feat/x — re-dispatch refused"})
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
	if len(reply.edits) != 1 {
		t.Fatalf("edits = %v, want exactly 1 refusal", reply.edits)
	}
	if !strings.Contains(reply.edits[0], "Re-dispatch refused") {
		t.Errorf("refusal must say Re-dispatch refused, got %q", reply.edits[0])
	}
	if !strings.Contains(reply.edits[0], "already merged") {
		t.Errorf("refusal must carry server reason, got %q", reply.edits[0])
	}
}

func TestDispatchErrorTextParsesJSON(t *testing.T) {
	if got := dispatchErrorText([]byte(`{"error":"branch gone for a#b — re-dispatch refused"}`)); !strings.Contains(got, "branch gone") {
		t.Errorf("json error = %q, want branch gone", got)
	}
	if got := dispatchErrorText([]byte(`plain failure`)); got != "plain failure" {
		t.Errorf("plain error = %q, want passthrough", got)
	}
}

// IMP-111 AC-02/AC-03: a Review press goes to the review hook with repo and
// branch, never to the dispatch API, and leaves the card unedited.
func TestReviewCallbackCallsHookAndLeavesCard(t *testing.T) {
	dispatched := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	action, repo, branch, ok := parseTaskCallback("task:review:acme/widget#feat/x")
	if !ok || action != "review" || repo != "acme/widget" || branch != "feat/x" {
		t.Fatalf("review parse = %q %q %q %v", action, repo, branch, ok)
	}

	r := &Router{commands: make(map[string]Command)}
	r.SetDispatchConfig("dispatch-token", srv.URL)
	var gotRepo, gotBranch, gotUser string
	calls := 0
	r.SetTaskActionHook(func(ctx context.Context, msg channel.IncomingMessage, action, repo, branch string) error {
		if action != "review" {
			t.Fatalf("action = %q, want review", action)
		}
		calls++
		gotRepo, gotBranch, gotUser = repo, branch, msg.UserID
		return nil
	})
	reply := &taskEditReply{}
	msg := channel.IncomingMessage{
		Platform:     "discord",
		ChannelID:    "chan-1",
		UserID:       "op",
		IsCallback:   true,
		CallbackData: "task:review:acme/widget#feat/x",
		CallbackRef:  taskRef{},
		ReplyCtx:     reply,
	}
	if err := r.handleCallback(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if calls != 1 || gotRepo != "acme/widget" || gotBranch != "feat/x" || gotUser != "op" {
		t.Fatalf("hook calls=%d repo=%q branch=%q user=%q", calls, gotRepo, gotBranch, gotUser)
	}
	if dispatched {
		t.Fatal("a Review press must not call the dispatch API")
	}
	if len(reply.edits) != 0 {
		t.Fatalf("a Review press must not edit the card: %v", reply.edits)
	}
}

type taskButtonReply struct {
	taskEditReply
	sentText    string
	sentButtons []channel.Button
}

func (r *taskButtonReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	r.sentText = text
	r.sentButtons = buttons
	return nil, nil
}

// IMP-112 AC-02: compact callbacks resolve their id; merge keeps the dispatch
// path, retry/cancel go to the action hook, an unknown id is reported stale.
func TestShortTaskCallbacks(t *testing.T) {
	var dispatchedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		dispatchedBody = string(buf)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"task_id": "task-9"})
	}))
	defer srv.Close()

	r := &Router{commands: make(map[string]Command)}
	r.SetDispatchConfig("dispatch-token", srv.URL)
	r.SetTaskRefResolver(func(id string) (string, string, bool) {
		if id == "abcdef0123" {
			return "acme/widget", "feat/x", true
		}
		return "", "", false
	})
	var actions []string
	r.SetTaskActionHook(func(ctx context.Context, msg channel.IncomingMessage, action, repo, branch string) error {
		actions = append(actions, action+" "+repo+"#"+branch)
		return nil
	})
	press := func(data string) *taskEditReply {
		reply := &taskEditReply{}
		msg := channel.IncomingMessage{Platform: "telegram", ChannelID: "1", IsCallback: true, CallbackData: data, CallbackRef: taskRef{}, ReplyCtx: reply}
		if err := r.handleCallback(context.Background(), msg); err != nil {
			t.Fatalf("%s: %v", data, err)
		}
		return reply
	}

	if reply := press("ta:retry:abcdef0123"); len(reply.edits) != 0 {
		t.Fatalf("retry must not edit the message: %v", reply.edits)
	}
	press("ta:cancel:abcdef0123")
	if strings.Join(actions, ",") != "retry acme/widget#feat/x,cancel acme/widget#feat/x" {
		t.Fatalf("hook actions = %v", actions)
	}
	if reply := press("ta:merge:abcdef0123"); !strings.Contains(dispatchedBody, `"scope":"merge feat/x"`) || len(reply.edits) != 1 || !strings.Contains(reply.edits[0], "task-9") {
		t.Fatalf("merge dispatch body = %q, edits = %v", dispatchedBody, reply.edits)
	}
	if reply := press("ta:retry:0000000000"); len(reply.edits) != 1 || !strings.Contains(reply.edits[0], "out of date") {
		t.Fatalf("stale id edits = %v", reply.edits)
	}
	if len(actions) != 2 {
		t.Fatalf("a stale id must not reach the hook: %v", actions)
	}
}

// IMP-112 AC-05: /inbox replies with the hook's buttons; /digest with text.
func TestInboxAndDigestCommands(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	r.SetInboxHook(func(ctx context.Context, msg channel.IncomingMessage) (string, []channel.Button, error) {
		return "📥 Needs you — 1 PR", []channel.Button{{Label: "Retry #7", Value: "ta:retry:abcdef0123", Row: 1}}, nil
	})
	r.SetDigestHook(func(ctx context.Context, msg channel.IncomingMessage) (string, error) {
		return "☀️ occa — last 24h", nil
	})
	reply := &taskButtonReply{}
	msg := channel.IncomingMessage{Platform: "telegram", ChannelID: "1", Text: "/inbox", ReplyCtx: reply}
	if _, err := r.handleInbox(context.Background(), msg, ""); err != errReplied {
		t.Fatalf("inbox err = %v, want errReplied", err)
	}
	if !strings.Contains(reply.sentText, "Needs you") || len(reply.sentButtons) != 1 || reply.sentButtons[0].Value != "ta:retry:abcdef0123" {
		t.Fatalf("inbox reply = %q %+v", reply.sentText, reply.sentButtons)
	}
	text, err := r.handleDigest(context.Background(), msg, "")
	if err != nil || text != "☀️ occa — last 24h" {
		t.Fatalf("digest = %q %v", text, err)
	}
	if _, err := (&Router{commands: make(map[string]Command)}).handleInbox(context.Background(), msg, ""); err == nil {
		t.Fatal("inbox without the webhook relay must say so")
	}
}
