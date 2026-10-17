package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestHandleDispatchOpensThread(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	var sent []string
	var threadCalls int
	var gotChannel, gotParent, gotName string
	srv.SetSender(func(_ context.Context, p, c, text string) (string, error) {
		mu.Lock()
		sent = append(sent, text)
		mu.Unlock()
		return "parent-123", nil
	})
	srv.SetThreadStarter(func(_ context.Context, p, c, m, name string) (string, error) {
		mu.Lock()
		threadCalls++
		gotChannel, gotParent, gotName = c, m, name
		mu.Unlock()
		return "thread-456", nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 || !strings.Contains(sent[0], "Dispatched") || !strings.Contains(sent[0], payload["task_id"]) {
		t.Errorf("parent post = %v, want 1 Dispatched with %q", sent, payload["task_id"])
	}
	if threadCalls != 1 {
		t.Fatalf("thread starts = %d, want 1", threadCalls)
	}
	if gotChannel != "chan-1" || gotParent != "parent-123" {
		t.Errorf("thread target = %s/%s, want chan-1/parent-123", gotChannel, gotParent)
	}
	if !strings.Contains(gotName, "feat/x") {
		t.Errorf("thread name = %q, want branch feat/x", gotName)
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("registry miss")
	}
	if stored.RootMessageID != "parent-123" || stored.ThreadID != "thread-456" || stored.Status != TaskDispatched {
		t.Errorf("registry = %+v, want parent-123/thread-456/dispatched", stored)
	}
}

func TestHandleDispatchThreadFailureKeepsParent(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "parent-789", nil })
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		return "", context.DeadlineExceeded
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("registry miss")
	}
	if stored.RootMessageID != "parent-789" || stored.ThreadID != "" {
		t.Errorf("registry = %+v, want parent-789 with empty thread", stored)
	}
}
