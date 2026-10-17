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
	var sentChannels []string
	var threadCalls int
	var gotChannel, gotParent, gotName string
	srv.SetSender(func(_ context.Context, p, c, text string) (string, error) {
		mu.Lock()
		sent = append(sent, text)
		sentChannels = append(sentChannels, c)
		mu.Unlock()
		if strings.TrimSpace(c) == "chan-1" {
			return "parent-123", nil
		}
		return "status-1", nil
	})
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { return nil })
	srv.SetEditor(func(_ context.Context, _, _, _ string, _ string) error { return nil })
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
	var parents []string
	for i, text := range sent {
		if sentChannels[i] == "chan-1" {
			parents = append(parents, text)
		}
	}
	if len(parents) != 1 || !strings.Contains(parents[0], "Dispatched") || !strings.Contains(parents[0], payload["task_id"]) {
		t.Errorf("parent post = %v, want 1 Dispatched with %q", parents, payload["task_id"])
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

func TestHandleDispatchThreadFailureMarksFailedLoudly(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "parent-789", nil })
	var mu sync.Mutex
	var threadCalls int
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threadCalls++
		mu.Unlock()
		return "", context.DeadlineExceeded
	})
	var edits []string
	srv.SetEditor(func(_ context.Context, _, _, _ string, text string) error {
		mu.Lock()
		edits = append(edits, text)
		mu.Unlock()
		return nil
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
	if threadCalls != dispatchThreadAttempts {
		t.Errorf("thread starts = %d, want bounded %d", threadCalls, dispatchThreadAttempts)
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("registry miss")
	}
	if stored.RootMessageID != "parent-789" || stored.ThreadID != "" || stored.Status != TaskFailed {
		t.Errorf("registry = %+v, want parent-789 with empty thread marked failed", stored)
	}
	if len(edits) != 1 {
		t.Fatalf("failed-mark edits = %d, want 1 loud parent edit", len(edits))
	}
	if !strings.Contains(edits[0], "Dispatch failed") || !strings.Contains(edits[0], payload["task_id"]) || !strings.Contains(edits[0], "acme/widget#feat/x") {
		t.Errorf("failed-mark edit = %q, want loud repo+branch+task_id", edits[0])
	}
}

func TestHandleDispatchThreadRetryThenSucceeds(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "parent-1", nil })
	var mu sync.Mutex
	var threadCalls int
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threadCalls++
		calls := threadCalls
		mu.Unlock()
		if calls < 3 {
			return "", context.DeadlineExceeded
		}
		return "thread-9", nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if threadCalls != 3 {
		t.Errorf("thread starts = %d, want 3 with retry", threadCalls)
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("registry miss")
	}
	if stored.ThreadID != "thread-9" || stored.Status != TaskDispatched {
		t.Errorf("registry = %+v, want thread-9 dispatched", stored)
	}
}
