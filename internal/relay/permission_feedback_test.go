package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// feedbackTurnClient is a webhook turn client whose backend accepts a
// rejection with a message.
type feedbackTurnClient struct {
	*fakeWebhookTurnClient

	mu       sync.Mutex
	feedback []string
}

func (c *feedbackTurnClient) ReplyPermissionWithMessage(_ context.Context, requestID string, reply PermissionReply, message string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.feedback = append(c.feedback, requestID+"="+string(reply)+"|"+message)
	return nil
}

func TestReplyPermissionWithMessage(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(status)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, testConfig())
	if err := c.ReplyPermissionWithMessage(context.Background(), "per-1", PermissionReject, "not allowed"); err != nil {
		t.Fatalf("ReplyPermissionWithMessage: %v", err)
	}
	if gotPath != "/permission/per-1/reply" {
		t.Fatalf("path = %q", gotPath)
	}
	if len(gotBody) != 2 || gotBody["reply"] != "reject" || gotBody["message"] != "not allowed" {
		t.Fatalf("body = %v, want exactly reply+message", gotBody)
	}

	status = http.StatusNotFound
	if err := c.ReplyPermissionWithMessage(context.Background(), "per-1", PermissionReject, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("404 err = %v, want ErrNotFound", err)
	}
	status = http.StatusInternalServerError
	if err := c.ReplyPermissionWithMessage(context.Background(), "per-1", PermissionReject, "x"); err == nil {
		t.Fatal("500 must be an error")
	}
}

func TestHTTPClientImplementsPermissionFeedback(t *testing.T) {
	var client Client = NewHTTPClient("http://127.0.0.1:1", testConfig())
	if _, ok := client.(PermissionFeedbackReplier); !ok {
		t.Fatal("HTTPClient must implement PermissionFeedbackReplier")
	}
}

func TestDenialSendsFeedbackWhenSupported(t *testing.T) {
	logs := captureWebhookLogs(t)
	plain := &permissionDenyClient{}
	client := &feedbackTurnClient{fakeWebhookTurnClient: &fakeWebhookTurnClient{Client: plain}}
	turn := WebhookTurn{Client: client, DeliveryID: "del-1"}
	scope := turn.denyUnhandledPermission(context.Background(), Event{Type: "permission_asked", Permission: &PermissionRequest{
		ID: "per-1", Tool: "call-1", Permission: "external_directory", Patterns: []string{"/home/ubuntu/.docker/*"},
	}})
	if scope != "external_directory [/home/ubuntu/.docker/*]" {
		t.Fatalf("scope = %q", scope)
	}
	if len(plain.replies) != 0 {
		t.Fatalf("plain replies = %v, want none", plain.replies)
	}
	if len(client.feedback) != 1 || !strings.HasPrefix(client.feedback[0], "per-1=reject|") ||
		!strings.Contains(client.feedback[0], "external_directory [/home/ubuntu/.docker/*]") {
		t.Fatalf("feedback = %v", client.feedback)
	}
	recs := deniedRecords(logs)
	if len(recs) != 1 || fmt.Sprint(recs[0].attrs["feedback"]) != "true" {
		t.Fatalf("denial record = %+v, want feedback=true", recs)
	}
}

func TestDenialFallsBackToPlainReject(t *testing.T) {
	logs := captureWebhookLogs(t)
	plain := &permissionDenyClient{}
	turn := WebhookTurn{Client: plain, DeliveryID: "del-1"}
	turn.denyUnhandledPermission(context.Background(), Event{Type: "permission_asked", Permission: &PermissionRequest{
		ID: "per-1", Permission: "read", Patterns: []string{"/tmp/x"},
	}})
	if len(plain.replies) != 1 || plain.replies[0] != "per-1=reject" {
		t.Fatalf("replies = %v, want one plain reject", plain.replies)
	}
	recs := deniedRecords(logs)
	if len(recs) != 1 || fmt.Sprint(recs[0].attrs["feedback"]) != "false" {
		t.Fatalf("denial record = %+v, want feedback=false", recs)
	}
}

// AC-04: with feedback the agent keeps going and the turn ends with a result.
func TestFeedbackDenialTurnContinuesToResult(t *testing.T) {
	events := make(chan Event, 4)
	events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: "perm-1", Permission: "external_directory", Patterns: []string{"/home/ubuntu/.docker/*"}}}
	events <- Event{Type: "delta", Delta: "review posted without the docker config"}
	events <- Event{Type: "done"}
	close(events)
	client := &feedbackTurnClient{fakeWebhookTurnClient: &fakeWebhookTurnClient{Client: &permissionDenyClient{}, newEvents: func() <-chan Event { return events }}}
	turn := WebhookTurn{Client: client, Prompt: "review", DeliveryID: "del-1", AbortTimeout: time.Second, VerifyTimeout: 5 * time.Second}
	res, err := turn.Run(context.Background())
	if err != nil {
		t.Fatalf("turn.Run: %v", err)
	}
	if len(res.Denied) != 1 || res.Denied[0] != "external_directory [/home/ubuntu/.docker/*]" {
		t.Fatalf("denied = %+v, want the refusal recorded", res.Denied)
	}
	if len(client.feedback) != 1 {
		t.Fatalf("feedback replies = %v", client.feedback)
	}
}

// AC-05: a turn that still ends empty after the denial is reported as denied.
func TestFeedbackDenialEmptyTurnStillDenied(t *testing.T) {
	events := make(chan Event, 2)
	events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: "perm-1", Permission: "read", Patterns: []string{"/x"}}}
	events <- Event{Type: "done"}
	close(events)
	client := &feedbackTurnClient{fakeWebhookTurnClient: &fakeWebhookTurnClient{Client: &permissionDenyClient{}, newEvents: func() <-chan Event { return events }}}
	turn := WebhookTurn{Client: client, Prompt: "review", AbortTimeout: time.Second, VerifyTimeout: 5 * time.Second}
	if _, err := turn.Run(context.Background()); !errors.Is(err, ErrWebhookPermissionDenied) {
		t.Fatalf("err = %v, want ErrWebhookPermissionDenied", err)
	}
}

// AC-07: the message stays bounded for the largest scope permissionScope renders.
func TestDeniedPermissionMessageIsBounded(t *testing.T) {
	patterns := make([]string, 9)
	for i := range patterns {
		patterns[i] = strings.Repeat("y", 300)
	}
	msg := deniedPermissionMessage("external_directory", permissionScope(patterns))
	if len(msg) > 900 {
		t.Fatalf("message = %d bytes, want <= 900", len(msg))
	}
}
