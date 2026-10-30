package relay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// permissionDenyClient records the permission replies a lane sends.
type permissionDenyClient struct {
	Client

	mu      sync.Mutex
	replies []string
}

func (c *permissionDenyClient) ReplyPermission(_ context.Context, requestID string, reply PermissionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replies = append(c.replies, requestID+"="+string(reply))
	return nil
}

// TestWebhookTurnDeniesUnhandledPermission asserts a permission request in a
// lane without a handler is rejected instead of leaving the tool call
// running.
func TestWebhookTurnDeniesUnhandledPermission(t *testing.T) {
	events := make(chan Event, 4)
	events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: "perm-1", Tool: "read"}}
	events <- Event{Type: "delta", Delta: "the read was denied"}
	events <- Event{Type: "done"}
	close(events)

	client := &permissionDenyClient{}
	turn := WebhookTurn{
		Client:        client,
		Prompt:        "analyze",
		Platform:      "discord",
		ChannelID:     "chan-1",
		DeliveryID:    "del-1",
		AbortTimeout:  time.Second,
		VerifyTimeout: 5 * time.Second,
	}
	eventsFn := func() <-chan Event { return events }
	turn.Client = &fakeWebhookTurnClient{Client: client, newEvents: eventsFn}

	res, err := turn.Run(context.Background())
	if err != nil {
		t.Fatalf("turn.Run: %v", err)
	}
	if !strings.Contains(res.Output, "denied") {
		t.Fatalf("output = %q", res.Output)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.replies) != 1 {
		t.Fatalf("permission replies = %+v, want one denial", client.replies)
	}
	if client.replies[0] != "perm-1=reject" {
		t.Fatalf("permission reply = %q, want a rejection", client.replies[0])
	}
}

type failingPermissionClient struct{ Client }

func (failingPermissionClient) ReplyPermission(context.Context, string, PermissionReply) error {
	return errors.New("reply refused")
}

func deniedRecords(logs *captureHandler) []capturedRecord {
	var out []capturedRecord
	for _, rec := range logs.snapshot() {
		if rec.msg == "webhook: permission denied, no handler" {
			out = append(out, rec)
		}
	}
	return out
}

func TestPermissionDenialLogsWhatWasAsked(t *testing.T) {
	logs := captureWebhookLogs(t)
	turn := WebhookTurn{Client: &permissionDenyClient{}, DeliveryID: "del-1"}
	turn.denyUnhandledPermission(context.Background(), Event{Type: "permission_asked", Permission: &PermissionRequest{
		ID: "per-1", Tool: "call-1", Permission: "external_directory", Patterns: []string{"/home/ubuntu/bin/*"},
	}})

	recs := deniedRecords(logs)
	if len(recs) != 1 {
		t.Fatalf("denial lines = %d, want 1", len(recs))
	}
	want := map[string]string{
		"permission":  "external_directory",
		"patterns":    "[/home/ubuntu/bin/*]",
		"delivery_id": "del-1",
		"request_id":  "per-1",
		"tool":        "call-1",
	}
	for key, value := range want {
		if got := fmt.Sprint(recs[0].attrs[key]); got != value {
			t.Errorf("%s = %q, want %q", key, got, value)
		}
	}
}

func TestPermissionScopeIsBounded(t *testing.T) {
	long := strings.Repeat("x", 300)
	got := permissionScope([]string{"a", long, "c", "d", "e", "f", "g"})
	want := "[a " + strings.Repeat("x", 120) + "… c d e] +2 more"
	if got != want {
		t.Fatalf("scope = %q, want %q", got, want)
	}
	if got := permissionScope([]string{"git  push\n origin"}); got != "[git push origin]" {
		t.Fatalf("scope = %q, want whitespace collapsed", got)
	}
}

func TestPermissionDenialFieldsOnEveryBranch(t *testing.T) {
	ask := func(id string) Event {
		return Event{Type: "permission_asked", Permission: &PermissionRequest{
			ID: id, Tool: "call-1", Permission: "read", Patterns: []string{"/tmp/x"},
		}}
	}
	cases := map[string]WebhookTurn{
		"no request id": {Client: &permissionDenyClient{}, DeliveryID: "del-2"},
		"reply failed":  {Client: failingPermissionClient{}, DeliveryID: "del-2"},
	}
	for name, turn := range cases {
		t.Run(name, func(t *testing.T) {
			logs := captureWebhookLogs(t)
			id := "per-2"
			if name == "no request id" {
				id = ""
			}
			turn.denyUnhandledPermission(context.Background(), ask(id))
			recs := deniedRecords(logs)
			if len(recs) != 1 {
				t.Fatalf("denial lines = %d, want 1", len(recs))
			}
			for key, value := range map[string]string{"permission": "read", "patterns": "[/tmp/x]", "delivery_id": "del-2"} {
				if got := fmt.Sprint(recs[0].attrs[key]); got != value {
					t.Errorf("%s = %q, want %q", key, got, value)
				}
			}
		})
	}
}

func runDeniedTurn(t *testing.T, tail ...Event) (WebhookTurnResult, error) {
	t.Helper()
	events := make(chan Event, 4+len(tail))
	events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: "perm-1", Tool: "bash", Permission: "external_directory", Patterns: []string{"/home/ubuntu/bin/*"}}}
	for _, ev := range tail {
		events <- ev
	}
	close(events)
	turn := WebhookTurn{
		Prompt:        "merge",
		DeliveryID:    "del-1",
		AbortTimeout:  time.Second,
		VerifyTimeout: 5 * time.Second,
	}
	turn.Client = &fakeWebhookTurnClient{Client: &permissionDenyClient{}, newEvents: func() <-chan Event { return events }}
	return turn.Run(context.Background())
}

func TestWebhookTurnEndedByDenialReportsThePermission(t *testing.T) {
	res, err := runDeniedTurn(t, Event{Type: "done"})
	if !errors.Is(err, ErrWebhookPermissionDenied) {
		t.Fatalf("err = %v, want ErrWebhookPermissionDenied", err)
	}
	if errors.Is(err, ErrWebhookResponseIncomplete) {
		t.Fatalf("err = %v, a denied turn must not read as an incomplete response", err)
	}
	for _, want := range []string{"external_directory [/home/ubuntu/bin/*]", "empty output buffer"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err.Error(), want)
		}
	}
	if len(res.Denied) != 1 || res.Denied[0] != "external_directory [/home/ubuntu/bin/*]" {
		t.Fatalf("denied = %+v", res.Denied)
	}
}

func TestWebhookTurnWithResultAfterDenialSucceeds(t *testing.T) {
	res, err := runDeniedTurn(t, Event{Type: "delta", Delta: "merged without the helper"}, Event{Type: "done"})
	if err != nil {
		t.Fatalf("turn.Run: %v", err)
	}
	if len(res.Denied) != 1 {
		t.Fatalf("denied = %+v, want the refusal recorded", res.Denied)
	}
}

func TestWebhookTurnEmptyWithoutDenialStaysIncomplete(t *testing.T) {
	events := make(chan Event, 1)
	events <- Event{Type: "done"}
	close(events)
	turn := WebhookTurn{Prompt: "merge", AbortTimeout: time.Second, VerifyTimeout: 5 * time.Second}
	turn.Client = &fakeWebhookTurnClient{Client: &permissionDenyClient{}, newEvents: func() <-chan Event { return events }}
	_, err := turn.Run(context.Background())
	if !errors.Is(err, ErrWebhookResponseIncomplete) || errors.Is(err, ErrWebhookPermissionDenied) {
		t.Fatalf("err = %v, want only ErrWebhookResponseIncomplete", err)
	}
}

func TestWebhookTurnKeepsFirstDeniedScopes(t *testing.T) {
	events := make(chan Event, 6)
	for i := range 5 {
		events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: fmt.Sprintf("perm-%d", i), Permission: "read", Patterns: []string{fmt.Sprintf("/p%d", i)}}}
	}
	events <- Event{Type: "done"}
	close(events)
	turn := WebhookTurn{Prompt: "x", AbortTimeout: time.Second, VerifyTimeout: 5 * time.Second}
	turn.Client = &fakeWebhookTurnClient{Client: &permissionDenyClient{}, newEvents: func() <-chan Event { return events }}
	res, _ := turn.Run(context.Background())
	if len(res.Denied) != 3 || res.Denied[0] != "read [/p0]" || res.Denied[2] != "read [/p2]" {
		t.Fatalf("denied = %+v, want the first three scopes", res.Denied)
	}
}
