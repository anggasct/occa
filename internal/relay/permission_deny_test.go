package relay

import (
	"context"
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
