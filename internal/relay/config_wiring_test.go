package relay

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestConfigWiringReachesConsumers(t *testing.T) {
	cfg := Config{
		DiscoveryTimeout:    7 * time.Second,
		ClientTimeout:       77 * time.Second,
		MaxAttachmentBytes:  12345,
		MaxEventLineBytes:   99999,
		WebhookAbortTimeout: 9 * time.Second,
		VerifyTimeout:       11 * time.Second,
		StallFreshness:      3 * time.Minute,
		NoEventTimeout:      9 * time.Minute,
	}
	c := NewHTTPClient("http://127.0.0.1:1", cfg)
	if c.http.Timeout != 77*time.Second {
		t.Fatalf("client timeout = %v, want 77s", c.http.Timeout)
	}
	if c.maxAttachment != 12345 {
		t.Fatalf("maxAttachment = %d, want 12345", c.maxAttachment)
	}
	if c.maxLineBytes != 99999 {
		t.Fatalf("maxLineBytes = %d, want 99999", c.maxLineBytes)
	}
	if err := c.SendMessage(context.Background(), "s1", "hi", nil, []Attachment{{Filename: "big.bin", Data: make([]byte, 12346)}}); err == nil {
		t.Fatal("expected attachment-too-large for custom limit, got nil")
	}
	ch := make(chan Event, 8)
	big := strings.Repeat("z", 1000)
	go func() {
		_ = readSSE(context.Background(), strings.NewReader("event: message.part.delta\ndata: "+big+"\n\n"), ch, "", cfg.MaxEventLineBytes)
	}()
	if ev := <-ch; ev.Delta != big {
		t.Fatalf("custom line limit dropped 1000-byte line")
	}
	s := NewStreamer(nil, nil, 0, cfg.NoEventTimeout)
	if s.noEventTimeout != 9*time.Minute {
		t.Fatalf("streamer noEvent = %v, want 9m", s.noEventTimeout)
	}
	turn := WebhookTurn{AbortTimeout: cfg.WebhookAbortTimeout, VerifyTimeout: cfg.VerifyTimeout}
	if turn.AbortTimeout != 9*time.Second || turn.VerifyTimeout != 11*time.Second {
		t.Fatalf("webhook turn timeouts = %v/%v, want 9s/11s", turn.AbortTimeout, turn.VerifyTimeout)
	}
	fresh := TurnProgress{PromptSentAt: time.Now().Add(-10 * time.Minute), FirstDeltaAt: time.Now().Add(-9 * time.Minute), LastDeltaAt: time.Now().Add(-10 * time.Second), DeltaCount: 5}
	if got := ClassifyTimeoutFailure(fresh, 30*time.Minute, "m", cfg.StallFreshness); !strings.Contains(got, "long generation") {
		t.Fatalf("custom stall 3m misclassified fresh 10s delta: %q", got)
	}
	stale := TurnProgress{PromptSentAt: time.Now().Add(-10 * time.Minute), FirstDeltaAt: time.Now().Add(-9 * time.Minute), LastDeltaAt: time.Now().Add(-5 * time.Minute), DeltaCount: 5}
	if got := ClassifyTimeoutFailure(stale, 30*time.Minute, "m", cfg.StallFreshness); !strings.Contains(got, "stall mid-turn") {
		t.Fatalf("custom stall 3m misclassified stale 5m delta: %q", got)
	}
}
