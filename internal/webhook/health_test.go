package webhook

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func TestWebhookHealthyAfterStart(t *testing.T) {
	srv, _ := newTestServer(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	if srv.Healthy() {
		t.Fatal("Healthy() = true before Start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Stop(ctx) }()

	if !srv.Healthy() {
		t.Fatal("Healthy() = false after Start")
	}
	if srv.Addr() == "" {
		t.Fatal("Addr() empty after Start")
	}
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if srv.Healthy() {
		t.Fatal("Healthy() = true after Stop")
	}
}
