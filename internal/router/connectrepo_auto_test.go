package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

func TestHandleConnectRepoAutomation(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	r.registerDefaults()
	var created []string
	var pinged []string
	r.SetConnectRepoDeps(&ConnectRepoDeps{
		PublicBaseURL: "https://relay.example.com",
		CheckAdmin: func(ctx context.Context, repo string) (bool, error) {
			return true, nil
		},
		CreateHook: func(ctx context.Context, repo, hookURL, secret string, events []string) (string, error) {
			created = append(created, hookURL)
			return "hook-id", nil
		},
		AppendEndpoints: func(endpoints []ConnectEndpoint) error {
			if len(endpoints) != 4 {
				return errors.New("want 4 endpoints")
			}
			return nil
		},
		Restart: func(ctx context.Context) error { return nil },
		PingHook: func(ctx context.Context, repo, hookID string) error {
			pinged = append(pinged, hookID)
			if len(pinged) == 3 {
				return errors.New("ping boom")
			}
			return nil
		},
	})
	got, err := r.handleConnectRepo(context.Background(), channel.IncomingMessage{Platform: "discord", ChannelID: "chan-1"}, "acme/widget")
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	for _, want := range []string{"✅ review", "✅ fix", "❌ merge", "✅ merged", "PAT", "GitHub App"} {
		if !strings.Contains(got, want) {
			t.Errorf("automation output must contain %q:\n%s", want, got)
		}
	}
	if len(created) != 4 || len(pinged) != 4 {
		t.Errorf("created=%d pinged=%d, want 4 each", len(created), len(pinged))
	}
}

func TestHandleConnectRepoAdminDenied(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	r.registerDefaults()
	r.SetConnectRepoDeps(&ConnectRepoDeps{
		PublicBaseURL: "https://relay.example.com",
		CheckAdmin: func(ctx context.Context, repo string) (bool, error) {
			return false, nil
		},
		CreateHook: func(ctx context.Context, repo, hookURL, secret string, events []string) (string, error) {
			return "1", nil
		},
		AppendEndpoints: func(endpoints []ConnectEndpoint) error { return nil },
		Restart:         func(ctx context.Context) error { return nil },
		PingHook:        func(ctx context.Context, repo, hookID string) error { return nil },
	})
	got, err := r.handleConnectRepo(context.Background(), channel.IncomingMessage{}, "acme/widget")
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if !strings.Contains(got, "admin access") {
		t.Errorf("denied output must mention admin access:\n%s", got)
	}
}
