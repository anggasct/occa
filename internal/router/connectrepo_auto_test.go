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

func TestHandleConnectRepoCreateFailureSkipsFailedHook(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	r.registerDefaults()
	var appended []ConnectEndpoint
	restarted := false
	r.SetConnectRepoDeps(&ConnectRepoDeps{
		PublicBaseURL: "https://relay.example.com",
		CheckAdmin: func(ctx context.Context, repo string) (bool, error) {
			return true, nil
		},
		CreateHook: func(ctx context.Context, repo, hookURL, secret string, events []string) (string, error) {
			if strings.Contains(hookURL, "pr-fix") {
				return "", errors.New("create boom")
			}
			return "hook-id", nil
		},
		AppendEndpoints: func(endpoints []ConnectEndpoint) error {
			appended = append([]ConnectEndpoint(nil), endpoints...)
			return nil
		},
		Restart: func(ctx context.Context) error { restarted = true; return nil },
		PingHook: func(ctx context.Context, repo, hookID string) error {
			return nil
		},
	})
	got, err := r.handleConnectRepo(context.Background(), channel.IncomingMessage{Platform: "discord", ChannelID: "chan-1"}, "acme/widget")
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if !strings.Contains(got, "❌ fix") || !strings.Contains(got, "create boom") {
		t.Errorf("output must report the failed hook:\n%s", got)
	}
	if len(appended) != 3 {
		t.Fatalf("appended=%d, want 3 (failed hook excluded)", len(appended))
	}
	for _, ep := range appended {
		if ep.Workflow == "fix" || strings.Contains(ep.Path, "pr-fix") {
			t.Errorf("failed hook must not be appended: %+v", ep)
		}
	}
	if !restarted {
		t.Error("restart must still run when some hooks were created and appended")
	}
	if !strings.Contains(got, "✅ Config appended (3 endpoints)") {
		t.Errorf("output must report 3 appended endpoints:\n%s", got)
	}
}

func TestHandleConnectRepoAllCreateFailuresSkipAppendAndRestart(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	r.registerDefaults()
	appendCalls := 0
	restarted := false
	r.SetConnectRepoDeps(&ConnectRepoDeps{
		PublicBaseURL: "https://relay.example.com",
		CheckAdmin: func(ctx context.Context, repo string) (bool, error) {
			return true, nil
		},
		CreateHook: func(ctx context.Context, repo, hookURL, secret string, events []string) (string, error) {
			return "", errors.New("create boom")
		},
		AppendEndpoints: func(endpoints []ConnectEndpoint) error {
			appendCalls++
			return nil
		},
		Restart: func(ctx context.Context) error { restarted = true; return nil },
		PingHook: func(ctx context.Context, repo, hookID string) error {
			return nil
		},
	})
	got, err := r.handleConnectRepo(context.Background(), channel.IncomingMessage{Platform: "discord", ChannelID: "chan-1"}, "acme/widget")
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if appendCalls != 0 {
		t.Errorf("AppendEndpoints must not run when no hook was created (calls=%d)", appendCalls)
	}
	if restarted {
		t.Error("Restart must not run when no hook was created")
	}
	if !strings.Contains(got, "no hooks were created") {
		t.Errorf("output must explain the skipped append:\n%s", got)
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
