package router

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/health"
)

type testHealthStore struct{}

func (testHealthStore) Ping(context.Context) error                 { return nil }
func (testHealthStore) SchemaVersion(context.Context) (int, error) { return 8, nil }

type testHealthAgent struct{}

func (testHealthAgent) Running(context.Context) (int, bool, error) { return 99, true, nil }

func TestHealthCommandRepliesWhenConfigured(t *testing.T) {
	r, client, reply := newTestRouter()
	if _, ok := r.commands["health"]; !ok {
		t.Fatal("/health command not registered")
	}
	r.SetHealthReporter(health.New(
		health.WithStore(testHealthStore{}),
		health.WithAgent(testHealthAgent{}),
		health.WithVersion("1.2.3"),
		health.WithExpectedSchema(8),
	))
	if err := r.Route(context.Background(), msg("/health", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %d, want 1", len(reply.sends))
	}
	got := reply.sends[0]
	if !strings.Contains(got, "OCCA healthy") || !strings.Contains(got, "Binary: 1.2.3") || !strings.Contains(got, "DB: connected (schema v8)") {
		t.Errorf("reply missing healthy report: %q", got)
	}
	if client != nil {
		// /health must be model-free: it never resolves an agent instance.
		if provider, ok := r.instances.(*fakeInstanceProvider); ok && provider.calls != 0 {
			t.Errorf("agent instance created by /health (%d calls)", provider.calls)
		}
	}
}

func TestHealthCommandNotConfigured(t *testing.T) {
	r, _, reply := newTestRouter()
	if err := r.Route(context.Background(), msg("/health", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "not configured") {
		t.Errorf("reply = %q, want not-configured message", reply.sends)
	}
}

func TestHealthCommandInMenuAndHelp(t *testing.T) {
	r, _, _ := newTestRouter()
	found := false
	for _, c := range r.MenuCommands() {
		if c.Alias == "health" {
			found = true
		}
	}
	if !found {
		t.Error("/health missing from menu commands")
	}
	if !strings.Contains(r.helpText(), "/health") {
		t.Error("/health missing from help text")
	}
}
