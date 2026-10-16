package router

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

func TestHandleConnectRepo(t *testing.T) {
	r := &Router{commands: make(map[string]Command)}
	if _, ok := r.commands["connect-repo"]; ok {
		t.Fatal("router must register connect-repo in registerDefaults")
	}
	r.registerDefaults()
	cmd, ok := r.commands["connect-repo"]
	if !ok {
		t.Fatal("connect-repo command missing after registerDefaults")
	}
	if got, _ := cmd.Handler(context.Background(), channel.IncomingMessage{}, ""); !strings.Contains(got, "Usage") {
		t.Errorf("empty args = %q, want usage", got)
	}
	if got, _ := cmd.Handler(context.Background(), channel.IncomingMessage{}, "not a repo!!!"); !strings.Contains(got, "Invalid repository") {
		t.Errorf("bad repo = %q, want invalid warning", got)
	}
	got, err := cmd.Handler(context.Background(), channel.IncomingMessage{}, "Acme/Widget")
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	for _, want := range []string{
		"acme/widget",
		"review", "review-fix", "merge", "merged",
		"PAT", "GitHub App", "restart",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("checklist must mention %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "secret-") || strings.Contains(got, "ghp_") {
		t.Errorf("checklist must not embed real secrets:\n%s", got)
	}
}
