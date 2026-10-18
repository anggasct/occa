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
		"PAT", "GitHub App", "restart",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("checklist must mention %q:\n%s", want, got)
		}
	}
	workflowLines := 0
	seenFix := false
	for _, line := range strings.Split(got, "\n") {
		if !strings.HasPrefix(line, "•") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		workflow := fields[1]
		workflowLines++
		if workflow == "review-fix" {
			t.Errorf("workflow column must not use review-fix, got %q", line)
		}
		if workflow == "fix" {
			seenFix = true
		}
		if len(fields) >= 5 && fields[3] == "secret" {
			secret := fields[4]
			if len(secret) != 64 {
				t.Errorf("checklist secret must be directly usable (64 hex chars), got %q", line)
			}
		}
	}
	if workflowLines != 4 {
		t.Errorf("checklist must list 4 workflows, got %d:\n%s", workflowLines, got)
	}
	if !seenFix {
		t.Errorf("checklist must include the fix workflow row:\n%s", got)
	}
	if !strings.Contains(got, "/webhooks/github-acme-widget-pr-fix") {
		t.Errorf("checklist must keep the pr-fix route suffix:\n%s", got)
	}
	if strings.Contains(got, "…") {
		t.Errorf("checklist secrets must be directly usable, got truncation marker:\n%s", got)
	}
	if strings.Contains(got, "secret-") || strings.Contains(got, "ghp_") {
		t.Errorf("checklist must not embed real secrets:\n%s", got)
	}
}
