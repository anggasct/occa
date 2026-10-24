package webhook

import (
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func minimalEnvelope() WebhookEnvelope {
	return WebhookEnvelope{
		"event_type":      "pull_request",
		"delivery_id":     "del-1",
		"repository":      "acme/widget",
		"pr_number":       "7",
		"pr_url":          "https://github.com/acme/widget/pull/7",
		"title":           "t",
		"head_branch":     "feat/x",
		"base_branch":     "main",
		"review_verdict":  "approved",
		"comment_trigger": "",
		"comment_body":    "",
	}
}

func TestInjectTaskContextWithPathTask(t *testing.T) {
	store := NewDispatchStore()
	task := store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", SpecPath: "specs/feat-x.md", Scope: "ship it", Verify: "go test ./..."})
	envelope := minimalEnvelope()
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}
	injectTaskContext(envelope, key, store)
	if envelope["task_id"] != task.ID || envelope["task_spec"] != "specs/feat-x.md" || envelope["task_scope"] != "ship it" || envelope["task_verify"] != "go test ./..." {
		t.Fatalf("task keys wrong: %+v", envelope)
	}
	if envelope["task_spec_is_url"] != false {
		t.Fatalf("task_spec_is_url should be false for path")
	}
}

func TestInjectTaskContextWithURLTask(t *testing.T) {
	store := NewDispatchStore()
	store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", SpecPath: "https://jira.example.com/browse/ABC-1"})
	envelope := minimalEnvelope()
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}
	injectTaskContext(envelope, key, store)
	if envelope["task_spec_is_url"] != true || envelope["task_spec"] != "https://jira.example.com/browse/ABC-1" {
		t.Fatalf("URL task keys wrong: %+v", envelope)
	}
}

func TestInjectTaskContextWithoutTask(t *testing.T) {
	envelope := minimalEnvelope()
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}
	injectTaskContext(envelope, key, NewDispatchStore())
	if envelope["task_id"] != "" || envelope["task_brief"] != "" || envelope["task_spec"] != "" || envelope["task_spec_is_url"] != false || envelope["task_scope"] != "" || envelope["task_verify"] != "" {
		t.Fatalf("empty task should set empty keys: %+v", envelope)
	}
	rendered, err := renderTemplate("static", map[string]any{"webhook": envelope})
	if err != nil || rendered != "static" {
		t.Fatalf("static render = %q, %v", rendered, err)
	}
	oldPrompt := "PR {{.webhook.pr_number}} in {{.webhook.repository}}"
	before, err := renderTemplate(oldPrompt, map[string]any{"webhook": minimalEnvelope()})
	if err != nil {
		t.Fatalf("render before: %v", err)
	}
	after, err := renderTemplate(oldPrompt, map[string]any{"webhook": envelope})
	if err != nil {
		t.Fatalf("render after: %v", err)
	}
	if before != after {
		t.Fatalf("old prompt should render identically: %q vs %q", before, after)
	}
}

func TestBuiltinPromptsRenderOnMinimalEnvelope(t *testing.T) {
	for _, workflow := range []string{"review", "fix", "merge", "merged"} {
		prompt := config.BuiltinPrompt(workflow)
		if strings.TrimSpace(prompt) == "" {
			t.Fatalf("builtin %s is empty", workflow)
		}
		envelope := minimalEnvelope()
		envelope["task_id"] = ""
		envelope["task_brief"] = ""
		envelope["task_spec"] = ""
		envelope["task_spec_is_url"] = false
		envelope["task_scope"] = ""
		envelope["task_verify"] = ""
		if _, err := renderTemplate(prompt, map[string]any{"webhook": envelope}); err != nil {
			t.Fatalf("builtin %s render: %v", workflow, err)
		}
	}
}

func TestBuiltinMergedRendersTaskContext(t *testing.T) {
	store := NewDispatchStore()
	store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", SpecPath: "specs/feat-x.md", Scope: "ship it"})
	envelope := minimalEnvelope()
	injectTaskContext(envelope, WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}, store)
	rendered, err := renderTemplate(config.BuiltinPrompt("merged"), map[string]any{"webhook": envelope})
	if err != nil {
		t.Fatalf("render merged: %v", err)
	}
	if !strings.Contains(rendered, "specs/feat-x.md") || !strings.Contains(rendered, "ship it") {
		t.Fatalf("merged prompt should carry task context: %q", rendered)
	}
}
