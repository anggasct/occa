package webhook

import (
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func TestBuiltinRendersAcrossTriggerEvents(t *testing.T) {
	cases := []struct {
		workflow  string
		eventType string
		body      string
		triggers  []string
	}{
		{"review", "pull_request", `{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`, nil},
		{"review", "issue_comment", `{"action":"created","repository":{"full_name":"o/r"},"issue":{"number":7,"title":"Fix it","state":"open","pull_request":{"html_url":"https://github.com/o/r/pull/7"}},"comment":{"body":"please re-review","user":{"login":"someone"}}}`, []string{"please re-review"}},
		{"fix", "pull_request_review", `{"action":"submitted","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}},"review":{"state":"changes_requested","body":"**Verdict:** REQUEST_CHANGES","commit_id":"abc1234","html_url":"https://github.com/o/r/pull/7#r1","user":{"login":"reviewer"}}}`, nil},
		{"fix", "issue_comment", `{"action":"created","repository":{"full_name":"o/r"},"issue":{"number":7,"title":"Fix it","state":"open","pull_request":{"html_url":"https://github.com/o/r/pull/7"}},"comment":{"body":"please fix review","user":{"login":"someone"}}}`, []string{"please fix review"}},
		{"merge", "pull_request_review", `{"action":"submitted","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"owner"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}},"review":{"state":"approved","body":"lgtm","commit_id":"abc1234","html_url":"https://github.com/o/r/pull/7#r2","user":{"login":"owner"}}}`, nil},
		{"merge", "check_suite", `{"action":"completed","repository":{"full_name":"o/r"},"check_suite":{"status":"completed","conclusion":"success","app":{"name":"GitHub Actions"},"pull_requests":[{"number":7,"state":"open"}]}}`, nil},
		{"merged", "pull_request", `{"action":"closed","repository":{"full_name":"o/r"},"pull_request":{"number":7,"state":"closed","merged":true,"user":{"login":"owner"},"title":"T","head":{"ref":"feat/x","sha":"abc1234","repo":{"full_name":"o/r"}},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`, nil},
	}
	for _, tc := range cases {
		envelope := normalizeWebhook([]byte(tc.body), tc.eventType, "del-x", testVerdicts(), false, "", tc.triggers)
		injectTaskContext(envelope, ExtractExecutionKey([]byte(tc.body)), NewDispatchStore())
		rendered, err := renderTemplate(config.BuiltinPrompt(tc.workflow), map[string]any{"webhook": envelope})
		if err != nil {
			t.Fatalf("%s/%s without task: %v", tc.workflow, tc.eventType, err)
		}
		if rendered == "" {
			t.Fatalf("%s/%s rendered empty", tc.workflow, tc.eventType)
		}
	}
}

func TestBuiltinRendersWithTaskAcrossEvents(t *testing.T) {
	store := NewDispatchStore()
	store.Create(DispatchRequest{Repo: "o/r", Branch: "feat/x", SpecPath: "https://jira.example.com/browse/ABC-1", Scope: "s", Verify: "v"})
	bodies := []struct {
		eventType string
		body      string
		triggers  []string
	}{
		{"pull_request", `{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`, nil},
		{"issue_comment", `{"action":"created","repository":{"full_name":"o/r"},"issue":{"number":7,"title":"Fix it","state":"open","pull_request":{"html_url":"https://github.com/o/r/pull/7"}},"comment":{"body":"please re-review","user":{"login":"someone"}}}`, []string{"please re-review"}},
		{"check_suite", `{"action":"completed","repository":{"full_name":"o/r"},"check_suite":{"status":"completed","conclusion":"success","app":{"name":"GitHub Actions"},"pull_requests":[{"number":7,"state":"open"}]}}`, nil},
	}
	for _, tc := range bodies {
		envelope := normalizeWebhook([]byte(tc.body), tc.eventType, "del-x", testVerdicts(), false, "", tc.triggers)
		key := ExtractExecutionKey([]byte(tc.body))
		injectTaskContext(envelope, key, store)
		for _, workflow := range []string{"review", "fix", "merge", "merged"} {
			if _, err := renderTemplate(config.BuiltinPrompt(workflow), map[string]any{"webhook": envelope}); err != nil {
				t.Fatalf("%s/%s with task: %v", workflow, tc.eventType, err)
			}
		}
	}
	envelope := normalizeWebhook([]byte(`{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`), "pull_request", "del-x", testVerdicts(), false, "", nil)
	injectTaskContext(envelope, ExtractExecutionKey([]byte(`{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`)), store)
	if envelope["task_spec_is_url"] != true {
		t.Fatalf("pull_request should resolve URL task, got %+v", envelope)
	}
}
