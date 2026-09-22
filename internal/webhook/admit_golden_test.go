package webhook

import (
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func goldenPolicy() admissionPolicy {
	return policyFromConfig(config.WebhookPolicy{
		TrustReviewLogins: []string{"reviewbot"},
		Verdicts: map[string][]string{
			"approved":        {"approved"},
			"request_changes": {"request changes", "request_changes"},
		},
	})
}

func goldenEndpoint(workflow string, triggers []string, rules []config.AdmitRule, limits *config.EndpointLimits) config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "golden-" + workflow,
		Path:           "/golden-" + workflow,
		Secret:         "s",
		Workflow:       workflow,
		Platform:       "telegram",
		ChannelID:      "c1",
		Prompt:         "p",
		CommentTrigger: triggers,
		Admit:          rules,
		Limits:         limits,
	}
}

func goldenReviewerRules() []config.AdmitRule {
	return []config.AdmitRule{
		{Event: "pull_request", Actions: []string{"opened", "reopened", "ready_for_review"}},
		{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
	}
}

func goldenFixRules() []config.AdmitRule {
	return []config.AdmitRule{
		{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"changes_requested"}},
		{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"commented"}, ReviewVerdict: []string{"request_changes"}},
	}
}

func goldenMergeRules() []config.AdmitRule {
	return []config.AdmitRule{
		{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"approved"}},
		{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"commented"}, ReviewVerdict: []string{"approved"}, Require: []string{"approved_without_findings"}},
		{Event: "check_suite", Actions: []string{"completed"}, CheckStatus: "completed", CheckApp: "GitHub Actions", Require: []string{"pr_open", "pr_resolvable"}},
	}
}

func goldenMergedRules() []config.AdmitRule {
	merged := true
	return []config.AdmitRule{
		{Event: "pull_request", Actions: []string{"closed"}, Merged: &merged},
	}
}

func TestGoldenParityWithLegacyMatrices(t *testing.T) {
	policy := goldenPolicy()
	triggers := []string{"please re-review"}
	fixtures := []struct {
		name       string
		workflow   string
		event      string
		triggers   []string
		body       string
		legacyWant bool
		engineWant bool
	}{
		{"reviewer admits opened", "github_reviewer", "pull_request", nil, `{"action":"opened","pull_request":{"number":1}}`, true, true},
		{"reviewer admits reopened", "github_reviewer", "pull_request", nil, `{"action":"reopened","pull_request":{"number":1}}`, true, true},
		{"reviewer admits ready_for_review", "github_reviewer", "pull_request", nil, `{"action":"ready_for_review","pull_request":{"number":1}}`, true, true},
		{"reviewer rejects synchronize", "github_reviewer", "pull_request", nil, `{"action":"synchronize","pull_request":{"number":9,"state":"open"}}`, false, false},
		{"reviewer admits comment trigger", "github_reviewer", "issue_comment", triggers, `{"action":"created","issue":{"number":2,"state":"open","pull_request":{"html_url":"https://example/pull/2"}},"comment":{"body":"please re-review"}}`, true, true},
		{"reviewer rejects review event", "github_reviewer", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, false, false},
		{"fix admits changes_requested", "github_fix", "pull_request_review", nil, `{"action":"submitted","review":{"state":"changes_requested"}}`, true, true},
		{"fix rejects closed pull", "github_fix", "pull_request", nil, `{"action":"closed","pull_request":{"merged":true}}`, false, false},
		{"merge admits approved", "github_merge", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, true, true},
		{"merge rejects comment trigger", "github_merge", "issue_comment", triggers, `{"action":"created","issue":{"number":2,"state":"open","pull_request":{"html_url":"https://example/pull/2"}},"comment":{"body":"please re-review"}}`, false, false},
		{"merged admits closed merged", "github_merged", "pull_request", nil, `{"action":"closed","pull_request":{"merged":true}}`, true, true},
		{"merged rejects review", "github_merged", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, false, false},
		{"check suite admits completed", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","conclusion":"success","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10,"state":"open"}]}}`, true, true},
		{"check suite rejects other app", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","app":{"name":"Travis CI"},"pull_requests":[{"number":10}]}}`, false, false},
		{"check suite rejects requested", "github_merge", "check_suite", nil, `{"action":"requested","check_suite":{"status":"completed","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10}]}}`, false, false},
		{"check suite rejects closed PR", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10,"state":"closed"}]}}`, false, false},
	}
	endpoints := map[string]config.EndpointConfig{
		"github_reviewer": goldenEndpoint("review", triggers, goldenReviewerRules(), &config.EndpointLimits{MaxRunsPerPR: 3, Window: 3600000000000}),
		"github_fix":      goldenEndpoint("fix", nil, goldenFixRules(), nil),
		"github_merge":    goldenEndpoint("merge", nil, goldenMergeRules(), nil),
		"github_merged":   goldenEndpoint("merged", nil, goldenMergedRules(), nil),
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			legacyAllowed, _ := legacyWorkflowAllows(fx.workflow, normalizeWebhook([]byte(fx.body), fx.event, "d", false, "", fx.triggers))
			if legacyAllowed != fx.legacyWant {
				t.Fatalf("legacy workflowAllows = %v, want %v", legacyAllowed, fx.legacyWant)
			}
			env := normalizeWebhook([]byte(fx.body), fx.event, "d", false, "", fx.triggers)
			engineAllowed, reason := admitDelivery(endpoints[fx.workflow], policy, env)
			if engineAllowed != fx.engineWant {
				t.Fatalf("engine admit = %v (%q), want %v", engineAllowed, reason, fx.engineWant)
			}
			if legacyAllowed != engineAllowed {
				t.Fatalf("parity break: legacy=%v engine=%v (%q)", legacyAllowed, engineAllowed, reason)
			}
		})
	}
}

func TestEngineSelfReviewFromConfig(t *testing.T) {
	policy := goldenPolicy()
	fix := goldenEndpoint("fix", nil, goldenFixRules(), nil)
	body := `{"action":"submitted","pull_request":{"user":{"login":"reviewbot"}},"review":{"state":"commented","body":"**Verdict:** REQUEST_CHANGES\n\nActionable findings: 1","user":{"login":"reviewbot"}}}`
	env := normalizeWebhook([]byte(body), "pull_request_review", "d", false, "", nil)
	if allowed, reason := admitDelivery(fix, policy, env); !allowed {
		t.Fatalf("trusted self-review must admit, got %q", reason)
	}
	legacyOther := `{"action":"submitted","pull_request":{"user":{"login":"other"}},"review":{"state":"commented","body":"**Verdict:** REQUEST_CHANGES\n\nActionable findings: 1","user":{"login":"other"}}}`
	legacyAllowed, _ := legacyWorkflowAllows("github_fix", normalizeWebhook([]byte(legacyOther), "pull_request_review", "d", false, "", nil))
	if legacyAllowed {
		t.Fatal("legacy must reject untrusted self-review")
	}
	other := strings.ReplaceAll(body, `"login":"reviewbot"`, `"login":"other"`)
	envOther := normalizeWebhook([]byte(other), "pull_request_review", "d", false, "", nil)
	if allowed, _ := admitDelivery(fix, policy, envOther); allowed {
		t.Fatal("untrusted self-review must not admit")
	}
}

func TestEngineCheckAppFromConfig(t *testing.T) {
	policy := goldenPolicy()
	merge := goldenEndpoint("merge", nil, goldenMergeRules(), nil)
	body := `{"action":"completed","check_suite":{"status":"completed","app":{"name":"Travis CI"},"pull_requests":[{"number":10}]}}`
	env := normalizeWebhook([]byte(body), "check_suite", "d", false, "", nil)
	if allowed, _ := admitDelivery(merge, policy, env); allowed {
		t.Fatal("non-configured app must not admit")
	}
	custom := merge
	custom.Admit[2].CheckApp = "Travis CI"
	envCustom := normalizeWebhook([]byte(body), "check_suite", "d", false, "", nil)
	if allowed, reason := admitDelivery(custom, policy, envCustom); !allowed {
		t.Fatalf("configured app must admit, got %q", reason)
	}
}

func TestVerdictVocabularyFromConfig(t *testing.T) {
	policy := goldenPolicy()
	if got := resolveVerdict("**Verdict:** APPROVED", policy.verdicts); got != "approved" {
		t.Fatalf("verdict = %q, want approved", got)
	}
	if got := resolveVerdict("request changes", policy.verdicts); got != "request_changes" {
		t.Fatalf("verdict = %q, want request_changes", got)
	}
	legacy := reviewVerdict("**Verdict:** APPROVED")
	if legacy != "approved" {
		t.Fatalf("legacy verdict = %q, want approved", legacy)
	}
}
