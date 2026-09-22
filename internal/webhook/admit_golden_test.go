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
		name         string
		workflow     string
		event        string
		triggers     []string
		body         string
		legacyWant   bool
		engineWant   bool
		legacyReason string
		engineReason string
	}{
		{"reviewer admits opened", "github_reviewer", "pull_request", nil, `{"action":"opened","pull_request":{"number":1}}`, true, true, "", ""},
		{"reviewer admits reopened", "github_reviewer", "pull_request", nil, `{"action":"reopened","pull_request":{"number":1}}`, true, true, "", ""},
		{"reviewer admits ready_for_review", "github_reviewer", "pull_request", nil, `{"action":"ready_for_review","pull_request":{"number":1}}`, true, true, "", ""},
		{"reviewer rejects synchronize", "github_reviewer", "pull_request", nil, `{"action":"synchronize","pull_request":{"number":9,"state":"open"}}`, false, false, "skipped: pull_request synchronize (push while PR open spawns no execution)", "endpoint golden-review admitted no rule for pull_request.synchronize"},
		{"reviewer admits comment trigger", "github_reviewer", "issue_comment", triggers, `{"action":"created","issue":{"number":2,"state":"open","pull_request":{"html_url":"https://example/pull/2"}},"comment":{"body":"please re-review"}}`, true, true, "", ""},
		{"reviewer rejects review event", "github_reviewer", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, false, false, "workflow github_reviewer rejected pull_request_review.submitted", "endpoint golden-review admitted no rule for pull_request_review.submitted"},
		{"fix admits changes_requested", "github_fix", "pull_request_review", nil, `{"action":"submitted","review":{"state":"changes_requested"}}`, true, true, "", ""},
		{"fix rejects closed pull", "github_fix", "pull_request", nil, `{"action":"closed","pull_request":{"merged":true}}`, false, false, "workflow github_fix rejected pull_request.closed", "endpoint golden-fix admitted no rule for pull_request.closed"},
		{"merge admits approved", "github_merge", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, true, true, "", ""},
		{"merge rejects comment trigger", "github_merge", "issue_comment", triggers, `{"action":"created","issue":{"number":2,"state":"open","pull_request":{"html_url":"https://example/pull/2"}},"comment":{"body":"please re-review"}}`, false, false, "workflow github_merge rejected issue_comment.created", "endpoint golden-merge admitted no rule for issue_comment.created"},
		{"merged admits closed merged", "github_merged", "pull_request", nil, `{"action":"closed","pull_request":{"merged":true}}`, true, true, "", ""},
		{"merged rejects review", "github_merged", "pull_request_review", nil, `{"action":"submitted","review":{"state":"approved"}}`, false, false, "workflow github_merged rejected pull_request_review.submitted", "endpoint golden-merged admitted no rule for pull_request_review.submitted"},
		{"check suite admits completed", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","conclusion":"success","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10,"state":"open"}]}}`, true, true, "", ""},
		{"check suite rejects other app", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","app":{"name":"Travis CI"},"pull_requests":[{"number":10}]}}`, false, false, "skipped: check_suite app \"Travis CI\" is not GitHub Actions", "endpoint golden-merge admitted no rule for check_suite.completed"},
		{"check suite rejects requested", "github_merge", "check_suite", nil, `{"action":"requested","check_suite":{"status":"completed","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10}]}}`, false, false, "skipped: check_suite status is requested.completed (only completed is admitted)", "endpoint golden-merge admitted no rule for check_suite.requested"},
		{"check suite rejects closed PR", "github_merge", "check_suite", nil, `{"action":"completed","check_suite":{"status":"completed","app":{"name":"GitHub Actions"},"pull_requests":[{"number":10,"state":"closed"}]}}`, false, false, "skipped: PR #10 is closed; no merge execution", "endpoint golden-merge admitted no rule for check_suite.completed"},
	}
	endpoints := map[string]config.EndpointConfig{
		"github_reviewer": goldenEndpoint("review", triggers, goldenReviewerRules(), &config.EndpointLimits{MaxRunsPerPR: 3, Window: 3600000000000}),
		"github_fix":      goldenEndpoint("fix", nil, goldenFixRules(), nil),
		"github_merge":    goldenEndpoint("merge", nil, goldenMergeRules(), nil),
		"github_merged":   goldenEndpoint("merged", nil, goldenMergedRules(), nil),
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			legacyAllowed, legacyReason := legacyWorkflowAllows(fx.workflow, normalizeWebhook([]byte(fx.body), fx.event, "d", policy.verdicts, false, "", fx.triggers))
			if legacyAllowed != fx.legacyWant {
				t.Fatalf("legacy workflowAllows = %v, want %v", legacyAllowed, fx.legacyWant)
			}
			if legacyReason != fx.legacyReason {
				t.Fatalf("legacy reason = %q, want %q", legacyReason, fx.legacyReason)
			}
			env := normalizeWebhook([]byte(fx.body), fx.event, "d", policy.verdicts, false, "", fx.triggers)
			engineAllowed, reason := admitDelivery(endpoints[fx.workflow], policy, env)
			if engineAllowed != fx.engineWant {
				t.Fatalf("engine admit = %v (%q), want %v", engineAllowed, reason, fx.engineWant)
			}
			if reason != fx.engineReason {
				t.Fatalf("engine reason = %q, want %q", reason, fx.engineReason)
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
	env := normalizeWebhook([]byte(body), "pull_request_review", "d", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(fix, policy, env); !allowed {
		t.Fatalf("trusted self-review must admit, got %q", reason)
	}
	legacyOther := `{"action":"submitted","pull_request":{"user":{"login":"other"}},"review":{"state":"commented","body":"**Verdict:** REQUEST_CHANGES\n\nActionable findings: 1","user":{"login":"other"}}}`
	legacyAllowed, _ := legacyWorkflowAllows("github_fix", normalizeWebhook([]byte(legacyOther), "pull_request_review", "d", policy.verdicts, false, "", nil))
	if legacyAllowed {
		t.Fatal("legacy must reject untrusted self-review")
	}
	other := strings.ReplaceAll(body, `"login":"reviewbot"`, `"login":"other"`)
	envOther := normalizeWebhook([]byte(other), "pull_request_review", "d", policy.verdicts, false, "", nil)
	if allowed, _ := admitDelivery(fix, policy, envOther); allowed {
		t.Fatal("untrusted self-review must not admit")
	}
}

func TestEngineCheckAppFromConfig(t *testing.T) {
	policy := goldenPolicy()
	merge := goldenEndpoint("merge", nil, goldenMergeRules(), nil)
	body := `{"action":"completed","check_suite":{"status":"completed","app":{"name":"Travis CI"},"pull_requests":[{"number":10}]}}`
	env := normalizeWebhook([]byte(body), "check_suite", "d", policy.verdicts, false, "", nil)
	if allowed, _ := admitDelivery(merge, policy, env); allowed {
		t.Fatal("non-configured app must not admit")
	}
	custom := merge
	custom.Admit[2].CheckApp = "Travis CI"
	envCustom := normalizeWebhook([]byte(body), "check_suite", "d", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(custom, policy, envCustom); !allowed {
		t.Fatalf("configured app must admit, got %q", reason)
	}
}

func TestEngineCheckAppFallsThroughToNextRule(t *testing.T) {
	policy := goldenPolicy()
	ep := goldenEndpoint("merge", nil, []config.AdmitRule{
		{Event: "check_suite", Actions: []string{"completed"}, CheckStatus: "completed", CheckApp: "GitHub Actions"},
		{Event: "check_suite", Actions: []string{"completed"}, CheckStatus: "completed", CheckApp: "Travis CI"},
	}, nil)
	body := `{"action":"completed","check_suite":{"status":"completed","app":{"name":"Travis CI"},"pull_requests":[{"number":10}]}}`
	env := normalizeWebhook([]byte(body), "check_suite", "d", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, env); !allowed {
		t.Fatalf("second check_app rule must admit, got %q", reason)
	}
}

func TestEngineUnlessFallsThroughToNextRule(t *testing.T) {
	policy := goldenPolicy()
	ep := goldenEndpoint("review", nil, []config.AdmitRule{
		{Event: "pull_request", Actions: []string{"opened", "synchronize"}, Unless: []string{"synchronize"}},
		{Event: "pull_request", Actions: []string{"synchronize"}},
	}, nil)
	body := `{"action":"synchronize","pull_request":{"number":9}}`
	env := normalizeWebhook([]byte(body), "pull_request", "d", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, env); !allowed {
		t.Fatalf("second rule after unless must admit, got %q", reason)
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
	body := `{"action":"submitted","review":{"state":"commented","body":"**Verdict:** APPROVED","user":{"login":"reviewbot"}}}`
	env := normalizeWebhook([]byte(body), "pull_request_review", "d", policy.verdicts, false, "", nil)
	if got := stringValue(env["review_verdict"]); got != "approved" {
		t.Fatalf("normalized verdict = %q, want approved", got)
	}
}

func TestVerdictCustomPhraseFromConfig(t *testing.T) {
	custom := map[string][]string{
		"approved":        {"looks good to me"},
		"request_changes": {"please revise"},
	}
	body := `{"action":"submitted","review":{"state":"commented","body":"**Verdict:** looks good to me","user":{"login":"reviewbot"}}}`
	env := normalizeWebhook([]byte(body), "pull_request_review", "d", custom, false, "", nil)
	if got := stringValue(env["review_verdict"]); got != "approved" {
		t.Fatalf("custom normalized verdict = %q, want approved", got)
	}
	legacyBody := `{"action":"submitted","review":{"state":"commented","body":"**Verdict:** APPROVED","user":{"login":"reviewbot"}}}`
	legacyEnv := normalizeWebhook([]byte(legacyBody), "pull_request_review", "d", custom, false, "", nil)
	if got := stringValue(legacyEnv["review_verdict"]); got != "" {
		t.Fatalf("default phrase under custom vocabulary = %q, want empty", got)
	}
}
