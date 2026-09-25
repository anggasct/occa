package webhook

import (
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func testPolicy() config.WebhookPolicy {
	return config.WebhookPolicy{
		TrustReviewLogins: []string{"reviewbot"},
		Verdicts: map[string][]string{
			"approved":        {"approved"},
			"request_changes": {"request changes", "request_changes"},
		},
	}
}

func testVerdicts() map[string][]string {
	return policyFromConfig(testPolicy()).verdicts
}

func testRuntime() config.WebhookRuntime {
	return config.WebhookRuntime{
		MaxBodySize:           config.ByteSize(10 * 1024 * 1024),
		MaxConcurrentEvents:   16,
		MaxQueuedPerKey:       8,
		ProcessingTimeout:     30 * time.Minute,
		ClaimGrace:            32 * time.Minute,
		RetryAfter:            30 * time.Second,
		WorkspaceRetryBackoff: []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second},
		Retention:             720 * time.Hour,
		RetentionKeep:         500,
		PruneInterval:         10 * time.Minute,
		DispatcherIdleTTL:     time.Hour,
		HTTPReadHeaderTimeout: 10 * time.Second,
		HTTPReadTimeout:       30 * time.Second,
		HTTPWriteTimeout:      30 * time.Second,
		HTTPIdleTimeout:       2 * time.Minute,
		ReviewDedupeWindow:    60 * time.Minute,
		IsolatedWorkspaceTTL:  24 * time.Hour,
	}
}

func defaultCatchAllAdmit() []config.AdmitRule {
	return []config.AdmitRule{{Event: "pull_request"}, {Event: "pull_request_review"}, {Event: "issue_comment"}, {Event: "check_suite"}, {Event: "ping"}, {Event: "opened"}}
}

func withAdmitDefaults(endpoints []config.EndpointConfig) []config.EndpointConfig {
	out := make([]config.EndpointConfig, len(endpoints))
	for i, ep := range endpoints {
		if len(ep.Admit) == 0 {
			ep.Admit = defaultCatchAllAdmit()
		}
		out[i] = ep
	}
	return out
}

func TestTestHelpersCompile(t *testing.T) {
	if len(withAdmitDefaults(nil)) != 0 {
		t.Fatal("empty endpoints must stay empty")
	}
}
