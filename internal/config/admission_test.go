package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFullConfig(t *testing.T, dir, webhooks string) string {
	t.Helper()
	content := `discord:
  allowed_sender_ids:
    - '123'
` + webhooks
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func fullRuntimeYAML() string {
	return `  policy:
    trust_review_logins: []
    verdicts:
      approved: ['approved']
      request_changes: ['request changes', 'request_changes']
  runtime:
    max_body_size: 10MB
    max_concurrent_events: 16
    max_queued_per_key: 8
    processing_timeout: 30m
    claim_grace: 32m
    retry_after: 30s
    workspace_retry_backoff: [30s, 60s, 120s]
    retention: 720h
    retention_keep: 500
    prune_interval: 10m
    dispatcher_idle_ttl: 1h
    http_read_header_timeout: 10s
    http_read_timeout: 30s
    http_write_timeout: 30s
    http_idle_timeout: 2m
    review_dedupe_window: 60m
    isolated_workspace_ttl: 24h
    usage_retention: 2160h
    usage_max_rows: 100000
    recovery_event_retention: 720h
`
}

func fullEndpointYAML(extra string) string {
	return `  endpoints:
    - name: test
      path: /test
      secret: s
      platform: telegram
      channel_id: c1
      prompt: p
      workspace:
        type: none
` + extra
}

func TestParseByteSize(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{"10MB", 10 * 1024 * 1024},
		{"10mb", 10 * 1024 * 1024},
		{"10 MB", 10 * 1024 * 1024},
		{"512KB", 512 * 1024},
		{"100", 100},
		{"100b", 100},
		{"2GB", 2 * 1024 * 1024 * 1024},
	}
	for _, tc := range cases {
		got, err := ParseByteSize(tc.raw)
		if err != nil {
			t.Fatalf("ParseByteSize(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("ParseByteSize(%q) = %d, want %d", tc.raw, got, tc.want)
		}
	}
	for _, raw := range []string{"", "0", "-5MB", "abc", "10XB"} {
		if _, err := ParseByteSize(raw); err == nil {
			t.Fatalf("ParseByteSize(%q) must fail", raw)
		}
	}
}

func TestFullConfigLoads(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      workflow: custom
      admit:
        - event: pull_request
          actions: [opened]
      limits:
        max_runs_per_pr: 3
        window: 60m
`)
	cfg, err := Load(writeFullConfig(t, t.TempDir(), webhooks))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rt := cfg.Webhooks.Runtime
	if int64(rt.MaxBodySize) != 10*1024*1024 || rt.MaxConcurrentEvents != 16 || rt.MaxQueuedPerKey != 8 {
		t.Fatalf("runtime head = %+v", rt)
	}
	if rt.ProcessingTimeout != 30*time.Minute || rt.ClaimGrace != 32*time.Minute || rt.RetryAfter != 30*time.Second {
		t.Fatalf("runtime timeouts = %+v", rt)
	}
	if len(rt.WorkspaceRetryBackoff) != 3 || rt.WorkspaceRetryBackoff[0] != 30*time.Second {
		t.Fatalf("runtime backoff = %+v", rt.WorkspaceRetryBackoff)
	}
	ep := cfg.Webhooks.Endpoints[0]
	if len(ep.Admit) != 1 || ep.Admit[0].Event != "pull_request" {
		t.Fatalf("admit = %+v", ep.Admit)
	}
	if ep.Limits == nil || ep.Limits.MaxRunsPerPR != 3 || ep.Limits.Window != time.Hour {
		t.Fatalf("limits = %+v", ep.Limits)
	}
}

func TestMissingRuntimeKeyFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n  policy:\n    trust_review_logins: []\n    verdicts:\n      approved: ['approved']\n      request_changes: ['request_changes']\n" + fullEndpointYAML(`      admit:
        - event: pull_request
          actions: [opened]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "webhooks.runtime") {
		t.Fatalf("Load error = %v, want webhooks.runtime failure", err)
	}
}

func TestMissingAdmitFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML("")
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "admit is required") {
		t.Fatalf("Load error = %v, want admit failure", err)
	}
}

func TestCommentTriggerWithoutLimitsFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      comment_trigger: ['please re-review']
      admit:
        - event: issue_comment
          actions: [created]
          require: [comment_trigger, pr_open]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "limits is required") {
		t.Fatalf("Load error = %v, want limits failure", err)
	}
}

func TestSkipEventsRejected(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      skip_events: [ping]
      admit:
        - event: pull_request
          actions: [opened]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "skip_events is removed") {
		t.Fatalf("Load error = %v, want skip_events failure", err)
	}
}

func TestUnknownRequireFlagFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      admit:
        - event: pull_request
          actions: [opened]
          require: [phase_of_moon]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "unknown require flag") {
		t.Fatalf("Load error = %v, want require flag failure", err)
	}
}

func TestUnknownFieldFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	webhooks := "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      admit:
        - event: pull_request
          actions: [opened]
          phases: [waxing]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil {
		t.Fatalf("Load must fail on unknown admit field")
	}
}

func TestUnknownPolicyKeyFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	base := fullRuntimeYAML()
	withBogus := strings.Replace(base, "  runtime:", "    bogus_policy_key: 1\n  runtime:", 1)
	webhooks := "webhooks:\n" + withBogus + fullEndpointYAML(`      admit:
        - event: pull_request
          actions: [opened]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "bogus_policy_key") {
		t.Fatalf("Load error = %v, want bogus_policy_key failure", err)
	}
}

func TestUnknownRuntimeKeyFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	withBogus := fullRuntimeYAML() + "    bogus_runtime_key: 1\n"
	webhooks := "webhooks:\n" + withBogus + fullEndpointYAML(`      admit:
        - event: pull_request
          actions: [opened]
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "bogus_runtime_key") {
		t.Fatalf("Load error = %v, want bogus_runtime_key failure", err)
	}
}
