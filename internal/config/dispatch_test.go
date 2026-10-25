package config

import (
	"strings"
	"testing"
	"time"
)

func dispatchWebhooks(extra string) string {
	return "webhooks:\n" + fullRuntimeYAML() + fullEndpointYAML(`      workflow: custom
      admit:
        - event: pull_request
          actions: [opened]
`) + extra
}

func TestDispatchEnabledLoads(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	webhooks := dispatchWebhooks(`  dispatch:
    enabled: true
    token: file-token
  status_icons:
    merged: "✅"
  status_colors:
    merged: 10157982
  fallback_poll_after: 5m
`)
	cfg, err := Load(writeFullConfig(t, t.TempDir(), webhooks))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Webhooks.Dispatch.Enabled || cfg.Webhooks.Dispatch.Path != "/dispatch" || cfg.Webhooks.Dispatch.Token != "file-token" {
		t.Errorf("dispatch = %+v", cfg.Webhooks.Dispatch)
	}
	if cfg.Webhooks.StatusIcons["merged"] != "✅" || cfg.Webhooks.StatusColors["merged"] != 10157982 {
		t.Errorf("status config = %v %v", cfg.Webhooks.StatusIcons, cfg.Webhooks.StatusColors)
	}
	if cfg.Webhooks.FallbackPollAfter != 5*time.Minute {
		t.Errorf("fallback = %v", cfg.Webhooks.FallbackPollAfter)
	}
}

func TestDispatchTokenRequired(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	webhooks := dispatchWebhooks(`  dispatch:
    enabled: true
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "dispatch.token") {
		t.Errorf("missing token must fail, got %v", err)
	}
}

func TestDispatchTokenEnvOverride(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "env-token")
	webhooks := dispatchWebhooks(`  dispatch:
    enabled: true
    token: file-token
`)
	cfg, err := Load(writeFullConfig(t, t.TempDir(), webhooks))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.Dispatch.Token != "env-token" {
		t.Errorf("token = %q, want env override", cfg.Webhooks.Dispatch.Token)
	}
}

func TestDispatchPathCollision(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "tok")
	webhooks := dispatchWebhooks(`  dispatch:
    enabled: true
    path: /test
    token: tok
`)
	if _, err := Load(writeFullConfig(t, t.TempDir(), webhooks)); err == nil || !strings.Contains(err.Error(), "duplicates endpoint path") {
		t.Errorf("path collision must fail, got %v", err)
	}
}

func TestStatusConfigValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	for _, extra := range []string{
		"  status_icons:\n    bogus: x\n",
		"  status_colors:\n    merged: 16777216\n",
		"  fallback_poll_after: -1m\n",
	} {
		if _, err := Load(writeFullConfig(t, t.TempDir(), dispatchWebhooks(extra))); err == nil {
			t.Errorf("invalid status config must fail: %q", extra)
		}
	}
}

func TestFallbackPollAfterDefaults(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	cfg, err := Load(writeFullConfig(t, t.TempDir(), dispatchWebhooks("")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.FallbackPollAfter != 10*time.Minute {
		t.Errorf("fallback default = %v, want 10m", cfg.Webhooks.FallbackPollAfter)
	}
}

func TestFallbackPollRepeatAfterDefaults(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	cfg, err := Load(writeFullConfig(t, t.TempDir(), dispatchWebhooks("")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.FallbackPollRepeatAfter == nil || *cfg.Webhooks.FallbackPollRepeatAfter != 30*time.Minute {
		t.Errorf("repeat default = %v, want 30m", cfg.Webhooks.FallbackPollRepeatAfter)
	}
}

func TestFallbackPollRepeatAfterExplicitZeroDisables(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	cfg, err := Load(writeFullConfig(t, t.TempDir(), dispatchWebhooks("  fallback_poll_repeat_after: 0s\n")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.FallbackPollRepeatAfter == nil || *cfg.Webhooks.FallbackPollRepeatAfter != 0 {
		t.Errorf("repeat = %v, want explicit zero", cfg.Webhooks.FallbackPollRepeatAfter)
	}
}

func TestFallbackPollRepeatAfterNegativeRejected(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	_, err := Load(writeFullConfig(t, t.TempDir(), dispatchWebhooks("  fallback_poll_repeat_after: -1m\n")))
	if err == nil || !strings.Contains(err.Error(), "fallback_poll_repeat_after") {
		t.Errorf("negative repeat must fail, got %v", err)
	}
}

func TestStatusConfigKeysNormalized(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	t.Setenv("OCCA_DISPATCH_TOKEN", "")
	webhooks := dispatchWebhooks("  status_icons:\n    Merged: \"\U0001F9EA\"\n  status_colors:\n    Merged: 1193046\n")
	cfg, err := Load(writeFullConfig(t, t.TempDir(), webhooks))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.StatusIcons["merged"] != "🧪" {
		t.Errorf("capitalized icon key must normalize, got %v", cfg.Webhooks.StatusIcons)
	}
	if cfg.Webhooks.StatusColors["merged"] != 1193046 {
		t.Errorf("capitalized color key must normalize, got %v", cfg.Webhooks.StatusColors)
	}
}
