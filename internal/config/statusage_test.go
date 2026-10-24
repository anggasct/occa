package config

import (
	"testing"
	"time"
)

func TestRepoRefreshDefaultsAndOverrides(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	content := "webhooks:\n" + fullRuntimeYAML() + `  defaults:
    refresh_interval: 15s
    idle_after: 2m
  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
    - repo: acme/gadget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/gadget
      refresh_interval: 5s
      idle_after: 30s
`
	path := writeConfig(t, t.TempDir(), content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := map[string][2]time.Duration{
		"github-widget-review": {15 * time.Second, 2 * time.Minute},
		"github-gadget-review": {5 * time.Second, 30 * time.Second},
	}
	for _, ep := range cfg.Webhooks.Endpoints {
		expected, ok := want[ep.Name]
		if !ok {
			continue
		}
		if ep.RefreshInterval != expected[0] || ep.IdleAfter != expected[1] {
			t.Fatalf("%s refresh/idle = %v/%v, want %v/%v", ep.Name, ep.RefreshInterval, ep.IdleAfter, expected[0], expected[1])
		}
	}
}

func TestRepoRefreshDefaultsFallBack(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, ep := range cfg.Webhooks.Endpoints {
		if ep.RefreshInterval != DefaultRefreshInterval || ep.IdleAfter != DefaultIdleAfter {
			t.Fatalf("%s refresh/idle = %v/%v, want the defaults", ep.Name, ep.RefreshInterval, ep.IdleAfter)
		}
	}
}
