package config

import "testing"

func TestRepoDiscoveryDefaultsOnAndRepoCanTurnItOff(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
    - repo: acme/gadget
      provider: github
      platform: discord
      channel_id: chan-2
      secret: s3cret
      checkout: /tmp/gadget
      discovery: false
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := map[string]bool{}
	for _, ep := range cfg.Webhooks.Endpoints {
		got[ep.Name] = ep.Discovery
	}
	if !got["github-widget-review"] {
		t.Fatal("widget review endpoint must default to discovery on")
	}
	if got["github-gadget-review"] {
		t.Fatal("gadget review endpoint must honour discovery: false")
	}
}
