package config

import (
	"strings"
	"testing"
)

func TestRepoAutoFixDefaultsOff(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
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
		if ep.AutoFix.CI || ep.AutoFix.Conflict {
			t.Fatalf("endpoint %s should default auto_fix off: %+v", ep.Name, ep.AutoFix)
		}
	}
}

func TestRepoAutoFixEnabled(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      auto_fix:
        ci: true
        conflict: true
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, ep := range cfg.Webhooks.Endpoints {
		if !ep.AutoFix.CI || !ep.AutoFix.Conflict {
			t.Fatalf("endpoint %s should carry auto_fix on: %+v", ep.Name, ep.AutoFix)
		}
	}
}

func TestRepoAutoFixValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := []struct {
		name    string
		autoFix string
		want    string
	}{
		{
			name:    "unknown key",
			autoFix: "      auto_fix:\n        turbo: true\n",
			want:    "supports only ci and conflict",
		},
		{
			name:    "non-bool value",
			autoFix: "      auto_fix:\n        ci: sometimes\n",
			want:    "cannot unmarshal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConfig(t, dir, repoConfigYAML("  repos:\n    - repo: acme/widget\n      provider: github\n      platform: discord\n      channel_id: chan-1\n      secret: s3cret\n      checkout: /tmp/widget\n"+tc.autoFix))
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
