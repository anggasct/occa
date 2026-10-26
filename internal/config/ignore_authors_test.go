package config

import (
	"strings"
	"testing"
)

func TestIgnoreAuthorsConfig(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  defaults:
    ignore_authors: ['dependabot[bot]', ' Renovate[bot] ']
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
      channel_id: chan-2
      secret: s3cret
      checkout: /tmp/gadget
      ignore_authors: [' Custom[bot] ', 'Custom[bot]']
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := map[string][]string{}
	for _, ep := range cfg.Webhooks.Endpoints {
		got[ep.Name] = ep.IgnoreAuthors
	}
	if v := strings.Join(got["github-widget-review"], ","); v != "dependabot[bot],Renovate[bot]" {
		t.Fatalf("widget ignore_authors = %q, want defaults", v)
	}
	if v := strings.Join(got["github-gadget-pr-fix"], ","); v != "Custom[bot]" {
		t.Fatalf("gadget ignore_authors = %q, want the repo override", v)
	}
	if len(got["github-gadget-merged"]) != 1 {
		t.Fatalf("gadget merged endpoint must carry the override, got %v", got["github-gadget-merged"])
	}
}

func TestIgnoreAuthorsEmptyConfigStaysEmpty(t *testing.T) {
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
		if len(ep.IgnoreAuthors) != 0 {
			t.Fatalf("%s ignore_authors = %v, want empty", ep.Name, ep.IgnoreAuthors)
		}
	}
}

func TestIgnoreAuthorsValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := map[string]struct {
		yaml string
		want string
	}{
		"defaults blank": {
			yaml: `  defaults:
    ignore_authors: ['']
  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
`,
			want: "webhooks.defaults.ignore_authors",
		},
		"defaults invalid login": {
			yaml: `  defaults:
    ignore_authors: ['bad login!']
  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
`,
			want: "webhooks.defaults.ignore_authors",
		},
		"repo blank": {
			yaml: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      ignore_authors: ['  ']
`,
			want: "ignore_authors",
		},
		"repo invalid login": {
			yaml: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      ignore_authors: ['bad login!']
`,
			want: "ignore_authors",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConfig(t, dir, repoConfigYAML(tc.yaml))
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want %q", err, tc.want)
			}
		})
	}
}
