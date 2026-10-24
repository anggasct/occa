package config

import (
	"strings"
	"testing"
)

func TestRepoCommentAuthors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  defaults:
    comment_authors: ['default-user', 'shared']
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
      comment_authors: ['anggasct', 'AnggaSCT', ' kumasct ']
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := map[string][]string{}
	for _, ep := range cfg.Webhooks.Endpoints {
		got[ep.Name] = ep.CommentAuthors
	}
	if v := strings.Join(got["github-widget-review"], ","); v != "default-user,shared" {
		t.Fatalf("widget review comment_authors = %q, want defaults", v)
	}
	if v := strings.Join(got["github-gadget-pr-fix"], ","); v != "anggasct,kumasct" {
		t.Fatalf("gadget fix comment_authors = %q, want anggasct,kumasct (deduped, trimmed)", v)
	}
	if len(got["github-gadget-merged"]) != 2 {
		t.Fatalf("gadget merged endpoint must carry the allowlist, got %v", got["github-gadget-merged"])
	}
}

func TestDefaultsCommentAuthorsRejectsBlankEntries(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	for name, authors := range map[string]string{
		"blank entry":       "['ok', '  ']",
		"all blank entries": "['  ', '']",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConfig(t, dir, repoConfigYAML(`  defaults:
    comment_authors: `+authors+`
  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
`))
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "webhooks.defaults.comment_authors") {
				t.Fatalf("Load error = %v, want webhooks.defaults.comment_authors validation error", err)
			}
		})
	}
}

func TestRepoCommentAuthorsRejectsEmptyEntry(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      comment_authors: ['ok', '  ']
`))
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "comment_authors") {
		t.Fatalf("Load error = %v, want comment_authors validation error", err)
	}
}
