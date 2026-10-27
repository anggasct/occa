package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeRepoPromptFiles(t *testing.T, dir string) {
	t.Helper()
	names := []string{"github-review.md", "github-pr-fix.md", "github-merge.md", "github-merged.md"}
	if err := os.MkdirAll(filepath.Join(dir, "webhooks"), 0o755); err != nil {
		t.Fatalf("mkdir prompt directory: %v", err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, "webhooks", name), []byte("prompt "+name), 0o644); err != nil {
			t.Fatalf("write prompt file: %v", err)
		}
	}
}

func repoConfigYAML(repos string) string {
	return "webhooks:\n" + fullRuntimeYAML() + repos
}

func TestReposExpandLockedEndpoints(t *testing.T) {
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
	if len(cfg.Webhooks.Endpoints) != 4 {
		t.Fatalf("endpoints = %d, want 4", len(cfg.Webhooks.Endpoints))
	}
	byWorkflow := map[string]EndpointConfig{}
	for _, ep := range cfg.Webhooks.Endpoints {
		byWorkflow[ep.Workflow] = ep
	}
	for _, workflow := range []string{"review", "fix", "merge", "merged"} {
		ep, ok := byWorkflow[workflow]
		if !ok {
			t.Fatalf("missing expanded endpoint for workflow %q", workflow)
		}
		if ep.Auth != "github_hmac_sha256" {
			t.Fatalf("%s auth = %q", workflow, ep.Auth)
		}
		if !ep.Thread {
			t.Fatalf("%s thread = false, want true", workflow)
		}
		if ep.NotifySkip == nil || *ep.NotifySkip {
			t.Fatalf("%s notify_skip not locked false", workflow)
		}
		if ep.Repository != "github.com/acme/widget" {
			t.Fatalf("%s repository = %q", workflow, ep.Repository)
		}
		if ep.Secret != "s3cret" || ep.Platform != "discord" || ep.ChannelID != "chan-1" {
			t.Fatalf("%s identity fields wrong: %+v", workflow, ep)
		}
		if ep.Automation == nil || ep.Automation.Default != AutomationReviewOnly {
			t.Fatalf("%s automation default = %+v, want review-only", workflow, ep.Automation)
		}
		if ep.Limits == nil || ep.Limits.MaxRunsPerPR != 3 || ep.Limits.Window != 60*time.Minute {
			t.Fatalf("%s limits = %+v, want 3/60m", workflow, ep.Limits)
		}
	}
	if got := byWorkflow["review"].Name; got != "github-widget-review" {
		t.Fatalf("review name = %q", got)
	}
	if got := byWorkflow["fix"].Name; got != "github-widget-pr-fix" {
		t.Fatalf("fix name = %q", got)
	}
	if got := byWorkflow["fix"].Path; got != "/webhooks/github-widget-pr-fix" {
		t.Fatalf("fix path = %q", got)
	}
	if byWorkflow["fix"].Prompt != BuiltinPrompt("fix") {
		t.Fatalf("fix prompt should fall back to builtin: %q", byWorkflow["fix"].Prompt)
	}
	if byWorkflow["review"].Prompt != BuiltinPrompt("review") {
		t.Fatalf("review prompt should fall back to builtin: %q", byWorkflow["review"].Prompt)
	}
	if byWorkflow["fix"].PromptSource != "builtin" || byWorkflow["review"].PromptSource != "builtin" {
		t.Fatalf("prompt source should be builtin: fix=%q review=%q", byWorkflow["fix"].PromptSource, byWorkflow["review"].PromptSource)
	}
	if ws := byWorkflow["fix"].Workspace; ws.Type != WorkspaceTypeGit || ws.Path != "/tmp/widget" || ws.Mode != WorkspaceModeMutable {
		t.Fatalf("fix workspace = %+v, want git mutable /tmp/widget", ws)
	}
	if ws := byWorkflow["review"].Workspace; ws.Type != WorkspaceTypeGit || ws.Path != "/tmp/widget" || ws.Mode != WorkspaceModeIsolated {
		t.Fatalf("review workspace = %+v, want git isolated /tmp/widget", ws)
	}
	for _, workflow := range []string{"merge", "merged"} {
		if ws := byWorkflow[workflow].Workspace; ws.Type != WorkspaceTypeNone {
			t.Fatalf("%s workspace = %+v, want none", workflow, ws)
		}
	}
	if triggers := byWorkflow["review"].CommentTrigger; len(triggers) != 1 || triggers[0] != "please re-review" {
		t.Fatalf("review triggers = %v", triggers)
	}
	if triggers := byWorkflow["fix"].CommentTrigger; len(triggers) != 2 || triggers[0] != "please fix review" || triggers[1] != "please fix ci" {
		t.Fatalf("fix triggers = %v", triggers)
	}
	if len(byWorkflow["review"].Admit) != 2 || len(byWorkflow["fix"].Admit) != 3 || len(byWorkflow["merge"].Admit) != 3 || len(byWorkflow["merged"].Admit) != 1 {
		t.Fatalf("admit rule counts wrong: review=%d fix=%d merge=%d merged=%d",
			len(byWorkflow["review"].Admit), len(byWorkflow["fix"].Admit), len(byWorkflow["merge"].Admit), len(byWorkflow["merged"].Admit))
	}
	merged := byWorkflow["merged"].Admit[0]
	if merged.Event != "pull_request" || merged.Merged == nil || !*merged.Merged {
		t.Fatalf("merged admit = %+v", merged)
	}
}

func TestReposDefaultsAndOverrides(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	writeRepoPromptFiles(t, dir)
	path := writeConfig(t, dir, repoConfigYAML(`  defaults:
    comment_trigger:
      review: ["tolong review"]
    limits:
      max_runs_per_pr: 5
      window: 30m
    escalation_mention: operator
  repos:
    - repo: acme/widget
      provider: github
      platform: telegram
      channel_id: chat-1
      secret: s3cret
      checkout: /tmp/widget
      comment_trigger:
        fix: ["fix it"]
      model: openai/gpt-4o
      automation:
        default: full
        authors:
          intern: review-only
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	byWorkflow := map[string]EndpointConfig{}
	for _, ep := range cfg.Webhooks.Endpoints {
		byWorkflow[ep.Workflow] = ep
	}
	if triggers := byWorkflow["review"].CommentTrigger; len(triggers) != 1 || triggers[0] != "tolong review" {
		t.Fatalf("review triggers should come from defaults: %v", triggers)
	}
	if triggers := byWorkflow["fix"].CommentTrigger; len(triggers) != 3 || triggers[0] != "please fix review" || triggers[1] != "please fix ci" || triggers[2] != "fix it" {
		t.Fatalf("fix triggers should merge the repo override with the defaults: %v", triggers)
	}
	if limits := byWorkflow["merge"].Limits; limits.MaxRunsPerPR != 5 || limits.Window != 30*time.Minute {
		t.Fatalf("limits should come from defaults: %+v", limits)
	}
	if got := cfg.Webhooks.Defaults.EscalationMention; got != "operator" {
		t.Fatalf("escalation_mention = %q, want operator", got)
	}
	if byWorkflow["review"].Model != "openai/gpt-4o" {
		t.Fatalf("model = %q", byWorkflow["review"].Model)
	}
	automation := byWorkflow["fix"].Automation
	if automation == nil || automation.Default != AutomationFull || automation.Authors["intern"] != AutomationReviewOnly {
		t.Fatalf("automation = %+v", automation)
	}
}

func TestReposValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := []struct {
		name  string
		repos string
		want  string
	}{
		{
			name: "missing provider",
			repos: `  repos:
    - repo: acme/widget
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
`,
			want: "provider",
		},
		{
			name: "unsupported provider",
			repos: `  repos:
    - repo: acme/widget
      provider: gitlab
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
`,
			want: "provider",
		},
		{
			name: "missing secret",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      checkout: /tmp/w
`,
			want: "secret",
		},
		{
			name: "bad repo identity",
			repos: `  repos:
    - repo: widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
`,
			want: "owner/name",
		},
		{
			name: "duplicate repo",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
    - repo: acme/widget
      provider: github
      platform: telegram
      channel_id: c2
      secret: s2
      checkout: /tmp/w2
`,
			want: "duplicates",
		},
		{
			name: "bad trigger workflow",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
      comment_trigger:
        merge: ["go"]
`,
			want: "comment_trigger",
		},
		{
			name: "bad automation mode",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
      automation:
        default: sometimes
`,
			want: "automation.default",
		},
		{
			name: "bad author mode",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
      automation:
        authors:
          bob: maybe
`,
			want: "automation.authors",
		},
		{
			name: "merged run empty",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
      on_merged:
        - run: ""
`,
			want: "on_merged",
		},
		{
			name: "merged prompt and prompt_file",
			repos: `  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: c
      secret: s
      checkout: /tmp/w
      on_merged:
        - prompt: "hi"
          prompt_file: "x.md"
`,
			want: "only one",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRepoPromptFiles(t, dir)
			path := writeConfig(t, dir, repoConfigYAML(tc.repos))
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load err = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestReposMergedActionValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	writeRepoPromptFiles(t, dir)
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      on_merged:
        - prompt_file: "merged-note.md"
        - run: "make changelog"
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, ep := range cfg.Webhooks.Endpoints {
		if ep.Workflow == "merged" {
			if len(ep.OnMerged) != 2 || ep.OnMerged[0].PromptFile != "merged-note.md" || ep.OnMerged[1].Run != "make changelog" {
				t.Fatalf("merged on_merged = %+v", ep.OnMerged)
			}
			return
		}
	}
	t.Fatal("no merged endpoint expanded")
}

func TestReposRejectLockedWorkflowInEndpoints(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  endpoints:
    - name: legacy
      path: /legacy
      workflow: review
      secret: s
      platform: telegram
      channel_id: c1
      prompt: p
      workspace:
        type: none
      admit:
        - event: ping
`))
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "webhooks.repos") {
		t.Fatalf("Load err = %v, want repos migration message", err)
	}
}

func TestReposCustomEndpointsStillLoad(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	writeRepoPromptFiles(t, dir)
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
  endpoints:
    - name: sentry
      path: /sentry
      workflow: custom
      secret: s2
      platform: telegram
      channel_id: chat-9
      prompt: alert
      workspace:
        type: none
      admit:
        - event: ping
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Webhooks.Endpoints) != 5 {
		t.Fatalf("endpoints = %d, want 4 expanded + 1 custom", len(cfg.Webhooks.Endpoints))
	}
	if cfg.Webhooks.Endpoints[0].Automation != nil {
		t.Fatalf("custom endpoint must not carry automation: %+v", cfg.Webhooks.Endpoints[0].Automation)
	}
}

func forumEndpoints(t *testing.T, yaml string) []EndpointConfig {
	t.Helper()
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	cfg, err := Load(writeConfig(t, dir, yaml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg.Webhooks.Endpoints
}

func TestRepoForumChannelWinsOverChannelID(t *testing.T) {
	endpoints := forumEndpoints(t, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      forum_channel_id: forum-repo
      secret: s3cret
      checkout: /tmp/widget
`))
	for _, ep := range endpoints {
		if ep.ChannelID != "forum-repo" {
			t.Fatalf("%s channel = %q, want forum-repo", ep.Workflow, ep.ChannelID)
		}
	}
}

func TestDefaultForumChannelReplacesPerRepoChannel(t *testing.T) {
	endpoints := forumEndpoints(t, "webhooks:\n  defaults:\n    forum_channel_id: forum-default\n"+fullRuntimeYAML()+`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
`)
	for _, ep := range endpoints {
		if ep.ChannelID != "forum-default" {
			t.Fatalf("%s channel = %q, want the default forum", ep.Workflow, ep.ChannelID)
		}
	}
}

func TestRepoWithoutChannelIDUsesForumDefault(t *testing.T) {
	endpoints := forumEndpoints(t, "webhooks:\n  defaults:\n    forum_channel_id: forum-default\n"+fullRuntimeYAML()+`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      secret: s3cret
      checkout: /tmp/widget
`)
	if len(endpoints) == 0 {
		t.Fatal("expected endpoints")
	}
	if endpoints[0].ChannelID != "forum-default" {
		t.Fatalf("channel = %q, want forum-default", endpoints[0].ChannelID)
	}
}

func TestRepoWithoutAnyChannelFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      secret: s3cret
      checkout: /tmp/widget
`))
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "channel_id") {
		t.Fatalf("Load error = %v, want a missing channel_id error", err)
	}
}
