package config

import (
	"reflect"
	"strings"
	"testing"
)

// IMP-114 AC-01.
func TestResolveMergePolicy(t *testing.T) {
	cases := []struct {
		name string
		in   *RepoMerge
		want MergePolicy
	}{
		{"absent", nil, MergePolicy{}},
		{"method and delete", &RepoMerge{Method: "Squash", DeleteBranch: true}, MergePolicy{Method: "squash", DeleteBranch: true}},
		{"stale approval paths", &RepoMerge{StaleApprovalPaths: []string{" .github/** ", "*.md"}}, MergePolicy{StaleApprovalPaths: []string{".github/**", "*.md"}}},
	}
	for _, tc := range cases {
		got, err := ResolveMergePolicy(tc.in, "webhooks.repos[0] (acme/widget): ")
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v %v, want %+v", tc.name, got, err, tc.want)
		}
	}
	bad := []struct {
		in   *RepoMerge
		want string
	}{
		{&RepoMerge{Method: "octopus"}, "merge.method"},
		{&RepoMerge{StaleApprovalPaths: []string{"*.md", " "}}, "merge.stale_approval_paths[1]"},
		{&RepoMerge{StaleApprovalPaths: []string{"docs/[a"}}, "merge.stale_approval_paths[0]"},
	}
	for _, tc := range bad {
		_, err := ResolveMergePolicy(tc.in, "webhooks.repos[0] (acme/widget): ")
		if err == nil || !strings.Contains(err.Error(), "webhooks.repos[0] (acme/widget): "+tc.want) {
			t.Errorf("%+v: err = %v, want it to name %s", tc.in, err, tc.want)
		}
	}
}

// IMP-114 AC-04.
func TestMatchesApprovalPath(t *testing.T) {
	cases := []struct {
		pattern, file string
		want          bool
	}{
		{".github/**", ".github/workflows/ci.yml", true},
		{".github/**", "src/.github/x", false},
		{"*.md", "docs/a/README.md", true},
		{"*.md", "README.md", true},
		{"*.md", "main.go", false},
		{"docs/**", "docs/guide/a.txt", true},
		{"docs/**", "docsx/a", false},
		{"LICENSE", "LICENSE", true},
		{"LICENSE", "third_party/LICENSE", true},
		{"LICENSE", "LICENSE.txt", false},
		{"config/*.yaml", "config/app.yaml", true},
		{"config/*.yaml", "config/sub/app.yaml", false},
		{"", "a.md", false},
	}
	for _, tc := range cases {
		if got := MatchesApprovalPath(tc.pattern, tc.file); got != tc.want {
			t.Errorf("MatchesApprovalPath(%q, %q) = %v, want %v", tc.pattern, tc.file, got, tc.want)
		}
	}
}

// IMP-114 AC-01/AC-02: every repo merge endpoint runs in code with its policy.
func TestExpandReposMergePolicy(t *testing.T) {
	repos := []RepoConfig{
		{Repo: "acme/widget", Provider: "github", Platform: "discord", ChannelID: "1", Secret: "s", Checkout: "/tmp/widget", Merge: &RepoMerge{Method: "rebase", StaleApprovalPaths: []string{"*.md"}}},
		{Repo: "acme/gadget", Provider: "github", Platform: "discord", ChannelID: "1", Secret: "s", Checkout: "/tmp/gadget"},
	}
	eps, err := expandRepos(repos, WebhookDefaults{})
	if err != nil {
		t.Fatalf("expandRepos: %v", err)
	}
	got := map[string]EndpointConfig{}
	for _, ep := range eps {
		got[ep.Name] = ep
	}
	if ep := got["github-widget-merge"]; ep.Merge.Method != "rebase" || len(ep.Merge.StaleApprovalPaths) != 1 || ep.PromptSource != PromptSourceCode || ep.Prompt != "" {
		t.Fatalf("widget merge endpoint = %+v", ep)
	}
	if ep := got["github-gadget-merge"]; ep.PromptSource != PromptSourceCode || ep.Merge.Method != "" {
		t.Fatalf("gadget merge endpoint = %+v", ep)
	}
	if p := got["github-widget-review"].Merge; !reflect.DeepEqual(p, MergePolicy{}) {
		t.Fatalf("non-merge endpoints carry no policy: %+v", p)
	}
}

// IMP-114 AC-01: a merge prompt refuses startup with the replacement keys.
func TestExpandReposRefusesMergePrompt(t *testing.T) {
	repos := []RepoConfig{
		{Repo: "acme/gadget", Provider: "github", Platform: "discord", ChannelID: "1", Secret: "s", Checkout: "/tmp/gadget", Prompts: map[string]RepoPromptRef{"merge": {Prompt: "merge it"}}},
	}
	_, err := expandRepos(repos, WebhookDefaults{})
	if err == nil || !strings.Contains(err.Error(), "webhooks.repos[0] (acme/gadget): prompts.merge is not supported") || !strings.Contains(err.Error(), "merge.stale_approval_paths") {
		t.Fatalf("err = %v, want the prompts.merge refusal", err)
	}
}

// IMP-114 AC-01: merge.mode is gone (strict decoder).
func TestLoadRefusesMergeMode(t *testing.T) {
	withMode := writeConfig(t, t.TempDir(), repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      merge:
        mode: agent
`))
	if _, err := Load(withMode); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("merge.mode err = %v, want an unknown-field refusal", err)
	}
}
