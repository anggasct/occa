package config

import (
	"strings"
	"testing"
)

// IMP-113 AC-01.
func TestResolveMergePolicy(t *testing.T) {
	cases := []struct {
		name   string
		in     *RepoMerge
		source string
		want   MergePolicy
	}{
		{"absent builtin", nil, "builtin", MergePolicy{Code: true}},
		{"absent custom prompt", nil, "file", MergePolicy{Code: false}},
		{"auto inline", &RepoMerge{Mode: "auto"}, "inline", MergePolicy{Code: false}},
		{"code forced", &RepoMerge{Mode: "code", Method: "Squash", DeleteBranch: true}, "file", MergePolicy{Code: true, Method: "squash", DeleteBranch: true}},
		{"agent forced", &RepoMerge{Mode: "agent"}, "builtin", MergePolicy{Code: false}},
	}
	for _, tc := range cases {
		got, err := ResolveMergePolicy(tc.in, tc.source, "webhooks.repos[0] (acme/widget): ")
		if err != nil || got != tc.want {
			t.Errorf("%s: got %+v %v, want %+v", tc.name, got, err, tc.want)
		}
	}
	for _, bad := range []*RepoMerge{{Mode: "robot"}, {Method: "octopus"}} {
		_, err := ResolveMergePolicy(bad, "builtin", "webhooks.repos[0] (acme/widget): ")
		if err == nil || !strings.Contains(err.Error(), "webhooks.repos[0] (acme/widget): merge.") {
			t.Errorf("%+v: err = %v, want it to name merge.<key>", bad, err)
		}
	}
}

// IMP-113 AC-01: repos expand the policy onto the merge endpoint only.
func TestExpandReposMergePolicy(t *testing.T) {
	repos := []RepoConfig{
		{Repo: "acme/widget", Provider: "github", Platform: "discord", ChannelID: "1", Secret: "s", Checkout: "/tmp/widget", Merge: &RepoMerge{Method: "rebase"}},
		{Repo: "acme/gadget", Provider: "github", Platform: "discord", ChannelID: "1", Secret: "s", Checkout: "/tmp/gadget", Prompts: map[string]RepoPromptRef{"merge": {Prompt: "merge it"}}},
	}
	eps, err := expandRepos(repos, WebhookDefaults{})
	if err != nil {
		t.Fatalf("expandRepos: %v", err)
	}
	got := map[string]MergePolicy{}
	for _, ep := range eps {
		got[ep.Name] = ep.Merge
	}
	if p := got["github-widget-merge"]; !p.Code || p.Method != "rebase" {
		t.Fatalf("widget merge policy = %+v", p)
	}
	if p := got["github-gadget-merge"]; p.Code {
		t.Fatalf("a custom merge prompt keeps the agent: %+v", p)
	}
	if p := got["github-widget-review"]; p != (MergePolicy{}) {
		t.Fatalf("non-merge endpoints carry no policy: %+v", p)
	}
}
