package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepoPromptsResolve(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "webhooks"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "webhooks", "custom-review.md"), []byte("custom review"), 0o644); err != nil {
		t.Fatalf("write custom prompt: %v", err)
	}
	path := writeConfig(t, dir, repoConfigYAML(`  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: chan-1
      secret: s3cret
      checkout: /tmp/widget
      prompts:
        review: { prompt_file: webhooks/custom-review.md }
        fix: { prompt: "inline fix" }
`))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	byWorkflow := map[string]EndpointConfig{}
	for _, ep := range cfg.Webhooks.Endpoints {
		byWorkflow[ep.Workflow] = ep
	}
	if byWorkflow["review"].Prompt != "custom review" || byWorkflow["review"].PromptSource != "file" {
		t.Fatalf("review should load file: prompt=%q source=%q", byWorkflow["review"].Prompt, byWorkflow["review"].PromptSource)
	}
	if byWorkflow["fix"].Prompt != "inline fix" || byWorkflow["fix"].PromptSource != "inline" {
		t.Fatalf("fix should use inline: prompt=%q source=%q", byWorkflow["fix"].Prompt, byWorkflow["fix"].PromptSource)
	}
	if byWorkflow["merge"].Prompt != BuiltinPrompt("merge") || byWorkflow["merge"].PromptSource != "builtin" {
		t.Fatalf("merge should fall back to builtin: source=%q", byWorkflow["merge"].PromptSource)
	}
	if byWorkflow["merged"].Prompt != BuiltinPrompt("merged") || byWorkflow["merged"].PromptSource != "builtin" {
		t.Fatalf("merged should fall back to builtin: source=%q", byWorkflow["merged"].PromptSource)
	}
}

func TestRepoPromptsValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := []struct {
		name    string
		prompts string
		want    string
	}{
		{
			name: "unknown workflow",
			prompts: `      prompts:
        deploy: { prompt: "x" }
`,
			want: "supports only review, fix, merge and merged",
		},
		{
			name: "both prompt and file",
			prompts: `      prompts:
        review: { prompt: "x", prompt_file: webhooks/x.md }
`,
			want: "exactly one of prompt, prompt_file",
		},
		{
			name: "neither prompt nor file",
			prompts: `      prompts:
        review: {}
`,
			want: "exactly one of prompt, prompt_file",
		},
		{
			name: "missing file fails",
			prompts: `      prompts:
        review: { prompt_file: webhooks/does-not-exist.md }
`,
			want: "prompt_file",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConfig(t, dir, repoConfigYAML("  repos:\n    - repo: acme/widget\n      provider: github\n      platform: discord\n      channel_id: chan-1\n      secret: s3cret\n      checkout: /tmp/widget\n"+tc.prompts))
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestCustomEndpointRequiresPrompt(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	path := writeConfig(t, dir, `webhooks:
`+fullRuntimeYAML()+`  endpoints:
    - name: noprompt
      path: /noprompt
      secret: s
      platform: telegram
      channel_id: c1
      workflow: custom
      workspace:
        type: none
      admit:
        - event: ping
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "exactly one of prompt, prompt_file") {
		t.Fatalf("Load error = %v, want prompt requirement", err)
	}
}

func TestBuiltinPromptKnownWorkflows(t *testing.T) {
	for _, workflow := range []string{"review", "fix", "merge", "merged"} {
		if got := BuiltinPrompt(workflow); strings.TrimSpace(got) == "" {
			t.Fatalf("BuiltinPrompt(%q) is empty", workflow)
		}
	}
	if got := BuiltinPrompt("custom"); got != "" {
		t.Fatalf("BuiltinPrompt(custom) = %q, want empty", got)
	}
}
