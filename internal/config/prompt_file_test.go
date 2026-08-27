package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWebhookPromptFileLoadsRelativeToConfigDirectory(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "webhooks"), 0o755); err != nil {
		t.Fatalf("mkdir prompt directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "webhooks", "review.md"), []byte("Review {{.webhook.pr_number}}"), 0o644); err != nil {
		t.Fatalf("write prompt file: %v", err)
	}
	path := writeConfig(t, dir, `webhooks:
  endpoints:
    - name: reviewer
      path: /review
      workflow: github_reviewer
      secret: secret
      platform: discord
      channel_id: channel
      prompt_file: webhooks/review.md
      workspace:
        type: none
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ep := cfg.Webhooks.Endpoints[0]
	if ep.Prompt != "Review {{.webhook.pr_number}}" {
		t.Fatalf("Prompt = %q, want loaded file contents", ep.Prompt)
	}
	if ep.PromptFile != "webhooks/review.md" {
		t.Fatalf("PromptFile = %q, want configured relative path", ep.PromptFile)
	}
}

func TestWebhookPromptFileValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	tests := []struct {
		name       string
		promptFile string
		prompt     string
		prepare    func(t *testing.T, dir string)
		want       string
	}{
		{name: "traversal", promptFile: "../prompt.md", want: "path escapes config directory"},
		{name: "missing", promptFile: "missing.md", want: "prompt file"},
		{
			name:       "empty",
			promptFile: "empty.md",
			prepare: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, "empty.md"), []byte(" \n"), 0o644); err != nil {
					t.Fatalf("write empty prompt: %v", err)
				}
			},
			want: "file is empty",
		},
		{
			name:       "directory is not a prompt file",
			promptFile: "prompts",
			prepare: func(t *testing.T, dir string) {
				if err := os.Mkdir(filepath.Join(dir, "prompts"), 0o755); err != nil {
					t.Fatalf("mkdir prompt path: %v", err)
				}
			},
			want: "prompt_file",
		},
		{
			name:       "prompt conflict",
			promptFile: "prompt.md",
			prompt:     "inline",
			prepare: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, "prompt.md"), []byte("file"), 0o644); err != nil {
					t.Fatalf("write prompt: %v", err)
				}
			},
			want: "both prompt and prompt_file",
		},
		{
			name:       "unreadable",
			promptFile: "unreadable.md",
			prepare: func(t *testing.T, dir string) {
				path := filepath.Join(dir, "unreadable.md")
				if err := os.WriteFile(path, []byte("secret prompt"), 0o000); err != nil {
					t.Fatalf("write unreadable prompt: %v", err)
				}
			},
			want: "unreadable",
		},
		{
			name:       "symlink escapes",
			promptFile: "prompt.md",
			prepare: func(t *testing.T, dir string) {
				target := filepath.Join(t.TempDir(), "outside.md")
				if err := os.WriteFile(target, []byte("outside"), 0o644); err != nil {
					t.Fatalf("write outside prompt: %v", err)
				}
				if err := os.Symlink(target, filepath.Join(dir, "prompt.md")); err != nil {
					t.Fatalf("symlink prompt: %v", err)
				}
			},
			want: "path escapes config directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.prepare != nil {
				tt.prepare(t, dir)
			}
			content := fmt.Sprintf(`webhooks:
  endpoints:
    - name: reviewer
      path: /review
      secret: secret
      platform: discord
      channel_id: channel
      prompt: %q
      prompt_file: %q
      workspace:
        type: none
`, tt.prompt, tt.promptFile)
			_, err := Load(writeConfig(t, dir, content))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestWebhookUnknownWorkflowFailsConfigLoad(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `webhooks:
  endpoints:
    - name: invalid
      path: /invalid
      workflow: github_everything
      secret: secret
      platform: discord
      channel_id: channel
      prompt: inline
      workspace:
        type: none
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "workflow is unsupported") {
		t.Fatalf("Load error = %v, want unsupported workflow error", err)
	}
}

func TestWebhookInlinePromptRemainsCompatibleWithoutWorkflow(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `webhooks:
  endpoints:
    - name: legacy
      path: /legacy
      secret: secret
      platform: discord
      channel_id: channel
      prompt: inline prompt
      workspace:
        type: none
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ep := cfg.Webhooks.Endpoints[0]
	if ep.Prompt != "inline prompt" || ep.Workflow != "" {
		t.Fatalf("legacy endpoint = %+v, want inline prompt and no workflow gate", ep)
	}
}
