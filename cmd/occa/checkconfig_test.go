package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWebhooksCheckConfigAcceptsExample(t *testing.T) {
	if got := runWebhooksCheckConfig([]string{"--config", filepath.Join("..", "..", "config.example.yaml")}); got != 0 {
		t.Fatalf("check-config exit = %d, want 0", got)
	}
}

func TestWebhooksCheckConfigRejectsIncomplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "discord:\n  allowed_sender_ids:\n    - '123'\nwebhooks:\n  endpoints:\n    - name: test\n      path: /test\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n      workspace:\n        type: none\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if got := runWebhooksCheckConfig([]string{"--config", path}); got != 1 {
		t.Fatalf("check-config exit = %d, want 1", got)
	}
}
