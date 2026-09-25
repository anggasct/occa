package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeExampleParity(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	example := filepath.Join("..", "..", "config.example.yaml")
	data, err := os.ReadFile(example)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "webhooks"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "webhooks", "example-review.md"), []byte("review"), 0o644); err != nil {
		t.Fatalf("write prompt: %v", err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load example: %v", err)
	}
	rt := cfg.Runtime
	if rt.Loop.MinInterval != 30*time.Second || rt.Loop.MaxInterval != time.Hour {
		t.Fatalf("loop interval = %v/%v", rt.Loop.MinInterval, rt.Loop.MaxInterval)
	}
	if rt.Loop.MaxDuration != 4*time.Hour || rt.Loop.MinDuration != time.Minute {
		t.Fatalf("loop duration = %v/%v", rt.Loop.MaxDuration, rt.Loop.MinDuration)
	}
	if rt.Loop.IterationTimeout != 10*time.Minute || rt.Loop.MaxWallAge != 4*time.Hour {
		t.Fatalf("loop timeouts = %v/%v", rt.Loop.IterationTimeout, rt.Loop.MaxWallAge)
	}
	if rt.Loop.MinCount != 2 || rt.Loop.MaxCount != 60 || rt.Loop.MaxPromptRunes != 1000 || rt.Loop.MaxPerConversation != 1 || rt.Loop.MaxGlobal != 20 {
		t.Fatalf("loop counts = %+v", rt.Loop)
	}
	if rt.Relay.DiscoveryTimeout != 5*time.Second || rt.Relay.ClientTimeout != 3*time.Minute {
		t.Fatalf("relay timeouts = %v/%v", rt.Relay.DiscoveryTimeout, rt.Relay.ClientTimeout)
	}
	if int64(rt.Relay.MaxAttachmentSize) != 10*1024*1024 || rt.Relay.MaxEventLineBytes != 1114112 {
		t.Fatalf("relay sizes = %d/%d", int64(rt.Relay.MaxAttachmentSize), rt.Relay.MaxEventLineBytes)
	}
	if rt.Relay.WebhookAbortTimeout != 5*time.Second || rt.Relay.VerifyTimeout != 15*time.Second || rt.Relay.StallFreshness != 2*time.Minute || rt.Relay.NoEventTimeout != 15*time.Minute {
		t.Fatalf("relay webhook = %+v", rt.Relay)
	}
	if rt.Router.ContextStaleAfter != 15*time.Minute || rt.Router.ProgressQuietThreshold != 90*time.Second {
		t.Fatalf("router flow = %+v", rt.Router)
	}
	if rt.Router.MaxQueuedMessages != 5 || rt.Router.MaxPickerSessions != 6 || rt.Router.MaxPickerPages != 5 {
		t.Fatalf("router picker = %+v", rt.Router)
	}
	if rt.Router.ModelBrowserTTL != 30*time.Minute || rt.Router.ModelBrowserPage != 10 || rt.Router.ModelBrowserNavRows != 100 || rt.Router.ModelBrowserCap != 1000 {
		t.Fatalf("router model browser = %+v", rt.Router)
	}
	if rt.Router.AgentBrowserTTL != 30*time.Minute || rt.Router.AgentBrowserCap != 256 {
		t.Fatalf("router agent browser = %+v", rt.Router)
	}
	if rt.Router.QuestionTombstoneTTL != 10*time.Minute || rt.Router.PermissionTombstoneTTL != 10*time.Minute || rt.Router.AttributionTTL != 30*time.Second {
		t.Fatalf("router ttls = %+v", rt.Router)
	}
	if rt.Router.RecoveryBudget != 60*time.Second || rt.Router.RecoveryBaseBackoff != 10*time.Second || rt.Router.RecoveryMaxBackoff != 40*time.Second {
		t.Fatalf("router recovery = %+v", rt.Router)
	}
	if rt.Router.UsagePageSize != 5 || rt.Router.UsageDefaultWindow != 168*time.Hour {
		t.Fatalf("router usage = %+v", rt.Router)
	}
	if rt.Channels.Discord.DownloadTimeout != 60*time.Second || int64(rt.Channels.Discord.MaxDownloadSize) != 10*1024*1024 {
		t.Fatalf("discord channels = %+v", rt.Channels.Discord)
	}
	if rt.Channels.Telegram.DownloadTimeout != 60*time.Second || int64(rt.Channels.Telegram.MaxDownloadSize) != 10*1024*1024 || rt.Channels.Telegram.InitTimeout != 15*time.Second || rt.Channels.Telegram.InitAttempts != 3 {
		t.Fatalf("telegram channels = %+v", rt.Channels.Telegram)
	}
	if rt.MCP.ReadHeaderTimeout != 10*time.Second || rt.MCP.ReadTimeout != 30*time.Second || rt.MCP.WriteTimeout != 5*time.Minute || rt.MCP.IdleTimeout != 2*time.Minute {
		t.Fatalf("mcp = %+v", rt.MCP)
	}
	if rt.Process.ReadinessTimeout != 30*time.Second || rt.Process.StopGrace != 5*time.Second || rt.Process.ControlTimeout != 2*time.Second {
		t.Fatalf("process = %+v", rt.Process)
	}
	if rt.Scheduler.StopGrace != 5*time.Second {
		t.Fatalf("scheduler = %+v", rt.Scheduler)
	}
	if rt.Health.ProbeTimeout != 1500*time.Millisecond {
		t.Fatalf("health = %+v", rt.Health)
	}
	if rt.Store.UsageRetention != 2160*time.Hour || rt.Store.UsageMaxRows != 100000 || rt.Store.RecoveryEventRetention != 720*time.Hour {
		t.Fatalf("store = %+v", rt.Store)
	}
}

func TestRuntimeMissingKeyFails(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	yaml := "discord:\n  allowed_sender_ids:\n    - '123'\n" + validRuntimeYAML()
	yaml = replaceFirst(yaml, "    min_interval: 30s\n", "")
	path := writeConfig(t, t.TempDir(), yaml)
	// writeConfig auto-appends runtime when missing, so build raw path instead
	_ = path
	raw := "discord:\n  allowed_sender_ids:\n    - '123'\nruntime:\n  loop:\n    max_interval: 1h\n"
	rawPath := filepath.Join(t.TempDir(), "raw.yaml")
	if err := os.WriteFile(rawPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write raw: %v", err)
	}
	if _, err := Load(rawPath); err == nil {
		t.Fatal("expected missing runtime.loop.min_interval to fail")
	}
}

func replaceFirst(s, old, new string) string {
	for i := 0; i < len(s)-len(old); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}
