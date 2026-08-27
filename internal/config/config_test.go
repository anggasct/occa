package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaultsFromEmptyFile(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminID != "admin123" {
		t.Errorf("AdminID = %q, want admin123", cfg.AdminID)
	}
	if cfg.Agent.Binary != "opencode" {
		t.Errorf("Binary = %q, want opencode", cfg.Agent.Binary)
	}
	if cfg.Agent.PortRange != "4096-4116" {
		t.Errorf("PortRange = %q, want 4096-4116", cfg.Agent.PortRange)
	}
	if cfg.Agent.MaxInstances != 5 {
		t.Errorf("MaxInstances = %d, want 5", cfg.Agent.MaxInstances)
	}
	if cfg.Agent.IdleTimeout != 20*time.Minute {
		t.Errorf("IdleTimeout = %v, want 20m", cfg.Agent.IdleTimeout)
	}
	if cfg.Agent.AutoInstall {
		t.Error("AutoInstall = true, want false by default")
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("Format = %q, want text", cfg.Logging.Format)
	}
}

func TestLoadMissingAdminID(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "")
	t.Setenv("OCCA_ADMIN_ID", "")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error when OCCA_ADMIN_ID is missing")
	}
}

func TestLoadParsesYAML(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `
agent:
  binary: /usr/bin/opencode
  port_range: 5000-5010
  max_instances: 3
  idle_timeout: 5m
  default_workdir: /tmp/proj
  auto_install: true
database:
  path: /tmp/occa.db
logging:
  format: json
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Agent.Binary != "/usr/bin/opencode" {
		t.Errorf("Binary = %q", cfg.Agent.Binary)
	}
	if cfg.Agent.PortRange != "5000-5010" {
		t.Errorf("PortRange = %q", cfg.Agent.PortRange)
	}
	if cfg.Agent.MaxInstances != 3 {
		t.Errorf("MaxInstances = %d", cfg.Agent.MaxInstances)
	}
	if cfg.Agent.IdleTimeout != 5*time.Minute {
		t.Errorf("IdleTimeout = %v", cfg.Agent.IdleTimeout)
	}
	if cfg.Agent.DefaultWorkdir != "/tmp/proj" {
		t.Errorf("DefaultWorkdir = %q", cfg.Agent.DefaultWorkdir)
	}
	if !cfg.Agent.AutoInstall {
		t.Error("AutoInstall = false, want true")
	}
	if cfg.Database.Path != "/tmp/occa.db" {
		t.Errorf("Database.Path = %q", cfg.Database.Path)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Format = %q", cfg.Logging.Format)
	}
}

func TestLoadEnvOverridesYAML(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "agent:\n  binary: from-file\n")

	t.Setenv("OCCA_AGENT_BINARY", "from-env")
	t.Setenv("OCCA_AGENT_MAX_INSTANCES", "9")
	t.Setenv("OCCA_AGENT_AUTO_INSTALL", "true")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Agent.Binary != "from-env" {
		t.Errorf("Binary = %q, want from-env (env must override YAML)", cfg.Agent.Binary)
	}
	if cfg.Agent.MaxInstances != 9 {
		t.Errorf("MaxInstances = %d, want 9", cfg.Agent.MaxInstances)
	}
	if !cfg.Agent.AutoInstall {
		t.Error("AutoInstall = false, want true (env must override default)")
	}
}

func TestLoadInvalidAutoInstallEnvErrors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "")
	t.Setenv("OCCA_AGENT_AUTO_INSTALL", "not-a-bool")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid OCCA_AGENT_AUTO_INSTALL")
	}
}

func TestLoadExplicitMissingErrors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("expected error for missing explicit config path")
	}
}

func TestLoadCorruptYAMLErrors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "agent: [unclosed")
	if _, err := Load(path); err == nil {
		t.Fatal("expected parse error for corrupt YAML")
	}
}

func TestLoadInvalidDurationErrors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "agent:\n  idle_timeout: notaduration\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid idle_timeout")
	}
}

func TestLoadInvalidLogFormatErrors(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "logging:\n  format: xml\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid logging.format")
	}
}

func TestBootstrapCreatesDefault(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := filepath.Join(t.TempDir(), "sub", "config.yaml")
	if err := bootstrap(path); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file not created: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load bootstrapped config: %v", err)
	}
	if cfg.Agent.Binary != "opencode" {
		t.Errorf("Binary = %q, want opencode", cfg.Agent.Binary)
	}
	if cfg.Agent.IdleTimeout != 20*time.Minute {
		t.Errorf("IdleTimeout = %v, want 20m", cfg.Agent.IdleTimeout)
	}
}

func TestDBPathFromConfig(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "database:\n  path: /var/lib/occa/occa.db\n")
	got, err := DBPath(path)
	if err != nil {
		t.Fatalf("DBPath: %v", err)
	}
	if got != "/var/lib/occa/occa.db" {
		t.Errorf("DBPath = %q, want /var/lib/occa/occa.db", got)
	}
}

func TestDBPathEnvOverride(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "database:\n  path: /var/lib/occa/occa.db\n")
	t.Setenv("OCCA_DB_PATH", "/tmp/env.db")
	got, err := DBPath(path)
	if err != nil {
		t.Fatalf("DBPath: %v", err)
	}
	if got != "/tmp/env.db" {
		t.Errorf("DBPath = %q, want /tmp/env.db (env must override YAML)", got)
	}
}

func TestDBPathNoAdminRequired(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "")
	path := writeConfig(t, t.TempDir(), "database:\n  path: /var/lib/occa/occa.db\n")
	if _, err := DBPath(path); err != nil {
		t.Fatalf("DBPath must not require OCCA_ADMIN_ID: %v", err)
	}
}

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("no home dir: %v", err)
	}

	got, err := expandHome("~")
	if err != nil {
		t.Fatalf("expandHome(~): %v", err)
	}
	if got != home {
		t.Errorf("expandHome(~) = %q, want %q", got, home)
	}

	got, err = expandHome("~/proj")
	if err != nil {
		t.Fatalf("expandHome(~/proj): %v", err)
	}
	if got != filepath.Join(home, "proj") {
		t.Errorf("expandHome(~/proj) = %q, want %q", got, filepath.Join(home, "proj"))
	}

	got, err = expandHome("/abs/path")
	if err != nil {
		t.Fatalf("expandHome(/abs/path): %v", err)
	}
	if got != "/abs/path" {
		t.Errorf("expandHome(/abs/path) = %q, want /abs/path", got)
	}
}

func TestWebhookLoopbackValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	tests := []struct {
		bind    string
		wantErr bool
	}{
		{"127.0.0.1:8787", false},
		{"localhost:8787", false},
		{"[::1]:8787", false},
		{"127.0.0.1", true},
		{"0.0.0.0:8787", true},
		{"192.168.1.1:8787", true},
		{"example.com:8787", true},
	}
	for _, tt := range tests {
		t.Run(tt.bind, func(t *testing.T) {
			yaml := fmt.Sprintf("webhooks:\n  bind: %q\n  endpoints:\n    - name: test\n      path: /test\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n      workspace:\n        type: none\n", tt.bind)
			path := writeConfig(t, t.TempDir(), yaml)
			_, err := Load(path)
			if tt.wantErr && err == nil {
				t.Fatal("expected invalid bind to fail validation")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestWebhookNoEndpointsSkipsValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Webhooks.Endpoints) != 0 {
		t.Fatalf("expected no webhook endpoints, got %d", len(cfg.Webhooks.Endpoints))
	}
}

func TestWebhookEmptySecretValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `webhooks:
  endpoints:
    - name: test
      path: /test
      platform: telegram
      channel_id: c1
      prompt: p
      workspace:
        type: none
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected empty webhook secret to fail validation")
	}
	if !strings.Contains(err.Error(), "webhooks.endpoints[0].secret") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWebhookDuplicatePathValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `webhooks:
  endpoints:
    - name: first
      path: /same
      secret: first-secret
      platform: telegram
      channel_id: c1
      prompt: p
      workspace:
        type: none
    - name: second
      path: /same
      secret: second-secret
      platform: telegram
      channel_id: c2
      prompt: p
      workspace:
        type: none
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected duplicate webhook path to fail validation")
	}
	if !strings.Contains(err.Error(), "webhooks.endpoints[1].path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWebhookAuthModeValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")

	tests := []struct {
		name      string
		yaml      string
		wantErr   bool
		errSubstr string
	}{
		{
			name: "explicit github_hmac_sha256 passes",
			yaml: `webhooks:
  endpoints:
    - name: gh
      path: /gh
      auth: github_hmac_sha256
      secret: supersecret
      platform: discord
      channel_id: c1
      prompt: p
      workspace:
        type: none`,
			wantErr: false,
		},
		{
			name: "whitespace padded github_hmac_sha256 normalized",
			yaml: `webhooks:
  endpoints:
    - name: gh-padded
      path: /gh-padded
      auth: "  github_hmac_sha256  "
      secret: supersecret
      platform: discord
      channel_id: c1
      prompt: p
      workspace:
        type: none`,
			wantErr: false,
		},
		{
			name: "explicit legacy_bearer passes",
			yaml: `webhooks:
  endpoints:
    - name: legacy
      path: /legacy
      auth: legacy_bearer
      secret: supersecret
      platform: discord
      channel_id: c1
      prompt: p
      workspace:
        type: none`,
			wantErr: false,
		},
		{
			name: "unsupported auth mode fails",
			yaml: `webhooks:
  endpoints:
    - name: invalid
      path: /invalid
      auth: oauth2_token
      secret: supersecret
      platform: discord
      channel_id: c1
      prompt: p
      workspace:
        type: none`,
			wantErr:   true,
			errSubstr: "webhooks.endpoints[0].auth is unsupported",
		},
		{
			name: "github_hmac_sha256 with empty secret fails",
			yaml: `webhooks:
  endpoints:
    - name: gh
      path: /gh
      auth: github_hmac_sha256
      secret: "   "
      platform: discord
      channel_id: c1
      prompt: p
      workspace:
        type: none`,
			wantErr:   true,
			errSubstr: "webhooks.endpoints[0].secret must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, t.TempDir(), tt.yaml)
			cfg, err := Load(path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errSubstr)
				}
				if !strings.Contains(err.Error(), tt.errSubstr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(cfg.Webhooks.Endpoints) != 1 {
					t.Fatalf("expected 1 endpoint, got %d", len(cfg.Webhooks.Endpoints))
				}
				if tt.name == "whitespace padded github_hmac_sha256 normalized" {
					if cfg.Webhooks.Endpoints[0].Auth != "github_hmac_sha256" {
						t.Fatalf("expected normalized auth 'github_hmac_sha256', got %q", cfg.Webhooks.Endpoints[0].Auth)
					}
				}
			}
		})
	}
}

func TestLoadDiscordPolicy(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `discord:
  trigger_role_ids:
    - " role-1 "
  trusted_bot_senders:
    - user_id: " bot-1 "
      channel_ids:
        - " channel-1 "
        - channel-2
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Discord.TriggerRoleIDs) != 1 || cfg.Discord.TriggerRoleIDs[0] != "role-1" {
		t.Fatalf("TriggerRoleIDs = %#v, want [role-1]", cfg.Discord.TriggerRoleIDs)
	}
	if len(cfg.Discord.TrustedBotSenders) != 1 {
		t.Fatalf("TrustedBotSenders = %#v, want one sender", cfg.Discord.TrustedBotSenders)
	}
	sender := cfg.Discord.TrustedBotSenders[0]
	if sender.UserID != "bot-1" || strings.Join(sender.ChannelIDs, ",") != "channel-1,channel-2" {
		t.Fatalf("trusted sender = %#v, want trimmed IDs", sender)
	}
}

func TestLoadDiscordPolicyValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	tests := []struct {
		name      string
		yaml      string
		errSubstr string
	}{
		{name: "empty user", yaml: "discord:\n  trusted_bot_senders:\n    - user_id: \" \"\n      channel_ids: [channel-1]\n", errSubstr: "discord.trusted_bot_senders[0].user_id"},
		{name: "empty channels", yaml: "discord:\n  trusted_bot_senders:\n    - user_id: bot-1\n      channel_ids: []\n", errSubstr: "discord.trusted_bot_senders[0].channel_ids"},
		{name: "empty channel", yaml: "discord:\n  trusted_bot_senders:\n    - user_id: bot-1\n      channel_ids: [\" \"]\n", errSubstr: "discord.trusted_bot_senders[0].channel_ids[0]"},
		{name: "duplicate channel", yaml: "discord:\n  trusted_bot_senders:\n    - user_id: bot-1\n      channel_ids: [channel-1, channel-1]\n", errSubstr: "discord.trusted_bot_senders[0].channel_ids[1]"},
		{name: "duplicate sender", yaml: "discord:\n  trusted_bot_senders:\n    - user_id: bot-1\n      channel_ids: [channel-1]\n    - user_id: bot-1\n      channel_ids: [channel-2]\n", errSubstr: "discord.trusted_bot_senders[1].user_id"},
		{name: "duplicate role", yaml: "discord:\n  trigger_role_ids: [role-1, role-1]\n", errSubstr: "discord.trigger_role_ids[1]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, t.TempDir(), tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.errSubstr) {
				t.Fatalf("Load error = %v, want substring %q", err, tt.errSubstr)
			}
		})
	}
}

func TestEndpointWorkspaceValidation(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := []struct {
		name      string
		workspace string
		repo      string
		wantErr   string
	}{
		{"missing type", "", "", `workspace.type is required`},
		{"invalid type", "      workspace:\n        type: filesystem\n", "", `workspace.type is required`},
		{"none rejects path", "      workspace:\n        type: none\n        path: /tmp/x\n", "", "must be empty when workspace.type is none"},
		{"none rejects mode", "      workspace:\n        type: none\n        mode: isolated\n", "", "must be empty when workspace.type is none"},
		{"git requires path", "      workspace:\n        type: git\n        mode: isolated\n", "github.com/o/r", "workspace.path is required"},
		{"git requires mode", "      workspace:\n        type: git\n        path: /tmp/x\n", "github.com/o/r", "workspace.mode is required"},
		{"git invalid mode", "      workspace:\n        type: git\n        path: /tmp/x\n        mode: adhoc\n", "github.com/o/r", "workspace.mode is required"},
		{"git requires repository", "      workspace:\n        type: git\n        path: /tmp/x\n        mode: isolated\n", "", "repository binding is required"},
		{"git invalid repository", "      workspace:\n        type: git\n        path: /tmp/x\n        mode: isolated\n", "a/b/c/d/e", "must be owner/repo or host/owner/repo"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			yaml := "webhooks:\n  endpoints:\n    - name: test\n      path: /test\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n" + tt.workspace
			if tt.repo != "" {
				yaml += "      repository: " + tt.repo + "\n"
			}
			path := writeConfig(t, t.TempDir(), yaml)
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestEndpointWorkspaceGitBindingNormalizes(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	dir := t.TempDir()
	yaml := "webhooks:\n  endpoints:\n    - name: gh\n      path: /gh\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n      repository: https://GitHub.com/TestOwner/MyRepo.git\n      workspace:\n        type: git\n        path: relative/checkout\n        mode: isolated\n"
	path := writeConfig(t, dir, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ep := cfg.Webhooks.Endpoints[0]
	if ep.Repository != "github.com/testowner/myrepo" {
		t.Fatalf("repository binding = %q, want canonical github.com/testowner/myrepo", ep.Repository)
	}
	want := filepath.Clean(filepath.Join(dir, "relative/checkout"))
	if ep.Workspace.Path != want {
		t.Fatalf("workspace path = %q, want config-relative %q", ep.Workspace.Path, want)
	}
	if ep.Workspace.Mode != WorkspaceModeIsolated {
		t.Fatalf("mode = %q", ep.Workspace.Mode)
	}
}

func TestEndpointPathIngressPrefixRejected(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	for _, p := range []string{"/occa", "/occa/gh", "/gh?x=1", "/a/../b", "gh"} {
		yaml := "webhooks:\n  endpoints:\n    - name: test\n      path: " + strconv.Quote(p) + "\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n      workspace:\n        type: none\n"
		path := writeConfig(t, t.TempDir(), yaml)
		if _, err := Load(path); err == nil {
			t.Fatalf("path %q must fail validation", p)
		}
	}
}
