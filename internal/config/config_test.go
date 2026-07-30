package config

import (
	"os"
	"path/filepath"
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
	path := writeConfig(t, t.TempDir(), "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
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
	if cfg.Logging.Format != "text" {
		t.Errorf("Format = %q, want text", cfg.Logging.Format)
	}
}

func TestLoadParsesYAML(t *testing.T) {
	path := writeConfig(t, t.TempDir(), `
agent:
  binary: /usr/bin/opencode
  port_range: 5000-5010
  max_instances: 3
  idle_timeout: 5m
  default_workdir: /tmp/proj
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
	if cfg.Database.Path != "/tmp/occa.db" {
		t.Errorf("Database.Path = %q", cfg.Database.Path)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Format = %q", cfg.Logging.Format)
	}
}

func TestLoadEnvOverridesYAML(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "agent:\n  binary: from-file\n")

	t.Setenv("OCCA_AGENT_BINARY", "from-env")
	t.Setenv("OCCA_AGENT_MAX_INSTANCES", "9")

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
}

func TestLoadExplicitMissingErrors(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("expected error for missing explicit config path")
	}
}

func TestLoadCorruptYAMLErrors(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "agent: [unclosed")
	if _, err := Load(path); err == nil {
		t.Fatal("expected parse error for corrupt YAML")
	}
}

func TestLoadInvalidDurationErrors(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "agent:\n  idle_timeout: notaduration\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid idle_timeout")
	}
}

func TestLoadInvalidLogFormatErrors(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "logging:\n  format: xml\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid logging.format")
	}
}

func TestBootstrapCreatesDefault(t *testing.T) {
	// Nested path verifies MkdirAll creates parent directories.
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
