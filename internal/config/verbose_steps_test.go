package config

import (
	"strings"
	"testing"
)

func TestVerboseStepsDefaultsToFalse(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.Defaults.VerboseSteps {
		t.Error("verbose_steps must default to false")
	}
}

func TestVerboseStepsIsAccepted(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `
webhooks:
  defaults:
    verbose_steps: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Webhooks.Defaults.VerboseSteps {
		t.Error("verbose_steps must be read as true")
	}
}

func TestUnknownDefaultsKeyIsRejected(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `
webhooks:
  defaults:
    verbose_step: true
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "verbose_step") {
		t.Fatalf("Load error = %v, want an unknown key failure", err)
	}
}

func TestPhaseStatusIconsAreAccepted(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `
webhooks:
  status_icons:
    queued: ⏳
    review: 👀
    fix: 🔨
    merge: 🔀
    blocked: 🔴
  defaults:
    status_icons:
      merge: 🌀
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.StatusIcons["merge"] != "🌀" {
		t.Errorf("merge icon = %q, want the defaults override", cfg.Webhooks.StatusIcons["merge"])
	}
	if cfg.Webhooks.StatusIcons["blocked"] != "🔴" {
		t.Errorf("blocked icon = %q, want the phase icon", cfg.Webhooks.StatusIcons["blocked"])
	}
}

func TestUnknownStatusIconKeyIsRejected(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	path := writeConfig(t, t.TempDir(), `
webhooks:
  status_icons:
    not_a_phase: 🧪
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "not_a_phase") {
		t.Fatalf("Load error = %v, want an unknown icon key failure", err)
	}
}
