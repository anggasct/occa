package config

import (
	"strings"
	"testing"
)

const digestYAML = "webhooks:\n  digest:\n    at: \"08:30\"\n    timezone: Asia/Jakarta\n    platform: discord\n    channel_id: \"1519697125707878512\"\n"

// IMP-112 AC-06: the digest is off without a block and validated with one.
func TestDigestConfig(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cfg, err := Load(writeConfig(t, t.TempDir(), ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Webhooks.Digest != nil {
		t.Fatalf("no block must mean no digest: %+v", cfg.Webhooks.Digest)
	}

	cfg, err = Load(writeConfig(t, t.TempDir(), digestYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	d := cfg.Webhooks.Digest
	if d == nil || d.Hour != 8 || d.Minute != 30 || d.Location == nil || d.Location.String() != "Asia/Jakarta" || d.Platform != "discord" || d.ChannelID != "1519697125707878512" {
		t.Fatalf("digest = %+v", d)
	}

	cases := []struct{ name, yaml, want string }{
		{"bad at", strings.Replace(digestYAML, `"08:30"`, `"8am"`, 1), "webhooks.digest.at"},
		{"bad timezone", strings.Replace(digestYAML, "Asia/Jakarta", "Mars/Olympus", 1), "webhooks.digest.timezone"},
		{"no channel", strings.Replace(digestYAML, "    channel_id: \"1519697125707878512\"\n", "", 1), "webhooks.digest.channel_id"},
		{"no platform", strings.Replace(digestYAML, "    platform: discord\n", "", 1), "webhooks.digest.platform"},
		{"unknown key", digestYAML + "    colour: blue\n", "colour"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OCCA_ADMIN_ID", "admin123")
			_, err := Load(writeConfig(t, t.TempDir(), tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load err = %v, want it to name %q", err, tc.want)
			}
		})
	}
}
