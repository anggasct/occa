package config

import (
	"testing"
)

func TestEndpointNotifySkipParsing(t *testing.T) {
	t.Setenv("OCCA_ADMIN_ID", "admin123")
	cases := []struct {
		name    string
		extra   string
		want    bool
		wantNil bool
	}{
		{"absent defaults true", "", true, true},
		{"explicit true", "      notify_skip: true\n", true, false},
		{"explicit false", "      notify_skip: false\n", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			yaml := testWebhookConfigYAML("    - name: test\n      path: /test\n      secret: s\n      platform: telegram\n      channel_id: c1\n      prompt: p\n      admit:\n        - event: ping\n      workspace:\n        type: none\n" + tc.extra)
			path := writeConfig(t, t.TempDir(), yaml)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(cfg.Webhooks.Endpoints) != 1 {
				t.Fatalf("endpoints = %d, want 1", len(cfg.Webhooks.Endpoints))
			}
			ep := cfg.Webhooks.Endpoints[0]
			if got := ep.NotifyOnSkip(); got != tc.want {
				t.Fatalf("NotifyOnSkip() = %v, want %v", got, tc.want)
			}
			if tc.wantNil && ep.NotifySkip != nil {
				t.Fatalf("NotifySkip = %v, want nil for absent key", *ep.NotifySkip)
			}
			if !tc.wantNil && ep.NotifySkip == nil {
				t.Fatal("NotifySkip is nil, want non-nil for explicit key")
			}
		})
	}
}
