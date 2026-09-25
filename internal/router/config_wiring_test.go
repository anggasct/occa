package router

import (
	"testing"
	"time"
)

func TestRouterConfigWiringReachesBrokers(t *testing.T) {
	cfg := Config{
		ContextStaleAfter:      7 * time.Minute,
		ProgressQuietThreshold: 33 * time.Second,
		MaxQueuedMessages:      9,
		MaxPickerSessions:      11,
		MaxPickerPages:         13,
		ModelBrowserTTL:        17 * time.Minute,
		ModelBrowserPage:       19,
		ModelBrowserNavRows:    21,
		ModelBrowserCap:        23,
		AgentBrowserTTL:        27 * time.Minute,
		AgentBrowserCap:        29,
		QuestionTombstoneTTL:   31 * time.Minute,
		PermissionTombstoneTTL: 37 * time.Minute,
		RecoveryBudget:         41 * time.Second,
		RecoveryBaseBackoff:    43 * time.Second,
		RecoveryMaxBackoff:     47 * time.Second,
		UsagePageSize:          53,
		UsageDefaultWindow:     59 * time.Hour,
		StreamerNoEventTimeout: 61 * time.Minute,
	}
	r, _, _, _ := newTestRouterWithAccess()
	*r = *NewWithAllowlists(r.instances, r.store, r.defaultWorkdir, r.adminID, r.discordSenders, r.telegramSenders, cfg)
	if r.responses.maxQueued != 9 {
		t.Fatalf("maxQueued = %d, want 9", r.responses.maxQueued)
	}
	if r.permissions.tombstoneTTL != 37*time.Minute {
		t.Fatalf("permission ttl = %v, want 37m", r.permissions.tombstoneTTL)
	}
	if r.questions.tombstoneTTL != 31*time.Minute {
		t.Fatalf("question ttl = %v, want 31m", r.questions.tombstoneTTL)
	}
	if r.modelBrowser.ttl != 17*time.Minute || r.modelBrowser.page != 19 || r.modelBrowser.navRow != 21 || r.modelBrowser.capSize != 23 {
		t.Fatalf("model browser = %+v, want 17m/19/21/23", r.modelBrowser)
	}
	if r.agentBrowser.ttl != 27*time.Minute || r.agentBrowser.capSize != 29 {
		t.Fatalf("agent browser = %+v, want 27m/29", r.agentBrowser)
	}
	if r.recovery.baseDelay != 43*time.Second || r.recovery.maxDelay != 47*time.Second {
		t.Fatalf("recovery backoff = %v/%v, want 43s/47s", r.recovery.baseDelay, r.recovery.maxDelay)
	}
	if r.recoveryBudget != 41*time.Second {
		t.Fatalf("recovery budget = %v, want 41s", r.recoveryBudget)
	}
	if r.contextStaleAfter != 7*time.Minute {
		t.Fatalf("staleAfter = %v, want 7m", r.contextStaleAfter)
	}
	if r.progressQuietThreshold != 33*time.Second {
		t.Fatalf("quiet = %v, want 33s", r.progressQuietThreshold)
	}
	if r.maxPickerSessions != 11 || r.maxPickerPages != 13 {
		t.Fatalf("picker = %d/%d, want 11/13", r.maxPickerSessions, r.maxPickerPages)
	}
	if r.usagePageSize != 53 {
		t.Fatalf("usage page = %d, want 53", r.usagePageSize)
	}
	if r.usageDefaultWindow != 59*time.Hour {
		t.Fatalf("usage window = %v, want 59h", r.usageDefaultWindow)
	}
	if r.streamerNoEventTimeout != 61*time.Minute {
		t.Fatalf("streamer timeout = %v, want 61m", r.streamerNoEventTimeout)
	}
	since := r.usageSince("7d", time.Now().UTC())
	if since == 0 {
		t.Fatal("custom usage window produced zero since")
	}
}
