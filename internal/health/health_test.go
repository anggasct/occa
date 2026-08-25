package health

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeStore struct {
	pingErr error
	ver     int
	verErr  error
}

func (f fakeStore) Ping(ctx context.Context) error { return f.pingErr }

func (f fakeStore) SchemaVersion(context.Context) (int, error) { return f.ver, f.verErr }

type fakeAgent struct {
	pid int
	ok  bool
}

func (f fakeAgent) Running(context.Context) (int, bool, error) { return f.pid, f.ok, nil }

type fakeChannel struct {
	name      string
	connected bool
	detail    string
}

func (f fakeChannel) Name() string              { return f.name }
func (f fakeChannel) Connected() (bool, string) { return f.connected, f.detail }

type fakeWebhook struct {
	addr    string
	healthy bool
}

func (f fakeWebhook) Addr() string  { return f.addr }
func (f fakeWebhook) Healthy() bool { return f.healthy }

func healthyReporter() *Reporter {
	return New(
		WithStore(fakeStore{ver: 8}),
		WithAgent(fakeAgent{pid: 1234, ok: true}),
		WithChannels(
			fakeChannel{name: "telegram", connected: true},
			fakeChannel{name: "discord", connected: true},
		),
		WithWebhook(fakeWebhook{addr: "127.0.0.1:8787", healthy: true}),
		WithVersion("1.0.0"),
		WithExpectedSchema(8),
	)
}

func TestRunHealthy(t *testing.T) {
	report := healthyReporter().Run(context.Background())
	if report.Status != StatusHealthy {
		t.Fatalf("status = %s, want healthy", report.Status)
	}
	rendered := report.Render()
	for _, want := range []string{
		"🟢 OCCA healthy",
		"Binary: 1.0.0",
		"DB: connected (schema v8)",
		"Agent: connected (pid 1234)",
		"Discord: connected",
		"Telegram: connected",
		"Webhook: listening 127.0.0.1:8787",
		"Last error: none",
		"Uptime: 0s",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("render missing %q:\n%s", want, rendered)
		}
	}
	if strings.Index(rendered, "Discord:") > strings.Index(rendered, "Telegram:") {
		t.Errorf("channels not rendered in stable order:\n%s", rendered)
	}
}

func TestRunDegradedAgentNotRunning(t *testing.T) {
	rep := healthyReporter()
	rep.agent = fakeAgent{ok: false}
	report := rep.Run(context.Background())
	if report.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded", report.Status)
	}
	for _, p := range report.Probes {
		if p.Name == "agent" && p.Detail != "not running" {
			t.Errorf("agent detail = %q, want not running", p.Detail)
		}
	}
	if !strings.Contains(report.Render(), "🟡 OCCA degraded") {
		t.Errorf("render missing degraded header:\n%s", report.Render())
	}
}

func TestRunDegradedSchemaMismatch(t *testing.T) {
	rep := healthyReporter()
	rep.store = fakeStore{ver: 7}
	report := rep.Run(context.Background())
	if report.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded", report.Status)
	}
	if !strings.Contains(report.Render(), "DB: schema v7 (expected v8)") {
		t.Errorf("render missing schema mismatch:\n%s", report.Render())
	}
}

func TestRunDegradedSchemaUnknown(t *testing.T) {
	rep := healthyReporter()
	rep.store = fakeStore{ver: 0, verErr: context.DeadlineExceeded}
	report := rep.Run(context.Background())
	if report.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded", report.Status)
	}
	if !strings.Contains(report.Render(), "DB: schema unknown") {
		t.Errorf("render missing schema unknown:\n%s", report.Render())
	}
}

func TestRunUnhealthyStoreDown(t *testing.T) {
	rep := healthyReporter()
	rep.store = fakeStore{pingErr: context.DeadlineExceeded}
	report := rep.Run(context.Background())
	if report.Status != StatusUnhealthy {
		t.Fatalf("status = %s, want unhealthy", report.Status)
	}
	rendered := report.Render()
	if !strings.Contains(rendered, "🔴 OCCA unhealthy") || !strings.Contains(rendered, "DB: unreachable") {
		t.Errorf("render missing unhealthy store:\n%s", rendered)
	}
}

func TestRunPartialDegradedChannels(t *testing.T) {
	rep := healthyReporter()
	rep.channels = []Channel{
		fakeChannel{name: "discord", connected: false, detail: "gateway not connected"},
		fakeChannel{name: "telegram", connected: true},
	}
	report := rep.Run(context.Background())
	if report.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded", report.Status)
	}
	rendered := report.Render()
	if !strings.Contains(rendered, "Discord: gateway not connected") || !strings.Contains(rendered, "Telegram: connected") {
		t.Errorf("render missing partial-degraded channels:\n%s", rendered)
	}
}

func TestRunWebhookNotConfigured(t *testing.T) {
	rep := healthyReporter()
	rep.webhook = nil
	report := rep.Run(context.Background())
	if report.Status != StatusHealthy {
		t.Fatalf("status = %s, want healthy", report.Status)
	}
	if !strings.Contains(report.Render(), "Webhook: not configured") {
		t.Errorf("render missing webhook not configured:\n%s", report.Render())
	}
}

func TestRunBoundedWhenProbeHangs(t *testing.T) {
	rep := healthyReporter()
	rep.probeTimeout = 100 * time.Millisecond
	rep.channels = []Channel{hangChannel{name: "discord"}}

	start := time.Now()
	report := rep.Run(context.Background())
	elapsed := time.Since(start)

	if elapsed >= 2*rep.probeTimeout {
		t.Fatalf("run took %s, want bounded by probe timeout", elapsed)
	}
	found := false
	for _, p := range report.Probes {
		if p.Name == "discord" && p.Status == StatusDegraded && p.Detail == "probe timed out" {
			found = true
		}
	}
	if !found {
		t.Errorf("hanging probe not reported as timed out: %+v", report.Probes)
	}
}

func TestRunBoundsMultipleHangingProbesTogether(t *testing.T) {
	rep := healthyReporter()
	rep.probeTimeout = 100 * time.Millisecond
	rep.channels = []Channel{
		hangChannel{name: "discord"},
		hangChannel{name: "telegram"},
	}

	start := time.Now()
	report := rep.Run(context.Background())
	if elapsed := time.Since(start); elapsed >= 2*rep.probeTimeout {
		t.Fatalf("run took %s, want independent probes to share one timeout window", elapsed)
	}
	for _, name := range []string{"discord", "telegram"} {
		found := false
		for _, p := range report.Probes {
			if p.Name == name && p.Status == StatusDegraded && p.Detail == "probe timed out" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s hanging probe not reported as timed out: %+v", name, report.Probes)
		}
	}
}

type hangChannel struct{ name string }

func (h hangChannel) Name() string { return h.name }
func (h hangChannel) Connected() (bool, string) {
	time.Sleep(5 * time.Second)
	return true, ""
}

func TestRunIndependentProbesWhenStoreDown(t *testing.T) {
	rep := healthyReporter()
	rep.store = fakeStore{pingErr: context.DeadlineExceeded}
	report := rep.Run(context.Background())
	byName := make(map[string]Probe)
	for _, p := range report.Probes {
		byName[p.Name] = p
	}
	if byName["store"].Status != StatusUnhealthy {
		t.Errorf("store probe = %s, want unhealthy", byName["store"].Status)
	}
	if byName["agent"].Status != StatusHealthy {
		t.Errorf("agent probe = %s, want healthy (store failure hides it)", byName["agent"].Status)
	}
	if byName["discord"].Status != StatusHealthy || byName["telegram"].Status != StatusHealthy {
		t.Errorf("channel probes hidden by store failure: %+v", report.Probes)
	}
}

func TestRecordErrorRedactsSecrets(t *testing.T) {
	const secret = "super-secret-token-abc"
	scrub := func(s string) string { return strings.ReplaceAll(s, secret, "[REDACTED]") }
	rep := New(
		WithStore(fakeStore{ver: 8}),
		WithAgent(fakeAgent{pid: 1, ok: true}),
		WithLastError(NewLastError(scrub)),
	)
	rep.RecordError("relay: got 401 with " + secret + "\nnext line")
	report := rep.Run(context.Background())
	if strings.Contains(report.LastError, secret) {
		t.Fatalf("last error leaked secret: %q", report.LastError)
	}
	if !strings.Contains(report.LastError, "[REDACTED]") {
		t.Fatalf("last error not redacted: %q", report.LastError)
	}
	if strings.Contains(report.LastError, "\n") {
		t.Fatalf("last error contains newline: %q", report.LastError)
	}
	if strings.Contains(report.Render(), secret) {
		t.Fatalf("render leaked secret:\n%s", report.Render())
	}
}

func TestRecordErrorScrubsBeforeTruncating(t *testing.T) {
	const secret = "super-secret-token-abc"
	scrub := func(s string) string { return strings.ReplaceAll(s, secret, "[REDACTED]") }
	rep := New(
		WithStore(fakeStore{ver: 8}),
		WithAgent(fakeAgent{pid: 1, ok: true}),
		WithLastError(NewLastError(scrub)),
	)
	rep.RecordError(strings.Repeat("x", maxLastErrorRunes-15) + secret + " trailing detail")

	lastError, _ := rep.lastErr.Get()
	if strings.Contains(lastError, secret) {
		t.Fatalf("last error leaked secret across truncation boundary: %q", lastError)
	}
	if !strings.Contains(lastError, "[REDACTED]") {
		t.Fatalf("last error was truncated before scrubbing: %q", lastError)
	}
	if len([]rune(lastError)) > maxLastErrorRunes {
		t.Fatalf("last error exceeds bound: %d runes", len([]rune(lastError)))
	}
}

func TestRecordErrorTruncates(t *testing.T) {
	rep := New(WithStore(fakeStore{ver: 8}), WithAgent(fakeAgent{pid: 1, ok: true}))
	rep.RecordError(strings.Repeat("x", 1000))
	report := rep.Run(context.Background())
	if len([]rune(report.LastError)) > maxLastErrorRunes+1 {
		t.Fatalf("last error not truncated: %d runes", len([]rune(report.LastError)))
	}
}

func TestProbeFailureRecordsLastError(t *testing.T) {
	rep := healthyReporter()
	rep.store = fakeStore{pingErr: context.DeadlineExceeded}
	report := rep.Run(context.Background())
	if report.LastError != "store: unreachable" {
		t.Fatalf("last error = %q, want store: unreachable", report.LastError)
	}
}

func TestLogFieldsCarryProbeStatus(t *testing.T) {
	report := healthyReporter().Run(context.Background())
	fields := report.LogFields()
	if len(fields) < 4 {
		t.Fatalf("expected at least 4 log fields, got %d", len(fields))
	}
	if fields[0].Key != "status" || fields[0].Value.String() != string(StatusHealthy) {
		t.Errorf("first log field = %v, want status=healthy", fields[0])
	}
}
