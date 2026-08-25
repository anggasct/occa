package health

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
	"unicode"
)

type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusDegraded  Status = "degraded"
	StatusUnhealthy Status = "unhealthy"
)

const defaultProbeTimeout = 1500 * time.Millisecond

type Probe struct {
	Name   string
	Status Status
	Detail string
}

type Report struct {
	Status    Status
	Probes    []Probe
	Version   string
	Uptime    time.Duration
	LastError string
}

type Store interface {
	Ping(ctx context.Context) error
	SchemaVersion(ctx context.Context) (int, error)
}

// Agent reports on the agent backend without creating an instance or a
// session. A pooled, live agent process with a pid is "running".
type Agent interface {
	Running(ctx context.Context) (pid int, ok bool, err error)
}

type Channel interface {
	Name() string
	Connected() (bool, string)
}

type Webhook interface {
	Addr() string
	Healthy() bool
}

type Reporter struct {
	store          Store
	agent          Agent
	channels       []Channel
	webhook        Webhook
	version        string
	expectedSchema int
	probeTimeout   time.Duration
	startedAt      time.Time
	lastErr        *LastError
}

type Option func(*Reporter)

func WithStore(s Store) Option {
	return func(r *Reporter) { r.store = s }
}

func WithAgent(a Agent) Option {
	return func(r *Reporter) { r.agent = a }
}

func WithChannels(cs ...Channel) Option {
	return func(r *Reporter) { r.channels = cs }
}

func WithWebhook(w Webhook) Option {
	return func(r *Reporter) { r.webhook = w }
}

func WithVersion(v string) Option {
	return func(r *Reporter) { r.version = v }
}

func WithExpectedSchema(version int) Option {
	return func(r *Reporter) { r.expectedSchema = version }
}

func WithProbeTimeout(d time.Duration) Option {
	return func(r *Reporter) { r.probeTimeout = d }
}

func WithLastError(l *LastError) Option {
	return func(r *Reporter) { r.lastErr = l }
}

func New(opts ...Option) *Reporter {
	r := &Reporter{
		probeTimeout: defaultProbeTimeout,
		startedAt:    time.Now(),
		lastErr:      NewLastError(nil),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Reporter) RecordError(msg string) {
	if r.lastErr != nil {
		r.lastErr.Set(msg)
	}
}

// Run executes every probe concurrently with its own short deadline and
// aggregates them into one Report. A slow or failing subsystem never hides
// the others, and the whole run is bounded by the probe timeout.
func (r *Reporter) Run(ctx context.Context) Report {
	type probeJob struct {
		name string
		fn   func(context.Context) Probe
	}
	jobs := []probeJob{
		{name: "store", fn: r.probeStore},
		{name: "agent", fn: r.probeAgent},
	}
	for _, ch := range sortedChannels(r.channels) {
		ch := ch
		jobs = append(jobs, probeJob{name: ch.Name(), fn: func(context.Context) Probe {
			return probeChannel(ch)
		}})
	}
	jobs = append(jobs, probeJob{name: "webhook", fn: func(context.Context) Probe {
		return r.probeWebhook()
	}})

	probes := make([]Probe, len(jobs))
	results := make(chan struct {
		index int
		probe Probe
	}, len(jobs))
	for i, job := range jobs {
		go func(index int, job probeJob) {
			results <- struct {
				index int
				probe Probe
			}{index: index, probe: r.probe(ctx, job.name, job.fn)}
		}(i, job)
	}
	for range jobs {
		result := <-results
		probes[result.index] = result.probe
	}

	for _, p := range probes {
		if p.Status != StatusHealthy {
			r.RecordError(p.Name + ": " + p.Detail)
		}
	}

	var lastErr string
	if r.lastErr != nil {
		lastErr, _ = r.lastErr.Get()
	}
	return Report{
		Status:    overall(probes),
		Probes:    probes,
		Version:   r.version,
		Uptime:    time.Since(r.startedAt).Truncate(time.Second),
		LastError: lastErr,
	}
}

// probe runs fn with a fresh, short deadline derived from the caller's
// context. A probe that ignores its deadline is still reported as degraded
// instead of hanging the report.
func (r *Reporter) probe(ctx context.Context, name string, fn func(context.Context) Probe) Probe {
	probeCtx, cancel := context.WithTimeout(ctx, r.probeTimeout)
	defer cancel()

	done := make(chan Probe, 1)
	go func() {
		done <- fn(probeCtx)
	}()
	select {
	case p := <-done:
		return p
	case <-probeCtx.Done():
		return Probe{Name: name, Status: StatusDegraded, Detail: "probe timed out"}
	}
}

func (r *Reporter) probeStore(ctx context.Context) Probe {
	if r.store == nil {
		return Probe{Name: "store", Status: StatusHealthy, Detail: "not configured"}
	}
	if err := r.store.Ping(ctx); err != nil {
		return Probe{Name: "store", Status: StatusUnhealthy, Detail: "unreachable"}
	}
	version, err := r.store.SchemaVersion(ctx)
	if err != nil {
		return Probe{Name: "store", Status: StatusDegraded, Detail: "schema unknown"}
	}
	if r.expectedSchema > 0 && version != r.expectedSchema {
		return Probe{Name: "store", Status: StatusDegraded, Detail: fmt.Sprintf("schema v%d (expected v%d)", version, r.expectedSchema)}
	}
	return Probe{Name: "store", Status: StatusHealthy, Detail: fmt.Sprintf("connected (schema v%d)", version)}
}

func (r *Reporter) probeAgent(ctx context.Context) Probe {
	if r.agent == nil {
		return Probe{Name: "agent", Status: StatusHealthy, Detail: "not configured"}
	}
	pid, ok, err := r.agent.Running(ctx)
	if err != nil {
		return Probe{Name: "agent", Status: StatusDegraded, Detail: "not running"}
	}
	if !ok {
		return Probe{Name: "agent", Status: StatusDegraded, Detail: "not running"}
	}
	return Probe{Name: "agent", Status: StatusHealthy, Detail: fmt.Sprintf("connected (pid %d)", pid)}
}

func probeChannel(ch Channel) Probe {
	ok, detail := ch.Connected()
	if ok {
		return Probe{Name: ch.Name(), Status: StatusHealthy, Detail: "connected"}
	}
	if detail == "" {
		detail = "not connected"
	}
	return Probe{Name: ch.Name(), Status: StatusDegraded, Detail: detail}
}

func (r *Reporter) probeWebhook() Probe {
	if r.webhook == nil {
		return Probe{Name: "webhook", Status: StatusHealthy, Detail: "not configured"}
	}
	if !r.webhook.Healthy() {
		return Probe{Name: "webhook", Status: StatusDegraded, Detail: "not listening"}
	}
	detail := "listening"
	if addr := r.webhook.Addr(); addr != "" {
		detail = "listening " + addr
	}
	return Probe{Name: "webhook", Status: StatusHealthy, Detail: detail}
}

func sortedChannels(channels []Channel) []Channel {
	out := make([]Channel, len(channels))
	copy(out, channels)
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

func overall(probes []Probe) Status {
	unhealthy, degraded := false, false
	for _, p := range probes {
		switch p.Status {
		case StatusUnhealthy:
			unhealthy = true
		case StatusDegraded:
			degraded = true
		}
	}
	switch {
	case unhealthy:
		return StatusUnhealthy
	case degraded:
		return StatusDegraded
	default:
		return StatusHealthy
	}
}

// Render formats the report for the chat surface, per the documented UX.
// It only ever prints probe labels, short details, and the sanitized last
// error — never raw errors, payloads, or secrets.
func (r Report) Render() string {
	var b strings.Builder
	switch r.Status {
	case StatusUnhealthy:
		b.WriteString("🔴 OCCA unhealthy\n")
	case StatusDegraded:
		b.WriteString("🟡 OCCA degraded\n")
	default:
		b.WriteString("🟢 OCCA healthy\n")
	}

	version := r.Version
	if version == "" {
		version = "dev"
	}
	fmt.Fprintf(&b, "Binary: %s\n", version)

	for _, p := range r.Probes {
		fmt.Fprintf(&b, "%s: %s\n", probeLabel(p.Name), p.Detail)
	}

	if r.LastError == "" {
		b.WriteString("Last error: none\n")
	} else {
		fmt.Fprintf(&b, "Last error: %s\n", r.LastError)
	}
	fmt.Fprintf(&b, "Uptime: %s\n", formatUptime(r.Uptime))
	return b.String()
}

func (r Report) LogFields() []slog.Attr {
	attrs := []slog.Attr{
		slog.String("status", string(r.Status)),
		slog.String("version", r.Version),
		slog.Duration("uptime", r.Uptime),
		slog.String("last_error", r.LastError),
	}
	for _, p := range r.Probes {
		attrs = append(attrs, slog.Group(p.Name,
			slog.String("status", string(p.Status)),
			slog.String("detail", p.Detail),
		))
	}
	return attrs
}

func probeLabel(name string) string {
	switch name {
	case "store":
		return "DB"
	case "agent":
		return "Agent"
	case "webhook":
		return "Webhook"
	}
	if name == "" {
		return name
	}
	runes := []rune(name)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func formatUptime(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	case m > 0:
		return fmt.Sprintf("%dm %ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}
