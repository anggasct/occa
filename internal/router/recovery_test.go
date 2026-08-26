package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/process"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type fakeRecoveryEventRepo struct {
	mu     sync.Mutex
	events []store.RecoveryEvent
	putErr error
}

func newFakeRecoveryEventRepo() *fakeRecoveryEventRepo {
	return &fakeRecoveryEventRepo{}
}

func (f *fakeRecoveryEventRepo) Put(_ context.Context, e store.RecoveryEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	f.events = append(f.events, e)
	return nil
}

func (f *fakeRecoveryEventRepo) List(_ context.Context, limit int) ([]store.RecoveryEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit > 0 && len(f.events) > limit {
		return append([]store.RecoveryEvent(nil), f.events[len(f.events)-limit:]...), nil
	}
	return append([]store.RecoveryEvent(nil), f.events...), nil
}

func (f *fakeRecoveryEventRepo) snapshot() []store.RecoveryEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.RecoveryEvent(nil), f.events...)
}

// recoveryProvider wraps fakeInstanceProvider with failure injection: a
// spawn error, a channel that stalls the first recovery respawn (the
// Instance call right after a ForceStop), and stop counting for
// single-flight assertions.
type recoveryProvider struct {
	fakeInstanceProvider
	stops          int
	spawnErrOn     map[int64]error
	seq            atomic.Int64
	blockOnStop    chan struct{}
	blockAfterStop chan struct{}
	mu             sync.Mutex
}

func newRecoveryProvider(client *fakeRelayClient) *recoveryProvider {
	return &recoveryProvider{
		fakeInstanceProvider: fakeInstanceProvider{client: client},
		spawnErrOn:           make(map[int64]error),
	}
}

func (p *recoveryProvider) Instance(ctx context.Context, workdir string) (AgentInstance, error) {
	p.mu.Lock()
	block := p.blockAfterStop
	p.blockAfterStop = nil
	p.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := p.spawnErrOn[p.seq.Add(1)]; err != nil {
		p.mu.Lock()
		p.calls++
		p.mu.Unlock()
		return nil, err
	}
	return p.fakeInstanceProvider.Instance(ctx, workdir)
}

func (p *recoveryProvider) ForceStop(workdir string) {
	p.mu.Lock()
	p.stops++
	if p.blockOnStop != nil {
		p.blockAfterStop = p.blockOnStop
		p.blockOnStop = nil
	}
	p.mu.Unlock()
	p.fakeInstanceProvider.ForceStop(workdir)
}

func (p *recoveryProvider) stopCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stops
}

func newRecoveryTestRouter(provider InstanceProvider, client *fakeRelayClient, sessions *fakeSessionRepo) (*Router, *fakeRecoveryEventRepo) {
	overrideRepo := newFakeOverrideRepo()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "user1",
		Role:      "admin",
	}
	overrideRepo.overrides["telegram:chat1:user2"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "user2",
		Role:      "allow",
	}
	overrideRepo.overrides["telegram:chat1:user3"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "user3",
		Role:      "allow",
	}
	if sessions == nil {
		sessions = &fakeSessionRepo{}
	}
	recoveryRepo := newFakeRecoveryEventRepo()
	st := &fakeStore{
		sessionRepo:    sessions,
		channelRepo:    newFakeChannelRepo(),
		overrideRepo:   overrideRepo,
		scheduleRepo:   &fakeScheduleRepo{},
		recoveryEvents: recoveryRepo,
	}
	return New(provider, st, "/default-workdir", ""), recoveryRepo
}

func waitForRecoveryEvents(t *testing.T, repo *fakeRecoveryEventRepo, want int) []store.RecoveryEvent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events := repo.snapshot()
		if len(events) >= want {
			return events
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected at least %d recovery events, got %d", want, len(repo.snapshot()))
	return nil
}

func TestRecoveryCoordinatorSingleFlight(t *testing.T) {
	c := newRecoveryCoordinator()
	if ok, reason := c.beginAttempt("/w"); !ok {
		t.Fatalf("first beginAttempt refused: %s", reason)
	}
	if ok, reason := c.beginAttempt("/w"); ok || reason != "in_flight" {
		t.Fatalf("second beginAttempt = (%v, %q), want refused in_flight", ok, reason)
	}
	c.finishAttempt("/w", store.RecoveryOutcomeResumed)
	if ok, reason := c.beginAttempt("/w"); !ok {
		t.Fatalf("beginAttempt after finish refused: %s", reason)
	}
}

func TestRecoveryCoordinatorSuppressedTriggersLeaveBackoffUnchanged(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newRecoveryCoordinator()
	c.now = func() time.Time { return now }
	c.baseDelay = 10 * time.Second

	if ok, _ := c.beginAttempt("/w"); !ok {
		t.Fatal("first attempt must be allowed")
	}
	c.finishAttempt("/w", store.RecoveryOutcomeFailed)
	for i := 0; i < 3; i++ {
		if ok, reason := c.beginAttempt("/w"); ok || reason != "backoff" {
			t.Fatalf("refused trigger %d = (%v, %q), want backoff refusal", i, ok, reason)
		}
	}
	now = now.Add(10 * time.Second)
	if ok, reason := c.beginAttempt("/w"); !ok {
		t.Fatalf("retry at base backoff refused (%s) — refusals must not extend the window", reason)
	}
}

func TestRecoveryCoordinatorBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newRecoveryCoordinator()
	c.now = func() time.Time { return now }
	c.baseDelay = 10 * time.Second
	c.maxDelay = 40 * time.Second

	if ok, _ := c.beginAttempt("/w"); !ok {
		t.Fatal("first attempt must be allowed")
	}
	c.finishAttempt("/w", store.RecoveryOutcomeFailed)
	if ok, reason := c.beginAttempt("/w"); ok || reason != "backoff" {
		t.Fatalf("immediate retry = (%v, %q), want refused backoff", ok, reason)
	}
	now = now.Add(10 * time.Second)
	if ok, _ := c.beginAttempt("/w"); !ok {
		t.Fatal("retry after base backoff must be allowed")
	}
	c.finishAttempt("/w", store.RecoveryOutcomeFailed)

	now = now.Add(10 * time.Second)
	if ok, _ := c.beginAttempt("/w"); ok {
		t.Fatal("second consecutive failure must double the backoff window")
	}
	now = now.Add(11 * time.Second)
	if ok, _ := c.beginAttempt("/w"); !ok {
		t.Fatal("retry after doubled backoff must be allowed")
	}
	c.finishAttempt("/w", store.RecoveryOutcomeResumed)

	now = now.Add(time.Second)
	if ok, _ := c.beginAttempt("/w"); !ok {
		t.Fatal("success must reset the failure counter to base backoff")
	}
}

func TestClassifyDispatchFailure(t *testing.T) {
	if _, ok := classifyDispatchFailure(relay.ErrTimeout); !ok {
		t.Error("send timeout must classify as recoverable")
	}
	if _, ok := classifyDispatchFailure(fmt.Errorf("relay: %w: boom", relay.ErrUnreachable)); !ok {
		t.Error("unreachable must classify as recoverable")
	}
	if _, ok := classifyDispatchFailure(errors.New("unexpected status 500")); ok {
		t.Error("unknown errors must not classify as recoverable")
	}
}

func TestRecoveryAfterAgentExitResumesSession(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	r, repo := newRecoveryTestRouter(provider, client, nil)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("hello agent", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	ev := events[0]
	if ev.Trigger != store.RecoveryTriggerProcessExit {
		t.Errorf("trigger = %q, want process_exit", ev.Trigger)
	}
	if ev.Outcome != store.RecoveryOutcomeResumed {
		t.Errorf("outcome = %q, want resumed", ev.Outcome)
	}
	if !strings.HasPrefix(ev.CorrelationID, "rcv-") {
		t.Errorf("correlation id = %q, want rcv- prefix", ev.CorrelationID)
	}
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1", provider.stopCount())
	}
	if provider.calls != 2 {
		t.Errorf("Instance calls = %d, want 2 (initial + respawn)", provider.calls)
	}
	if client.sendCalls != 1 {
		t.Errorf("send calls = %d, want 1 — the failed prompt must not be replayed", client.sendCalls)
	}
	joined := strings.Join(reply.sends, "\n")
	if !strings.Contains(joined, "resumed") || !strings.Contains(joined, "/status") {
		t.Errorf("reply missing resume result or /status guidance: %q", joined)
	}
}

func TestRecoveryAfterSendTimeoutRestartsAgent(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: timeout", relay.ErrTimeout)
	provider := newRecoveryProvider(client)
	r, repo := newRecoveryTestRouter(provider, client, nil)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("slow prompt", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Trigger != store.RecoveryTriggerSendTimeout {
		t.Errorf("trigger = %q, want send_timeout", events[0].Trigger)
	}
	if events[0].Outcome != store.RecoveryOutcomeResumed {
		t.Errorf("outcome = %q, want resumed", events[0].Outcome)
	}
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1", provider.stopCount())
	}
}

func TestRecoveryRecreatesMissingSessionAndReportsContextLoss(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	client.missingSessions = map[string]bool{"sess-0": true}
	provider := newRecoveryProvider(client)
	r, repo := newRecoveryTestRouter(provider, client, nil)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("hello again", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Outcome != store.RecoveryOutcomeRecreated {
		t.Errorf("outcome = %q, want recreated", events[0].Outcome)
	}
	joined := strings.Join(reply.sends, "\n")
	if !strings.Contains(joined, "lost") || !strings.Contains(joined, "context is gone") {
		t.Errorf("reply missing context-loss report: %q", joined)
	}
	if !strings.Contains(joined, "/status") {
		t.Errorf("reply missing /status guidance: %q", joined)
	}
	if client.sendCalls != 1 {
		t.Errorf("send calls = %d, want 1 — no prompt replay after context loss", client.sendCalls)
	}
}

func waitForReplyContaining(t *testing.T, reply *fakeReplyCtx, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(reply.sentText(), want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("reply %q never contained %q", reply.sentText(), want)
}

func TestRecoveryDuplicateTriggerRunsSingleAttempt(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	unblock := make(chan struct{})
	provider.blockOnStop = unblock // stall the first recovery respawn
	r, _ := newRecoveryTestRouter(provider, client, nil)

	reply1 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "first", reply1)); err != nil {
		t.Fatalf("Route: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && provider.stopCount() == 0 {
		time.Sleep(time.Millisecond)
	}
	if provider.stopCount() != 1 {
		t.Fatalf("first recovery did not start, stops = %d", provider.stopCount())
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user2", "second", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForReplyContaining(t, reply2, "skipped")

	close(unblock)
	waitForResponse(t, r)
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1 for the whole window", provider.stopCount())
	}
	if provider.calls != 3 {
		t.Errorf("Instance calls = %d, want 3 (two initial + one respawn)", provider.calls)
	}
}

func TestRecoveryBackoffSuppressesRepeatedFailures(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	provider.spawnErrOn[2] = fmt.Errorf("process: spawn: %w", errors.New("binary not found"))
	r, repo := newRecoveryTestRouter(provider, client, nil)
	r.recovery.baseDelay = time.Hour

	reply1 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "first", reply1)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)
	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Outcome != store.RecoveryOutcomeFailed {
		t.Fatalf("outcome = %q, want failed", events[0].Outcome)
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user2", "second", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)
	events = waitForRecoveryEvents(t, repo, 2)
	if events[1].Outcome != store.RecoveryOutcomeSuppressed {
		t.Fatalf("outcome = %q, want suppressed", events[1].Outcome)
	}
	if joined := strings.Join(reply2.sends, "\n"); !strings.Contains(joined, "skipped") {
		t.Errorf("suppressed reply missing guidance: %q", joined)
	}
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1 — backoff must block a second stop", provider.stopCount())
	}
}

func TestRecoveryStreamTerminatedSessionIntactSkipsRestart(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.incompleteStream = true
	provider := newRecoveryProvider(client)
	sessions := &fakeSessionRepo{activeID: "sess-0"}
	r, repo := newRecoveryTestRouter(provider, client, sessions)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("stream drops", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Trigger != store.RecoveryTriggerStreamEnded {
		t.Errorf("trigger = %q, want stream_terminated", events[0].Trigger)
	}
	if events[0].Outcome != store.RecoveryOutcomeResumed {
		t.Errorf("outcome = %q, want resumed", events[0].Outcome)
	}
	if provider.stopCount() != 0 {
		t.Errorf("ForceStop calls = %d, want 0 — intact session must not restart", provider.stopCount())
	}
	if provider.calls != 1 {
		t.Errorf("Instance calls = %d, want 1 (no respawn)", provider.calls)
	}
	joined := strings.Join(reply.sends, "\n")
	if !strings.Contains(joined, "intact") || !strings.Contains(joined, "/status") {
		t.Errorf("reply missing intact-session report: %q", joined)
	}
}

func TestRecoveryStreamTerminatedMissingSessionRestarts(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.incompleteStream = true
	client.missingSessions = map[string]bool{"sess-0": true}
	provider := newRecoveryProvider(client)
	sessions := &fakeSessionRepo{activeID: "sess-0"}
	r, repo := newRecoveryTestRouter(provider, client, sessions)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("stream drops hard", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Outcome != store.RecoveryOutcomeRecreated {
		t.Errorf("outcome = %q, want recreated", events[0].Outcome)
	}
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1", provider.stopCount())
	}
	if provider.calls != 2 {
		t.Errorf("Instance calls = %d, want 2 (respawn after loss)", provider.calls)
	}
	if joined := strings.Join(reply.sends, "\n"); !strings.Contains(joined, "context is gone") {
		t.Errorf("reply missing context-loss report: %q", joined)
	}
}

func TestRecoveryReadinessFailureReportsActionableStatus(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	provider := newRecoveryProvider(client)
	provider.spawnErrOn[1] = fmt.Errorf("process: readiness for %q: %w after 30s", "/w", process.ErrReadinessTimeout)
	r, repo := newRecoveryTestRouter(provider, client, nil)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("agent never starts", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Trigger != store.RecoveryTriggerReadinessTimeout {
		t.Errorf("trigger = %q, want readiness_timeout", events[0].Trigger)
	}
	if events[0].Outcome != store.RecoveryOutcomeFailed {
		t.Errorf("outcome = %q, want failed", events[0].Outcome)
	}
	if joined := strings.Join(reply.sends, "\n"); !strings.Contains(joined, "failed to start") || !strings.Contains(joined, "/status") {
		t.Errorf("reply missing actionable start failure: %q", joined)
	}
	if client.sendCalls != 0 {
		t.Errorf("send calls = %d, want 0", client.sendCalls)
	}
	r.responses.mu.Lock()
	active := len(r.responses.active)
	r.responses.mu.Unlock()
	if active != 0 {
		t.Fatalf("response slot still held after readiness failure: %d active", active)
	}
}

func TestRecoveryCanceledDuringRespawnTerminates(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	provider.blockOnStop = make(chan struct{})
	r, repo := newRecoveryTestRouter(provider, client, nil)
	r.recoveryBudget = 100 * time.Millisecond
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("cancel me", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := waitForRecoveryEvents(t, repo, 1)
	if events[0].Outcome != store.RecoveryOutcomeFailed {
		t.Errorf("outcome = %q, want failed after cancellation", events[0].Outcome)
	}
	r.responses.mu.Lock()
	active := len(r.responses.active)
	r.responses.mu.Unlock()
	if active != 0 {
		t.Fatalf("response slot leaked after canceled recovery: %d active", active)
	}
}

func TestRecoveryThirdTriggerDuringInFlightRecoveryStaysSuppressed(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	unblock := make(chan struct{})
	provider.blockOnStop = unblock // stall the first recovery respawn
	r, repo := newRecoveryTestRouter(provider, client, nil)

	reply1 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "first", reply1)); err != nil {
		t.Fatalf("Route user1: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && provider.stopCount() == 0 {
		time.Sleep(time.Millisecond)
	}
	if provider.stopCount() != 1 {
		t.Fatalf("first recovery did not start, stops = %d", provider.stopCount())
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user2", "second", reply2)); err != nil {
		t.Fatalf("Route user2: %v", err)
	}
	waitForReplyContaining(t, reply2, "skipped")

	reply3 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user3", "third", reply3)); err != nil {
		t.Fatalf("Route user3: %v", err)
	}
	waitForReplyContaining(t, reply3, "skipped")

	close(unblock)
	waitForResponse(t, r)

	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1 — a suppressed trigger must not clear the in-flight slot", provider.stopCount())
	}
	if provider.calls != 4 {
		t.Errorf("Instance calls = %d, want 4 (three initial spawns + one respawn)", provider.calls)
	}
	events := repo.snapshot()
	if len(events) != 3 {
		t.Fatalf("recovery events = %d, want 3", len(events))
	}
	nonSuppressed := 0
	for _, ev := range events {
		if ev.Outcome != store.RecoveryOutcomeSuppressed {
			nonSuppressed++
		}
	}
	if nonSuppressed != 1 {
		t.Errorf("non-suppressed recovery outcomes = %d, want exactly 1", nonSuppressed)
	}
	if last := events[len(events)-1]; last.Outcome != store.RecoveryOutcomeResumed {
		t.Errorf("final outcome = %q, want resumed — the in-flight attempt must finish undisturbed", last.Outcome)
	}
}

func TestRecoveryRepeatedSuppressionDoesNotEscalateBackoff(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	provider := newRecoveryProvider(client)
	provider.spawnErrOn[2] = errors.New("spawn failed")
	r, _ := newRecoveryTestRouter(provider, client, nil)

	current := time.Unix(1000, 0)
	r.recovery.now = func() time.Time { return current }
	r.recovery.baseDelay = time.Hour
	r.recovery.maxDelay = 4 * time.Hour

	reply1 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "first", reply1)); err != nil {
		t.Fatalf("Route user1: %v", err)
	}
	waitForReplyContaining(t, reply1, "recovery failed")

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user2", "second", reply2)); err != nil {
		t.Fatalf("Route user2: %v", err)
	}
	waitForReplyContaining(t, reply2, "skipped")

	current = current.Add(time.Hour + time.Minute)

	reply3 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user3", "third", reply3)); err != nil {
		t.Fatalf("Route user3: %v", err)
	}
	waitForReplyContaining(t, reply3, "restarted")
	if strings.Contains(reply3.sentText(), "skipped") {
		t.Fatalf("suppression must not extend the backoff window: %q", reply3.sentText())
	}
}

func TestRecoveryDispatchAndStreamBothTerminalRunSingleRecovery(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-0"}
	client.sendErr = fmt.Errorf("relay: %w: connection refused", relay.ErrUnreachable)
	client.closeEventsOnSendErr = true
	provider := newRecoveryProvider(client)
	r, repo := newRecoveryTestRouter(provider, client, nil)
	reply := &fakeReplyCtx{}

	if err := r.Route(context.Background(), msg("double failure", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	events := repo.snapshot()
	if len(events) != 1 {
		t.Fatalf("recovery events = %d, want exactly 1 for a double-terminal response", len(events))
	}
	if events[0].Trigger != store.RecoveryTriggerProcessExit {
		t.Errorf("trigger = %q, want process_exit — the dispatch root cause wins", events[0].Trigger)
	}
	if events[0].Outcome != store.RecoveryOutcomeResumed {
		t.Errorf("outcome = %q, want resumed", events[0].Outcome)
	}
	if provider.stopCount() != 1 {
		t.Errorf("ForceStop calls = %d, want 1", provider.stopCount())
	}
	if provider.calls != 2 {
		t.Errorf("Instance calls = %d, want 2 (initial + one respawn)", provider.calls)
	}
	joined := reply.sentText()
	if strings.Count(joined, "The agent was restarted") != 1 {
		t.Errorf("expected exactly one recovery outcome notice, got: %q", joined)
	}
	if strings.Contains(joined, "skipped") {
		t.Errorf("a single response must never report suppression: %q", joined)
	}
}
