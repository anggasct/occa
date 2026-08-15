package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type responseReply struct {
	mu       sync.Mutex
	sends    []string
	edits    []string
	activity chan struct{}
}

type responseRef string

func (r responseRef) ID() string { return string(r) }

func newResponseReply() *responseReply {
	return &responseReply{activity: make(chan struct{}, 8)}
}

func (r *responseReply) SendTyping() error { return nil }

func (r *responseReply) Send(text string) (channel.MessageRef, error) {
	r.mu.Lock()
	r.sends = append(r.sends, text)
	r.mu.Unlock()
	r.signalActivity()
	return responseRef("response"), nil
}

func (r *responseReply) SendWithButtons(text string, _ []channel.Button) (channel.MessageRef, error) {
	return r.Send(text)
}

func (r *responseReply) Edit(_ channel.MessageRef, text string) error {
	r.mu.Lock()
	r.edits = append(r.edits, text)
	r.mu.Unlock()
	r.signalActivity()
	return nil
}

func (r *responseReply) EditWithButtons(_ channel.MessageRef, text string, _ []channel.Button) error {
	return r.Edit(nil, text)
}

func (r *responseReply) signalActivity() {
	select {
	case r.activity <- struct{}{}:
	default:
	}
}

func (r *responseReply) texts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(append([]string(nil), r.sends...), r.edits...)
}

func (r *responseReply) contains(text string) bool {
	for _, got := range r.texts() {
		if strings.Contains(got, text) {
			return true
		}
	}
	return false
}

func (r *responseReply) hasExact(text string) bool {
	for _, got := range r.texts() {
		if got == text {
			return true
		}
	}
	return false
}

type responseClient struct {
	mu           sync.Mutex
	events       chan relay.Event
	eventsCall   int
	sendCalls    int
	commandCalls int
	started      chan struct{}
	startOnce    sync.Once
	dispatch     func(context.Context, chan<- relay.Event) error
}

func newResponseClient(dispatch func(context.Context, chan<- relay.Event) error) *responseClient {
	return &responseClient{started: make(chan struct{}), dispatch: dispatch}
}

func (c *responseClient) CreateSession(_ context.Context) (string, error) { return "session", nil }

func (c *responseClient) GetSession(_ context.Context, _ string) (*relay.SessionInfo, error) {
	return &relay.SessionInfo{}, nil
}

func (c *responseClient) SessionExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (c *responseClient) AbortSession(_ context.Context, _ string) error { return nil }

func (c *responseClient) SendMessage(ctx context.Context, _ string, _ string, _ *relay.ModelRef, _ []relay.Attachment) error {
	c.mu.Lock()
	c.sendCalls++
	events := c.events
	dispatch := c.dispatch
	c.mu.Unlock()
	c.startOnce.Do(func() { close(c.started) })
	return c.runDispatch(ctx, events, dispatch)
}

func (c *responseClient) RunCommand(ctx context.Context, _ string, _ string) error {
	c.mu.Lock()
	c.commandCalls++
	events := c.events
	dispatch := c.dispatch
	c.mu.Unlock()
	c.startOnce.Do(func() { close(c.started) })
	return c.runDispatch(ctx, events, dispatch)
}

func (c *responseClient) runDispatch(ctx context.Context, events chan<- relay.Event, dispatch func(context.Context, chan<- relay.Event) error) error {
	if dispatch != nil {
		return dispatch(ctx, events)
	}
	events <- relay.Event{Type: "delta", Delta: "response"}
	events <- relay.Event{Type: "done"}
	close(events)
	return nil
}

func (c *responseClient) Providers(_ context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}

func (c *responseClient) ReplyPermission(_ context.Context, _ string, _ relay.PermissionReply) error {
	return nil
}
func (c *responseClient) AnswerQuestion(_ context.Context, _ string, _ [][]string) error {
	return nil
}
func (c *responseClient) RejectQuestion(_ context.Context, _ string) error {
	return nil
}
func (c *responseClient) ListCommands(_ context.Context) ([]relay.CommandInfo, error) {
	return nil, nil
}
func (c *responseClient) SummarizeSession(_ context.Context, _, _, _ string) error { return nil }
func (c *responseClient) RevertMessage(_ context.Context, _, _ string) error       { return nil }
func (c *responseClient) UnrevertSession(_ context.Context, _ string) error        { return nil }
func (c *responseClient) ListMessages(_ context.Context, _ string) ([]relay.MessageInfo, error) {
	return nil, nil
}

func (c *responseClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventsCall++
	c.events = make(chan relay.Event, 8)
	return c.events, nil
}

func (c *responseClient) eventCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.eventsCall
}

func newResponseRouter(client relay.Client) (*Router, *fakeStore) {
	overrides := newFakeOverrideRepo()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow",
	}
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}
	r := New(&fakeInstanceProvider{client: client}, st, "/default-workdir", "")
	return r, st
}

func responseMessage(userID, channelID, text string, reply channel.ReplyContext) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: channelID,
		UserID:    userID,
		Text:      text,
		IsMention: true,
		ReplyCtx:  reply,
	}
}

func waitForReply(t *testing.T, reply *responseReply, text string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if reply.contains(text) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("reply did not contain %q: %v", text, reply.texts())
}

func TestResponseCoordinatorSubscribesBeforeDispatch(t *testing.T) {
	client := newResponseClient(nil)
	r, _ := newResponseRouter(client)
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	waitForResponse(t, r)

	if client.eventCalls() != 1 {
		t.Fatalf("Events calls = %d, want 1", client.eventCalls())
	}
	if !reply.contains("response") {
		t.Fatalf("response was not delivered: %v", reply.texts())
	}
	if reply.contains("Response stream ended before completion") {
		t.Fatalf("complete response emitted incomplete notice: %v", reply.texts())
	}
}

func TestResponseCoordinatorUsesSamePathForCommands(t *testing.T) {
	client := newResponseClient(nil)
	r, _ := newResponseRouter(client)
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "/plan build", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("command did not start")
	}
	waitForResponse(t, r)

	client.mu.Lock()
	sends, commands := client.sendCalls, client.commandCalls
	client.mu.Unlock()
	if sends != 0 || commands != 1 {
		t.Fatalf("dispatch counts = send %d, command %d", sends, commands)
	}
	if !reply.contains("response") {
		t.Fatalf("command response was not delivered: %v", reply.texts())
	}
}

func waitForAllResponses(t *testing.T, r *Router) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.responses.mu.Lock()
		active := len(r.responses.active)
		queued := len(r.responses.queues)
		r.responses.mu.Unlock()
		if active == 0 && queued == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("responses did not finish")
}

func TestResponseCoordinatorBusyQueuesSecondPrompt(t *testing.T) {
	release := make(chan struct{})
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "partial"}
		select {
		case <-release:
			events <- relay.Event{Type: "done"}
			close(events)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()
	secondReply := newResponseReply()

	start := time.Now()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("first Route: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Route blocked for %s", elapsed)
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("first dispatch did not start")
	}
	waitForReply(t, firstReply, "partial")

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "second", secondReply)); err != nil {
		t.Fatalf("second Route: %v", err)
	}
	if !secondReply.hasExact("⏳ Queued — 1 message(s) will run after the current response finishes.") {
		t.Fatalf("queued response = %v", secondReply.texts())
	}
	if client.eventCalls() != 1 {
		t.Fatalf("Events calls = %d, want 1", client.eventCalls())
	}

	close(release)
	waitForAllResponses(t, r)

	if client.eventCalls() != 2 {
		t.Fatalf("Events calls = %d, want 2", client.eventCalls())
	}
}

func TestResponseCoordinatorDispatchAndStreamOverlap(t *testing.T) {
	reply := newResponseReply()
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "waiting"}
		select {
		case <-reply.activity:
		case <-ctx.Done():
			return ctx.Err()
		}
		events <- relay.Event{Type: "done"}
		close(events)
		return nil
	})
	r, _ := newResponseRouter(client)

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForReply(t, reply, "waiting")
	waitForResponse(t, r)
	if !reply.contains("waiting") {
		t.Fatalf("overlapped response missing: %v", reply.texts())
	}
}

func TestResponseCoordinatorDispatchFailureReleasesSlot(t *testing.T) {
	var calls int
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		calls++
		if calls == 1 {
			return errors.New("dispatch failed")
		}
		events <- relay.Event{Type: "delta", Delta: "recovered"}
		events <- relay.Event{Type: "done"}
		close(events)
		return nil
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()
	secondReply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("first Route: %v", err)
	}
	waitForReply(t, firstReply, "Agent unreachable")
	waitForResponse(t, r)

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "second", secondReply)); err != nil {
		t.Fatalf("second Route: %v", err)
	}
	waitForReply(t, secondReply, "recovered")
	waitForResponse(t, r)
	if calls != 2 {
		t.Fatalf("dispatch calls = %d, want 2", calls)
	}
}

func TestResponseCoordinatorPrematureEOFNotifiesAndReleasesSlot(t *testing.T) {
	client := newResponseClient(func(_ context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "partial"}
		close(events)
		return nil
	})
	r, _ := newResponseRouter(client)
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForReply(t, reply, "Response stream ended before completion")
	waitForResponse(t, r)
	if !reply.contains("partial") {
		t.Fatalf("buffered response was not final-synced: %v", reply.texts())
	}
	if !reply.hasExact("⚠️ Response stream ended before completion. The task may still be running; check /status.") {
		t.Fatalf("missing exact incomplete notice: %v", reply.texts())
	}
}

func TestResponseCoordinatorCancellationIsSilent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "partial"}
		<-ctx.Done()
		return ctx.Err()
	})
	r, _ := newResponseRouter(client)
	reply := newResponseReply()

	if err := r.Route(ctx, responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	cancel()
	waitForResponse(t, r)
	for _, text := range reply.texts() {
		if strings.Contains(text, "Agent unreachable") || strings.Contains(text, "Response stream ended") {
			t.Fatalf("cancellation sent failure message: %v", reply.texts())
		}
	}
}

type responseProvider struct {
	clients map[string]relay.Client
}

func (p *responseProvider) Instance(_ context.Context, workdir string) (AgentInstance, error) {
	client := p.clients[workdir]
	if client == nil {
		return nil, errors.New("missing response client")
	}
	return &fakeInstance{client: client, workdir: workdir}, nil
}

func (p *responseProvider) ForceStop(_ string) {}

func TestProgressNoticeText(t *testing.T) {
	if got := progressNotice(60); got != "⏳ Still working... (1m)" {
		t.Fatalf("progressNotice(60) = %q, want %q", got, "⏳ Still working... (1m)")
	}
	if got := progressNotice(120); got != "⏳ Still working... (2m)" {
		t.Fatalf("progressNotice(120) = %q, want %q", got, "⏳ Still working... (2m)")
	}
	if got := progressNotice(180); got != "⏳ Still working... (3m)" {
		t.Fatalf("progressNotice(180) = %q, want %q", got, "⏳ Still working... (3m)")
	}
}

func TestResponseTimeoutForceStopsAndReplies(t *testing.T) {
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		return relay.ErrTimeout
	})
	provider := &fakeInstanceProvider{client: client}
	overrides := newFakeOverrideRepo()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow",
	}
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}
	r := New(provider, st, "/default-workdir", "")
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForReply(t, reply, "The agent stopped responding")
	waitForResponse(t, r)

	if provider.stopped != "/default-workdir" {
		t.Fatalf("stopped = %q, want /default-workdir", provider.stopped)
	}
	if !reply.contains("⚠️ The agent stopped responding after 3 minutes and was restarted automatically. Please send your message again.") {
		t.Fatalf("unexpected reply text: %v", reply.texts())
	}
}

func TestResponseCoordinatorAllowsDifferentChannels(t *testing.T) {
	first := newResponseClient(nil)
	second := newResponseClient(nil)
	overrides := newFakeOverrideRepo()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	overrides.overrides["telegram:chat2:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat2", UserID: "user1", Role: "allow"}
	channels := newFakeChannelRepo()
	channels.channels["telegram:chat2"] = &store.Channel{Platform: "telegram", ChannelID: "chat2", Workdir: "/chat2", ListenMode: "mention"}
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  channels,
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}
	r := New(&responseProvider{clients: map[string]relay.Client{
		"/default-workdir": first,
		"/chat2":           second,
	}}, st, "/default-workdir", "")

	firstReply := newResponseReply()
	secondReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("first Route: %v", err)
	}
	if err := r.Route(context.Background(), responseMessage("user1", "chat2", "second", secondReply)); err != nil {
		t.Fatalf("second Route: %v", err)
	}
	waitForReply(t, firstReply, "response")
	waitForReply(t, secondReply, "response")
	waitForResponse(t, r)

	first.mu.Lock()
	firstCalls := first.sendCalls
	first.mu.Unlock()
	second.mu.Lock()
	secondCalls := second.sendCalls
	second.mu.Unlock()
	if firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("send calls = first %d, second %d", firstCalls, secondCalls)
	}
}

type removerReply struct {
	*responseReply
	deleted []channel.MessageRef
}

func (r *removerReply) Delete(ref channel.MessageRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, ref)
	return nil
}

var _ channel.MessageRemover = (*removerReply)(nil)

func TestProgressTickerDeletesNoticeOnStop(t *testing.T) {
	reply := &removerReply{responseReply: newResponseReply()}
	stopCh := make(chan struct{})

	go startProgressTicker(context.Background(), reply, stopCh, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reply.mu.Lock()
		sends := len(reply.sends)
		reply.mu.Unlock()
		if sends > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	reply.mu.Lock()
	if len(reply.sends) == 0 {
		reply.mu.Unlock()
		t.Fatal("ticker did not send progress notice")
	}
	reply.mu.Unlock()

	close(stopCh)
	time.Sleep(20 * time.Millisecond)

	reply.mu.Lock()
	deletedCount := len(reply.deleted)
	reply.mu.Unlock()

	if deletedCount != 1 {
		t.Fatalf("expected Delete called once, got %d", deletedCount)
	}
}

func TestResponseCoordinatorQueueMethods(t *testing.T) {
	coord := newResponseCoordinator()
	key := responseKey{platform: "telegram", channelID: "chat1", userID: "user1"}
	msg := channel.IncomingMessage{Text: "test"}
	ctx := context.Background()

	if depth, ok := coord.enqueue(key, ctx, msg); ok || depth != 0 {
		t.Fatalf("enqueue while idle = (%d, %v), want (0, false)", depth, ok)
	}

	cancelCalled := false
	if !coord.acquire(key, func() { cancelCalled = true }) {
		t.Fatal("acquire failed")
	}

	for i := 1; i <= 5; i++ {
		depth, ok := coord.enqueue(key, ctx, msg)
		if !ok || depth != i {
			t.Fatalf("enqueue iteration %d = (%d, %v), want (%d, true)", i, depth, ok, i)
		}
	}

	if depth, ok := coord.enqueue(key, ctx, msg); ok || depth != 0 {
		t.Fatalf("6th enqueue = (%d, %v), want (0, false)", depth, ok)
	}

	if depth := coord.queueDepth(key); depth != 5 {
		t.Fatalf("queueDepth = %d, want 5", depth)
	}

	drained := coord.drain(key)
	if len(drained) != 5 {
		t.Fatalf("drained len = %d, want 5", len(drained))
	}
	if depth := coord.queueDepth(key); depth != 0 {
		t.Fatalf("queueDepth after drain = %d, want 0", depth)
	}

	_ = cancelCalled
}

func TestRouterMessageQueueFullRejection(t *testing.T) {
	release := make(chan struct{})
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "running"}
		select {
		case <-release:
			events <- relay.Event{Type: "done"}
			close(events)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("first Route: %v", err)
	}
	waitForReply(t, firstReply, "running")

	for i := 1; i <= 5; i++ {
		qReply := newResponseReply()
		if err := r.Route(context.Background(), responseMessage("user1", "chat1", "queued", qReply)); err != nil {
			t.Fatalf("queued Route %d: %v", i, err)
		}
		expectedNotice := fmt.Sprintf("⏳ Queued — %d message(s) will run after the current response finishes.", i)
		if !qReply.hasExact(expectedNotice) {
			t.Fatalf("queued reply %d = %v, want %q", i, qReply.texts(), expectedNotice)
		}
	}

	fullReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "full", fullReply)); err != nil {
		t.Fatalf("full Route: %v", err)
	}
	if !fullReply.hasExact(busyResponseMessage) {
		t.Fatalf("full queue reply = %v, want busyResponseMessage %q", fullReply.texts(), busyResponseMessage)
	}

	close(release)
	waitForAllResponses(t, r)
}

func TestRouterMessageQueueResetAndSessionNewClearQueue(t *testing.T) {
	release := make(chan struct{})
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "active"}
		select {
		case <-release:
			events <- relay.Event{Type: "done"}
			close(events)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForReply(t, firstReply, "active")

	qReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "second", qReply)); err != nil {
		t.Fatalf("Route second: %v", err)
	}
	if !qReply.hasExact("⏳ Queued — 1 message(s) will run after the current response finishes.") {
		t.Fatalf("queued reply = %v", qReply.texts())
	}

	resetReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "/reset", resetReply)); err != nil {
		t.Fatalf("Route reset: %v", err)
	}
	if !resetReply.contains("Session reset") {
		t.Fatalf("reset reply = %v", resetReply.texts())
	}

	close(release)
	waitForAllResponses(t, r)

	key := responseKey{platform: "telegram", channelID: "chat1", userID: "user1"}
	if depth := r.responses.queueDepth(key); depth != 0 {
		t.Fatalf("queueDepth after reset = %d, want 0", depth)
	}
}

func TestRouterMessageQueueStopKeepsQueue(t *testing.T) {
	release := make(chan struct{})
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "active"}
		select {
		case <-release:
			events <- relay.Event{Type: "done"}
			close(events)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("Route first: %v", err)
	}
	waitForReply(t, firstReply, "active")

	qReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "second", qReply)); err != nil {
		t.Fatalf("Route second: %v", err)
	}

	stopReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "/stop", stopReply)); err != nil {
		t.Fatalf("Route stop: %v", err)
	}

	close(release)
	waitForAllResponses(t, r)

	client.mu.Lock()
	sendCalls := client.sendCalls
	client.mu.Unlock()
	if sendCalls != 2 {
		t.Fatalf("sendCalls = %d, want 2", sendCalls)
	}
}

func TestRouterStatusIncludesQueueLine(t *testing.T) {
	release := make(chan struct{})
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "active"}
		select {
		case <-release:
			events <- relay.Event{Type: "done"}
			close(events)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r, _ := newResponseRouter(client)
	firstReply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "first", firstReply)); err != nil {
		t.Fatalf("Route first: %v", err)
	}
	waitForReply(t, firstReply, "active")

	q1Reply := newResponseReply()
	_ = r.Route(context.Background(), responseMessage("user1", "chat1", "q1", q1Reply))
	q2Reply := newResponseReply()
	_ = r.Route(context.Background(), responseMessage("user1", "chat1", "q2", q2Reply))

	statusReply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "/status", statusReply)); err != nil {
		t.Fatalf("Route status: %v", err)
	}

	if !statusReply.contains("Queue: 2 message(s)") {
		t.Fatalf("status reply missing queue info: %v", statusReply.texts())
	}

	close(release)
	waitForAllResponses(t, r)
}
