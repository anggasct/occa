package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type fakeReplyCtx struct {
	sends   []string
	buttons [][]channel.Button
	edits   []string
}

func (f *fakeReplyCtx) SendTyping() error { return nil }
func (f *fakeReplyCtx) Send(text string) (channel.MessageRef, error) {
	f.sends = append(f.sends, text)
	return fakeRef{id: "1"}, nil
}
func (f *fakeReplyCtx) Edit(ref channel.MessageRef, text string) error {
	f.edits = append(f.edits, text)
	return nil
}
func (f *fakeReplyCtx) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	f.edits = append(f.edits, text)
	f.buttons = append(f.buttons, buttons)
	return nil
}
func (f *fakeReplyCtx) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	f.sends = append(f.sends, text)
	f.buttons = append(f.buttons, buttons)
	return fakeRef{id: "1"}, nil
}

type fakeRef struct{ id string }

func (f fakeRef) ID() string { return f.id }

// pendingResponse pairs the event stream and dispatch-completion channel of
// one in-flight response, so several responses can share one fake client.
type pendingResponse struct {
	events       chan relay.Event
	dispatchDone chan struct{}
}

type fakeRelayClient struct {
	sessionID          string
	lastMsg            string
	rawMsg             string
	lastCmd            string
	providers          relay.Providers
	providersErr       error
	lastModel          *relay.ModelRef
	providerCalls      int
	sendCalls          int
	createSessionCalls int
	commands           []relay.CommandInfo
	commandsErr        error
	blockSend          chan struct{}
	sessionSeq         int
	deltaBeforeDone    string
	abortCalls         []string
	abortErr           error
	sessionInfo        *relay.SessionInfo
	sessionInfoErr     error

	mu           sync.Mutex
	responses    []pendingResponse
	dispatchDone chan struct{}
}

func (f *fakeRelayClient) CreateSession(_ context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createSessionCalls++
	f.sessionID = fmt.Sprintf("sess-%d", f.sessionSeq)
	f.sessionSeq++
	return f.sessionID, nil
}
func (f *fakeRelayClient) GetSession(_ context.Context, _ string) (*relay.SessionInfo, error) {
	if f.sessionInfoErr != nil {
		return nil, f.sessionInfoErr
	}
	if f.sessionInfo != nil {
		return f.sessionInfo, nil
	}
	return &relay.SessionInfo{}, nil
}
func (f *fakeRelayClient) SessionExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (f *fakeRelayClient) AbortSession(_ context.Context, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abortCalls = append(f.abortCalls, sessionID)
	return f.abortErr
}
func (f *fakeRelayClient) SendMessage(_ context.Context, _, text string, model *relay.ModelRef, _ []relay.Attachment) error {
	f.mu.Lock()
	f.sendCalls++
	f.rawMsg = text
	if model != nil {
		copy := *model
		f.lastModel = &copy
	} else {
		f.lastModel = nil
	}
	if idx := strings.Index(text, "\n\n—\nOCCA"); idx >= 0 {
		text = text[:idx]
	}
	f.lastMsg = text
	f.mu.Unlock()
	if f.deltaBeforeDone != "" {
		f.finishResponseWithDelta(f.deltaBeforeDone)
	} else {
		f.finishResponse()
	}
	return nil
}

func (f *fakeRelayClient) finishResponseWithDelta(delta string) {
	if f.blockSend != nil {
		<-f.blockSend
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.responses) == 0 {
		return
	}
	resp := f.responses[0]
	f.responses = f.responses[1:]
	resp.events <- relay.Event{Type: relay.EventDelta, Delta: delta}
	resp.events <- relay.Event{Type: "done"}
	close(resp.events)
	close(resp.dispatchDone)
}

func (f *fakeRelayClient) finishResponse() {
	if f.blockSend != nil {
		<-f.blockSend
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.responses) == 0 {
		return
	}
	resp := f.responses[0]
	f.responses = f.responses[1:]
	resp.events <- relay.Event{Type: "done"}
	close(resp.events)
	close(resp.dispatchDone)
}
func (f *fakeRelayClient) Providers(_ context.Context) (relay.Providers, error) {
	f.providerCalls++
	return f.providers, f.providersErr
}
func (f *fakeRelayClient) AnswerQuestion(_ context.Context, _ string, _ [][]string) error { return nil }
func (f *fakeRelayClient) RejectQuestion(_ context.Context, _ string) error               { return nil }
func (f *fakeRelayClient) ReplyPermission(_ context.Context, _ string, _ relay.PermissionReply) error {
	return nil
}
func (f *fakeRelayClient) ListCommands(_ context.Context) ([]relay.CommandInfo, error) {
	return f.commands, f.commandsErr
}
func (f *fakeRelayClient) RunCommand(_ context.Context, _, cmd string) error {
	f.lastCmd = cmd
	f.finishResponse()
	return nil
}
func (f *fakeRelayClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := pendingResponse{events: make(chan relay.Event, 1), dispatchDone: make(chan struct{})}
	f.responses = append(f.responses, resp)
	f.dispatchDone = resp.dispatchDone
	return resp.events, nil
}

func waitForDispatch(t *testing.T, client *fakeRelayClient) {
	t.Helper()
	select {
	case <-client.dispatchDone:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not complete")
	}
}

func waitForResponse(t *testing.T, r *Router) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		r.responses.mu.Lock()
		active := len(r.responses.active)
		r.responses.mu.Unlock()
		if active == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("response did not finish")
}

type fakeOverrideRepo struct {
	overrides map[string]*store.UserOverride
	getErr    error
}

func newFakeOverrideRepo() *fakeOverrideRepo {
	return &fakeOverrideRepo{overrides: make(map[string]*store.UserOverride)}
}

func (f *fakeOverrideRepo) key(platform, channelID, userID string) string {
	return platform + ":" + channelID + ":" + userID
}

func (f *fakeOverrideRepo) Get(_ context.Context, platform, channelID, userID string) (*store.UserOverride, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.overrides[f.key(platform, channelID, userID)], nil
}

func (f *fakeOverrideRepo) UpsertRole(_ context.Context, platform, channelID, userID, role string) error {
	k := f.key(platform, channelID, userID)
	o, ok := f.overrides[k]
	if !ok {
		o = &store.UserOverride{ChannelID: channelID, Platform: platform, UserID: userID}
		f.overrides[k] = o
	}
	o.Role = role
	return nil
}

func (f *fakeOverrideRepo) UpsertModel(_ context.Context, platform, channelID, userID, model string) error {
	k := f.key(platform, channelID, userID)
	o, ok := f.overrides[k]
	if !ok {
		o = &store.UserOverride{ChannelID: channelID, Platform: platform, UserID: userID, Role: "deny"}
		f.overrides[k] = o
	}
	o.Model = model
	return nil
}

func (f *fakeOverrideRepo) Delete(_ context.Context, platform, channelID, userID string) error {
	delete(f.overrides, f.key(platform, channelID, userID))
	return nil
}

func (f *fakeOverrideRepo) ListByChannel(_ context.Context, platform, channelID string) ([]store.UserOverride, error) {
	var result []store.UserOverride
	for _, o := range f.overrides {
		if o.Platform == platform && o.ChannelID == channelID {
			result = append(result, *o)
		}
	}
	return result, nil
}

type fakeChannelRepo struct {
	channels map[string]*store.Channel
	getErr   error
}

func newFakeChannelRepo() *fakeChannelRepo {
	return &fakeChannelRepo{channels: make(map[string]*store.Channel)}
}

func (f *fakeChannelRepo) key(platform, channelID string) string { return platform + ":" + channelID }

func (f *fakeChannelRepo) Get(_ context.Context, platform, channelID string) (*store.Channel, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.channels[f.key(platform, channelID)], nil
}

func (f *fakeChannelRepo) channel(platform, channelID string) *store.Channel {
	k := f.key(platform, channelID)
	ch := f.channels[k]
	if ch == nil {
		ch = &store.Channel{ChannelID: channelID, Platform: platform, ListenMode: "mention"}
		f.channels[k] = ch
	}
	return ch
}

func (f *fakeChannelRepo) UpsertModel(_ context.Context, platform, channelID, model string) error {
	f.channel(platform, channelID).Model = model
	return nil
}

func (f *fakeChannelRepo) UpsertListenMode(_ context.Context, platform, channelID, listenMode string) error {
	f.channel(platform, channelID).ListenMode = listenMode
	return nil
}

func (f *fakeChannelRepo) UpsertWorkdir(_ context.Context, platform, channelID, workdir string) error {
	f.channel(platform, channelID).Workdir = workdir
	return nil
}

type fakeStore struct {
	sessionRepo  *fakeSessionRepo
	channelRepo  *fakeChannelRepo
	overrideRepo *fakeOverrideRepo
	scheduleRepo *fakeScheduleRepo
}

func (f *fakeStore) SessionRepo() store.SessionRepo   { return f.sessionRepo }
func (f *fakeStore) ChannelRepo() store.ChannelRepo   { return f.channelRepo }
func (f *fakeStore) OverrideRepo() store.OverrideRepo { return f.overrideRepo }
func (f *fakeStore) ScheduleRepo() store.ScheduleRepo { return f.scheduleRepo }
func (f *fakeStore) Close() error                     { return nil }

type fakeScheduleRepo struct{}

func (f *fakeScheduleRepo) Create(_ context.Context, s *store.Schedule) (int64, error) { return 1, nil }
func (f *fakeScheduleRepo) Delete(_ context.Context, _, _ string, _ int64) error       { return nil }
func (f *fakeScheduleRepo) List(_ context.Context, _, _ string) ([]store.Schedule, error) {
	return nil, nil
}
func (f *fakeScheduleRepo) ListAll(_ context.Context) ([]store.Schedule, error) { return nil, nil }

type fakeSessionRepo struct {
	activeID string
	activeBy map[string]string
	titles   map[string]string
	sessions []store.Session
}

func (f *fakeSessionRepo) sessionKey(platform, channelID, threadID, userID string) string {
	return platform + ":" + channelID + ":" + threadID + ":" + userID
}

func (f *fakeSessionRepo) Active(_ context.Context, platform, channelID, threadID, userID string) (string, int, error) {
	if f.activeBy == nil {
		return f.activeID, 0, nil
	}
	return f.activeBy[f.sessionKey(platform, channelID, threadID, userID)], 0, nil
}

func (f *fakeSessionRepo) SetActive(_ context.Context, platform, channelID, threadID, userID, sessionID string, _ int) error {
	f.activeID = sessionID
	if f.activeBy == nil {
		f.activeBy = make(map[string]string)
	}
	f.activeBy[f.sessionKey(platform, channelID, threadID, userID)] = sessionID
	if len(f.sessions) > 0 {
		for i := range f.sessions {
			f.sessions[i].Active = (f.sessions[i].AgentSessionID == sessionID)
		}
	}
	return nil
}

func (f *fakeSessionRepo) Deactivate(_ context.Context, platform, channelID, threadID, userID string) error {
	f.activeID = ""
	if f.activeBy != nil {
		delete(f.activeBy, f.sessionKey(platform, channelID, threadID, userID))
	}
	if len(f.sessions) > 0 {
		for i := range f.sessions {
			f.sessions[i].Active = false
		}
	}
	return nil
}

func (f *fakeSessionRepo) SetTitle(ctx context.Context, id int64, title string) error {
	if f.titles == nil {
		f.titles = make(map[string]string)
	}
	if len(f.sessions) > 0 {
		for i := range f.sessions {
			if f.sessions[i].ID == id || f.sessions[i].AgentSessionID == f.activeID {
				f.sessions[i].Title = title
				f.titles[f.sessions[i].AgentSessionID] = title
				return nil
			}
		}
	}
	sessions, _ := f.List(ctx, "telegram", "chat1")
	for _, s := range sessions {
		if s.ID == id {
			f.titles[s.AgentSessionID] = title
			return nil
		}
	}
	if f.activeID != "" {
		f.titles[f.activeID] = title
	}
	return nil
}

func (f *fakeSessionRepo) List(_ context.Context, platform, channelID string) ([]store.Session, error) {
	if len(f.sessions) > 0 {
		return f.sessions, nil
	}
	if f.activeBy == nil {
		if f.activeID != "" {
			return []store.Session{{ID: 1, AgentSessionID: f.activeID, Active: true, Title: f.titles[f.activeID]}}, nil
		}
		return nil, nil
	}
	prefix := platform + ":" + channelID + ":"
	var sessions []store.Session
	for key, id := range f.activeBy {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(key, prefix), ":", 2)
		threadID, userID := "", ""
		if len(parts) == 2 {
			threadID, userID = parts[0], parts[1]
		}
		sessID := int64(len(sessions) + 1)
		sessions = append(sessions, store.Session{
			ID:             sessID,
			AgentSessionID: id,
			Active:         true,
			ThreadID:       threadID,
			UserID:         userID,
			Title:          f.titles[id],
		})
	}
	return sessions, nil
}
func (f *fakeSessionRepo) ThreadChannel(_ context.Context, platform, threadID string) (string, error) {
	if f.activeBy == nil {
		return "", nil
	}
	for key := range f.activeBy {
		parts := strings.SplitN(key, ":", 4)
		if len(parts) == 4 && parts[0] == platform && parts[2] == threadID {
			return parts[1], nil
		}
	}
	return "", nil
}

func (f *fakeSessionRepo) Delete(_ context.Context, id int64) error { return nil }

type fakeInstance struct {
	client  relay.Client
	pid     int
	workdir string
}

func (f *fakeInstance) Client() relay.Client { return f.client }
func (f *fakeInstance) End()                 {}
func (f *fakeInstance) PID() int             { return f.pid }
func (f *fakeInstance) Workdir() string      { return f.workdir }

type fakeInstanceProvider struct {
	client  relay.Client
	err     error
	calls   int
	stopped string
}

func (p *fakeInstanceProvider) Instance(_ context.Context, workdir string) (AgentInstance, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	return &fakeInstance{client: p.client, workdir: workdir}, nil
}

func (p *fakeInstanceProvider) ForceStop(workdir string) {
	p.stopped = workdir
}

func newTestRouterWithAccess() (*Router, *fakeRelayClient, *fakeReplyCtx, *fakeOverrideRepo) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	overrideRepo := newFakeOverrideRepo()
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrideRepo,
		scheduleRepo: &fakeScheduleRepo{},
	}
	provider := &fakeInstanceProvider{client: client}
	r := New(provider, st, "/default-workdir", "")
	reply := &fakeReplyCtx{}
	return r, client, reply, overrideRepo
}

func newTestRouter() (*Router, *fakeRelayClient, *fakeReplyCtx) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "user1",
		Role:      "admin",
	}
	return r, client, reply
}

func msg(text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		UserID:    "user1",
		Text:      text,
		IsMention: true,
		ReplyCtx:  reply,
	}
}

func msgFrom(userID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		UserID:    userID,
		Text:      text,
		IsMention: true,
		ReplyCtx:  reply,
	}
}

func msgIn(channelID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: channelID,
		UserID:    "user1",
		Text:      text,
		IsMention: true,
		ReplyCtx:  reply,
	}
}

func TestRoutePassthrough(t *testing.T) {
	r, client, reply := newTestRouter()
	err := r.Route(context.Background(), msg("hello world", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello world" {
		t.Fatalf("expected passthrough 'hello world', got %q", client.lastMsg)
	}
}

func TestRoutePassthroughCommand(t *testing.T) {
	r, client, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/plan build a thing", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastCmd != "/plan build a thing" {
		t.Fatalf("expected command passthrough, got %q", client.lastCmd)
	}
}

func TestRouteHelp(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if !strings.Contains(reply.sends[0], "/help") {
		t.Fatalf("unexpected help: %q", reply.sends[0])
	}
}

func TestNormalizeCommandAlias(t *testing.T) {
	cases := map[string]string{
		"/occa_help":         "/help",
		"/occa_session list": "/session list",
		"/occa:help":         "/help",
		"hello world":        "hello world",
		"/occa_status extra": "/status extra",
		"/occa:model x":      "/model x",
	}
	for in, want := range cases {
		if got := normalizeCommandAlias(in); got != want {
			t.Fatalf("normalizeCommandAlias(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRouteAcceptsUnderscoreAlias(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa_help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if !strings.Contains(reply.sends[0], "/help") {
		t.Fatalf("unexpected help via alias: %q", reply.sends[0])
	}
}

func TestRouteAcceptsUnderscoreAliasWithArgs(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa_session list", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected session response")
	}
	if strings.Contains(reply.sends[0], "Usage:") {
		t.Fatalf("alias with args did not reach the session handler: %q", reply.sends[0])
	}
}

func TestRouteLegacyColonAlias(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	err := r.Route(context.Background(), msg("/occa:model openai/gpt-4o", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Personal model set: openai/gpt-4o") {
		t.Fatalf("expected model set via legacy colon alias, got %v", reply.sends)
	}
}

func TestHelpListsAgentCommands(t *testing.T) {
	r, client, reply := newTestRouter()
	client.commands = []relay.CommandInfo{
		{Name: "plan", Description: "Create a plan", Source: "command"},
	}
	err := r.Route(context.Background(), msg("/help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if !strings.Contains(reply.sends[0], "Agent commands:") {
		t.Fatalf("expected agent commands section, got: %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "/plan — Create a plan") {
		t.Fatalf("expected plan command listed, got: %q", reply.sends[0])
	}
}

func TestHelpOmitsAgentCommandsOnError(t *testing.T) {
	r, client, reply := newTestRouter()
	client.commandsErr = errors.New("agent unreachable")
	err := r.Route(context.Background(), msg("/help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if strings.Contains(reply.sends[0], "Agent commands:") {
		t.Fatalf("expected no agent commands section on error, got: %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "/help") {
		t.Fatalf("expected base help text still present, got: %q", reply.sends[0])
	}
}

func TestHelpOmitsAgentCommandsWhenEmpty(t *testing.T) {
	r, client, reply := newTestRouter()
	client.commands = nil
	err := r.Route(context.Background(), msg("/help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if strings.Contains(reply.sends[0], "Agent commands:") {
		t.Fatalf("expected no agent commands section when empty, got: %q", reply.sends[0])
	}
}

func TestMenuCommandsCoversRegisteredCommands(t *testing.T) {
	r, _, _ := newTestRouter()
	menu := r.MenuCommands()
	if len(menu) != len(r.commands) {
		t.Fatalf("MenuCommands has %d entries, registered commands has %d", len(menu), len(r.commands))
	}
	for _, m := range menu {
		if _, ok := r.commands[m.Alias]; !ok {
			t.Fatalf("alias %q has no matching registered command", m.Alias)
		}
	}
}

func TestRouteUnknownCommandPassthrough(t *testing.T) {
	r, client, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/xyz arg1", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastCmd != "/xyz arg1" {
		t.Fatalf("expected unknown command passthrough to agent, got %q", client.lastCmd)
	}
}

func TestRouteLegacyUnknownCommandShowsHelp(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa:foo", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help fallback")
	}
	if !strings.Contains(reply.sends[0], "/help") {
		t.Fatalf("expected help text for unknown legacy command, got: %q", reply.sends[0])
	}
}

func TestRouteStatus(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/status", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected status response")
	}
	if !strings.Contains(reply.sends[0], "Agent") {
		t.Fatalf("unexpected status: %q", reply.sends[0])
	}
}

func TestRouteReset(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/reset", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reset response")
	}
	if !strings.Contains(reply.sends[0], "reset") {
		t.Fatalf("unexpected reset: %q", reply.sends[0])
	}
}

func TestRouteSessionList(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/session list", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected session list response")
	}
}

func TestFirstMessageTitleCapture(t *testing.T) {
	r, client, reply := newTestRouter()

	// 1. First message stamps title
	longMsg := "  Fix bug in   authentication logic\nand clean up session handling  "
	err := r.Route(context.Background(), msg(longMsg, reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	sessions, err := r.store.SessionRepo().List(context.Background(), "telegram", "chat1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	expectedTitle := "Fix bug in authentication logic and clean up session handlin…"
	if sessions[0].Title != expectedTitle {
		t.Fatalf("title = %q, want %q", sessions[0].Title, expectedTitle)
	}

	// 2. Second message does NOT overwrite title
	err = r.Route(context.Background(), msg("second message should be ignored for title", reply))
	if err != nil {
		t.Fatalf("Route second: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	sessions, err = r.store.SessionRepo().List(context.Background(), "telegram", "chat1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if sessions[0].Title != expectedTitle {
		t.Fatalf("title after second message = %q, want unchanged %q", sessions[0].Title, expectedTitle)
	}

	// 3. /session new does NOT stamp a title
	err = r.Route(context.Background(), msg("/session new", reply))
	if err != nil {
		t.Fatalf("Route /session new: %v", err)
	}
	sessions, err = r.store.SessionRepo().List(context.Background(), "telegram", "chat1")
	if err != nil {
		t.Fatalf("List after new: %v", err)
	}
	for _, s := range sessions {
		if s.Active && s.Title != "" {
			t.Fatalf("new session should not have title stamped, got %q", s.Title)
		}
	}
}

func TestSessionListTitles(t *testing.T) {
	r, _, reply := newTestRouter()
	ctx := context.Background()

	_ = r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "", "sess-1", 100)
	sessions, _ := r.store.SessionRepo().List(ctx, "telegram", "chat1")
	if len(sessions) > 0 {
		_ = r.store.SessionRepo().SetTitle(ctx, sessions[0].ID, "Title One")
	}

	err := r.Route(ctx, msg("/session list", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected session list response")
	}
	out := reply.sends[len(reply.sends)-1]
	if !strings.Contains(out, `→ sess-1 "Title One"`) {
		t.Fatalf("expected title in list output, got: %q", out)
	}
}

func TestStatusContextMeter(t *testing.T) {
	t.Run("with session info and limit", func(t *testing.T) {
		r, client, reply := newTestRouter()
		client.sessionInfo = &relay.SessionInfo{
			Cost:  0.05,
			Model: relay.ModelRef{ProviderID: "openai", ID: "gpt-4o"},
			Tokens: relay.SessionTokens{
				Input: 12000,
			},
		}
		client.providers = relay.Providers{
			All: []relay.Provider{
				{
					ID: "openai",
					Models: map[string]json.RawMessage{
						"gpt-4o": json.RawMessage(`{"limit":{"context":128000}}`),
					},
				},
			},
		}

		err := r.Route(context.Background(), msg("/status", reply))
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		if len(reply.sends) == 0 {
			t.Fatal("expected status response")
		}
		out := reply.sends[0]
		if !strings.Contains(out, "Context: 12.0k / 128.0k tokens (9%) · cost: $0.05") {
			t.Fatalf("unexpected status output: %q", out)
		}
	})

	t.Run("with unknown limit", func(t *testing.T) {
		r, client, reply := newTestRouter()
		client.sessionInfo = &relay.SessionInfo{
			Cost: 0.05,
			Tokens: relay.SessionTokens{
				Input: 12000,
			},
		}

		err := r.Route(context.Background(), msg("/status", reply))
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		if len(reply.sends) == 0 {
			t.Fatal("expected status response")
		}
		out := reply.sends[0]
		if !strings.Contains(out, "Context: 12.0k tokens used · cost: $0.05") {
			t.Fatalf("unexpected status output: %q", out)
		}
	})

	t.Run("with GetSession error", func(t *testing.T) {
		r, client, reply := newTestRouter()
		client.sessionInfoErr = errors.New("agent error")

		err := r.Route(context.Background(), msg("/status", reply))
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		if len(reply.sends) == 0 {
			t.Fatal("expected status response")
		}
		out := reply.sends[0]
		if strings.Contains(out, "Context:") {
			t.Fatalf("expected Context line omitted on error, got: %q", out)
		}
		if !strings.Contains(out, "Agent connected") {
			t.Fatalf("expected status to succeed without Context line, got: %q", out)
		}
	})
}

func TestAccessDeniedForUnknownUser(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	err := r.Route(context.Background(), msgFrom("stranger", "hello", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatal("message should not reach the agent for denied user")
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Access denied") {
		t.Fatalf("expected access denied, got: %v", reply.sends)
	}
}

func TestIngressAuthorizationMatrix(t *testing.T) {
	roles := []struct {
		name   string
		userID string
		role   string
		denied bool
	}{
		{name: "unknown", userID: "stranger", denied: true},
		{name: "deny", userID: "user1", role: "deny", denied: true},
		{name: "allow", userID: "user1", role: "allow"},
		{name: "admin", userID: "user1", role: "admin"},
	}
	actions := []struct {
		name     string
		text     string
		callback bool
	}{
		{name: "ordinary", text: "hello"},
		{name: "non-admin command", text: "/help"},
		{name: "admin command", text: "/allow user2"},
		{name: "permission callback", callback: true, text: "permission:req-1:once"},
	}

	for _, role := range roles {
		for _, action := range actions {
			t.Run(role.name+"/"+action.name, func(t *testing.T) {
				r, client, reply, overrideRepo := newTestRouterWithAccess()
				provider := r.instances.(*fakeInstanceProvider)
				if role.role != "" {
					overrideRepo.overrides[overrideRepo.key("telegram", "chat1", role.userID)] = &store.UserOverride{
						ChannelID: "chat1",
						Platform:  "telegram",
						UserID:    role.userID,
						Role:      role.role,
					}
				}

				m := channel.IncomingMessage{
					Platform:     "telegram",
					ChannelID:    "chat1",
					UserID:       role.userID,
					Text:         action.text,
					IsMention:    true,
					IsCallback:   action.callback,
					CallbackData: action.text,
					ReplyCtx:     reply,
				}

				if err := r.Route(context.Background(), m); err != nil {
					t.Fatalf("Route: %v", err)
				}
				if action.name == "ordinary" && !role.denied {
					waitForDispatch(t, client)
					waitForResponse(t, r)
				}

				if role.denied {
					if len(reply.sends) != 1 || reply.sends[0] != accessDeniedMessage {
						t.Fatalf("denied response = %v, want exactly %q", reply.sends, accessDeniedMessage)
					}
					if provider.calls != 0 {
						t.Fatalf("instance provider calls = %d, want 0", provider.calls)
					}
					if client.lastMsg != "" || client.lastCmd != "" {
						t.Fatalf("denied request reached client: message=%q command=%q", client.lastMsg, client.lastCmd)
					}
					return
				}

				if len(reply.sends) > 0 && reply.sends[0] == accessDeniedMessage {
					t.Fatalf("authorized request was denied: %v", reply.sends)
				}
				switch {
				case action.name == "ordinary":
					if client.lastMsg != "hello" {
						t.Fatalf("ordinary message = %q, want hello", client.lastMsg)
					}
				case action.name == "non-admin command":
					if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "/help") {
						t.Fatalf("non-admin command response = %v", reply.sends)
					}
				case action.name == "admin command":
					if role.role == "admin" {
						if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Allowed user: user2") {
							t.Fatalf("admin command response = %v", reply.sends)
						}
					} else if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Admin access required") {
						t.Fatalf("non-admin command response = %v", reply.sends)
					}
				case action.name == "permission callback":
					if provider.calls != 0 {
						t.Fatalf("callback instance provider calls = %d, want 0", provider.calls)
					}
				}
			})
		}
	}
}

func TestIngressAuthorizationStoreErrorFailsClosed(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	provider := r.instances.(*fakeInstanceProvider)
	overrideRepo.getErr = errors.New("database unavailable")

	if err := r.Route(context.Background(), msgFrom("stranger", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || reply.sends[0] != accessVerifyMessage {
		t.Fatalf("verification response = %v, want exactly %q", reply.sends, accessVerifyMessage)
	}
	if provider.calls != 0 || client.lastMsg != "" {
		t.Fatalf("store error reached downstream: provider calls=%d, message=%q", provider.calls, client.lastMsg)
	}
}

func TestAccessAllowedAfterAllow(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}

	err := r.Route(context.Background(), msg("/allow user2", reply))
	if err != nil {
		t.Fatalf("Route allow: %v", err)
	}

	reply2 := &fakeReplyCtx{}
	err = r.Route(context.Background(), msgFrom("user2", "hello", reply2))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello" {
		t.Fatalf("expected passthrough after allow, got %q", client.lastMsg)
	}
}

func TestAccessDeniedAfterDeny(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}
	overrideRepo.overrides["telegram:chat1:user2"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user2", Role: "allow"}

	err := r.Route(context.Background(), msg("/deny user2", reply))
	if err != nil {
		t.Fatalf("Route deny: %v", err)
	}

	reply2 := &fakeReplyCtx{}
	err = r.Route(context.Background(), msgFrom("user2", "hello", reply2))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatal("denied user should not reach the agent")
	}
}

func TestNonAdminCannotAllow(t *testing.T) {
	r, _, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow"}

	err := r.Route(context.Background(), msg("/allow user2", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Admin access required") {
		t.Fatalf("expected admin required, got: %v", reply.sends)
	}
}

func TestLastAdminCannotBeDenied(t *testing.T) {
	r, _, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}

	err := r.Route(context.Background(), msg("/deny user1", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "last admin") {
		t.Fatalf("expected last admin guard, got: %v", reply.sends)
	}
}

func TestPerChannelScoping(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow"}

	msgChat2 := channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat2",
		UserID:    "user1",
		Text:      "hello",
		IsMention: true,
		ReplyCtx:  reply,
	}
	err := r.Route(context.Background(), msgChat2)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatal("user allowed in chat1 should be denied in chat2")
	}
}

func TestNonAdminCannotSetDir(t *testing.T) {
	r, _, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow"}

	err := r.Route(context.Background(), msg("/dir "+t.TempDir(), reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Admin access required") {
		t.Fatalf("expected admin required, got: %v", reply.sends)
	}
}

func TestDirSetAndView(t *testing.T) {
	r, _, reply := newTestRouter()
	dir := t.TempDir()

	if err := r.Route(context.Background(), msg("/dir "+dir, reply)); err != nil {
		t.Fatalf("Route dir set: %v", err)
	}
	if !strings.Contains(reply.sends[0], "✅ Workdir set") {
		t.Fatalf("expected workdir set confirmation, got %q", reply.sends[0])
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/dir", reply2)); err != nil {
		t.Fatalf("Route dir view: %v", err)
	}
	if !strings.Contains(reply2.sends[0], dir) || !strings.Contains(reply2.sends[0], "(exists)") {
		t.Fatalf("expected view to show %q (exists), got %q", dir, reply2.sends[0])
	}
}

// fakeChatCommandSetter is a ReplyContext that also implements
// channel.ChatCommandSetter, so setDir's type-assertion succeeds and its
// best-effort update path can be exercised and observed.
type fakeChatCommandSetter struct {
	*fakeReplyCtx
	commands []channel.MenuCommand
	done     chan struct{}
}

func (f *fakeChatCommandSetter) SetChatCommands(commands []channel.MenuCommand) error {
	f.commands = commands
	close(f.done)
	return nil
}

func TestDirChangeUpdatesChatCommands(t *testing.T) {
	r, client, reply := newTestRouter()
	client.commands = []relay.CommandInfo{{Name: "plan", Description: "Create a plan"}}
	setter := &fakeChatCommandSetter{fakeReplyCtx: reply, done: make(chan struct{})}

	m := channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		UserID:    "user1",
		Text:      "/dir " + t.TempDir(),
		IsMention: true,
		ReplyCtx:  setter,
	}

	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "✅ Workdir set") {
		t.Fatalf("expected workdir set confirmation, got %q", reply.sends[0])
	}

	select {
	case <-setter.done:
	case <-time.After(time.Second):
		t.Fatal("SetChatCommands was not called")
	}

	var hasHelpAlias, hasAgentCommand bool
	for _, c := range setter.commands {
		if c.Alias == "help" {
			hasHelpAlias = true
		}
		if c.Alias == "plan" && c.Description == "Create a plan" {
			hasAgentCommand = true
		}
	}
	if !hasHelpAlias {
		t.Fatalf("expected help in the union, got %+v", setter.commands)
	}
	if !hasAgentCommand {
		t.Fatalf("expected the agent's plan command in the union, got %+v", setter.commands)
	}
}

func TestDirChangeSkipsCommandUpdateWhenUnsupported(t *testing.T) {
	// newTestRouter's plain *fakeReplyCtx does not implement
	// channel.ChatCommandSetter — setDir must behave exactly as before.
	r, _, reply := newTestRouter()
	if err := r.Route(context.Background(), msg("/dir "+t.TempDir(), reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "✅ Workdir set") {
		t.Fatalf("expected workdir set confirmation, got %q", reply.sends[0])
	}
}

func TestDirSetInvalid(t *testing.T) {
	r, _, reply := newTestRouter()
	if err := r.Route(context.Background(), msg("/dir /nonexistent/path/xyz123", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Directory not found") {
		t.Fatalf("expected not-found error, got %q", reply.sends[0])
	}
}

func TestDirSetNotADirectory(t *testing.T) {
	r, _, reply := newTestRouter()
	file := filepath.Join(t.TempDir(), "afile.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := r.Route(context.Background(), msg("/dir "+file, reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Not a directory") {
		t.Fatalf("expected not-a-directory error, got %q", reply.sends[0])
	}
}

func TestDirThreadIsolation(t *testing.T) {
	r, _, _ := newTestRouter()
	st := r.store.(*fakeStore)
	dir := t.TempDir()

	// Access control has no thread-inherits-parent fallback — an admin needs their own
	// override row scoped to the thread's own channel_id.
	st.overrideRepo.overrides["telegram:thread1:user1"] = &store.UserOverride{ChannelID: "thread1", Platform: "telegram", UserID: "user1", Role: "admin"}

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgIn("thread1", "/dir "+dir, reply)); err != nil {
		t.Fatalf("Route thread dir: %v", err)
	}

	threadCh := st.channelRepo.channels["telegram:thread1"]
	if threadCh == nil || threadCh.Workdir != dir {
		t.Fatalf("thread workdir = %+v, want %q", threadCh, dir)
	}
	if parent := st.channelRepo.channels["telegram:chat1"]; parent != nil && parent.Workdir != "" {
		t.Fatalf("parent workdir should stay empty, got %q", parent.Workdir)
	}
}

func TestDirChangeResetsSession(t *testing.T) {
	r, _, _ := newTestRouter()
	st := r.store.(*fakeStore)
	st.sessionRepo.activeID = "sess-old"
	dir := t.TempDir()

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/dir "+dir, reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if st.sessionRepo.activeID != "" {
		t.Fatalf("expected active session cleared after workdir change, got %q", st.sessionRepo.activeID)
	}
}

func TestPerPlatformScoping(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow"}

	msgDiscordSameID := channel.IncomingMessage{
		Platform:  "discord",
		ChannelID: "chat1",
		UserID:    "user1",
		Text:      "hello",
		IsMention: true,
		ReplyCtx:  reply,
	}
	err := r.Route(context.Background(), msgDiscordSameID)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatal("user allowed on telegram:chat1 should be denied on discord:chat1 despite same channel_id")
	}
}

func TestBootstrapAdminFirstMessage(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	overrideRepo := newFakeOverrideRepo()
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrideRepo,
		scheduleRepo: &fakeScheduleRepo{},
	}
	provider := &fakeInstanceProvider{client: client}
	r := New(provider, st, "/default-workdir", "admin123")
	reply := &fakeReplyCtx{}

	err := r.Route(context.Background(), msgFrom("admin123", "hello admin", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello admin" {
		t.Fatalf("expected passthrough for bootstrap admin, got %q", client.lastMsg)
	}

	o, err := overrideRepo.Get(context.Background(), "telegram", "chat1", "admin123")
	if err != nil {
		t.Fatalf("Get override: %v", err)
	}
	if o == nil || o.Role != "admin" {
		t.Fatalf("expected lazy upsert of admin role row, got %+v", o)
	}
}

func TestBootstrapAdminCanRunCommandsImmediately(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	overrideRepo := newFakeOverrideRepo()
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrideRepo,
		scheduleRepo: &fakeScheduleRepo{},
	}
	provider := &fakeInstanceProvider{client: client}
	r := New(provider, st, "/default-workdir", "admin123")
	reply := &fakeReplyCtx{}

	err := r.Route(context.Background(), msgFrom("admin123", "/allow user2", reply))
	if err != nil {
		t.Fatalf("Route command: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Allowed user: user2") {
		t.Fatalf("expected allow confirmation, got %v", reply.sends)
	}

	o, err := overrideRepo.Get(context.Background(), "telegram", "chat1", "admin123")
	if err != nil || o == nil || o.Role != "admin" {
		t.Fatalf("expected admin row created for bootstrap admin, got %+v", o)
	}
}

func TestBootstrapAdminDoesNotWeakenDenyForOthers(t *testing.T) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	overrideRepo := newFakeOverrideRepo()
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrideRepo,
		scheduleRepo: &fakeScheduleRepo{},
	}
	provider := &fakeInstanceProvider{client: client}
	r := New(provider, st, "/default-workdir", "admin123")
	reply := &fakeReplyCtx{}

	err := r.Route(context.Background(), msgFrom("stranger", "hello", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatal("stranger should be denied")
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Access denied") {
		t.Fatalf("expected access denied, got %v", reply.sends)
	}
}

func TestListenModeEnforcement(t *testing.T) {
	tests := []struct {
		name       string
		listenMode string
		isMention  bool
		isThread   bool
		isCommand  bool
		wantFwd    bool
	}{
		{"mention mode, no mention", "mention", false, false, false, false},
		{"mention mode, with mention", "mention", true, false, false, true},
		{"all mode, no mention", "all", false, false, false, true},
		{"all mode, with mention", "all", true, false, false, true},
		{"thread mode, in thread", "thread", false, true, false, true},
		{"thread mode, not thread no mention", "thread", false, false, false, false},
		{"thread mode, not thread with mention", "thread", true, false, false, true},
		{"no channel row defaults to mention, no mention", "", false, false, false, false},
		{"no channel row defaults to mention, with mention", "", true, false, false, true},
		{"command bypasses listen mode", "mention", false, false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, client, reply, overrideRepo := newTestRouterWithAccess()
			overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
				ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
			}

			if tt.listenMode != "" {
				st := r.store.(*fakeStore)
				st.channelRepo.channels["telegram:chat1"] = &store.Channel{
					ChannelID:  "chat1",
					Platform:   "telegram",
					ListenMode: tt.listenMode,
				}
			}

			text := "hello"
			if tt.isCommand {
				text = "/help"
			}

			m := channel.IncomingMessage{
				Platform:  "telegram",
				ChannelID: "chat1",
				UserID:    "user1",
				Text:      text,
				IsMention: tt.isMention,
				IsThread:  tt.isThread,
				ReplyCtx:  reply,
			}

			if err := r.Route(context.Background(), m); err != nil {
				t.Fatalf("Route: %v", err)
			}
			if !tt.isCommand && tt.wantFwd {
				waitForDispatch(t, client)
				waitForResponse(t, r)
			}

			if tt.isCommand {
				if len(reply.sends) == 0 {
					t.Fatal("expected command response")
				}
				return
			}

			gotFwd := client.lastMsg != ""
			if gotFwd != tt.wantFwd {
				t.Fatalf("forwarded = %v, want %v", gotFwd, tt.wantFwd)
			}
		})
	}
}

func TestChannelViewDefault(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/channel", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "mention") {
		t.Fatalf("expected default listen mode 'mention', got: %v", reply.sends)
	}
}

func TestChannelViewSet(t *testing.T) {
	r, _, reply := newTestRouter()

	err := r.Route(context.Background(), msg("/channel all", reply))
	if err != nil {
		t.Fatalf("Route set: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "all") {
		t.Fatalf("expected confirmation with 'all', got: %v", reply.sends)
	}

	reply2 := &fakeReplyCtx{}
	err = r.Route(context.Background(), msg("/channel", reply2))
	if err != nil {
		t.Fatalf("Route view: %v", err)
	}
	if len(reply2.sends) == 0 || !strings.Contains(reply2.sends[0], "all") {
		t.Fatalf("expected stored mode 'all', got: %v", reply2.sends)
	}
}

func TestChannelInvalidMode(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/channel banana", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Usage") {
		t.Fatalf("expected usage help for invalid mode, got: %v", reply.sends)
	}
}

func TestChannelNonAdminDenied(t *testing.T) {
	r, _, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow"}

	err := r.Route(context.Background(), msg("/channel all", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Admin access required") {
		t.Fatalf("expected admin required, got: %v", reply.sends)
	}
}

func TestChannelThreadIsolation(t *testing.T) {
	r, _, _ := newTestRouter()
	st := r.store.(*fakeStore)
	st.overrideRepo.overrides["telegram:thread1:user1"] = &store.UserOverride{ChannelID: "thread1", Platform: "telegram", UserID: "user1", Role: "admin"}

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgIn("thread1", "/channel thread", reply)); err != nil {
		t.Fatalf("Route thread channel: %v", err)
	}

	threadCh := st.channelRepo.channels["telegram:thread1"]
	if threadCh == nil || threadCh.ListenMode != "thread" {
		t.Fatalf("thread listen_mode = %+v, want 'thread'", threadCh)
	}
	if parent := st.channelRepo.channels["telegram:chat1"]; parent != nil && parent.ListenMode == "thread" {
		t.Fatalf("parent listen_mode should not be changed, got %q", parent.ListenMode)
	}
}

type fakeTokenGen struct{}

func (f *fakeTokenGen) Generate(_, _ string) (string, error) { return "test-token-123", nil }

type failingTokenGen struct{}

func (f *failingTokenGen) Generate(_, _ string) (string, error) { return "", errors.New("rng failed") }

func TestPassthroughSkipsTokenLineWhenGenerationFails(t *testing.T) {
	r, client, reply := newTestRouter()
	r.SetTokenGenerator(&failingTokenGen{})

	if err := r.Route(context.Background(), msg("hello world", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.rawMsg == "" {
		t.Fatal("message was not dispatched")
	}
	if strings.Contains(client.rawMsg, "<occa:schedule_token>") {
		t.Fatalf("token line appended despite failure: %q", client.rawMsg)
	}
	if strings.Contains(client.rawMsg, "test-token") {
		t.Fatalf("weak token leaked into message: %q", client.rawMsg)
	}
}

func TestPassthroughAppendsScheduleToken(t *testing.T) {
	r, client, reply := newTestRouter()
	r.SetTokenGenerator(&fakeTokenGen{})

	if err := r.Route(context.Background(), msg("hello world", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if !strings.Contains(client.rawMsg, "<occa:schedule_token>test-token-123</occa:schedule_token>") {
		t.Fatalf("expected schedule token in message, got: %q", client.rawMsg)
	}
	if !strings.HasPrefix(client.rawMsg, "hello world") {
		t.Fatalf("original text should be preserved, got: %q", client.rawMsg)
	}
}

func TestShortFormCommandsAndLegacyAliases(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	tests := []struct {
		input        string
		wantSend     string
		wantPassthru string
	}{
		{input: "/help", wantSend: "OCCA commands:"},
		{input: "/occa:help", wantSend: "OCCA commands:"},
		{input: "/occa_help", wantSend: "OCCA commands:"},
		{input: "/status", wantSend: "Agent connected"},
		{input: "/session list", wantSend: "Sessions:"},
		{input: "/occa_session list", wantSend: "Sessions:"},
		{input: "/occa:session list", wantSend: "Sessions:"},
		{input: "/reset", wantSend: "Session reset"},
		{input: "/dir", wantSend: "Workdir:"},
		{input: "/allow user2", wantSend: "Allowed user: user2"},
		{input: "/deny user2", wantSend: "Denied user: user2"},
		{input: "/admin user2", wantSend: "Granted admin: user2"},
		{input: "/channel all", wantSend: "Listen mode set: all"},
		{input: "/model openai/gpt-4o", wantSend: "Personal model set: openai/gpt-4o"},
		{input: "/occa:model openai/gpt-4o", wantSend: "Personal model set: openai/gpt-4o"},
		{input: "/occa_model openai/gpt-4o", wantSend: "Personal model set: openai/gpt-4o"},
		{input: "/schedules", wantSend: "Scheduler not available"},
		{input: "/unknown_cmd arg", wantPassthru: "/unknown_cmd arg"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			reply.sends = nil
			client.lastCmd = ""
			err := r.Route(context.Background(), msg(tt.input, reply))
			if err != nil {
				t.Fatalf("Route(%q): %v", tt.input, err)
			}
			if tt.wantSend != "" {
				if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], tt.wantSend) {
					t.Fatalf("Route(%q) response = %v, want substring %q", tt.input, reply.sends, tt.wantSend)
				}
			}
			if tt.wantPassthru != "" {
				waitForDispatch(t, client)
				waitForResponse(t, r)
				if client.lastCmd != tt.wantPassthru {
					t.Fatalf("Route(%q) lastCmd = %q, want %q", tt.input, client.lastCmd, tt.wantPassthru)
				}
			}
		})
	}
}

func TestHandleStopWithActiveSession(t *testing.T) {
	r, client, reply := newTestRouter()
	ctx := context.Background()

	err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-active", 0)
	if err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	err = r.Route(ctx, msg("/stop", reply))
	if err != nil {
		t.Fatalf("Route /stop: %v", err)
	}

	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Stopped. Your conversation is kept") {
		t.Fatalf("expected stopped message, got: %v", reply.sends)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.abortCalls) != 1 || client.abortCalls[0] != "sess-active" {
		t.Fatalf("expected AbortSession call for sess-active, got: %v", client.abortCalls)
	}
}

func TestHandleStopNoActiveSession(t *testing.T) {
	r, client, reply := newTestRouter()
	ctx := context.Background()

	err := r.Route(ctx, msg("/stop", reply))
	if err != nil {
		t.Fatalf("Route /stop: %v", err)
	}

	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Nothing running to stop (no active session).") {
		t.Fatalf("expected nothing running message, got: %v", reply.sends)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.abortCalls) != 0 {
		t.Fatalf("expected 0 AbortSession calls, got: %v", client.abortCalls)
	}
}

func TestHandleSteerWithActiveSession(t *testing.T) {
	r, client, reply := newTestRouter()
	ctx := context.Background()

	err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-active", 0)
	if err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	err = r.Route(ctx, msg("/steer build a website", reply))
	if err != nil {
		t.Fatalf("Route /steer: %v", err)
	}

	client.mu.Lock()
	if len(client.abortCalls) != 1 || client.abortCalls[0] != "sess-active" {
		client.mu.Unlock()
		t.Fatalf("expected AbortSession for sess-active, got: %v", client.abortCalls)
	}
	client.mu.Unlock()

	waitForDispatch(t, client)
	waitForResponse(t, r)

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.createSessionCalls != 0 {
		t.Fatalf("expected 0 CreateSession calls, got %d", client.createSessionCalls)
	}
	if !strings.Contains(client.lastMsg, "New direction (previous task cancelled): build a website") {
		t.Fatalf("expected steer preamble in prompt, got: %q", client.lastMsg)
	}
}

func TestHandleSteerNoArgs(t *testing.T) {
	r, client, reply := newTestRouter()
	ctx := context.Background()

	err := r.Route(ctx, msg("/steer", reply))
	if err != nil {
		t.Fatalf("Route /steer: %v", err)
	}

	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Usage: /steer") {
		t.Fatalf("expected usage message, got: %v", reply.sends)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.abortCalls) != 0 {
		t.Fatalf("expected 0 AbortSession calls, got: %v", client.abortCalls)
	}
}

func TestHandleSteerNoSession(t *testing.T) {
	r, client, reply := newTestRouter()
	ctx := context.Background()

	err := r.Route(ctx, msg("/steer initial direction", reply))
	if err != nil {
		t.Fatalf("Route /steer: %v", err)
	}

	client.mu.Lock()
	if len(client.abortCalls) != 0 {
		client.mu.Unlock()
		t.Fatalf("expected 0 AbortSession calls, got: %v", client.abortCalls)
	}
	client.mu.Unlock()

	waitForDispatch(t, client)
	waitForResponse(t, r)

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.lastMsg != "initial direction" {
		t.Fatalf("expected prompt without preamble, got: %q", client.lastMsg)
	}
}

func TestStopAndSteerHelpAndMenu(t *testing.T) {
	r, _, reply := newTestRouter()
	ctx := context.Background()

	rawHelp := r.helpText()
	if !strings.Contains(rawHelp, "• /stop — stop the running response (session kept)") {
		t.Fatalf("raw help text missing /stop: %s", rawHelp)
	}
	if !strings.Contains(rawHelp, "• /steer <direction> — stop and redirect the agent (session kept)") {
		t.Fatalf("raw help text missing /steer: %s", rawHelp)
	}

	err := r.Route(ctx, msg("/help", reply))
	if err != nil {
		t.Fatalf("Route /help: %v", err)
	}

	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	helpText := reply.sends[0]
	if !strings.Contains(helpText, "/stop") {
		t.Fatalf("help text missing /stop: %s", helpText)
	}
	if !strings.Contains(helpText, "/steer") {
		t.Fatalf("help text missing /steer: %s", helpText)
	}

	menu := r.MenuCommands()
	foundStop := false
	foundSteer := false
	for _, cmd := range menu {
		if cmd.Alias == "stop" {
			foundStop = true
		}
		if cmd.Alias == "steer" {
			foundSteer = true
			if !cmd.HasArgs {
				t.Fatalf("expected steer HasArgs == true")
			}
		}
	}
	if !foundStop {
		t.Fatal("menu missing stop command")
	}
	if !foundSteer {
		t.Fatal("menu missing steer command")
	}
}

func TestStatusShowsActiveSessionTitle(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-titled",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-titled", Title: "llm-runtime PR #54 resolve config", Active: true, CreatedAt: time.Now().Unix()},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/status", reply))
	if err != nil {
		t.Fatalf("Route /status: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected status response")
	}
	want := "Session: llm-runtime PR #54 resolve config (sess-titled)"
	if !strings.Contains(reply.sends[0], want) {
		t.Fatalf("expected status to contain %q, got %q", want, reply.sends[0])
	}
}

func TestStatusShowsUntitledSessionIDOnly(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-untitled",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-untitled", Title: "", Active: true, CreatedAt: time.Now().Unix()},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/status", reply))
	if err != nil {
		t.Fatalf("Route /status: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected status response")
	}
	if !strings.Contains(reply.sends[0], "Session: sess-untitled") {
		t.Fatalf("expected status to contain Session: sess-untitled, got %q", reply.sends[0])
	}
	if strings.Contains(reply.sends[0], "Session:  (") {
		t.Fatalf("unexpected empty title parens in status: %q", reply.sends[0])
	}
}

func TestBareSessionRendersNumberedPicker(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	now := time.Now().Unix()
	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "Active Feature", Active: true, CreatedAt: now - 120},
			{ID: 2, AgentSessionID: "sess-2", Title: "Older Feature", Active: false, CreatedAt: now - 3600},
			{ID: 3, AgentSessionID: "sess-3", Title: "", Active: false, CreatedAt: now - 7200},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session", reply))
	if err != nil {
		t.Fatalf("Route /session: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected picker response")
	}
	text := reply.sends[0]
	if !strings.Contains(text, "Sessions:") {
		t.Fatalf("expected Sessions: header, got %q", text)
	}
	if !strings.Contains(text, "1. → Active Feature (2m ago)") {
		t.Fatalf("expected active row formatted with arrow, got %q", text)
	}
	if !strings.Contains(text, "2.   Older Feature (1h ago)") {
		t.Fatalf("expected second row formatted, got %q", text)
	}
	if !strings.Contains(text, "3.   sess-3 (2h ago)") {
		t.Fatalf("expected untitled third row fallback to id, got %q", text)
	}

	if len(reply.buttons) == 0 || len(reply.buttons[0]) != 3 {
		t.Fatalf("expected 3 buttons, got %v", reply.buttons)
	}
	b1 := reply.buttons[0][0]
	if b1.Label != "1" || b1.Value != "switch:sess-1" || b1.Row != 1 {
		t.Fatalf("unexpected button 1: %+v", b1)
	}
}

func TestSessionPickerBoundedToMax(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	var sessions []store.Session
	for i := 1; i <= 10; i++ {
		sessions = append(sessions, store.Session{
			ID:             int64(i),
			AgentSessionID: fmt.Sprintf("sess-%d", i),
			Title:          fmt.Sprintf("Session %d", i),
			Active:         i == 1,
			CreatedAt:      time.Now().Unix() - int64(i*60),
		})
	}

	stRepo := &fakeSessionRepo{activeID: "sess-1", sessions: sessions}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session", reply))
	if err != nil {
		t.Fatalf("Route /session: %v", err)
	}
	if len(reply.buttons) == 0 || len(reply.buttons[0]) != maxPickerSessions {
		t.Fatalf("expected %d buttons bounded by maxPickerSessions, got %d", maxPickerSessions, len(reply.buttons[0]))
	}
}

func TestSessionSwitchFullID(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "Old Session", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "Target Session", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session switch sess-2", reply))
	if err != nil {
		t.Fatalf("Route switch full id: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply")
	}
	if !strings.Contains(reply.sends[0], "✅ Switched to Target Session (sess-2)") {
		t.Fatalf("unexpected reply: %q", reply.sends[0])
	}
	if stRepo.activeID != "sess-2" {
		t.Fatalf("expected activeID sess-2, got %q", stRepo.activeID)
	}
}

func TestSessionSwitchNumericIndex(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "First Session", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "Second Session", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session switch 2", reply))
	if err != nil {
		t.Fatalf("Route switch index 2: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply")
	}
	if !strings.Contains(reply.sends[0], "✅ Switched to Second Session (sess-2)") {
		t.Fatalf("unexpected reply: %q", reply.sends[0])
	}
	if stRepo.activeID != "sess-2" {
		t.Fatalf("expected activeID sess-2, got %q", stRepo.activeID)
	}
}

func TestSessionSwitchTitleSubstringUnique(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "Fix authentication bug", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "Optimize SQL queries", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session switch sql", reply))
	if err != nil {
		t.Fatalf("Route switch substring: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply")
	}
	if !strings.Contains(reply.sends[0], "✅ Switched to Optimize SQL queries (sess-2)") {
		t.Fatalf("unexpected reply: %q", reply.sends[0])
	}
	if stRepo.activeID != "sess-2" {
		t.Fatalf("expected activeID sess-2, got %q", stRepo.activeID)
	}
}

func TestSessionSwitchTitleSubstringAmbiguous(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "Fix auth bug in handler", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "Fix UI bug in picker", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	err := r.Route(context.Background(), msg("/session switch bug", reply))
	if err != nil {
		t.Fatalf("Route switch ambiguous: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply")
	}
	if !strings.Contains(reply.sends[0], `Multiple sessions match "bug" — pick one:`) {
		t.Fatalf("expected ambiguous picker header, got %q", reply.sends[0])
	}
	if stRepo.activeID != "sess-1" {
		t.Fatalf("expected activeID unchanged sess-1, got %q", stRepo.activeID)
	}
}

func TestSessionSwitchCancelsAndDrainsQueue(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "Old", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "New", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	key := responseKey{platform: "telegram", channelID: "chat1", threadID: "", userID: "user1"}
	r.responses.acquire(key, func() {})
	r.responses.enqueue(key, context.Background(), msg("queued msg 1", reply))
	r.responses.enqueue(key, context.Background(), msg("queued msg 2", reply))

	err := r.Route(context.Background(), msg("/session switch sess-2", reply))
	if err != nil {
		t.Fatalf("Route switch with queue: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply")
	}
	if !strings.Contains(reply.sends[0], "Cleared 2 queued message(s) from the previous session.") {
		t.Fatalf("expected cleared queued message count in reply, got %q", reply.sends[0])
	}
}

func TestCallbackSwitchSuccessAndDeadID(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	stRepo := &fakeSessionRepo{
		activeID: "sess-1",
		sessions: []store.Session{
			{ID: 1, AgentSessionID: "sess-1", Title: "First", Active: true},
			{ID: 2, AgentSessionID: "sess-2", Title: "Second", Active: false},
		},
	}
	r.store = &fakeStore{
		sessionRepo:  stRepo,
		channelRepo:  newFakeChannelRepo(),
		overrideRepo: overrides,
		scheduleRepo: &fakeScheduleRepo{},
	}

	// 1. Success callback
	cbMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "switch:sess-2",
		CallbackRef:  fakeRef{id: "msg-1"},
		ReplyCtx:     reply,
	}

	err := r.Route(context.Background(), cbMsg)
	if err != nil {
		t.Fatalf("Route callback switch: %v", err)
	}
	if stRepo.activeID != "sess-2" {
		t.Fatalf("expected activeID sess-2, got %q", stRepo.activeID)
	}
	if len(reply.edits) == 0 || !strings.Contains(reply.edits[0], "✅ Switched to Second (sess-2)") {
		t.Fatalf("expected edit with switched text, got %v", reply.edits)
	}

	// 2. Dead ID callback
	deadCbMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "switch:sess-dead",
		CallbackRef:  fakeRef{id: "msg-1"},
		ReplyCtx:     reply,
	}

	reply.edits = nil
	err = r.Route(context.Background(), deadCbMsg)
	if err != nil {
		t.Fatalf("Route callback switch dead: %v", err)
	}
	if len(reply.edits) == 0 || reply.edits[0] != "Session not found." {
		t.Fatalf("expected Session not found. edit, got %v", reply.edits)
	}
}
