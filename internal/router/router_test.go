package router

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type fakeReplyCtx struct {
	sends []string
}

func (f *fakeReplyCtx) SendTyping() error { return nil }
func (f *fakeReplyCtx) Send(text string) (channel.MessageRef, error) {
	f.sends = append(f.sends, text)
	return fakeRef{id: "1"}, nil
}
func (f *fakeReplyCtx) Edit(ref channel.MessageRef, text string) error { return nil }
func (f *fakeReplyCtx) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	f.sends = append(f.sends, text)
	return fakeRef{id: "1"}, nil
}

type fakeRef struct{ id string }

func (f fakeRef) ID() string { return f.id }

type fakeRelayClient struct {
	sessionID string
	lastMsg   string
	rawMsg    string
	lastCmd   string
}

func (f *fakeRelayClient) CreateSession(_ context.Context) (string, error) {
	return f.sessionID, nil
}
func (f *fakeRelayClient) SendMessage(_ context.Context, _, text string, _ []relay.Attachment) error {
	f.rawMsg = text
	if idx := strings.Index(text, "\n\n—\nOCCA"); idx >= 0 {
		text = text[:idx]
	}
	f.lastMsg = text
	return nil
}
func (f *fakeRelayClient) RunCommand(_ context.Context, _, cmd string) error {
	f.lastCmd = cmd
	return nil
}
func (f *fakeRelayClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
	return nil, nil
}

type fakeOverrideRepo struct {
	overrides map[string]*store.UserOverride
}

func newFakeOverrideRepo() *fakeOverrideRepo {
	return &fakeOverrideRepo{overrides: make(map[string]*store.UserOverride)}
}

func (f *fakeOverrideRepo) key(platform, channelID, userID string) string {
	return platform + ":" + channelID + ":" + userID
}

func (f *fakeOverrideRepo) Get(_ context.Context, platform, channelID, userID string) (*store.UserOverride, error) {
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
}

func newFakeChannelRepo() *fakeChannelRepo {
	return &fakeChannelRepo{channels: make(map[string]*store.Channel)}
}

func (f *fakeChannelRepo) key(platform, channelID string) string { return platform + ":" + channelID }

func (f *fakeChannelRepo) Get(_ context.Context, platform, channelID string) (*store.Channel, error) {
	return f.channels[f.key(platform, channelID)], nil
}

func (f *fakeChannelRepo) Upsert(_ context.Context, ch *store.Channel) error {
	f.channels[f.key(ch.Platform, ch.ChannelID)] = ch
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
func (f *fakeScheduleRepo) Delete(_ context.Context, _, _ string, _ int64) error            { return nil }
func (f *fakeScheduleRepo) List(_ context.Context, _, _ string) ([]store.Schedule, error) { return nil, nil }
func (f *fakeScheduleRepo) ListAll(_ context.Context) ([]store.Schedule, error)          { return nil, nil }

type fakeSessionRepo struct {
	activeID string
}

func (f *fakeSessionRepo) Active(_ context.Context, platform, channelID string) (string, error) {
	return f.activeID, nil
}
func (f *fakeSessionRepo) SetActive(_ context.Context, platform, channelID, sessionID string) error {
	f.activeID = sessionID
	return nil
}
func (f *fakeSessionRepo) Deactivate(_ context.Context, platform, channelID string) error {
	f.activeID = ""
	return nil
}
func (f *fakeSessionRepo) List(_ context.Context, platform, channelID string) ([]store.Session, error) {
	if f.activeID != "" {
		return []store.Session{{ID: 1, AgentSessionID: f.activeID, Active: true}}, nil
	}
	return nil, nil
}
func (f *fakeSessionRepo) Delete(_ context.Context, id int64) error { return nil }

type fakeInstance struct {
	client relay.Client
}

func (f *fakeInstance) Client() relay.Client { return f.client }
func (f *fakeInstance) End()                 {}

type fakeInstanceProvider struct {
	client relay.Client
}

func (p *fakeInstanceProvider) Instance(_ context.Context, _ string) (AgentInstance, error) {
	return &fakeInstance{client: p.client}, nil
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
	if client.lastCmd != "/plan build a thing" {
		t.Fatalf("expected command passthrough, got %q", client.lastCmd)
	}
}

func TestRouteHelp(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa:help", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help response")
	}
	if !strings.Contains(reply.sends[0], "/occa:help") {
		t.Fatalf("unexpected help: %q", reply.sends[0])
	}
}

func TestRouteUnknownCommand(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa:foo", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected help fallback")
	}
	if !strings.Contains(reply.sends[0], "/occa:help") {
		t.Fatalf("expected help text for unknown command, got: %q", reply.sends[0])
	}
}

func TestRouteStatus(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa:status", reply))
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
	err := r.Route(context.Background(), msg("/occa:reset", reply))
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
	err := r.Route(context.Background(), msg("/occa:session list", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected session list response")
	}
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

func TestAccessAllowedAfterAllow(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}

	err := r.Route(context.Background(), msg("/occa:allow user2", reply))
	if err != nil {
		t.Fatalf("Route allow: %v", err)
	}

	reply2 := &fakeReplyCtx{}
	err = r.Route(context.Background(), msgFrom("user2", "hello", reply2))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "hello" {
		t.Fatalf("expected passthrough after allow, got %q", client.lastMsg)
	}
}

func TestAccessDeniedAfterDeny(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}
	overrideRepo.overrides["telegram:chat1:user2"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user2", Role: "allow"}

	err := r.Route(context.Background(), msg("/occa:deny user2", reply))
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

	err := r.Route(context.Background(), msg("/occa:allow user2", reply))
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

	err := r.Route(context.Background(), msg("/occa:deny user1", reply))
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

	err := r.Route(context.Background(), msg("/occa:dir "+t.TempDir(), reply))
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

	if err := r.Route(context.Background(), msg("/occa:dir "+dir, reply)); err != nil {
		t.Fatalf("Route dir set: %v", err)
	}
	if !strings.Contains(reply.sends[0], "✅ Workdir set") {
		t.Fatalf("expected workdir set confirmation, got %q", reply.sends[0])
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/occa:dir", reply2)); err != nil {
		t.Fatalf("Route dir view: %v", err)
	}
	if !strings.Contains(reply2.sends[0], dir) || !strings.Contains(reply2.sends[0], "(exists)") {
		t.Fatalf("expected view to show %q (exists), got %q", dir, reply2.sends[0])
	}
}

func TestDirSetInvalid(t *testing.T) {
	r, _, reply := newTestRouter()
	if err := r.Route(context.Background(), msg("/occa:dir /nonexistent/path/xyz123", reply)); err != nil {
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
	if err := r.Route(context.Background(), msg("/occa:dir "+file, reply)); err != nil {
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
	if err := r.Route(context.Background(), msgIn("thread1", "/occa:dir "+dir, reply)); err != nil {
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
	if err := r.Route(context.Background(), msg("/occa:dir "+dir, reply)); err != nil {
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

	err := r.Route(context.Background(), msgFrom("admin123", "/occa:allow user2", reply))
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
				text = "/occa:help"
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
	err := r.Route(context.Background(), msg("/occa:channel", reply))
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "mention") {
		t.Fatalf("expected default listen mode 'mention', got: %v", reply.sends)
	}
}

func TestChannelViewSet(t *testing.T) {
	r, _, reply := newTestRouter()

	err := r.Route(context.Background(), msg("/occa:channel all", reply))
	if err != nil {
		t.Fatalf("Route set: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "all") {
		t.Fatalf("expected confirmation with 'all', got: %v", reply.sends)
	}

	reply2 := &fakeReplyCtx{}
	err = r.Route(context.Background(), msg("/occa:channel", reply2))
	if err != nil {
		t.Fatalf("Route view: %v", err)
	}
	if len(reply2.sends) == 0 || !strings.Contains(reply2.sends[0], "all") {
		t.Fatalf("expected stored mode 'all', got: %v", reply2.sends)
	}
}

func TestChannelInvalidMode(t *testing.T) {
	r, _, reply := newTestRouter()
	err := r.Route(context.Background(), msg("/occa:channel banana", reply))
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

	err := r.Route(context.Background(), msg("/occa:channel all", reply))
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
	if err := r.Route(context.Background(), msgIn("thread1", "/occa:channel thread", reply)); err != nil {
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

func (f *fakeTokenGen) Generate(_, _ string) string { return "test-token-123" }

func TestPassthroughAppendsScheduleToken(t *testing.T) {
	r, client, reply := newTestRouter()
	r.SetTokenGenerator(&fakeTokenGen{})

	if err := r.Route(context.Background(), msg("hello world", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(client.rawMsg, "OCCA schedule token: test-token-123") {
		t.Fatalf("expected schedule token in message, got: %q", client.rawMsg)
	}
	if !strings.HasPrefix(client.rawMsg, "hello world") {
		t.Fatalf("original text should be preserved, got: %q", client.rawMsg)
	}
}
