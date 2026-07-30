package router

import (
	"context"
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

type fakeRef struct{ id string }

func (f fakeRef) ID() string { return f.id }

type fakeRelayClient struct {
	sessionID string
	lastMsg   string
	lastCmd   string
}

func (f *fakeRelayClient) CreateSession(_ context.Context) (string, error) {
	return f.sessionID, nil
}
func (f *fakeRelayClient) SendMessage(_ context.Context, _, text string) error {
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

type fakeStore struct {
	sessionRepo  *fakeSessionRepo
	overrideRepo *fakeOverrideRepo
}

func (f *fakeStore) SessionRepo() store.SessionRepo   { return f.sessionRepo }
func (f *fakeStore) ChannelRepo() store.ChannelRepo   { return nil }
func (f *fakeStore) OverrideRepo() store.OverrideRepo { return f.overrideRepo }
func (f *fakeStore) Close() error                     { return nil }

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
func (f *fakeSessionRepo) List(_ context.Context, platform, channelID string) ([]store.Session, error) {
	if f.activeID != "" {
		return []store.Session{{ID: 1, AgentSessionID: f.activeID, Active: true}}, nil
	}
	return nil, nil
}
func (f *fakeSessionRepo) Delete(_ context.Context, id int64) error { return nil }

func newTestRouterWithAccess() (*Router, *fakeRelayClient, *fakeReplyCtx, *fakeOverrideRepo) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	overrideRepo := newFakeOverrideRepo()
	st := &fakeStore{
		sessionRepo:  &fakeSessionRepo{},
		overrideRepo: overrideRepo,
	}
	resolver := relay.NewSessionResolver(st.SessionRepo(), client)
	r := New(client, st, resolver)
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
