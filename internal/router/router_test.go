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

type fakeStore struct{}

func (f *fakeStore) SessionRepo() store.SessionRepo   { return &fakeSessionRepo{} }
func (f *fakeStore) ChannelRepo() store.ChannelRepo    { return nil }
func (f *fakeStore) OverrideRepo() store.OverrideRepo  { return nil }
func (f *fakeStore) Close() error                      { return nil }

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
		return []store.Session{{ID: 1, OpenCodeSessionID: f.activeID, Active: true}}, nil
	}
	return nil, nil
}
func (f *fakeSessionRepo) Delete(_ context.Context, id int64) error { return nil }

func newTestRouter() (*Router, *fakeRelayClient, *fakeReplyCtx) {
	client := &fakeRelayClient{sessionID: "sess-new"}
	st := &fakeStore{}
	resolver := relay.NewSessionResolver(st.SessionRepo(), client)
	r := New(client, st, resolver)
	reply := &fakeReplyCtx{}
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
	if !strings.Contains(reply.sends[0], "OpenCode") {
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
