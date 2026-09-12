package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func agentTestAgents() []relay.AgentInfo {
	return []relay.AgentInfo{
		{Name: "build", Description: "General software development agent", Mode: "primary", Native: true},
		{Name: "reviewer", Description: "Code reviewer", Mode: "primary", Native: false},
		{Name: "planner", Description: "Architectural planning", Mode: "primary", Native: true},
	}
}

func TestAgentNoSessionAdminSetsChannelDefault(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	if err := r.Route(context.Background(), msgFrom("user1", "/agent reviewer", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply, got none")
	}
	if !strings.Contains(reply.sends[0], "✅ Channel agent set: reviewer") {
		t.Fatalf("expected channel confirmation, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "Scope: this channel") {
		t.Fatalf("expected channel scope line, got %q", reply.sends[0])
	}
	st := r.store.(*fakeStore)
	ch := st.channelRepo.channels["telegram:chat1"]
	if ch == nil || ch.Agent != "reviewer" {
		t.Fatalf("channel agent = %+v, want reviewer", ch)
	}
	if len(client.switchAgentCalls) != 0 {
		t.Fatalf("no-session set must not switch, got %v", client.switchAgentCalls)
	}
}

func TestAgentNoSessionNonAdminSetsPersonalDefault(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.agents = agentTestAgents()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}
	if err := r.Route(context.Background(), msg("/agent reviewer", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply, got none")
	}
	if !strings.Contains(reply.sends[0], "✅ Personal agent set: reviewer") {
		t.Fatalf("expected personal confirmation, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "Scope: personal") {
		t.Fatalf("expected personal scope line, got %q", reply.sends[0])
	}
	o := overrides.overrides["telegram:chat1:user1"]
	if o == nil || o.Agent != "reviewer" {
		t.Fatalf("personal agent = %+v, want reviewer", o)
	}
	st := r.store.(*fakeStore)
	if ch := st.channelRepo.channels["telegram:chat1"]; ch != nil && ch.Agent != "" {
		t.Fatalf("channel agent must stay empty for non-admin, got %q", ch.Agent)
	}
}

func TestAgentNoSessionInvalidNameStoresNothing(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	if err := r.Route(context.Background(), msgFrom("user1", "/agent nosuchagent", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Agent not found") {
		t.Fatalf("expected unknown-agent error, got %v", reply.sends)
	}
	st := r.store.(*fakeStore)
	if ch := st.channelRepo.channels["telegram:chat1"]; ch != nil && ch.Agent != "" {
		t.Fatalf("nothing must be stored, channel agent = %q", ch.Agent)
	}
}

func TestAgentNoSessionDefaultClearsAtCallerScope(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	if err := r.Route(context.Background(), msgFrom("user1", "/agent default", reply)); err != nil {
		t.Fatalf("Route admin clear: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Channel agent cleared") {
		t.Fatalf("expected channel clear confirmation, got %v", reply.sends)
	}
	if ch := st.channelRepo.channels["telegram:chat1"]; ch.Agent != "" {
		t.Fatalf("channel agent = %q, want empty", ch.Agent)
	}

	r2, client2, reply2, overrides := newTestRouterWithAccess()
	client2.agents = agentTestAgents()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow", Agent: "planner",
	}
	st2 := r2.store.(*fakeStore)
	st2.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	if err := r2.Route(context.Background(), msg("/agent default", reply2)); err != nil {
		t.Fatalf("Route non-admin clear: %v", err)
	}
	if len(reply2.sends) == 0 || !strings.Contains(reply2.sends[0], "Personal agent cleared") {
		t.Fatalf("expected personal clear confirmation, got %v", reply2.sends)
	}
	if o := overrides.overrides["telegram:chat1:user1"]; o.Agent != "" {
		t.Fatalf("personal agent = %q, want empty", o.Agent)
	}
	if ch := st2.channelRepo.channels["telegram:chat1"]; ch.Agent != "reviewer" {
		t.Fatalf("non-admin clear must not touch channel default, got %q", ch.Agent)
	}
}

func TestAgentNoSessionShowsEffectiveAgent(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	m := msgFrom("user1", "/agent", reply)
	m.IsThread = true
	m.ThreadID = "thread-1"
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply, got none")
	}
	if !strings.Contains(reply.sends[0], "reviewer") {
		t.Fatalf("expected effective agent reviewer, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "channel default") {
		t.Fatalf("expected channel scope, got %q", reply.sends[0])
	}
	if strings.Contains(reply.sends[0], "Send a message first") {
		t.Fatalf("stale guidance must be gone, got %q", reply.sends[0])
	}
}

func TestAgentNoSessionGuidanceText(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	adminMsg := msgFrom("user1", "/agent", reply)
	adminMsg.IsThread = true
	adminMsg.ThreadID = "thread-1"
	if err := r.Route(context.Background(), adminMsg); err != nil {
		t.Fatalf("Route admin: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected reply, got none")
	}
	if !strings.Contains(reply.sends[0], "No active session here") {
		t.Fatalf("expected honest guidance, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "per thread") {
		t.Fatalf("expected per-thread model line, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "/agent <name>") {
		t.Fatalf("expected escape hatch, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "channel-wide") {
		t.Fatalf("admin variant must mention channel-wide, got %q", reply.sends[0])
	}
	if strings.Contains(reply.sends[0], "Send a message first") {
		t.Fatalf("stale text must be gone, got %q", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0], "opencode default") {
		t.Fatalf("expected opencode default tier, got %q", reply.sends[0])
	}

	r2, client2, reply2, overrides := newTestRouterWithAccess()
	client2.agents = agentTestAgents()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}
	userMsg := msg("/agent", reply2)
	userMsg.IsThread = true
	userMsg.ThreadID = "thread-1"
	if err := r2.Route(context.Background(), userMsg); err != nil {
		t.Fatalf("Route non-admin: %v", err)
	}
	if len(reply2.sends) == 0 {
		t.Fatal("expected reply, got none")
	}
	if strings.Contains(reply2.sends[0], "channel-wide") {
		t.Fatalf("non-admin must not see channel-wide variant, got %q", reply2.sends[0])
	}
	if strings.Contains(reply2.sends[0], "Send a message first") {
		t.Fatalf("stale text must be gone, got %q", reply2.sends[0])
	}
}

func TestAgentNewSessionResolvesPersonalOverChannel(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	o := &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Agent: "planner"}
	st.overrideRepo.overrides["telegram:chat1:user1"] = o

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "hello world", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if len(client.switchAgentCalls) != 1 {
		t.Fatalf("expected 1 SwitchAgent call, got %v", client.switchAgentCalls)
	}
	if client.switchAgentCalls[0].name != "planner" {
		t.Fatalf("personal must beat channel, switched to %q", client.switchAgentCalls[0].name)
	}
	_ = reply
}

func TestAgentNewSessionResolvesChannelWhenNoPersonal(t *testing.T) {
	r, client, _ := newTestRouter()
	client.agents = agentTestAgents()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "hello world", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if len(client.switchAgentCalls) != 1 || client.switchAgentCalls[0].name != "reviewer" {
		t.Fatalf("expected channel SwitchAgent, got %v", client.switchAgentCalls)
	}
}

func TestAgentNewSessionWithoutDefaultSkipsSwitch(t *testing.T) {
	r, client, _ := newTestRouter()
	client.agents = agentTestAgents()
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "hello world", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if len(client.switchAgentCalls) != 0 {
		t.Fatalf("tier 3 must not switch, got %v", client.switchAgentCalls)
	}
	if client.lastMsg != "hello world" {
		t.Fatalf("expected passthrough, got %q", client.lastMsg)
	}
}

func TestAgentNewSessionSwitchFailureStillCompletes(t *testing.T) {
	r, client, _ := newTestRouter()
	client.agents = agentTestAgents()
	client.switchAgentErr = errors.New("boom")
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("user1", "hello world", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello world" {
		t.Fatalf("turn must complete on default agent, got %q", client.lastMsg)
	}
	if len(client.switchAgentCalls) != 1 {
		t.Fatalf("expected attempted switch, got %v", client.switchAgentCalls)
	}
}

func TestAgentInSessionUnchangedAndPersistsNothing(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "thread-1", "", "sess-1", 100); err != nil {
		t.Fatal(err)
	}
	m := msgFrom("user1", "/agent reviewer", reply)
	m.IsThread = true
	m.ThreadID = "thread-1"
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Switched to agent reviewer") {
		t.Fatalf("expected in-session switch reply, got %v", reply.sends)
	}
	if !strings.Contains(reply.sends[0], "Scope: this conversation") {
		t.Fatalf("expected thread scope line, got %v", reply.sends)
	}
	if len(client.switchAgentCalls) != 1 || client.switchAgentCalls[0].sessionID != "sess-1" {
		t.Fatalf("must switch that session only, got %v", client.switchAgentCalls)
	}
	st := r.store.(*fakeStore)
	if ch := st.channelRepo.channels["telegram:chat1"]; ch != nil && ch.Agent != "" {
		t.Fatalf("in-session switch in thread must persist nothing to channel, channel agent = %q", ch.Agent)
	}
}

func TestAgentRootChannelSessionSwitchesAndPersistsChannelDefault(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}
	if err := r.Route(context.Background(), msgFrom("user1", "/agent reviewer", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "✅ Channel agent set: reviewer") {
		t.Fatalf("expected channel agent set confirmation, got %v", reply.sends)
	}
	if !strings.Contains(reply.sends[0], "Scope: this channel") {
		t.Fatalf("expected channel scope line, got %v", reply.sends)
	}
	if len(client.switchAgentCalls) != 1 || client.switchAgentCalls[0].sessionID != "sess-1" {
		t.Fatalf("must switch active session too, got %v", client.switchAgentCalls)
	}
	st := r.store.(*fakeStore)
	if ch := st.channelRepo.channels["telegram:chat1"]; ch == nil || ch.Agent != "reviewer" {
		t.Fatalf("root channel switch must persist to channel agent, channel agent = %+v", ch)
	}
}

func TestAgentSessionNewAppliesChannelDefault(t *testing.T) {
	r, client, reply := newTestRouter()
	client.agents = agentTestAgents()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "mention", Agent: "reviewer",
	}
	if err := r.Route(context.Background(), msgFrom("user1", "/session new", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(client.switchAgentCalls) != 1 || client.switchAgentCalls[0].name != "reviewer" {
		t.Fatalf("expected default switch on /session new, got %v", client.switchAgentCalls)
	}
}
