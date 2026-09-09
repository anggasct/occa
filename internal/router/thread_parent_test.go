package router

import (
	"context"
	"errors"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

type countingResolver struct {
	calls  int
	parent string
	err    error
}

func (c *countingResolver) fn(threadID string) (string, error) {
	c.calls++
	return c.parent, c.err
}

func orphanMsg(threadID, parentID, userID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:        "discord",
		ChannelID:       threadID,
		ParentChannelID: parentID,
		ThreadID:        threadID,
		UserID:          userID,
		Text:            text,
		IsMention:       true,
		IsThread:        true,
		ReplyCtx:        reply,
	}
}

func knownThreadMsg(threadID, parentID, userID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:        "discord",
		ChannelID:       parentID,
		ParentChannelID: parentID,
		ThreadID:        threadID,
		UserID:          userID,
		Text:            text,
		IsMention:       true,
		IsThread:        true,
		ReplyCtx:        reply,
	}
}

func seedParentAdmin(st *fakeStore, parent, user string) {
	st.overrideRepo.overrides["discord:"+parent+":"+user] = &store.UserOverride{
		ChannelID: parent, Platform: "discord", UserID: user, Role: "admin",
	}
}

func TestOrphanThreadAuthorizesViaParentMessage(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello" {
		t.Fatalf("orphan message not passed through, lastMsg=%q sends=%v", client.lastMsg, reply.sends)
	}
	if resolver.calls != 0 {
		t.Fatalf("message-source resolution must not call adapter, calls=%d", resolver.calls)
	}
	tc := threadConfigsOf(r).configs["discord:parent-1:thread-orphan"]
	if tc == nil {
		t.Fatal("thread config not materialized for resolved orphan")
	}
	if tc.ChannelID != "parent-1" || tc.ThreadID != "thread-orphan" {
		t.Fatalf("materialized with wrong key: %+v", tc)
	}
}

func TestOrphanThreadDeniedWithoutParentRole(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatalf("denied orphan must not reach agent, got %q", client.lastMsg)
	}
	if len(reply.sends) == 0 || reply.sends[0] != accessDeniedMessage {
		t.Fatalf("expected deny, got %v", reply.sends)
	}
}

func TestOrphanThreadResolvesViaSession(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	st.sessionRepo.activeBy = map[string]string{
		"discord:parent-1:thread-orphan:user1": "sess-1",
	}
	resolver := &countingResolver{parent: "should-not-be-called"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastMsg != "hello" {
		t.Fatalf("session-resolved orphan not passed through, lastMsg=%q sends=%v", client.lastMsg, reply.sends)
	}
	if resolver.calls != 0 {
		t.Fatalf("session hit must precede adapter call, calls=%d", resolver.calls)
	}
}

func TestOrphanThreadResolvesViaAPICached(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "", "user1", "first", reply)); err != nil {
		t.Fatalf("Route first: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if resolver.calls != 1 {
		t.Fatalf("first orphan message must call adapter once, calls=%d", resolver.calls)
	}
	tc := threadConfigsOf(r).configs["discord:parent-1:thread-orphan"]
	if tc == nil {
		t.Fatal("API-resolved mapping not materialized")
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "second", reply2)); err != nil {
		t.Fatalf("Route second: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if resolver.calls != 1 {
		t.Fatalf("second message must not call adapter again, calls=%d", resolver.calls)
	}
	if client.lastMsg != "second" {
		t.Fatalf("second message not passed through, lastMsg=%q", client.lastMsg)
	}
}

func TestOrphanThreadAPIFailureRetry(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{err: errors.New("discord unavailable")}
	r.SetThreadParentResolver(resolver.fn)
	provider := r.instances.(*fakeInstanceProvider)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || reply.sends[0] != accessVerifyMessage {
		t.Fatalf("expected retry message, got %v", reply.sends)
	}
	if provider.calls != 0 || client.lastMsg != "" {
		t.Fatalf("retry must not reach agent: calls=%d msg=%q", provider.calls, client.lastMsg)
	}
	if tc := threadConfigsOf(r).configs["discord:parent-1:thread-orphan"]; tc != nil {
		t.Fatalf("retry must not materialize state, got %+v", tc)
	}
	if len(st.sessionRepo.activeBy) != 0 {
		t.Fatalf("retry must not create session, got %v", st.sessionRepo.activeBy)
	}
}

func TestOrphanThread404Denied(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	resolver := &countingResolver{err: channel.ErrThreadNotFound}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-gone", "", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastMsg != "" {
		t.Fatalf("deleted thread must not reach agent, got %q", client.lastMsg)
	}
	if len(reply.sends) == 0 || reply.sends[0] != accessDeniedMessage {
		t.Fatalf("expected deny for 404, got %v", reply.sends)
	}
}

func TestKnownThreadMakesZeroAPICalls(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), knownThreadMsg("thread-1", "parent-1", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if resolver.calls != 0 {
		t.Fatalf("known thread must make zero adapter calls, calls=%d", resolver.calls)
	}
	if client.lastMsg != "hello" {
		t.Fatalf("known thread not passed through, lastMsg=%q", client.lastMsg)
	}
}

func TestOrphanThreadInheritsParentWorkdirModel(t *testing.T) {
	r, client, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent-1"] = &store.Channel{
		ChannelID: "parent-1", Platform: "discord", Workdir: "/parent-wd", Model: "openai/gpt-4o", ListenMode: "all",
	}
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	tc := threadConfigsOf(r).configs["discord:parent-1:thread-orphan"]
	if tc == nil {
		t.Fatal("thread config not materialized")
	}
	if tc.Workdir != "/parent-wd" || tc.Model != "openai/gpt-4o" {
		t.Fatalf("snapshot must inherit parent workdir/model, got %+v", tc)
	}
	got, err := r.effectiveWorkdir(context.Background(), knownThreadMsg("thread-orphan", "parent-1", "user1", "", reply))
	if err != nil || got != "/parent-wd" {
		t.Fatalf("effectiveWorkdir=%q err=%v, want /parent-wd", got, err)
	}
	got, err = r.effectiveWorkdir(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "", reply))
	if err != nil || got != "/parent-wd" {
		t.Fatalf("orphan effectiveWorkdir=%q err=%v, want /parent-wd", got, err)
	}
	resolution, err := r.resolveModel(context.Background(), knownThreadMsg("thread-orphan", "parent-1", "user1", "", reply))
	if err != nil || resolution.model == nil || formatModelRef(*resolution.model) != "openai/gpt-4o" {
		t.Fatalf("model must resolve from parent, got %+v err=%v", resolution.model, err)
	}
}

func TestTelegramThreadUnchanged(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	m := channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: "chat1",
		ThreadID:  "topic-1",
		UserID:    "user1",
		Text:      "hello",
		IsMention: true,
		IsThread:  true,
		ReplyCtx:  reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if resolver.calls != 0 {
		t.Fatalf("telegram must never call parent resolver, calls=%d", resolver.calls)
	}
	if client.lastMsg != "hello" {
		t.Fatalf("telegram thread not passed through, lastMsg=%q", client.lastMsg)
	}
}

func TestOrphanAdminCommandAllowedViaParent(t *testing.T) {
	r, _, reply, _ := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	seedParentAdmin(st, "parent-1", "user1")
	resolver := &countingResolver{parent: "parent-1"}
	r.SetThreadParentResolver(resolver.fn)

	if err := r.Route(context.Background(), orphanMsg("thread-orphan", "parent-1", "user1", "/allow user2", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || reply.sends[0] != "✅ Allowed user: user2" {
		t.Fatalf("admin command in orphan must use parent scope, got %v", reply.sends)
	}
	o, err := st.overrideRepo.Get(context.Background(), "discord", "thread-orphan", "user2")
	if err != nil || o == nil || o.Role != "allow" {
		t.Fatalf("allow must land on thread scope, got %+v err=%v", o, err)
	}
}
