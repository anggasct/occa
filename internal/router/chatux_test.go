package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func threadMsg(channelID, threadID, userID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "discord",
		ChannelID: channelID,
		ThreadID:  threadID,
		UserID:    userID,
		Text:      text,
		IsMention: true,
		IsThread:  threadID != "",
		ReplyCtx:  reply,
	}
}

func TestConversationKeyPolicy(t *testing.T) {
	msgChannel := func(userID string) channel.IncomingMessage {
		return channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: userID}
	}
	msgThread := func(threadID, userID string) channel.IncomingMessage {
		return channel.IncomingMessage{Platform: "discord", ChannelID: "chat1", ThreadID: threadID, UserID: userID, IsThread: true}
	}

	t.Run("group messages isolate per user", func(t *testing.T) {
		threadA, userA := conversationKey(msgChannel("alice"))
		threadB, userB := conversationKey(msgChannel("bob"))
		if threadA != "" || userA != "alice" || threadB != "" || userB != "bob" {
			t.Fatalf("group keys = (%q,%q)/(%q,%q), want per-user", threadA, userA, threadB, userB)
		}
	})

	t.Run("thread messages share per thread", func(t *testing.T) {
		threadA, userA := conversationKey(msgThread("thread-1", "alice"))
		threadB, userB := conversationKey(msgThread("thread-1", "bob"))
		if threadA != "thread-1" || userA != "" || threadB != "thread-1" || userB != "" {
			t.Fatalf("thread keys = (%q,%q)/(%q,%q), want shared per thread", threadA, userA, threadB, userB)
		}
	})
}

func TestTwoUsersSameChannelSeparateSessions(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:alice"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "alice", Role: "allow"}
	overrideRepo.overrides["telegram:chat1:bob"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "bob", Role: "allow"}
	st := r.store.(*fakeStore)

	release := make(chan struct{})
	client.blockSend = release

	replyA := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("alice", "hello alice", replyA)); err != nil {
		t.Fatalf("Route alice: %v", err)
	}

	replyB := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("bob", "hello bob", replyB)); err != nil {
		t.Fatalf("Route bob: %v", err)
	}
	if len(replyB.sends) != 0 {
		t.Fatalf("bob's task must be admitted while alice's is in flight, got busy notice: %v", replyB.sends)
	}

	close(release)
	waitForDispatch(t, client)
	waitForResponse(t, r)

	if client.sendCalls != 2 {
		t.Fatalf("expected both messages dispatched, got %d sends", client.sendCalls)
	}
	keyA := st.sessionRepo.sessionKey("telegram", "chat1", "", "alice")
	keyB := st.sessionRepo.sessionKey("telegram", "chat1", "", "bob")
	if keyA == keyB {
		t.Fatal("alice and bob must not share a session key")
	}
	if st.sessionRepo.activeBy[keyA] == "" || st.sessionRepo.activeBy[keyB] == "" {
		t.Fatalf("expected separate active sessions per user, got %+v", st.sessionRepo.activeBy)
	}
	if st.sessionRepo.activeBy[keyA] == st.sessionRepo.activeBy[keyB] {
		t.Fatal("alice and bob must not resolve the same agent session")
	}
}

func TestSameConversationIsSingleFlight(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:alice"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "alice", Role: "allow"}

	block := make(chan struct{})
	client.blockSend = block

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("alice", "first task", reply)); err != nil {
		t.Fatalf("Route first: %v", err)
	}

	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msgFrom("alice", "second task", reply2)); err != nil {
		t.Fatalf("Route second: %v", err)
	}
	expectedQueuedNotice := "⏳ Queued — 1 message(s) will run after the current response finishes."
	if len(reply2.sends) != 1 || reply2.sends[0] != expectedQueuedNotice {
		t.Fatalf("second task reply = %v, want queued notice %q", reply2.sends, expectedQueuedNotice)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		calls := client.sendCalls
		client.mu.Unlock()
		if calls >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	client.mu.Lock()
	sendCalls := client.sendCalls
	client.mu.Unlock()
	if sendCalls != 1 {
		t.Fatalf("sendCalls = %d, want 1 before unblocking", sendCalls)
	}

	close(block)
	waitForDispatch(t, client)

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		calls := client.sendCalls
		client.mu.Unlock()
		if calls >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	client.mu.Lock()
	sendCalls = client.sendCalls
	client.mu.Unlock()
	if sendCalls != 2 {
		t.Fatalf("sendCalls = %d, want 2 after completion", sendCalls)
	}
}

func TestDifferentThreadsRunConcurrently(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	for _, user := range []string{"alice", "bob"} {
		overrideRepo.overrides["discord:chat1:"+user] = &store.UserOverride{ChannelID: "chat1", Platform: "discord", UserID: user, Role: "allow"}
	}

	block := make(chan struct{})
	client.blockSend = block

	replyA := &fakeReplyCtx{}
	if err := r.Route(context.Background(), threadMsg("chat1", "thread-1", "alice", "hi", replyA)); err != nil {
		t.Fatalf("Route thread-1: %v", err)
	}

	replyB := &fakeReplyCtx{}
	if err := r.Route(context.Background(), threadMsg("chat1", "thread-2", "bob", "hi", replyB)); err != nil {
		t.Fatalf("Route thread-2: %v", err)
	}
	if len(replyB.sends) != 0 {
		t.Fatalf("thread-2 task should be admitted, got busy notice: %v", replyB.sends)
	}

	close(block)
	waitForDispatch(t, client)
	waitForResponse(t, r)
}

func TestSessionNewResetsOnlyCurrentConversation(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	for _, key := range []string{"discord:chat1:alice", "discord:chat1:bob"} {
		overrideRepo.overrides[key] = &store.UserOverride{ChannelID: "chat1", Platform: "discord", UserID: strings.Split(key, ":")[2], Role: "allow"}
	}
	st := r.store.(*fakeStore)
	client.blockSend = nil

	replyA := &fakeReplyCtx{}
	if err := r.Route(context.Background(), threadMsg("chat1", "topic-1", "alice", "hello", replyA)); err != nil {
		t.Fatalf("Route topic-1: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	topic1Key := st.sessionRepo.sessionKey("discord", "chat1", "topic-1", "")
	topic2Key := st.sessionRepo.sessionKey("discord", "chat1", "topic-2", "")
	if st.sessionRepo.activeBy[topic1Key] == "" {
		t.Fatalf("expected an active session for topic-1, got %+v", st.sessionRepo.activeBy)
	}
	before := st.sessionRepo.activeBy[topic1Key]

	replyB := &fakeReplyCtx{}
	if err := r.Route(context.Background(), threadMsg("chat1", "topic-2", "alice", "/session new", replyB)); err != nil {
		t.Fatalf("Route /new in topic-2: %v", err)
	}
	if st.sessionRepo.activeBy[topic1Key] != before {
		t.Fatalf("topic-1 session must survive topic-2's reset, got %q", st.sessionRepo.activeBy[topic1Key])
	}
	if st.sessionRepo.activeBy[topic2Key] == "" {
		t.Fatalf("expected a new session for topic-2, got %+v", st.sessionRepo.activeBy)
	}
}
