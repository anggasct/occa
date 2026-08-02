package discord

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

func TestNormalizeMessageResolvesThreadScopeOnce(t *testing.T) {
	calls := 0
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			calls++
			return &discordgo.Channel{ID: channelID, ParentID: "parent", Type: discordgo.ChannelTypeGuildPublicThread}, nil
		},
	}
	a.setBotID("bot")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread",
		Author:    &discordgo.User{ID: "user"},
		Content:   "hello",
	})

	if calls != 1 {
		t.Fatalf("channel lookup calls = %d, want 1", calls)
	}
	if got.ParentChannelID != "parent" || !got.IsThread || got.ChannelScopeUnresolved {
		t.Fatalf("unexpected normalized scope: %+v", got)
	}
}

func TestNormalizeMessageMarksFailedScopeLookup(t *testing.T) {
	a := &Adapter{
		channelLookup: func(string) (*discordgo.Channel, error) {
			return nil, errors.New("lookup failed")
		},
	}
	a.setBotID("bot")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread",
		Author:    &discordgo.User{ID: "user"},
	})

	if !got.ChannelScopeUnresolved || got.ParentChannelID != "" {
		t.Fatalf("unexpected failed scope normalization: %+v", got)
	}
}

func TestSplitMessageShort(t *testing.T) {
	chunks := render.Split("hello", 2000)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 1500)
	para2 := strings.Repeat("b", 1500)
	text := para1 + "\n\n" + para2

	chunks := render.Split(text, 2000)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 2000 {
			t.Fatalf("chunk %d exceeds 2000: %d", i, len(chunk))
		}
	}
}

func TestComponentRowsEmptyRemovesButtons(t *testing.T) {
	if components := componentRows(nil); components == nil || len(components) != 0 {
		t.Fatalf("empty components = %+v", components)
	}
	components := componentRows([]channel.Button{{Label: "Allow", Value: "allow"}})
	if len(components) != 1 {
		t.Fatalf("button components = %+v", components)
	}
}

func TestDownloadAttachmentTimeout(t *testing.T) {
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never respond until the client gives up
	}))
	defer blocked.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 200 * time.Millisecond}}
	start := time.Now()
	atts := a.downloadAttachments(&discordgo.Message{
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "stalled.bin", ContentType: "application/octet-stream", URL: blocked.URL},
		},
	})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("download blocked for %v", elapsed)
	}
	if len(atts) != 0 {
		t.Fatalf("expected stalled attachment dropped, got %d", len(atts))
	}
}

func TestDownloadAttachmentSucceeds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("file-data"))
	}))
	defer ts.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 5 * time.Second}}
	atts := a.downloadAttachments(&discordgo.Message{
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "ok.txt", ContentType: "text/plain", URL: ts.URL},
		},
	})
	if len(atts) != 1 || string(atts[0].Data) != "file-data" {
		t.Fatalf("unexpected attachments: %+v", atts)
	}
}

// fakeDiscordSession builds a discordgo session whose REST calls hit a local
// httptest server, so adapter behavior can be exercised without a gateway.
func fakeDiscordSession(t *testing.T, handle func(r *http.Request) ([]byte, int)) *discordgo.Session {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status := handle(r)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	s, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	direct := &http.Client{}
	s.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		u := *r.URL
		u.Scheme = "http"
		u.Host = strings.TrimPrefix(ts.URL, "http://")
		r.URL = &u
		return direct.Do(r)
	})
	return s
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestAutoThreadCreatesThreadAndRescopesMessage covers the auto-thread flow: an @mention in
// an auto-thread channel creates a thread from the message, keeps access
// scope on the parent channel, and routes replies into the thread.
func TestAutoThreadCreatesThreadAndRescopesMessage(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/threads") {
			return []byte(`{"id":"thread-9","name":"summarize the repo","type":12}`), http.StatusOK
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/channels/thread-9") {
			return []byte(`{"id":"thread-9","parent_id":"channel-1","type":12}`), http.StatusOK
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetAutoThreadPolicy(func(channelID string) (bool, error) { return true, nil })

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "channel-1",
		ID:        "msg-1",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "<@bot1> summarize the repo",
		Mentions:  []*discordgo.User{{ID: "bot1"}},
	})

	if got.ThreadID != "thread-9" || !got.IsThread || got.IsMention != true {
		t.Fatalf("auto-thread message not re-scoped to the thread: %+v", got)
	}
	if got.ChannelID != "channel-1" || got.ParentChannelID != "channel-1" {
		t.Fatalf("access scope must stay on the parent channel, got channel=%q parent=%q", got.ChannelID, got.ParentChannelID)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.channelID != "thread-9" {
		t.Fatalf("reply context must target the new thread, got %+v", got.ReplyCtx)
	}
}

// TestAutoThreadFollowUpNeedsNoMention covers follow-ups: messages in a
// thread OCCA created are treated as bot-directed and scoped to the parent
// channel for authorization.
func TestAutoThreadFollowUpNeedsNoMention(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/channels/thread-9") {
			return []byte(`{"id":"thread-9","parent_id":"channel-1","type":12}`), http.StatusOK
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetAutoThreadPolicy(func(channelID string) (bool, error) { return true, nil })
	a.trackThread("thread-9")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread-9",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "continue please",
	})

	if !got.IsMention {
		t.Fatal("follow-up in an auto-created thread must not need a mention")
	}
	if got.ThreadID != "thread-9" || !got.IsThread {
		t.Fatalf("follow-up must stay scoped to the thread: %+v", got)
	}
	if got.ChannelID != "channel-1" {
		t.Fatalf("follow-up access scope must be the parent channel, got %q", got.ChannelID)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.channelID != "thread-9" {
		t.Fatalf("follow-up reply must land in the thread, got %+v", got.ReplyCtx)
	}
}

// TestAutoThreadDisabledRepliesInline covers auto_thread=0: the
// @mention replies inline in the channel, no thread is created.
func TestAutoThreadDisabledRepliesInline(t *testing.T) {
	var threadCalls int
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/threads") {
			threadCalls++
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetAutoThreadPolicy(func(channelID string) (bool, error) { return false, nil })

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "channel-1",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "<@bot1> hello",
		Mentions:  []*discordgo.User{{ID: "bot1"}},
	})

	if threadCalls != 0 {
		t.Fatalf("expected no thread creation with auto_thread disabled, got %d calls", threadCalls)
	}
	if got.ThreadID != "" || got.IsThread {
		t.Fatalf("expected inline reply without thread scope: %+v", got)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.channelID != "channel-1" {
		t.Fatalf("reply context must target the channel, got %+v", got.ReplyCtx)
	}
}

// TestAutoThreadFailureFallsBackInline: a failed thread creation keeps the
// message inline in the parent channel.
func TestAutoThreadFailureFallsBackInline(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/threads") {
			return []byte(`{"message":"missing permissions"}`), http.StatusForbidden
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetAutoThreadPolicy(func(channelID string) (bool, error) { return true, nil })

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "channel-1",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "<@bot1> hello",
		Mentions:  []*discordgo.User{{ID: "bot1"}},
	})

	if got.ThreadID != "" || got.IsThread {
		t.Fatalf("expected inline fallback after thread failure: %+v", got)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.channelID != "channel-1" {
		t.Fatalf("reply context must stay on the channel after thread failure, got %+v", got.ReplyCtx)
	}
}

func TestThreadNameSanitization(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"<@1234> summarize the repo", "summarize the repo"},
		{"", "OCCA chat"},
		{"   ", "OCCA chat"},
		{"a`b@c#d?e:f*", "abcdef"},
		{strings.Repeat("x", 120), strings.Repeat("x", 100)},
	}
	for _, c := range cases {
		if got := threadName(c.in); got != c.want {
			t.Fatalf("threadName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestAutoThreadFollowUpSurvivesRestart: after a restart the in-memory
// participation map is empty, but the persisted ownership check still
// recognizes an OCCA-created thread (its session key lives in the parent
// channel), so follow-ups keep working without a mention.
func TestAutoThreadFollowUpSurvivesRestart(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/channels/thread-9") {
			return []byte(`{"id":"thread-9","parent_id":"channel-1","type":12}`), http.StatusOK
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetAutoThreadPolicy(func(channelID string) (bool, error) { return true, nil })
	a.SetOwnedThreadCheck(func(threadID string) (bool, error) {
		return threadID == "thread-9", nil
	})

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread-9",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "continue please",
	})

	if !got.IsMention {
		t.Fatal("follow-up after restart must still not need a mention")
	}
	if got.ThreadID != "thread-9" || !got.IsThread {
		t.Fatalf("follow-up must stay scoped to the thread: %+v", got)
	}
	if got.ChannelID != "channel-1" {
		t.Fatalf("follow-up access scope must be the parent channel after restart, got %q", got.ChannelID)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.channelID != "thread-9" {
		t.Fatalf("follow-up reply must land in the thread, got %+v", got.ReplyCtx)
	}
}

// TestUserThreadNotTreatedAsOwned: a thread whose session keys to itself
// (user-created thread the bot only participated in) is not re-scoped.
func TestUserThreadNotTreatedAsOwned(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/channels/user-thread") {
			return []byte(`{"id":"user-thread","parent_id":"channel-1","type":12}`), http.StatusOK
		}
		return []byte(`{"id":"channel-1","type":0}`), http.StatusOK
	})

	a := &Adapter{session: session}
	a.setBotID("bot1")
	a.SetOwnedThreadCheck(func(threadID string) (bool, error) {
		return false, nil
	})

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "user-thread",
		Author:    &discordgo.User{ID: "user-1"},
		Content:   "hello",
		Mentions:  []*discordgo.User{{ID: "bot1"}},
	})

	if got.ChannelID != "user-thread" {
		t.Fatalf("user-created thread must keep its own channel scope, got %q", got.ChannelID)
	}
}
