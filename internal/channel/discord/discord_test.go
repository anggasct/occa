package discord

import (
	"encoding/json"
	"errors"
	"io"
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

// TestOutboundMessagesSuppressMentions covers the allowed-mentions hardening:
// every outbound call site sends allowed_mentions with the zero-value shape
// (parse:null, replied_user:false) so Discord pings nothing.
func TestOutboundMessagesSuppressMentions(t *testing.T) {
	var bodies []string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		return []byte(`{"id":"1"}`), http.StatusOK
	})

	a := &Adapter{session: session}
	if err := a.Notify("ch-1", "notify text"); err != nil {
		t.Fatalf("Notify: %v", err)
	}

	rc := &replyContext{session: session, channelID: "ch-1"}
	if _, err := rc.Send("plain send"); err != nil {
		t.Fatalf("Send channel path: %v", err)
	}
	if _, err := rc.SendWithButtons("with buttons", []channel.Button{{Label: "Allow", Value: "allow"}}); err != nil {
		t.Fatalf("SendWithButtons: %v", err)
	}
	if err := rc.Edit(messageRef{id: "msg-1"}, "edited"); err != nil {
		t.Fatalf("Edit: %v", err)
	}
	if err := rc.EditWithButtons(messageRef{id: "msg-1"}, "edited", []channel.Button{{Label: "Allow", Value: "allow"}}); err != nil {
		t.Fatalf("EditWithButtons: %v", err)
	}

	interactionRC := &replyContext{
		session:     session,
		channelID:   "ch-1",
		interaction: &discordgo.Interaction{AppID: "app-1", Token: "tok-1"},
	}
	if _, err := interactionRC.Send("interaction send"); err != nil {
		t.Fatalf("Send interaction path: %v", err)
	}

	if len(bodies) != 6 {
		t.Fatalf("expected 6 outbound calls, got %d", len(bodies))
	}
	want := `"allowed_mentions":{"parse":null,"replied_user":false}`
	for i, body := range bodies {
		if !strings.Contains(body, want) {
			t.Fatalf("call %d body missing allowed_mentions suppression: %s", i, body)
		}
	}
}

// TestComponentRowsGroupsByRow: buttons sharing a Row hint land in one
// action row; Row 0 keeps the legacy all-in-one-row layout.
func TestComponentRowsGroupsByRow(t *testing.T) {
	components := componentRows([]channel.Button{
		{Label: "a", Value: "1", Row: 1},
		{Label: "b", Value: "2", Row: 1},
		{Label: "c", Value: "3"},
		{Label: "d", Value: "4"},
	})
	rows := components
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (%+v)", len(rows), rows)
	}
	row1, ok := rows[0].(discordgo.ActionsRow)
	if !ok || len(row1.Components) != 2 {
		t.Fatalf("grouped row = %+v", rows[0])
	}
	row2, ok := rows[1].(discordgo.ActionsRow)
	if !ok || len(row2.Components) != 2 {
		t.Fatalf("legacy row = %+v", rows[1])
	}
}

// TestComponentRowsChunksOversizedRows: a same-Row group larger than
// Discord's 5-button row cap is chunked into multiple action rows.
func TestComponentRowsChunksOversizedRows(t *testing.T) {
	buttons := make([]channel.Button, 6)
	for i := range buttons {
		buttons[i] = channel.Button{Label: string(rune('a' + i)), Value: "v", Row: 1}
	}
	components := componentRows(buttons)
	if len(components) != 2 {
		t.Fatalf("rows = %d, want 2", len(components))
	}
	row1, ok1 := components[0].(discordgo.ActionsRow)
	row2, ok2 := components[1].(discordgo.ActionsRow)
	if !ok1 || !ok2 || len(row1.Components) != 5 || len(row2.Components) != 1 {
		t.Fatalf("row sizes = %d/%d, want 5/1", len(row1.Components), len(row2.Components))
	}
}

// TestComponentRowsBrowserPageWithinCap: a full 10-item browser page (5+5
// items plus one nav row) stays within Discord's 5-action-row message cap.
func TestComponentRowsBrowserPageWithinCap(t *testing.T) {
	buttons := make([]channel.Button, 0, 14)
	for i := 0; i < 10; i++ {
		buttons = append(buttons, channel.Button{Label: string(rune('a' + i)), Value: "v", Row: i/5 + 1})
	}
	for _, label := range []string{"⬅️ Providers", "◀️ Prev", "Next ▶️", "✖️ Close"} {
		buttons = append(buttons, channel.Button{Label: label, Value: "v", Row: 100})
	}
	components := componentRows(buttons)
	if len(components) != 3 {
		t.Fatalf("action rows = %d, want 3", len(components))
	}
	for _, c := range components {
		row, ok := c.(discordgo.ActionsRow)
		if !ok || len(row.Components) > 5 {
			t.Fatalf("row exceeds Discord's 5-button cap: %+v", c)
		}
	}
}

// TestSendWithButtonsResolvesInteraction covers the deferred-interaction
// fix: when the reply context holds a pending interaction, SendWithButtons
// edits the deferred response (content + buttons) instead of sending a new
// channel message, consumes the interaction, and returns the edited ref.
func TestSendWithButtonsResolvesInteraction(t *testing.T) {
	var paths []string
	var bodies []string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		body, _ := io.ReadAll(r.Body)
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, string(body))
		return []byte(`{"id":"edited-1"}`), http.StatusOK
	})

	rc := &replyContext{
		session:     session,
		channelID:   "ch-1",
		interaction: &discordgo.Interaction{AppID: "app-1", Token: "tok-1"},
	}
	ref, err := rc.SendWithButtons("resolve me", []channel.Button{{Label: "Allow", Value: "allow"}})
	if err != nil {
		t.Fatalf("SendWithButtons: %v", err)
	}
	if ref.ID() != "edited-1" {
		t.Fatalf("ref id = %q, want %q", ref.ID(), "edited-1")
	}
	if rc.interaction != nil {
		t.Fatal("interaction must be consumed after the edit")
	}
	if len(paths) != 1 || !strings.Contains(paths[0], "/webhooks/app-1/tok-1/messages/@original") {
		t.Fatalf("expected one interaction response edit, got paths %v", paths)
	}
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"content":"resolve me"`) || !strings.Contains(bodies[0], `"custom_id":"allow"`) {
		t.Fatalf("edit body must carry content and buttons, got %s", bodies)
	}
}

// TestSendWithButtonsClampsInteractionContent covers review finding 1: the
// deferred-interaction path must clamp the content to Discord's limit just
// like the channel-message path, so a long permission/question prompt cannot
// exceed 2000 units and fail with BASE_TYPE_MAX_LENGTH.
func TestSendWithButtonsClampsInteractionContent(t *testing.T) {
	var body string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		return []byte(`{"id":"edited-1"}`), http.StatusOK
	})

	rc := &replyContext{
		session:     session,
		channelID:   "ch-1",
		interaction: &discordgo.Interaction{AppID: "app-1", Token: "tok-1"},
	}
	long := strings.Repeat("x", render.DiscordLimit+500)
	if _, err := rc.SendWithButtons(long, []channel.Button{{Label: "Allow", Value: "allow"}}); err != nil {
		t.Fatalf("SendWithButtons: %v", err)
	}

	var edit struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(body), &edit); err != nil {
		t.Fatalf("decode edit body %q: %v", body, err)
	}
	if len([]rune(edit.Content)) > render.DiscordLimit {
		t.Fatalf("edited content is %d runes, over DiscordLimit %d", len([]rune(edit.Content)), render.DiscordLimit)
	}
	if !strings.HasSuffix(edit.Content, "…") {
		t.Fatalf("edited content must end with the clamp marker, got %q", edit.Content)
	}
}

// TestSendWithButtonsFallbackAfterInteraction covers the fallback: once the
// pending interaction is consumed by the first call, a second
// SendWithButtons sends a regular channel message.
func TestSendWithButtonsFallbackAfterInteraction(t *testing.T) {
	var paths []string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		paths = append(paths, r.URL.Path)
		return []byte(`{"id":"new-1"}`), http.StatusOK
	})

	rc := &replyContext{
		session:     session,
		channelID:   "ch-1",
		interaction: &discordgo.Interaction{AppID: "app-1", Token: "tok-1"},
	}
	if _, err := rc.SendWithButtons("first", []channel.Button{{Label: "Allow", Value: "allow"}}); err != nil {
		t.Fatalf("first SendWithButtons: %v", err)
	}
	ref, err := rc.SendWithButtons("second", []channel.Button{{Label: "Deny", Value: "deny"}})
	if err != nil {
		t.Fatalf("second SendWithButtons: %v", err)
	}
	if ref.ID() != "new-1" {
		t.Fatalf("ref id = %q, want %q", ref.ID(), "new-1")
	}
	if len(paths) != 2 || !strings.Contains(paths[0], "/webhooks/app-1/tok-1/messages/@original") || !strings.Contains(paths[1], "/channels/ch-1/messages") {
		t.Fatalf("expected edit then channel send, got paths %v", paths)
	}
}

// TestSendWithButtonsNoInteraction covers the unchanged path: without a
// pending interaction SendWithButtons sends a new channel message and
// returns its ref.
func TestSendWithButtonsNoInteraction(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		return []byte(`{"id":"plain-1"}`), http.StatusOK
	})

	rc := &replyContext{session: session, channelID: "ch-1"}
	ref, err := rc.SendWithButtons("no interaction", []channel.Button{{Label: "Allow", Value: "allow"}})
	if err != nil {
		t.Fatalf("SendWithButtons: %v", err)
	}
	if ref.ID() != "plain-1" {
		t.Fatalf("ref id = %q, want %q", ref.ID(), "plain-1")
	}
}

// TestNormalizeMessageSetsSourceRef: an ordinary user message carries a
// SourceRef pointing at the triggering message for read-receipt reactions.
func TestNormalizeMessageSetsSourceRef(t *testing.T) {
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			return &discordgo.Channel{ID: channelID, Type: discordgo.ChannelTypeGuildText}, nil
		},
	}
	a.setBotID("bot")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "channel-1",
		ID:        "msg-1",
		Author:    &discordgo.User{ID: "user"},
		Content:   "<@bot> hello",
		Mentions:  []*discordgo.User{{ID: "bot"}},
	})

	if got.SourceRef == nil || got.SourceRef.ID() != "msg-1" {
		t.Fatalf("SourceRef = %+v, want message ref msg-1", got.SourceRef)
	}
}

// TestAutoThreadReactionTargetsSourceChannel: in the auto-thread case the
// reaction channel stays on the parent channel that hosts the user's message,
// and SourceRef points at the triggering message.
func TestAutoThreadReactionTargetsSourceChannel(t *testing.T) {
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/threads") {
			return []byte(`{"id":"thread-9","name":"summarize the repo","type":12}`), http.StatusOK
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

	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok {
		t.Fatalf("reply context type: %T", got.ReplyCtx)
	}
	// Reply lands in the thread, but reactions must target the parent channel.
	if rc.channelID != "thread-9" {
		t.Fatalf("reply channelID = %q, want thread-9", rc.channelID)
	}
	if rc.reactionChannelID != "channel-1" {
		t.Fatalf("reactionChannelID = %q, want source channel-1", rc.reactionChannelID)
	}
	if got.SourceRef == nil || got.SourceRef.ID() != "msg-1" {
		t.Fatalf("SourceRef = %+v, want msg-1", got.SourceRef)
	}
}

// TestInteractionMessagesHaveNoSourceRef: interaction-driven messages (slash
// commands) carry no SourceRef — there is no ordinary user message to react on.
func TestInteractionMessagesHaveNoSourceRef(t *testing.T) {
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(*http.Request) (*http.Response, error) {
		return jsonResponse(200, "{}"), nil
	}}}

	a := New("fake-token", nil)
	interaction := &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{
		ID:        "int-1",
		Token:     "int-token",
		Type:      discordgo.InteractionApplicationCommand,
		ChannelID: "chan-1",
		Data:      discordgo.ApplicationCommandInteractionData{Name: "help"},
		User:      &discordgo.User{ID: "user-1"},
	}}

	var got channel.IncomingMessage
	a.handleApplicationCommandInteraction(s, interaction, func(m channel.IncomingMessage) { got = m })

	if got.SourceRef != nil {
		t.Fatalf("interaction message must not carry a source ref, got %+v", got.SourceRef)
	}
}
