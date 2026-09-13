package discord

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

type fakeRoundTripper struct {
	do func(*http.Request) (*http.Response, error)
}

func (f fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f.do(req) }

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func newUnconnectedSession(t *testing.T) *discordgo.Session {
	t.Helper()
	s, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}
	if s.State.User != nil {
		t.Fatal("precondition changed: State.User is populated before Open")
	}
	return s
}

func TestConfigureDoesNotReadGatewayState(t *testing.T) {
	a := New("fake-token", nil)
	a.configure(newUnconnectedSession(t), func(channel.IncomingMessage) {})

	if a.selfID() != "" {
		t.Fatalf("bot identity known before READY: %q", a.selfID())
	}
}

func TestReadyPopulatesBotIdentity(t *testing.T) {
	a := New("fake-token", nil)
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})

	if a.selfID() != "bot-42" {
		t.Fatalf("selfID = %q, want bot-42", a.selfID())
	}
}

func TestReadyWithoutUserIsIgnored(t *testing.T) {
	a := New("fake-token", nil)
	a.onReady(&discordgo.Ready{})

	if a.selfID() != "" {
		t.Fatalf("selfID = %q, want empty", a.selfID())
	}
}

func TestMessageBeforeReadyIsStillDelivered(t *testing.T) {
	a := New("fake-token", nil)
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	var got []string
	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan",
		Author:    &discordgo.User{ID: "human"},
		Content:   "hello",
	}}, func(m channel.IncomingMessage) { got = append(got, m.Text) })

	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("message before READY not delivered: %v", got)
	}
}

func TestOwnMessageDroppedOnceIdentityKnown(t *testing.T) {
	a := New("fake-token", nil)
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})

	delivered := 0
	deliver := func(channel.IncomingMessage) { delivered++ }

	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan",
		Author:    &discordgo.User{ID: "bot-42"},
		Content:   "echo",
	}}, deliver)
	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan",
		Author:    &discordgo.User{ID: "someone", Bot: true},
		Content:   "other bot",
	}}, deliver)
	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan",
		Author:    &discordgo.User{ID: "someone", Bot: true},
		Mentions:  []*discordgo.User{{ID: "bot-42"}},
		Content:   "mentioned bot",
	}}, deliver)

	if delivered != 0 {
		t.Fatalf("delivered = %d, want 0", delivered)
	}
}

func TestRegisterCommandsSendsBulkOverwrite(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody []byte
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		gotPath = req.URL.Path
		gotMethod = req.Method
		gotBody, _ = io.ReadAll(req.Body)
		return jsonResponse(200, "[]"), nil
	}}}

	a := &Adapter{session: s, menu: []channel.MenuCommand{
		{Alias: "help", Description: "Show available commands"},
		{Alias: "session", Description: "Manage sessions", HasArgs: true},
	}}
	a.registerCommands(&discordgo.Ready{Application: &discordgo.Application{ID: "app-1"}})

	if gotMethod != http.MethodPut {
		t.Fatalf("expected PUT, got %q", gotMethod)
	}
	if !strings.Contains(gotPath, "applications/app-1/commands") {
		t.Fatalf("expected bulk-overwrite path, got %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "help") || !strings.Contains(string(gotBody), "session") {
		t.Fatalf("expected both commands in request body, got %q", gotBody)
	}
	if !strings.Contains(string(gotBody), `"name":"args"`) {
		t.Fatalf("expected args option for session, got %q", gotBody)
	}
}

func TestSanitizeCommandNameDiscord(t *testing.T) {
	cases := map[string]string{
		"customize-opencode":    "customize-opencode",
		"help":                  "help",
		"UPPER":                 "upper",
		strings.Repeat("a", 40): strings.Repeat("a", 32),
	}
	for in, want := range cases {
		if got := sanitizeCommandName(in); got != want {
			t.Fatalf("sanitizeCommandName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSetChatCommandsUsesGuildScope(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody []byte
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		gotPath = req.URL.Path
		gotMethod = req.Method
		gotBody, _ = io.ReadAll(req.Body)
		return jsonResponse(200, "[]"), nil
	}}}

	rc := &replyContext{session: s, guildID: "guild-1", appID: "app-1"}
	err := rc.SetChatCommands([]channel.MenuCommand{
		{Alias: "help", Description: "Show available commands"},
	})
	if err != nil {
		t.Fatalf("SetChatCommands: %v", err)
	}

	if gotMethod != http.MethodPut {
		t.Fatalf("expected PUT, got %q", gotMethod)
	}
	if !strings.Contains(gotPath, "applications/app-1/guilds/guild-1/commands") {
		t.Fatalf("expected guild-scoped path, got %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "help") {
		t.Fatalf("expected help in request body, got %q", gotBody)
	}
}

func TestSanitizeDescriptionDiscord(t *testing.T) {
	short := "Show available commands"
	if got := sanitizeDescription(short); got != short {
		t.Fatalf("sanitizeDescription(%q) = %q, want unchanged", short, got)
	}

	long := strings.Repeat("a", 300)
	got := sanitizeDescription(long)
	if len([]rune(got)) != 100 {
		t.Fatalf("sanitizeDescription truncated to %d runes, want 100", len([]rune(got)))
	}
}

func TestSetChatCommandsTruncatesLongDescription(t *testing.T) {
	var gotBody []byte
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		gotBody, _ = io.ReadAll(req.Body)
		return jsonResponse(200, "[]"), nil
	}}}

	rc := &replyContext{session: s, guildID: "guild-1", appID: "app-1"}
	longDescription := strings.Repeat("a", 300)
	err := rc.SetChatCommands([]channel.MenuCommand{
		{Alias: "customize-opencode", Description: longDescription},
	})
	if err != nil {
		t.Fatalf("SetChatCommands: %v", err)
	}

	if strings.Contains(string(gotBody), longDescription) {
		t.Fatalf("expected description to be truncated, got %q", gotBody)
	}
}

func TestSetChatCommandsNoOpsWithoutGuild(t *testing.T) {
	called := false
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		called = true
		return jsonResponse(200, "[]"), nil
	}}}

	rc := &replyContext{session: s, appID: "app-1"}
	if err := rc.SetChatCommands([]channel.MenuCommand{{Alias: "help", Description: "x"}}); err != nil {
		t.Fatalf("SetChatCommands: %v", err)
	}
	if called {
		t.Fatal("expected no HTTP call for a DM (no guild)")
	}
}

func TestRegisterCommandsSkipsWhenMenuEmpty(t *testing.T) {
	called := false
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		called = true
		return jsonResponse(200, "[]"), nil
	}}}

	a := &Adapter{session: s}
	a.registerCommands(&discordgo.Ready{Application: &discordgo.Application{ID: "app-1"}})

	if called {
		t.Fatal("expected no HTTP call when menu is empty")
	}
}

func TestRegisterCommandsSkipsWhenApplicationIDMissing(t *testing.T) {
	called := false
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		called = true
		return jsonResponse(200, "[]"), nil
	}}}

	a := &Adapter{session: s, menu: []channel.MenuCommand{{Alias: "help", Description: "x"}}}
	a.registerCommands(&discordgo.Ready{})

	if called {
		t.Fatal("expected no HTTP call when Application is nil")
	}
}

func TestRegisterCommandsFailureDoesNotPanic(t *testing.T) {
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		return jsonResponse(500, `{"message":"boom"}`), nil
	}}}

	a := &Adapter{session: s, menu: []channel.MenuCommand{{Alias: "help", Description: "x"}}}
	a.registerCommands(&discordgo.Ready{Application: &discordgo.Application{ID: "app-1"}}) // must not panic
}

func TestApplicationCommandInteractionReconstructsAliasedText(t *testing.T) {
	s := newUnconnectedSession(t)
	s.Client = &http.Client{Transport: fakeRoundTripper{do: func(req *http.Request) (*http.Response, error) {
		return jsonResponse(200, "{}"), nil
	}}}

	a := New("fake-token", nil)
	interaction := &discordgo.InteractionCreate{Interaction: &discordgo.Interaction{
		ID:        "int-1",
		Token:     "int-token",
		Type:      discordgo.InteractionApplicationCommand,
		ChannelID: "chan-1",
		Data: discordgo.ApplicationCommandInteractionData{
			Name: "session",
			Options: []*discordgo.ApplicationCommandInteractionDataOption{
				{Name: "args", Value: "list"},
			},
		},
		User: &discordgo.User{ID: "user-1"},
	}}

	var got channel.IncomingMessage
	a.handleApplicationCommandInteraction(s, interaction, func(m channel.IncomingMessage) { got = m })

	if got.Text != "/session list" {
		t.Fatalf("reconstructed text = %q, want %q", got.Text, "/session list")
	}
	if got.Platform != "discord" || got.UserID != "user-1" || got.ChannelID != "chan-1" {
		t.Fatalf("unexpected message fields: %+v", got)
	}
}

func TestIdentityWriteAndReadAreConcurrencySafe(t *testing.T) {
	a := New("fake-token", nil)
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	}()
	go func() {
		defer wg.Done()
		a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
			ChannelID: "chan",
			Author:    &discordgo.User{ID: "human"},
		}}, func(channel.IncomingMessage) {})
	}()
	wg.Wait()
}

func TestAllowlistedBotAdmission(t *testing.T) {
	a := NewWithPolicy("fake-token", nil, AllowlistPolicy{AllowedSenderIDs: []string{"trusted-bot"}})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "occa-bot"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	tests := []struct {
		name      string
		authorID  string
		channelID string
		mentions  []*discordgo.User
		want      bool
	}{
		{name: "listed bot admitted in any channel", authorID: "trusted-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}, want: true},
		{name: "listed bot admitted in new channel", authorID: "trusted-bot", channelID: "other-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}, want: true},
		{name: "unlisted bot dropped", authorID: "other-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}},
		{name: "listed bot still needs bot-user mention", authorID: "trusted-bot", channelID: "allowed-channel"},
		{name: "role mention alone never counts", authorID: "trusted-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "someone-else"}}},
		{name: "self bot", authorID: "occa-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delivered := 0
			a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
				GuildID:   "guild",
				ChannelID: tt.channelID,
				Author:    &discordgo.User{ID: tt.authorID, Bot: true},
				Mentions:  tt.mentions,
			}}, func(channel.IncomingMessage) { delivered++ })
			if got := delivered == 1; got != tt.want {
				t.Fatalf("delivered = %d, want match=%v", delivered, tt.want)
			}
		})
	}
}

func TestAllowlistedBotThreadAdmission(t *testing.T) {
	const (
		trustedBotID = "trusted-bot"
		occaBotID    = "occa-bot"
		threadID     = "thread-1"
		parentID     = "parent-channel"
	)

	tests := []struct {
		name        string
		authorID    string
		bot         bool
		channelType discordgo.ChannelType
		parentID    string
		owned       bool
		mention     bool
		want        bool
	}{
		{name: "listed bot in owned thread", authorID: trustedBotID, bot: true, channelType: discordgo.ChannelTypeGuildPublicThread, parentID: parentID, owned: true, mention: true, want: true},
		{name: "unlisted bot in owned thread", authorID: "other-bot", bot: true, channelType: discordgo.ChannelTypeGuildPublicThread, parentID: parentID, owned: true, mention: true},
		{name: "listed bot without mention", authorID: trustedBotID, bot: true, channelType: discordgo.ChannelTypeGuildPublicThread, parentID: parentID, owned: true},
		{name: "self bot", authorID: occaBotID, bot: true, channelType: discordgo.ChannelTypeGuildPublicThread, parentID: parentID, owned: true, mention: true},
		{name: "human compatibility", authorID: "human", channelType: discordgo.ChannelTypeGuildPublicThread, parentID: parentID, mention: false, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewWithPolicy("fake-token", nil, AllowlistPolicy{AllowedSenderIDs: []string{trustedBotID}})
			a.setBotID(occaBotID)
			a.channelLookup = func(string) (*discordgo.Channel, error) {
				return &discordgo.Channel{Type: tt.channelType, ParentID: tt.parentID}, nil
			}
			a.SetOwnedThreadCheck(func(string) (bool, error) { return tt.owned, nil })

			var got channel.IncomingMessage
			delivered := 0
			message := &discordgo.Message{
				GuildID:   "guild",
				ChannelID: threadID,
				Author:    &discordgo.User{ID: tt.authorID, Bot: tt.bot},
			}
			if tt.mention {
				message.Mentions = []*discordgo.User{{ID: occaBotID}}
			}
			a.onMessage(&discordgo.MessageCreate{Message: message}, func(message channel.IncomingMessage) {
				delivered++
				got = message
			})

			if (delivered == 1) != tt.want {
				t.Fatalf("delivered = %d, want %v", delivered, tt.want)
			}
			if !tt.want || !tt.bot {
				return
			}
			if got.ChannelID != parentID || got.ParentChannelID != parentID || got.ThreadID != threadID || !got.IsThread || !got.IsMention {
				t.Fatalf("unexpected normalized thread message: %+v", got)
			}
			rc, ok := got.ReplyCtx.(*replyContext)
			if !ok || rc.channelID != threadID {
				t.Fatalf("reply context channel = %q, want %q", rc.channelID, threadID)
			}
		})
	}
}

func TestRoleMentionNeverCountsAsOccaMention(t *testing.T) {
	a := NewWithPolicy("fake-token", nil, AllowlistPolicy{AllowedSenderIDs: []string{"human"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	for _, roleID := range []string{"role-1", "role-2"} {
		t.Run(roleID, func(t *testing.T) {
			var got channel.IncomingMessage
			a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
				GuildID:      "guild",
				ChannelID:    "channel",
				Author:       &discordgo.User{ID: "human"},
				MentionRoles: []string{roleID},
			}}, func(message channel.IncomingMessage) { got = message })
			if got.IsMention {
				t.Fatalf("role mention %q must never count as an OCCA mention", roleID)
			}
		})
	}
}
