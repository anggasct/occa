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

func TestTrustedBotAdmission(t *testing.T) {
	policy := TrustedBotPolicy{
		TriggerRoleIDs: []string{"role-1"},
		TrustedBotSenders: []TrustedBotSender{{
			UserID:     "trusted-bot",
			ChannelIDs: []string{"allowed-channel"},
		}},
	}
	a := NewWithPolicy("fake-token", nil, policy)
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "occa-bot"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	tests := []struct {
		name       string
		authorID   string
		channelID  string
		mentions   []*discordgo.User
		mentionIDs []string
		want       bool
	}{
		{name: "user mention", authorID: "trusted-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}, want: true},
		{name: "configured role mention", authorID: "trusted-bot", channelID: "allowed-channel", mentionIDs: []string{"role-1"}, want: true},
		{name: "sender mismatch", authorID: "other-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}},
		{name: "channel mismatch", authorID: "trusted-bot", channelID: "other-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}},
		{name: "missing mention", authorID: "trusted-bot", channelID: "allowed-channel"},
		{name: "unconfigured role", authorID: "trusted-bot", channelID: "allowed-channel", mentionIDs: []string{"other-role"}},
		{name: "self bot", authorID: "occa-bot", channelID: "allowed-channel", mentions: []*discordgo.User{{ID: "occa-bot"}}, mentionIDs: []string{"role-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delivered := 0
			a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
				GuildID:      "guild",
				ChannelID:    tt.channelID,
				Author:       &discordgo.User{ID: tt.authorID, Bot: true},
				Mentions:     tt.mentions,
				MentionRoles: tt.mentionIDs,
			}}, func(channel.IncomingMessage) { delivered++ })
			if got := delivered == 1; got != tt.want {
				t.Fatalf("delivered = %d, want match=%v", delivered, tt.want)
			}
		})
	}
}

func TestConfiguredRoleMentionNormalization(t *testing.T) {
	a := NewWithPolicy("fake-token", nil, TrustedBotPolicy{TriggerRoleIDs: []string{"role-1"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}

	tests := []struct {
		name       string
		mentionIDs []string
		want       bool
	}{
		{name: "configured role", mentionIDs: []string{"role-1"}, want: true},
		{name: "arbitrary role", mentionIDs: []string{"role-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got channel.IncomingMessage
			a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
				GuildID:      "guild",
				ChannelID:    "channel",
				Author:       &discordgo.User{ID: "human"},
				MentionRoles: tt.mentionIDs,
			}}, func(message channel.IncomingMessage) { got = message })
			if got.IsMention != tt.want {
				t.Fatalf("IsMention = %v, want %v", got.IsMention, tt.want)
			}
		})
	}
}
