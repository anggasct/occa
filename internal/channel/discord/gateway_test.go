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
		{Alias: "occa_help", Description: "Show available commands"},
		{Alias: "occa_session", Description: "Manage sessions", HasArgs: true},
	}}
	a.registerCommands(&discordgo.Ready{Application: &discordgo.Application{ID: "app-1"}})

	if gotMethod != http.MethodPut {
		t.Fatalf("expected PUT, got %q", gotMethod)
	}
	if !strings.Contains(gotPath, "applications/app-1/commands") {
		t.Fatalf("expected bulk-overwrite path, got %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "occa_help") || !strings.Contains(string(gotBody), "occa_session") {
		t.Fatalf("expected both commands in request body, got %q", gotBody)
	}
	if !strings.Contains(string(gotBody), `"name":"args"`) {
		t.Fatalf("expected args option for occa_session, got %q", gotBody)
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

	a := &Adapter{session: s, menu: []channel.MenuCommand{{Alias: "occa_help", Description: "x"}}}
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

	a := &Adapter{session: s, menu: []channel.MenuCommand{{Alias: "occa_help", Description: "x"}}}
	a.registerCommands(&discordgo.Ready{Application: &discordgo.Application{ID: "app-1"}}) // must not panic
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
