package telegram

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/anggasct/occa/internal/render"

	"github.com/anggasct/occa/internal/channel"
)

func fakeTelegramServer(t *testing.T, handleNonGetMe http.HandlerFunc) *tgbotapi.BotAPI {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "getMe") {
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"test","username":"testbot"}}`))
			return
		}
		handleNonGetMe(w, r)
	}))
	t.Cleanup(ts.Close)

	bot, err := tgbotapi.NewBotAPIWithAPIEndpoint("faketoken", ts.URL+"/bot%s/%s")
	if err != nil {
		t.Fatalf("NewBotAPIWithAPIEndpoint: %v", err)
	}
	return bot
}

func TestRegisterCommandsSendsSetMyCommands(t *testing.T) {
	var gotPath string
	var gotBody []byte
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	})

	a := &Adapter{bot: bot, menu: []channel.MenuCommand{
		{Alias: "occa_help", Description: "Show available commands"},
		{Alias: "occa_session", Description: "Manage sessions", HasArgs: true},
	}}
	a.registerCommands()

	if !strings.Contains(gotPath, "setMyCommands") {
		t.Fatalf("expected setMyCommands call, got path %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "occa_help") || !strings.Contains(string(gotBody), "occa_session") {
		t.Fatalf("expected both commands in request body, got %q", gotBody)
	}
}

func TestRegisterCommandsSkipsWhenMenuEmpty(t *testing.T) {
	called := false
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	})

	a := &Adapter{bot: bot}
	a.registerCommands()

	if called {
		t.Fatal("expected no HTTP call when menu is empty")
	}
}

func TestRegisterCommandsFailureDoesNotPanic(t *testing.T) {
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"ok":false,"description":"boom"}`))
	})

	a := &Adapter{bot: bot, menu: []channel.MenuCommand{{Alias: "occa_help", Description: "x"}}}
	a.registerCommands() // must not panic despite the failed request
}

func TestSplitMessageShort(t *testing.T) {
	chunks := render.Split("hello world", 4096)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0] != "hello world" {
		t.Fatalf("unexpected chunk: %q", chunks[0])
	}
}

func TestInlineKeyboardEmptyRemovesButtons(t *testing.T) {
	if markup := inlineKeyboard(nil); markup.InlineKeyboard == nil || len(markup.InlineKeyboard) != 0 {
		t.Fatalf("empty markup = %+v", markup)
	}
	markup := inlineKeyboard([]channel.Button{{Label: "Allow", Value: "allow"}})
	if len(markup.InlineKeyboard) != 1 || len(markup.InlineKeyboard[0]) != 1 {
		t.Fatalf("button markup = %+v", markup)
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 3000)
	para2 := strings.Repeat("b", 3000)
	text := para1 + "\n\n" + para2

	chunks := render.Split(text, 4096)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds max: %d", i, len(chunk))
		}
	}
}

func TestSplitMessageCodeBlock(t *testing.T) {
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = strings.Repeat("x", 80)
	}
	text := strings.Join(lines, "\n")

	chunks := render.Split(text, 4096)
	for i, chunk := range chunks {
		if len(chunk) > 4096 {
			t.Fatalf("chunk %d exceeds max: %d", i, len(chunk))
		}
	}
	rejoined := strings.Join(chunks, "\n")
	if len(rejoined) < len(text)-100 {
		t.Fatal("lost significant content during split")
	}
}

func TestSplitMessageNeverCutsRune(t *testing.T) {
	for _, chunk := range render.Split(strings.Repeat("日", 3000), 4096) {
		if !utf8.ValidString(chunk) {
			t.Fatal("chunk is not valid UTF-8")
		}
	}
}

func TestFetchFileTimeout(t *testing.T) {
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer blocked.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 200 * time.Millisecond}}
	start := time.Now()
	data, err := a.fetchFile(blocked.URL)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("download blocked for %v", elapsed)
	}
	if err == nil {
		t.Fatalf("expected timeout error, got data %q", data)
	}
}

func TestFetchFileSucceeds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("voice-data"))
	}))
	defer ts.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 5 * time.Second}}
	data, err := a.fetchFile(ts.URL)
	if err != nil {
		t.Fatalf("fetchFile: %v", err)
	}
	if string(data) != "voice-data" {
		t.Fatalf("unexpected data: %q", data)
	}
}
