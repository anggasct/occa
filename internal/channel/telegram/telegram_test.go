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
		{Alias: "help", Description: "Show available commands"},
		{Alias: "session", Description: "Manage sessions", HasArgs: true},
	}}
	a.registerCommands()

	if !strings.Contains(gotPath, "setMyCommands") {
		t.Fatalf("expected setMyCommands call, got path %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "help") || !strings.Contains(string(gotBody), "session") {
		t.Fatalf("expected both commands in request body, got %q", gotBody)
	}
}

func TestSanitizeCommandName(t *testing.T) {
	cases := map[string]string{
		"customize-opencode":    "customize_opencode",
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

func TestSetChatCommandsUsesChatScope(t *testing.T) {
	var gotPath string
	var gotBody []byte
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	})

	rc := &replyContext{bot: bot, chatID: 12345}
	err := rc.SetChatCommands([]channel.MenuCommand{
		{Alias: "help", Description: "Show available commands"},
		{Alias: "customize-opencode", Description: "Configure opencode"},
	})
	if err != nil {
		t.Fatalf("SetChatCommands: %v", err)
	}

	if !strings.Contains(gotPath, "setMyCommands") {
		t.Fatalf("expected setMyCommands call, got path %q", gotPath)
	}
	if !strings.Contains(string(gotBody), "help") {
		t.Fatalf("expected help in request body, got %q", gotBody)
	}
	if !strings.Contains(string(gotBody), "customize_opencode") {
		t.Fatalf("expected sanitized agent command name, got %q", gotBody)
	}
	if strings.Contains(string(gotBody), "customize-opencode") {
		t.Fatalf("expected hyphenated name to be sanitized away, got %q", gotBody)
	}
}

func TestSanitizeDescription(t *testing.T) {
	short := "Show available commands"
	if got := sanitizeDescription(short); got != short {
		t.Fatalf("sanitizeDescription(%q) = %q, want unchanged", short, got)
	}

	long := strings.Repeat("a", 300)
	got := sanitizeDescription(long)
	if utf8.RuneCountInString(got) != 256 {
		t.Fatalf("sanitizeDescription truncated to %d runes, want 256", utf8.RuneCountInString(got))
	}
}

func TestSetChatCommandsTruncatesLongDescription(t *testing.T) {
	var gotBody []byte
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	})

	rc := &replyContext{bot: bot, chatID: 12345}
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

	a := &Adapter{bot: bot, menu: []channel.MenuCommand{{Alias: "help", Description: "x"}}}
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

func TestNormalizeCarriesTopicThreadID(t *testing.T) {
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{}}`))
	})
	a := &Adapter{bot: bot}

	update := tgbotapi.Update{
		Message: &tgbotapi.Message{
			Chat: &tgbotapi.Chat{ID: 1001},
			From: &tgbotapi.User{ID: 42},
			Text: "hello",
			Entities: []tgbotapi.MessageEntity{
				{Type: "mention", Offset: 0, Length: 6},
			},
		},
	}
	got := a.normalize(update, 555)
	if got.ThreadID != "555" || !got.IsThread {
		t.Fatalf("expected topic ThreadID 555, got %+v", got)
	}
	rc, ok := got.ReplyCtx.(*replyContext)
	if !ok || rc.threadID != 555 {
		t.Fatalf("reply context must target the topic, got %+v", got.ReplyCtx)
	}

	plain := a.normalize(update, 0)
	if plain.ThreadID != "" || plain.IsThread {
		t.Fatalf("expected empty ThreadID outside a topic, got %+v", plain)
	}
}

func TestReplyContextSendsIntoTopic(t *testing.T) {
	var sentBodies []string
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sentBodies = append(sentBodies, string(body))
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	})

	rc := &replyContext{bot: bot, chatID: 1001, threadID: 555}
	if _, err := rc.Send("reply text"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := rc.SendTyping(); err != nil {
		t.Fatalf("SendTyping: %v", err)
	}
	if err := rc.Edit(messageRef{id: "7"}, "edited"); err != nil {
		t.Fatalf("Edit: %v", err)
	}

	foundSend, foundTyping := false, false
	if len(sentBodies) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(sentBodies))
	}
	for _, body := range sentBodies {
		if strings.Contains(body, "message_thread_id=555") {
			foundSend = true
		}
		if strings.Contains(body, "action=typing") {
			foundTyping = true
		}
	}
	if !foundSend || !foundTyping {
		t.Fatalf("expected message_thread_id on send and typing, bodies: %v", sentBodies)
	}
}

func TestReplyContextOutsideTopicOmitsThreadID(t *testing.T) {
	var sentBodies []string
	bot := fakeTelegramServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sentBodies = append(sentBodies, string(body))
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	})

	rc := &replyContext{bot: bot, chatID: 1001}
	if _, err := rc.Send("reply text"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	for _, body := range sentBodies {
		if strings.Contains(body, "message_thread_id") {
			t.Fatalf("unexpected message_thread_id outside topic: %q", body)
		}
	}
}

func TestInlineKeyboardGroupsByRow(t *testing.T) {
	markup := inlineKeyboard([]channel.Button{
		{Label: "a", Value: "1", Row: 1},
		{Label: "b", Value: "2", Row: 1},
		{Label: "c", Value: "3", Row: 2},
		{Label: "d", Value: "4"},
		{Label: "e", Value: "5"},
	})
	rows := markup.InlineKeyboard
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4 (%+v)", len(rows), rows)
	}
	if len(rows[0]) != 2 || len(rows[1]) != 1 || len(rows[2]) != 1 || len(rows[3]) != 1 {
		t.Fatalf("row sizes = %d/%d/%d/%d, want 2/1/1/1", len(rows[0]), len(rows[1]), len(rows[2]), len(rows[3]))
	}
	if rows[0][0].Text != "a" || rows[0][1].Text != "b" {
		t.Fatalf("grouped row = %+v", rows[0])
	}
}

func TestInlineKeyboardChunksOversizedRows(t *testing.T) {
	buttons := make([]channel.Button, 10)
	for i := range buttons {
		buttons[i] = channel.Button{Label: string(rune('a' + i)), Value: "v", Row: 1}
	}
	markup := inlineKeyboard(buttons)
	rows := markup.InlineKeyboard
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if len(rows[0]) != 8 || len(rows[1]) != 2 {
		t.Fatalf("row sizes = %d/%d, want 8/2", len(rows[0]), len(rows[1]))
	}
}
