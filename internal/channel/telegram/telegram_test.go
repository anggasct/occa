package telegram

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/render"

	"github.com/anggasct/occa/internal/channel"
)

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
		w.Write([]byte("voice-data"))
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
