package relay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSendMessageWithImageAttachment(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	att := []Attachment{{Filename: "screenshot.png", MimeType: "image/png", Data: []byte{0x89, 0x50, 0x4E, 0x47}}}
	err := c.SendMessage(context.Background(), "s1", "look at this", nil, att)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	parts, ok := gotBody["parts"].([]any)
	if !ok {
		t.Fatalf("expected parts array, got: %v", gotBody)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts (text + file), got %d", len(parts))
	}

	filePart := parts[1].(map[string]any)
	if filePart["type"] != "file" {
		t.Fatalf("expected file part, got type: %v", filePart["type"])
	}
	data := filePart["data"].(string)
	if !strings.HasPrefix(data, "data:image/png;base64,") {
		t.Fatalf("expected base64 data URL, got: %s", data[:40])
	}
}

func TestSendMessageWithTextAttachmentInlined(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	att := []Attachment{{Filename: "app.log", MimeType: "text/plain", Data: []byte("line1\nline2\nline3")}}
	err := c.SendMessage(context.Background(), "s1", "check this log", nil, att)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	parts := gotBody["parts"].([]any)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	textPart := parts[1].(map[string]any)
	if textPart["type"] != "text" {
		t.Fatalf("expected text part for text/* mime, got: %v", textPart["type"])
	}
	content := textPart["text"].(string)
	if !strings.Contains(content, "<attached_file name=\"app.log\">") {
		t.Fatalf("expected attached_file wrapper, got: %s", content)
	}
	if !strings.Contains(content, "line1\nline2\nline3") {
		t.Fatalf("expected file content inlined, got: %s", content)
	}
}

func TestSendMessageInvalidUTF8FallsBackToFilePart(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	invalidUTF8 := []byte{0xFF, 0xFE, 0x00, 0x01}
	att := []Attachment{{Filename: "data.txt", MimeType: "text/plain", Data: invalidUTF8}}
	err := c.SendMessage(context.Background(), "s1", "", nil, att)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	parts := gotBody["parts"].([]any)
	filePart := parts[0].(map[string]any)
	if filePart["type"] != "file" {
		t.Fatalf("expected file part for invalid UTF-8, got: %v", filePart["type"])
	}
}

func TestSendMessageAttachmentTooLarge(t *testing.T) {
	c := NewHTTPClient("http://127.0.0.1:1")
	bigData := make([]byte, maxAttachmentSize+1)
	att := []Attachment{{Filename: "huge.bin", MimeType: "application/octet-stream", Data: bigData}}
	err := c.SendMessage(context.Background(), "s1", "hello", nil, att)
	if !errors.Is(err, ErrAttachmentTooLarge) {
		t.Fatalf("expected ErrAttachmentTooLarge, got: %v", err)
	}
}

func TestSendMessageNoAttachmentsUsesContentField(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	err := c.SendMessage(context.Background(), "s1", "plain message", nil, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	parts, ok := gotBody["parts"].([]any)
	if !ok || len(parts) != 1 {
		t.Fatalf("expected one text part for no-attachment message, got: %v", gotBody)
	}
	textPart, ok := parts[0].(map[string]any)
	if !ok || textPart["type"] != "text" || textPart["text"] != "plain message" {
		t.Fatalf("unexpected text part: %v", parts[0])
	}
	if _, hasContent := gotBody["content"]; hasContent {
		t.Fatal("should not have content field")
	}
}

func TestSendMessageMultipleAttachments(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	atts := []Attachment{
		{Filename: "notes.txt", MimeType: "text/plain", Data: []byte("hello")},
		{Filename: "img.png", MimeType: "image/png", Data: []byte{0x89}},
		{Filename: "config.json", MimeType: "application/json", Data: []byte(`{"key":"val"}`)},
	}
	err := c.SendMessage(context.Background(), "s1", "multi", nil, atts)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	parts := gotBody["parts"].([]any)
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts (1 text + 3 attachments), got %d", len(parts))
	}
}

func TestIsTextLike(t *testing.T) {
	tests := []struct {
		mime string
		data []byte
		want bool
	}{
		{"text/plain", []byte("hello"), true},
		{"text/html", []byte("<b>hi</b>"), true},
		{"application/json", []byte(`{}`), true},
		{"application/xml", []byte("<x/>"), true},
		{"application/x-yaml", []byte("key: val"), true},
		{"image/png", []byte{0x89}, false},
		{"application/octet-stream", []byte("hello"), false},
		{"text/plain", []byte{0xFF, 0xFE}, false},
	}
	for _, tt := range tests {
		got := isTextLike(tt.mime, tt.data)
		if got != tt.want {
			t.Errorf("isTextLike(%q, %v) = %v, want %v", tt.mime, tt.data, got, tt.want)
		}
	}
}
