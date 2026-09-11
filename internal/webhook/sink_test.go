package webhook

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type fakeMessageRef struct {
	id string
}

func (r fakeMessageRef) ID() string {
	return r.id
}

func TestWebhookSinkClampingDiscord(t *testing.T) {
	var sentText, editedText string
	sendFn := func(ctx context.Context, channelID, text string) (string, error) {
		sentText = text
		return "msg-discord-1", nil
	}
	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		editedText = text
		return nil
	}

	sink := NewWebhookSink("chan-1", "discord", sendFn, editFn)

	oversized := strings.Repeat("A", 3000)
	handle, err := sink.Send(context.Background(), oversized)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if utf8.RuneCountInString(sentText) > render.DiscordLimit {
		t.Fatalf("sentText runes = %d, want <= %d", utf8.RuneCountInString(sentText), render.DiscordLimit)
	}
	if !strings.HasSuffix(sentText, "…") {
		t.Errorf("sentText should be clamped with ellipsis")
	}

	oversizedEdit := strings.Repeat("B", 3500)
	if err := handle.Edit(context.Background(), oversizedEdit); err != nil {
		t.Fatalf("Edit failed: %v", err)
	}
	if utf8.RuneCountInString(editedText) > render.DiscordLimit {
		t.Fatalf("editedText runes = %d, want <= %d", utf8.RuneCountInString(editedText), render.DiscordLimit)
	}
	if !strings.HasSuffix(editedText, "…") {
		t.Errorf("editedText should be clamped with ellipsis")
	}
}

func TestWebhookSinkClampingTelegram(t *testing.T) {
	var sentText, editedText string
	sendFn := func(ctx context.Context, channelID, text string) (string, error) {
		sentText = text
		return "msg-tg-1", nil
	}
	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		editedText = text
		return nil
	}

	sink := NewWebhookSink("-100123:456", "telegram", sendFn, editFn)

	oversized := strings.Repeat("X", 5000)
	handle, err := sink.Send(context.Background(), oversized)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if utf8.RuneCountInString(sentText) > render.TelegramLimit {
		t.Fatalf("sentText runes = %d, want <= %d", utf8.RuneCountInString(sentText), render.TelegramLimit)
	}
	if !strings.HasSuffix(sentText, "…") {
		t.Errorf("sentText should be clamped with ellipsis")
	}

	oversizedEdit := strings.Repeat("Y", 6000)
	if err := handle.Edit(context.Background(), oversizedEdit); err != nil {
		t.Fatalf("Edit failed: %v", err)
	}
	if utf8.RuneCountInString(editedText) > render.TelegramLimit {
		t.Fatalf("editedText runes = %d, want <= %d", utf8.RuneCountInString(editedText), render.TelegramLimit)
	}
	if !strings.HasSuffix(editedText, "…") {
		t.Errorf("editedText should be clamped with ellipsis")
	}
}

func TestWebhookSinkOptionsAndHandles(t *testing.T) {
	var typedChannel string
	sendFn := func(ctx context.Context, channelID, text string) (string, error) {
		return "m-1", nil
	}
	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		return nil
	}
	typingFn := func(ctx context.Context, channelID string) error {
		typedChannel = channelID
		return nil
	}

	sink := NewWebhookSink("custom-chan", "custom", sendFn, editFn,
		WithSinkLimit(500),
		WithSinkTyping(typingFn),
	)

	if err := sink.SendTyping(context.Background()); err != nil {
		t.Fatalf("SendTyping: %v", err)
	}
	if typedChannel != "custom-chan" {
		t.Errorf("typedChannel = %q, want custom-chan", typedChannel)
	}

	handle, err := sink.SendWithButtons(context.Background(), "btn text", []channel.Button{{Label: "test"}})
	if err != nil {
		t.Fatalf("SendWithButtons: %v", err)
	}
	if handle.Ref().ID() != "m-1" {
		t.Errorf("handle.Ref().ID() = %q, want m-1", handle.Ref().ID())
	}

	if err := handle.EditWithButtons(context.Background(), "edit btn", []channel.Button{{Label: "test"}}); err != nil {
		t.Fatalf("EditWithButtons: %v", err)
	}

	refHandle := sink.HandleFromRef(fakeMessageRef{id: "m-ref"})
	if refHandle == nil || refHandle.Ref().ID() != "m-ref" {
		t.Fatalf("HandleFromRef returned unexpected handle: %+v", refHandle)
	}
}
