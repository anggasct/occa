package webhook

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
)

type recordReplyContext struct {
	mu     sync.Mutex
	sends  []string
	edits  map[string][]string
	nextID int
}

func newRecordReplyContext() *recordReplyContext {
	return &recordReplyContext{
		edits: make(map[string][]string),
	}
}

func (r *recordReplyContext) SendTyping() error {
	return nil
}

func (r *recordReplyContext) Send(text string) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	id := fmt.Sprintf("msg-%d", r.nextID)
	r.sends = append(r.sends, text)
	return fakeMessageRef{id: id}, nil
}

func (r *recordReplyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	return r.Send(text)
}

func (r *recordReplyContext) Edit(ref channel.MessageRef, text string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := ref.ID()
	r.edits[id] = append(r.edits[id], text)
	return nil
}

func (r *recordReplyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	return r.Edit(ref, text)
}

type recordWebhookBackend struct {
	mu     sync.Mutex
	sends  []string
	edits  map[string][]string
	nextID int
}

func newRecordWebhookBackend() *recordWebhookBackend {
	return &recordWebhookBackend{
		edits: make(map[string][]string),
	}
}

func (b *recordWebhookBackend) Send(ctx context.Context, channelID, text string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := fmt.Sprintf("msg-%d", b.nextID)
	b.sends = append(b.sends, text)
	return id, nil
}

func (b *recordWebhookBackend) Edit(ctx context.Context, channelID, messageID, text string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.edits[messageID] = append(b.edits[messageID], text)
	return nil
}

func runStreamerScript(t *testing.T, s *relay.Streamer, events []relay.Event) {
	t.Helper()
	s.SetWorkingEditInterval(-1)
	now := time.Unix(1000, 0)
	s.SetNowFunc(func() time.Time { return now })

	ch := make(chan relay.Event, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)

	if err := s.Run(context.Background(), ch); err != nil {
		t.Fatalf("Streamer.Run failed: %v", err)
	}
}

func TestStreamerUnificationInteractiveVsWebhookDiscord(t *testing.T) {
	events := []relay.Event{
		{Type: relay.EventReasoning},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "go test ./..."},
		{Type: relay.EventDelta, Delta: "First part of the answer."},
		{Type: relay.EventDelta, Delta: " Second part of the answer."},
		{Type: relay.EventDone},
	}

	reply := newRecordReplyContext()
	interactiveStreamer := relay.NewStreamer(reply, render.New(), render.Discord)
	runStreamerScript(t, interactiveStreamer, events)

	backend := newRecordWebhookBackend()
	webhookSink := NewWebhookSink("thread-1", "discord", backend.Send, backend.Edit)
	webhookStreamer := relay.NewStreamerWithSink(webhookSink, render.New(), render.Discord)
	runStreamerScript(t, webhookStreamer, events)

	if !reflect.DeepEqual(reply.sends, backend.sends) {
		t.Fatalf("sends mismatch:\ninteractive: %+v\nwebhook:     %+v", reply.sends, backend.sends)
	}

	for id, interactiveEdits := range reply.edits {
		webhookEdits, ok := backend.edits[id]
		if !ok {
			t.Fatalf("missing edits for %s in webhook path", id)
		}
		if len(interactiveEdits) > 0 && len(webhookEdits) > 0 {
			lastInteractive := interactiveEdits[len(interactiveEdits)-1]
			lastWebhook := webhookEdits[len(webhookEdits)-1]
			if lastInteractive != lastWebhook {
				t.Errorf("final edit mismatch on %s:\ninteractive: %q\nwebhook:     %q", id, lastInteractive, lastWebhook)
			}
		}
	}
}

func TestStreamerUnificationInteractiveVsWebhookTelegram(t *testing.T) {
	events := []relay.Event{
		{Type: relay.EventReasoning},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "git status"},
		{Type: relay.EventDelta, Delta: "Telegram streamed result."},
		{Type: relay.EventDone},
	}

	reply := newRecordReplyContext()
	interactiveStreamer := relay.NewStreamer(reply, render.New(), render.Telegram)
	runStreamerScript(t, interactiveStreamer, events)

	backend := newRecordWebhookBackend()
	webhookSink := NewWebhookSink("-10012345:777", "telegram", backend.Send, backend.Edit)
	webhookStreamer := relay.NewStreamerWithSink(webhookSink, render.New(), render.Telegram)
	runStreamerScript(t, webhookStreamer, events)

	if !reflect.DeepEqual(reply.sends, backend.sends) {
		t.Fatalf("Telegram sends mismatch:\ninteractive: %+v\nwebhook:     %+v", reply.sends, backend.sends)
	}

	for id, interactiveEdits := range reply.edits {
		webhookEdits, ok := backend.edits[id]
		if !ok {
			t.Fatalf("missing edits for %s in Telegram webhook path", id)
		}
		if len(interactiveEdits) > 0 && len(webhookEdits) > 0 {
			lastInteractive := interactiveEdits[len(interactiveEdits)-1]
			lastWebhook := webhookEdits[len(webhookEdits)-1]
			if lastInteractive != lastWebhook {
				t.Errorf("final edit mismatch on %s:\ninteractive: %q\nwebhook:     %q", id, lastInteractive, lastWebhook)
			}
		}
	}
}

func TestWebhookTurnRootCardFirstTerminalCardLast(t *testing.T) {
	backend := newRecordWebhookBackend()
	channelID := "chan-123"

	envelope := WebhookEnvelope{
		"repository":  "org/repo",
		"pr_number":   "42",
		"head_branch": "feat/turn",
		"base_branch": "main",
		"delivery_id": "del-golden-1",
	}

	rootCard := FormatRootCard(envelope, "github_reviewer", "RUNNING", "", "", "discord")
	_, err := backend.Send(context.Background(), channelID, rootCard)
	if err != nil {
		t.Fatalf("root card send failed: %v", err)
	}

	sink := NewWebhookSink(channelID, "discord", backend.Send, backend.Edit)
	streamer := relay.NewStreamerWithSink(sink, render.New(), render.Discord)

	events := []relay.Event{
		{Type: relay.EventReasoning},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "go test ./..."},
		{Type: relay.EventDelta, Delta: "Fix completed successfully."},
		{Type: relay.EventDone},
	}
	runStreamerScript(t, streamer, events)

	terminalCard := FormatTerminalCard(envelope, "github_reviewer", "COMPLETED", "", "", "discord", 125*time.Second)
	_, err = backend.Send(context.Background(), channelID, terminalCard)
	if err != nil {
		t.Fatalf("terminal card send failed: %v", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.sends) < 3 {
		t.Fatalf("expected at least 3 messages (root card, streamer bubbles, terminal card), got %d: %+v", len(backend.sends), backend.sends)
	}

	firstMsg := backend.sends[0]
	if !strings.Contains(firstMsg, "Status: RUNNING") || !strings.Contains(firstMsg, "github_reviewer") {
		t.Errorf("first message must be root card with RUNNING status: %s", firstMsg)
	}

	lastMsg := backend.sends[len(backend.sends)-1]
	if !strings.Contains(lastMsg, "Status: ✅ COMPLETED (2m 5s)") || !strings.Contains(lastMsg, "github_reviewer") {
		t.Errorf("last message must be terminal card with COMPLETED status: %s", lastMsg)
	}

	var foundStreamedText bool
	for _, msg := range backend.sends[1 : len(backend.sends)-1] {
		if strings.Contains(msg, "Thinking") || strings.Contains(msg, "bash") || strings.Contains(msg, "Fix completed") {
			foundStreamedText = true
		}
	}
	if !foundStreamedText {
		t.Errorf("expected streamer content between root card and terminal card, got: %+v", backend.sends)
	}
}

func TestWebhookStreamerClampOnOversizedOutput(t *testing.T) {
	backend := newRecordWebhookBackend()
	sink := NewWebhookSink("thread-clamp", "discord", backend.Send, backend.Edit)
	streamer := relay.NewStreamerWithSink(sink, render.New(), render.Discord)

	oversized := strings.Repeat("Huge output line. ", 200)
	events := []relay.Event{
		{Type: relay.EventDelta, Delta: oversized},
		{Type: relay.EventDone},
	}
	runStreamerScript(t, streamer, events)

	backend.mu.Lock()
	defer backend.mu.Unlock()

	for _, text := range backend.sends {
		if utf8.RuneCountInString(text) > render.DiscordLimit {
			t.Errorf("sent message exceeded Discord limit (%d): runes=%d", render.DiscordLimit, utf8.RuneCountInString(text))
		}
	}
	for id, editList := range backend.edits {
		for _, text := range editList {
			if utf8.RuneCountInString(text) > render.DiscordLimit {
				t.Errorf("edited message %s exceeded Discord limit (%d): runes=%d", id, render.DiscordLimit, utf8.RuneCountInString(text))
			}
		}
	}
}

type fakeHeadlessClient struct {
	relay.Client
	events []relay.Event
}

func (c *fakeHeadlessClient) CreateSession(ctx context.Context) (string, error) {
	return "sess-headless-1", nil
}

func (c *fakeHeadlessClient) Events(ctx context.Context, sessionID string) (<-chan relay.Event, error) {
	ch := make(chan relay.Event, len(c.events))
	for _, ev := range c.events {
		ch <- ev
	}
	close(ch)
	return ch, nil
}

func (c *fakeHeadlessClient) SendMessage(ctx context.Context, sessionID, prompt string, model *relay.ModelRef, attachments []relay.Attachment) error {
	return nil
}

func (c *fakeHeadlessClient) ListMessages(ctx context.Context, sessionID string) ([]relay.MessageInfo, error) {
	return []relay.MessageInfo{
		{Role: "assistant", Completed: 1},
	}, nil
}

func (c *fakeHeadlessClient) AbortSession(ctx context.Context, sessionID string) error {
	return nil
}

func TestWebhookTurnHeadlessPolicyCompletesWithoutButtons(t *testing.T) {
	backend := newRecordWebhookBackend()
	sink := NewWebhookSink("thread-headless", "discord", backend.Send, backend.Edit)
	streamer := relay.NewStreamerWithSink(sink, render.New(), render.Discord)
	streamer.SetWorkingEditInterval(-1)

	client := &fakeHeadlessClient{
		events: []relay.Event{
			{
				Type: "permission_asked",
				Permission: &relay.PermissionRequest{
					ID:         "perm-1",
					Permission: "bash",
					Tool:       "bash",
				},
			},
			{
				Type: "question_asked",
				Question: &relay.QuestionRequest{
					ID: "q-1",
					Questions: []relay.QuestionInfo{
						{Question: "Should I proceed?"},
					},
				},
			},
			{Type: relay.EventDelta, Delta: "Headless completion output."},
			{Type: relay.EventDone},
		},
	}

	turn := relay.WebhookTurn{
		Client:       client,
		Prompt:       "Run task",
		Platform:     "discord",
		ChannelID:    "thread-headless",
		DeliveryID:   "del-headless-1",
		ExecutionKey: "key-1",
		Attempt:      1,
		Streamer:     streamer,
	}

	res, err := turn.Run(context.Background())
	if err != nil {
		t.Fatalf("turn.Run failed: %v", err)
	}
	if res.Output != "Headless completion output." {
		t.Errorf("output = %q, want 'Headless completion output.'", res.Output)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	for _, text := range backend.sends {
		if strings.Contains(text, "Button") || strings.Contains(text, "Should I proceed?") {
			t.Errorf("unexpected interactive button or question in sent text: %s", text)
		}
	}
}
