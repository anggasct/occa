package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

type questionClient struct {
	mu       sync.Mutex
	answered []string
	rejected []string
	fail     error
}

func (c *questionClient) CreateSession(_ context.Context) (string, error) { return "ses-1", nil }
func (c *questionClient) SendMessage(_ context.Context, _ string, _ string, _ *relay.ModelRef, _ []relay.Attachment) error {
	return nil
}
func (c *questionClient) Providers(_ context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}
func (c *questionClient) RunCommand(_ context.Context, _ string, _ string) error { return nil }
func (c *questionClient) ListCommands(_ context.Context) ([]relay.CommandInfo, error) {
	return nil, nil
}
func (c *questionClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
	return nil, nil
}
func (c *questionClient) ReplyPermission(_ context.Context, _ string, _ relay.PermissionReply) error {
	return nil
}

func (c *questionClient) AnswerQuestion(_ context.Context, requestID string, answers [][]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.answered = append(c.answered, requestID+"|"+strings.Join(answers[0], ","))
	return nil
}

func (c *questionClient) RejectQuestion(_ context.Context, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.rejected = append(c.rejected, requestID)
	return nil
}

type questionReply struct {
	mu     sync.Mutex
	nextID int
	sends  []permissionView
	edits  []permissionView
}

func (r *questionReply) SendTyping() error { return nil }
func (r *questionReply) Send(text string) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	ref := permissionRef("q-msg-" + string(rune('0'+r.nextID)))
	r.sends = append(r.sends, permissionView{ref: ref, text: text})
	return ref, nil
}
func (r *questionReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	ref := permissionRef("q-msg-" + string(rune('0'+r.nextID)))
	r.sends = append(r.sends, permissionView{ref: ref, text: text, buttons: append([]channel.Button(nil), buttons...)})
	return ref, nil
}
func (r *questionReply) Edit(_ channel.MessageRef, text string) error {
	return r.EditWithButtons(nil, text, nil)
}
func (r *questionReply) EditWithButtons(_ channel.MessageRef, text string, buttons []channel.Button) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.edits = append(r.edits, permissionView{ref: nil, text: text, buttons: buttons})
	return nil
}

func newQuestionTestHandler(client relay.Client, reply *questionReply) *questionPromptHandler {
	return &questionPromptHandler{
		broker:    newQuestionBroker(),
		client:    client,
		platform:  "telegram",
		channelID: "chat-1",
		sessionID: "ses-1",
		reply:     reply,
	}
}

func questionRequest() relay.QuestionRequest {
	return relay.QuestionRequest{
		ID:        "que-1",
		SessionID: "ses-1",
		Questions: []relay.QuestionInfo{
			{
				Question: "Pilih bahasa?",
				Header:   "Bahasa",
				Options: []relay.QuestionOption{
					{Label: "A", Description: "Opsi A"},
					{Label: "B"},
				},
			},
		},
	}
}

func TestQuestionPromptSendsOptions(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("expected 1 prompt send, got %d", len(reply.sends))
	}
	sent := reply.sends[0]
	if !strings.Contains(sent.text, "Pilih bahasa?") {
		t.Fatalf("prompt text missing question: %q", sent.text)
	}
	if len(sent.buttons) != 3 {
		t.Fatalf("expected 2 option buttons + skip, got %d", len(sent.buttons))
	}
	if sent.buttons[0].Label != "A" || sent.buttons[1].Label != "B" {
		t.Fatalf("unexpected option labels: %+v", sent.buttons)
	}
}

func TestQuestionAnswerSubmitsAndCloses(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := strings.Split(reply.sends[0].buttons[0].Value, ":")[1]

	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:" + token + ":0:1",
		CallbackRef:  reply.sends[0].ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	client.mu.Lock()
	answered := append([]string(nil), client.answered...)
	client.mu.Unlock()
	if len(answered) != 1 || answered[0] != "que-1|B" {
		t.Fatalf("answers = %v, want que-1|B", answered)
	}
	last := reply.edits[len(reply.edits)-1]
	if last.text != "✅ Answered: B" {
		t.Fatalf("terminal view = %q, want answered B", last.text)
	}
	if last.buttons != nil {
		t.Fatalf("terminal view should clear buttons")
	}
}

func TestQuestionRejectSubmits(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := strings.Split(reply.sends[0].buttons[0].Value, ":")[1]
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:" + token + ":skip",
		CallbackRef:  reply.sends[0].ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	client.mu.Lock()
	rejected := append([]string(nil), client.rejected...)
	client.mu.Unlock()
	if len(rejected) != 1 || rejected[0] != "que-1" {
		t.Fatalf("rejected = %v, want que-1", rejected)
	}
	if last := reply.edits[len(reply.edits)-1]; last.text != "❌ Skipped" {
		t.Fatalf("terminal view = %q, want skipped", last.text)
	}
}

func TestQuestionAnswerFailureKeepsPromptOpen(t *testing.T) {
	client := &questionClient{fail: errors.New("agent down")}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := strings.Split(reply.sends[0].buttons[0].Value, ":")[1]
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:" + token + ":0:0",
		CallbackRef:  reply.sends[0].ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	last := reply.edits[len(reply.edits)-1]
	if !strings.Contains(last.text, "Try again") {
		t.Fatalf("expected retry view, got %q", last.text)
	}
	if len(last.buttons) != 3 {
		t.Fatalf("retry view must restore buttons")
	}
}

func TestQuestionCallbackFromWrongOriginExpires(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:bad-token:0:0",
		CallbackRef:  permissionRef("other"),
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if last := reply.edits[len(reply.edits)-1]; last.text != questionExpiredMessage {
		t.Fatalf("expected expired view, got %q", last.text)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.answered)+len(client.rejected) != 0 {
		t.Fatalf("no answer should be submitted for a bad token")
	}
}
