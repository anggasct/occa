package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

type questionClient struct {
	mu       sync.Mutex
	answered []string
	rejected []string
	fail     error
	// lastAnswers captures the full answers slice passed to AnswerQuestion
	// so tests can assert no nil/empty entry is ever sent.
	lastAnswers [][]string
}

func (c *questionClient) CreateSession(_ context.Context) (string, error) { return "ses-1", nil }
func (c *questionClient) GetSession(_ context.Context, _ string) (*relay.SessionInfo, error) {
	return &relay.SessionInfo{}, nil
}
func (c *questionClient) SessionExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
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
func (c *questionClient) AbortSession(_ context.Context, _ string) error           { return nil }
func (c *questionClient) SummarizeSession(_ context.Context, _, _, _ string) error { return nil }
func (c *questionClient) RevertMessage(_ context.Context, _, _ string) error       { return nil }
func (c *questionClient) UnrevertSession(_ context.Context, _ string) error        { return nil }
func (c *questionClient) ListMessages(_ context.Context, _ string) ([]relay.MessageInfo, error) {
	return nil, nil
}
func (c *questionClient) ListAgents(_ context.Context) ([]relay.AgentInfo, error) {
	return nil, nil
}
func (c *questionClient) SwitchAgent(_ context.Context, _, _ string) error {
	return nil
}

func (c *questionClient) AnswerQuestion(_ context.Context, requestID string, answers [][]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.answered = append(c.answered, requestID+"|"+strings.Join(answers[0], ","))
	c.lastAnswers = append([][]string(nil), answers...)
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
	mu                  sync.Mutex
	nextID              int
	sends               []permissionView
	edits               []permissionView
	editFail            error
	sendFail            error
	sendWithButtonsFail error
}

func (r *questionReply) SendTyping() error { return nil }
func (r *questionReply) Send(text string) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sendFail != nil {
		return nil, r.sendFail
	}
	r.nextID++
	ref := permissionRef("q-msg-" + string(rune('0'+r.nextID)))
	r.sends = append(r.sends, permissionView{ref: ref, text: text})
	return ref, nil
}
func (r *questionReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sendWithButtonsFail != nil {
		return nil, r.sendWithButtonsFail
	}
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
	if r.editFail != nil {
		return r.editFail
	}
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
	if sent.buttons[0].Label != "Q1 · 1" || sent.buttons[1].Label != "Q1 · 2" {
		t.Fatalf("unexpected option labels: %+v", sent.buttons)
	}
}

func TestQuestionPromptPreservesLongOptionDetails(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)
	longLabel := strings.Repeat("label ", 20)
	longDescription := strings.Repeat("description ", 20)
	req := questionRequest()
	req.Questions[0].Options = []relay.QuestionOption{{Label: longLabel, Description: longDescription}}

	if err := h.Prompt(context.Background(), req); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	sent := reply.sends[0]
	if !strings.Contains(sent.text, longLabel) || !strings.Contains(sent.text, longDescription) {
		t.Fatalf("full option details missing from prompt: %q", sent.text)
	}
	if sent.buttons[0].Label != "Q1 · 1" {
		t.Fatalf("button label = %q, want compact question index", sent.buttons[0].Label)
	}
	token := strings.Split(sent.buttons[0].Value, ":")[1]
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:" + token + ":0:0",
		CallbackRef:  sent.ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle long-label callback: %v", err)
	}
	client.mu.Lock()
	answered := append([]string(nil), client.answered...)
	client.mu.Unlock()
	if len(answered) != 1 || answered[0] != "que-1|"+longLabel {
		t.Fatalf("callback answer = %v, want original long label", answered)
	}
}

func TestQuestionButtonsUseUnambiguousIndexesAcrossQuestions(t *testing.T) {
	questions := questionRequest().Questions
	questions = append(questions, relay.QuestionInfo{
		Question: "Pilih database?",
		Options:  []relay.QuestionOption{{Label: "X"}},
	})
	buttons := questionButtons("token", questions)
	want := []string{"Q1 · 1", "Q1 · 2", "Q2 · 1", "❌ Skip"}
	if len(buttons) != len(want) {
		t.Fatalf("button count = %d, want %d", len(buttons), len(want))
	}
	for i, label := range want {
		if buttons[i].Label != label {
			t.Fatalf("button %d label = %q, want %q", i, buttons[i].Label, label)
		}
	}
}

func TestQuestionPromptSplitsDetailsBeforeActionMessage(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)
	h.split = func(text string) []string {
		if strings.Contains(text, "Agent has a question") {
			return []string{"detail part 1", "detail part 2"}
		}
		return []string{text}
	}

	if err := h.Prompt(context.Background(), questionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if len(reply.sends) != 3 {
		t.Fatalf("send count = %d, want 2 detail messages plus action message", len(reply.sends))
	}
	if reply.sends[0].buttons != nil || reply.sends[1].buttons != nil {
		t.Fatalf("detail messages must be text-only: %+v", reply.sends)
	}
	if reply.sends[2].text != "❓ Choose an option:" || len(reply.sends[2].buttons) != 3 {
		t.Fatalf("action message = %+v", reply.sends[2])
	}

	token := strings.Split(reply.sends[2].buttons[0].Value, ":")[1]
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: "question:" + token + ":0:1",
		CallbackRef:  reply.sends[2].ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle action callback: %v", err)
	}
	client.mu.Lock()
	answered := append([]string(nil), client.answered...)
	client.mu.Unlock()
	if len(answered) != 1 || answered[0] != "que-1|B" {
		t.Fatalf("callback mapping = %v, want original option label", answered)
	}
}

func TestQuestionPromptFailureRejectsPendingRequest(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{sendWithButtonsFail: errors.New("platform unavailable")}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), questionRequest()); err == nil {
		t.Fatal("Prompt should fail when action message cannot be sent")
	}
	if len(client.rejected) != 1 || client.rejected[0] != "que-1" {
		t.Fatalf("rejected requests = %v, want que-1", client.rejected)
	}
	if len(reply.sends) != 1 || reply.sends[0].text != "⚠️ Could not show the question. The request was stopped." {
		t.Fatalf("failure notice = %+v", reply.sends)
	}
	if len(h.broker.records) != 0 {
		t.Fatal("failed prompt must not remain pending")
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
	if !strings.Contains(last.text, "Could not submit your answer") {
		t.Fatalf("expected retry view, got %q", last.text)
	}
	if len(last.buttons) != 3 {
		t.Fatalf("retry view must restore buttons")
	}
}

func TestQuestionRetryGuardNoDuplicateEdits(t *testing.T) {
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

	for i := 0; i < 3; i++ {
		if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
			t.Fatalf("handle tap %d: %v", i, err)
		}
	}

	reply.mu.Lock()
	editCount := len(reply.edits)
	reply.mu.Unlock()

	if editCount != 1 {
		t.Fatalf("expected EditWithButtons called at most once per failure state, got %d edits", editCount)
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

func TestQuestionAnswerMultipleQuestionsNoNilEntries(t *testing.T) {
	// Regression: with more than one question, unanswered questions used to be
	// left as nil in the answers slice. nil marshals to JSON null and opencode
	// rejects it with HTTP 400 ("Expected QuestionAnswer, got null"), which
	// made option taps fail. Every entry must be a non-nil (possibly empty)
	// string slice.
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	req := questionRequest()
	req.Questions = append(req.Questions, relay.QuestionInfo{
		Question: "Pilih database?",
		Header:   "Database",
		Options: []relay.QuestionOption{
			{Label: "X"},
			{Label: "Y"},
		},
	})
	if err := h.Prompt(context.Background(), req); err != nil {
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
	answers := append([][]string(nil), client.lastAnswers...)
	client.mu.Unlock()
	if len(answers) != 2 {
		t.Fatalf("answers len = %d, want 2 (one per question)", len(answers))
	}
	for i, a := range answers {
		if a == nil {
			t.Fatalf("answers[%d] is nil — would marshal to JSON null and cause HTTP 400", i)
		}
	}
	if len(answers[0]) != 1 || answers[0][0] != "B" {
		t.Fatalf("answers[0] = %v, want [B]", answers[0])
	}
	if len(answers[1]) != 0 {
		t.Fatalf("answers[1] = %v, want empty (unanswered)", answers[1])
	}
}

func TestQuestionMultiQuestionTerminalText(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	req := questionRequest()
	req.Questions = append(req.Questions, relay.QuestionInfo{
		Question: "Pilih database?",
		Header:   "Database",
		Options: []relay.QuestionOption{
			{Label: "X"},
			{Label: "Y"},
		},
	})
	if err := h.Prompt(context.Background(), req); err != nil {
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

	last := reply.edits[len(reply.edits)-1]
	want := "✅ Question 1: B — question 2 skipped (tap an option to answer, or Skip)."
	if last.text != want {
		t.Fatalf("multi-question terminal text = %q, want %q", last.text, want)
	}
}

func TestQuestionSingleQuestionTerminalTextUnchanged(t *testing.T) {
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

	last := reply.edits[len(reply.edits)-1]
	want := "✅ Answered: B"
	if last.text != want {
		t.Fatalf("single-question terminal text = %q, want %q", last.text, want)
	}
}

func TestFormatQuestionRetryMessageJSONParsing(t *testing.T) {
	err := errors.New(`relay: answer question: unexpected status 400: {"message":"Expected a string starting with que, got x"}`)
	got := formatQuestionRetryMessage(err)
	want := "⚠️ Could not submit your answer — Expected a string starting with que, got x. Try again or tap Skip."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTruncateRunes(t *testing.T) {
	longString := strings.Repeat("🚀", 120)
	result := truncateRunes(longString, 100)
	if !utf8.ValidString(result) {
		t.Fatalf("result is not valid UTF-8: %q", result)
	}
	if !strings.HasSuffix(result, "…") {
		t.Fatalf("expected truncated string to end with '…', got %q", result)
	}
	if runeCount := utf8.RuneCountInString(result); runeCount != 101 {
		t.Fatalf("rune count = %d, want 101", runeCount)
	}
}

func TestFormatQuestionRetryMessageTruncatesRunes(t *testing.T) {
	longErrMsg := errors.New("relay: answer question: unexpected status 400: " + strings.Repeat("🎉", 120))
	got := formatQuestionRetryMessage(longErrMsg)
	if !utf8.ValidString(got) {
		t.Fatalf("formatted retry message is not valid UTF-8: %q", got)
	}
}

func TestQuestionRetryGuardEditFailureRetries(t *testing.T) {
	client := &questionClient{fail: errors.New("agent down")}
	reply := &questionReply{editFail: errors.New("edit failed")}
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
		t.Fatalf("handle tap 1: %v", err)
	}
	reply.mu.Lock()
	if len(reply.edits) != 0 {
		t.Fatalf("expected 0 successful edits, got %d", len(reply.edits))
	}
	reply.mu.Unlock()

	reply.mu.Lock()
	reply.editFail = nil
	reply.mu.Unlock()

	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle tap 2: %v", err)
	}
	reply.mu.Lock()
	if len(reply.edits) != 1 {
		t.Fatalf("expected 1 successful edit after tap 2, got %d", len(reply.edits))
	}
	reply.mu.Unlock()

	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle tap 3: %v", err)
	}
	reply.mu.Lock()
	if len(reply.edits) != 1 {
		t.Fatalf("expected guard to prevent tap 3 edit, got %d edits", len(reply.edits))
	}
	reply.mu.Unlock()
}
