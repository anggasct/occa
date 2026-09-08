package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

func threeQuestionRequest() relay.QuestionRequest {
	return relay.QuestionRequest{
		ID:        "que-3",
		SessionID: "ses-1",
		Questions: []relay.QuestionInfo{
			{
				Question: "Pick language?",
				Header:   "Language",
				Options: []relay.QuestionOption{
					{Label: "Go", Description: "Fast and simple"},
					{Label: "Python"},
				},
			},
			{
				Question: "Pick database?",
				Header:   "Database",
				Options: []relay.QuestionOption{
					{Label: "SQLite"},
					{Label: "Postgres"},
				},
			},
			{
				Question: "Pick region?",
				Header:   "Region",
				Options: []relay.QuestionOption{
					{Label: "Jakarta"},
					{Label: "Singapore"},
				},
			},
		},
	}
}

func questionToken(t *testing.T, value string) string {
	t.Helper()
	parts := strings.Split(value, ":")
	if len(parts) < 2 || parts[1] == "" {
		t.Fatalf("button value carries no token: %q", value)
	}
	return parts[1]
}

func tapQuestion(t *testing.T, h *questionPromptHandler, reply *questionReply, ref channel.MessageRef, data string) {
	t.Helper()
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat-1",
		IsCallback:   true,
		CallbackData: data,
		CallbackRef:  ref,
		ReplyCtx:     reply,
	}
	if err := h.broker.HandleQuestionCallback(context.Background(), msg); err != nil {
		t.Fatalf("handle %q: %v", data, err)
	}
}

func wizardRecord(t *testing.T, h *questionPromptHandler, token string) *questionRecord {
	t.Helper()
	h.broker.mu.Lock()
	defer h.broker.mu.Unlock()
	record, ok := h.broker.records[token]
	if !ok {
		t.Fatalf("no broker record for token %q", token)
	}
	return record
}

func answeredCount(client *questionClient) int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return len(client.answered)
}

func lastWizardAnswers(client *questionClient) [][]string {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([][]string(nil), client.lastAnswers...)
}

func twoQuestionRequest() relay.QuestionRequest {
	req := threeQuestionRequest()
	req.Questions = req.Questions[:2]
	req.ID = "que-2"
	return req
}

func TestQuestionWizardPromptShowsOnlyFirstQuestion(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), threeQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("expected 1 prompt send, got %d", len(reply.sends))
	}
	sent := reply.sends[0]
	if !strings.Contains(sent.text, "(1/3)") {
		t.Fatalf("prompt header missing (1/3) counter: %q", sent.text)
	}
	if !strings.Contains(sent.text, "Pick language?") {
		t.Fatalf("prompt missing first question: %q", sent.text)
	}
	if !strings.Contains(sent.text, "Go") || !strings.Contains(sent.text, "Fast and simple") {
		t.Fatalf("prompt missing full first-question option details: %q", sent.text)
	}
	if strings.Contains(sent.text, "Pick database?") || strings.Contains(sent.text, "Pick region?") {
		t.Fatalf("wizard prompt must not render later questions: %q", sent.text)
	}
	wantLabels := []string{"1", "2", "⏭ Skip question", "❌ Cancel"}
	if len(sent.buttons) != len(wantLabels) {
		t.Fatalf("button count = %d, want %d (%v)", len(sent.buttons), len(wantLabels), sent.buttons)
	}
	for i, label := range wantLabels {
		if sent.buttons[i].Label != label {
			t.Fatalf("button %d label = %q, want %q", i, sent.buttons[i].Label, label)
		}
	}
	token := questionToken(t, sent.buttons[0].Value)
	if sent.buttons[0].Value != "question:"+token+":0:0" || sent.buttons[1].Value != "question:"+token+":0:1" {
		t.Fatalf("option buttons must address question 0: %+v", sent.buttons)
	}
	if sent.buttons[2].Value != "question:"+token+":skipone" {
		t.Fatalf("skip button = %q, want skipone verb", sent.buttons[2].Value)
	}
	if sent.buttons[3].Value != "question:"+token+":cancel" {
		t.Fatalf("cancel button = %q, want cancel verb", sent.buttons[3].Value)
	}
}

func TestQuestionWizardHappyPathAdvancesAndSubmitsOnce(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), threeQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if answeredCount(client) != 0 {
		t.Fatal("answering a non-final question must not submit")
	}
	if len(reply.edits) != 1 {
		t.Fatalf("expected in-place advance edit, got %d edits", len(reply.edits))
	}
	if !strings.Contains(reply.edits[0].text, "(2/3)") || !strings.Contains(reply.edits[0].text, "Pick database?") {
		t.Fatalf("advance view = %q, want second question", reply.edits[0].text)
	}
	if got := wizardRecord(t, h, token).currentIdx; got != 1 {
		t.Fatalf("currentIdx = %d, want 1", got)
	}

	tapQuestion(t, h, reply, origin, "question:"+token+":1:1")
	if answeredCount(client) != 0 {
		t.Fatal("answering a non-final question must not submit")
	}
	if !strings.Contains(reply.edits[1].text, "(3/3)") || !strings.Contains(reply.edits[1].text, "Pick region?") {
		t.Fatalf("advance view = %q, want third question", reply.edits[1].text)
	}

	tapQuestion(t, h, reply, origin, "question:"+token+":2:0")
	if answeredCount(client) != 1 {
		t.Fatalf("expected exactly one submit, got %d", answeredCount(client))
	}
	answers := lastWizardAnswers(client)
	if len(answers) != 3 {
		t.Fatalf("answers len = %d, want 3", len(answers))
	}
	want := [][]string{{"Go"}, {"Postgres"}, {"Jakarta"}}
	for i := range want {
		if len(answers[i]) != 1 || answers[i][0] != want[i][0] {
			t.Fatalf("answers[%d] = %v, want %v", i, answers[i], want[i])
		}
	}
	last := reply.edits[len(reply.edits)-1]
	wantTerminal := "✅ Answered (3/3):\n1. Go\n2. Postgres\n3. Jakarta"
	if last.text != wantTerminal {
		t.Fatalf("terminal view = %q, want %q", last.text, wantTerminal)
	}
	if last.buttons != nil {
		t.Fatal("terminal view should clear buttons")
	}
	h.broker.mu.Lock()
	_, stillPending := h.broker.records[token]
	h.broker.mu.Unlock()
	if stillPending {
		t.Fatal("resolved wizard record must be removed")
	}
}

func TestQuestionWizardMixedAnswerSkipSubmitsPerIndexValues(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), threeQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:1")
	tapQuestion(t, h, reply, origin, "question:"+token+":skipone")
	tapQuestion(t, h, reply, origin, "question:"+token+":2:0")

	if answeredCount(client) != 1 {
		t.Fatalf("expected exactly one submit, got %d", answeredCount(client))
	}
	answers := lastWizardAnswers(client)
	if len(answers) != 3 {
		t.Fatalf("answers len = %d, want 3", len(answers))
	}
	for i, a := range answers {
		if a == nil {
			t.Fatalf("answers[%d] is nil — would marshal to JSON null", i)
		}
	}
	if len(answers[0]) != 1 || answers[0][0] != "Python" {
		t.Fatalf("answers[0] = %v, want [Python]", answers[0])
	}
	if len(answers[1]) != 0 {
		t.Fatalf("answers[1] = %v, want empty (skipped)", answers[1])
	}
	if len(answers[2]) != 1 || answers[2][0] != "Jakarta" {
		t.Fatalf("answers[2] = %v, want [Jakarta]", answers[2])
	}
	last := reply.edits[len(reply.edits)-1]
	wantTerminal := "✅ Answered (2/3):\n1. Python\n2. skipped\n3. Jakarta"
	if last.text != wantTerminal {
		t.Fatalf("terminal view = %q, want %q", last.text, wantTerminal)
	}
}

func TestQuestionWizardCancelRejectsOnce(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), threeQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	tapQuestion(t, h, reply, origin, "question:"+token+":cancel")

	client.mu.Lock()
	rejected := append([]string(nil), client.rejected...)
	answered := len(client.answered)
	client.mu.Unlock()
	if len(rejected) != 1 || rejected[0] != "que-3" {
		t.Fatalf("rejected = %v, want [que-3]", rejected)
	}
	if answered != 0 {
		t.Fatalf("cancel must never submit answers, got %d submits", answered)
	}
	last := reply.edits[len(reply.edits)-1]
	if last.text != "❌ Question request cancelled." {
		t.Fatalf("terminal view = %q, want cancelled view", last.text)
	}
	h.broker.mu.Lock()
	_, stillPending := h.broker.records[token]
	h.broker.mu.Unlock()
	if stillPending {
		t.Fatal("cancelled wizard record must be removed")
	}
}

func TestQuestionWizardLegacySkipCancelsWholeRequest(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":skip")

	client.mu.Lock()
	rejected := append([]string(nil), client.rejected...)
	answered := len(client.answered)
	client.mu.Unlock()
	if len(rejected) != 1 || rejected[0] != "que-2" {
		t.Fatalf("rejected = %v, want [que-2]", rejected)
	}
	if answered != 0 {
		t.Fatalf("legacy skip must never submit answers, got %d submits", answered)
	}
}

func TestQuestionWizardFinalSubmitFailureRetriesFromLastQuestion(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")

	client.mu.Lock()
	client.fail = errors.New("agent down")
	client.mu.Unlock()
	tapQuestion(t, h, reply, origin, "question:"+token+":1:1")
	if answeredCount(client) != 0 {
		t.Fatal("failed submit must not record an answer")
	}
	last := reply.edits[len(reply.edits)-1]
	if !strings.Contains(last.text, "Could not submit your answer") {
		t.Fatalf("expected retry view, got %q", last.text)
	}
	if len(last.buttons) != 4 {
		t.Fatalf("retry view must re-render last question buttons, got %d", len(last.buttons))
	}
	if last.buttons[0].Value != "question:"+token+":1:0" {
		t.Fatalf("retry buttons must address last question: %+v", last.buttons)
	}

	client.mu.Lock()
	client.fail = nil
	client.mu.Unlock()
	tapQuestion(t, h, reply, origin, "question:"+token+":1:1")
	if answeredCount(client) != 1 {
		t.Fatalf("expected one submit after retry, got %d", answeredCount(client))
	}
	answers := lastWizardAnswers(client)
	if len(answers) != 2 || len(answers[0]) != 1 || answers[0][0] != "Go" || len(answers[1]) != 1 || answers[1][0] != "Postgres" {
		t.Fatalf("resubmitted answers = %v, want [[Go] [Postgres]]", answers)
	}
	terminal := reply.edits[len(reply.edits)-1]
	if terminal.text != "✅ Answered (2/2):\n1. Go\n2. Postgres" {
		t.Fatalf("terminal view = %q", terminal.text)
	}
}

func TestQuestionWizardBusyCallbackDroppedWithoutExpiredView(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	wizardRecord(t, h, token).state = questionHandling

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if len(reply.edits) != 0 {
		t.Fatalf("busy callback must not render any view, got %d edits", len(reply.edits))
	}
	if answeredCount(client) != 0 {
		t.Fatal("busy callback must not submit")
	}

	wizardRecord(t, h, token).state = questionPending
	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if len(reply.edits) != 1 || !strings.Contains(reply.edits[0].text, "(2/2)") {
		t.Fatalf("flow must resume after busy drop: %+v", reply.edits)
	}
}

func TestQuestionWizardStaleQuestionIndexDropped(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:1")
	editsAfterAdvance := len(reply.edits)

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if len(reply.edits) != editsAfterAdvance {
		t.Fatal("stale question tap must not advance or render")
	}
	if answeredCount(client) != 0 {
		t.Fatal("stale question tap must not submit")
	}
	if got := wizardRecord(t, h, token).currentIdx; got != 1 {
		t.Fatalf("currentIdx = %d, want 1 (unchanged)", got)
	}

	tapQuestion(t, h, reply, origin, "question:"+token+":1:0")
	answers := lastWizardAnswers(client)
	if len(answers) != 2 || answers[0][0] != "Python" || answers[1][0] != "SQLite" {
		t.Fatalf("staged first answer must survive stale tap, got %v", answers)
	}
}

func TestQuestionWizardSlidingTTLCompletesAfterTenMinutes(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	record := wizardRecord(t, h, token)
	record.createdAt = time.Now().Add(-11 * time.Minute)
	record.expiresAt = time.Now().Add(time.Minute)

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if answeredCount(client) != 0 {
		t.Fatal("first step must advance, not submit")
	}
	if time.Until(wizardRecord(t, h, token).expiresAt) < 9*time.Minute {
		t.Fatal("accepted callback must refresh expiry by a full TTL")
	}

	tapQuestion(t, h, reply, origin, "question:"+token+":1:0")
	if answeredCount(client) != 1 {
		t.Fatal("wizard older than 10m must still complete while in active use")
	}
}

func TestQuestionWizardAbandonedRecordExpires(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), twoQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	record := wizardRecord(t, h, token)
	record.expiresAt = time.Now().Add(-time.Minute)

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")
	if answeredCount(client) != 0 {
		t.Fatal("expired wizard must not advance or submit")
	}
	last := reply.edits[len(reply.edits)-1]
	if last.text != questionExpiredMessage {
		t.Fatalf("expected expired view, got %q", last.text)
	}
}

func TestQuestionWizardAdvanceEditFailureReasksSameQuestion(t *testing.T) {
	client := &questionClient{}
	reply := &questionReply{}
	h := newQuestionTestHandler(client, reply)

	if err := h.Prompt(context.Background(), threeQuestionRequest()); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	token := questionToken(t, reply.sends[0].buttons[0].Value)
	origin := reply.sends[0].ref

	tapQuestion(t, h, reply, origin, "question:"+token+":0:0")

	reply.mu.Lock()
	reply.editFail = errors.New("edit failed")
	reply.mu.Unlock()
	tapQuestion(t, h, reply, origin, "question:"+token+":1:1")
	if answeredCount(client) != 0 {
		t.Fatal("non-final step must not submit")
	}
	if got := wizardRecord(t, h, token).currentIdx; got != 1 {
		t.Fatalf("currentIdx = %d, want 1 (re-ask same question)", got)
	}

	reply.mu.Lock()
	reply.editFail = nil
	reply.mu.Unlock()
	tapQuestion(t, h, reply, origin, "question:"+token+":1:1")
	if got := wizardRecord(t, h, token).currentIdx; got != 2 {
		t.Fatalf("currentIdx = %d, want 2 after re-ask", got)
	}
	tapQuestion(t, h, reply, origin, "question:"+token+":2:1")
	if answeredCount(client) != 1 {
		t.Fatalf("expected one submit, got %d", answeredCount(client))
	}
	answers := lastWizardAnswers(client)
	if len(answers) != 3 || answers[0][0] != "Go" || answers[1][0] != "Postgres" || answers[2][0] != "Singapore" {
		t.Fatalf("staged answers must survive advance failure, got %v", answers)
	}
}

func TestParseQuestionCallbackWizardVerbs(t *testing.T) {
	token, qIdx, optIdx, action, ok := parseQuestionCallback("question:tok:0:1")
	if !ok || token != "tok" || qIdx != 0 || optIdx != 1 || action != questionActionAnswer {
		t.Fatalf("answer verb = %q,%d,%d,%v,%v", token, qIdx, optIdx, action, ok)
	}
	token, _, _, action, ok = parseQuestionCallback("question:tok:skip")
	if !ok || token != "tok" || action != questionActionSkipAll {
		t.Fatalf("legacy skip verb = %q,%v,%v", token, action, ok)
	}
	token, _, _, action, ok = parseQuestionCallback("question:tok:skipone")
	if !ok || token != "tok" || action != questionActionSkipOne {
		t.Fatalf("skipone verb = %q,%v,%v", token, action, ok)
	}
	token, _, _, action, ok = parseQuestionCallback("question:tok:cancel")
	if !ok || token != "tok" || action != questionActionCancel {
		t.Fatalf("cancel verb = %q,%v,%v", token, action, ok)
	}
	for _, data := range []string{"", "garbage", "question::0:0", "question:tok:skip:extra", "question:tok:x:y", "question:tok:unknown"} {
		if _, _, _, _, ok := parseQuestionCallback(data); ok {
			t.Fatalf("callback %q must not parse", data)
		}
	}
}

func TestQuestionSummaryTextCountsAnswered(t *testing.T) {
	questions := threeQuestionRequest().Questions
	got := questionSummaryText(questions, [][]string{{"Go"}, {}, {"Jakarta"}})
	want := "✅ Answered (2/3):\n1. Go\n2. skipped\n3. Jakarta"
	if got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
}
