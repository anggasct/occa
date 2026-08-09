package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

const (
	questionTombstoneTTL   = 10 * time.Minute
	questionExpiredMessage = "⌛ Question request expired."
	questionAnsweredLabel  = "✅ Answered"
	questionSkippedLabel   = "❌ Skipped"
)

type questionState uint8

const (
	questionPending questionState = iota
	questionHandling
	questionResolved
	questionExpired
)

type questionRecord struct {
	token         string
	client        relay.Client
	platform      string
	channelID     string
	sessionID     string
	requestID     string
	questions     []relay.QuestionInfo
	reply         channel.ReplyContext
	origin        channel.MessageRef
	state         questionState
	lastRetryText string
	createdAt     time.Time
	expiresAt     time.Time
}

type questionBroker struct {
	mu      sync.Mutex
	records map[string]*questionRecord
}

type questionPromptHandler struct {
	broker    *questionBroker
	encode    func(string) string
	client    relay.Client
	platform  string
	channelID string
	sessionID string
	reply     channel.ReplyContext
}

func newQuestionBroker() *questionBroker {
	return &questionBroker{records: make(map[string]*questionRecord)}
}

func (h *questionPromptHandler) Prompt(ctx context.Context, request relay.QuestionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	token, err := permissionToken()
	if err != nil {
		return fmt.Errorf("question: generate token: %w", err)
	}
	sessionID := request.SessionID
	if sessionID == "" {
		sessionID = h.sessionID
	}
	record := &questionRecord{
		token:     token,
		client:    h.client,
		platform:  h.platform,
		channelID: h.channelID,
		sessionID: sessionID,
		requestID: request.ID,
		questions: request.Questions,
		reply:     h.reply,
		state:     questionPending,
		createdAt: time.Now(),
		expiresAt: time.Now().Add(questionTombstoneTTL),
	}

	h.broker.mu.Lock()
	h.broker.cleanupLocked(time.Now())
	h.broker.records[token] = record
	h.broker.mu.Unlock()

	text := questionPromptText(request.Questions)
	if h.encode != nil {
		text = h.encode(text)
	}

	ref, err := h.reply.SendWithButtons(text, questionButtons(token, request.Questions))
	if err != nil {
		h.broker.removePending(record)
		return fmt.Errorf("question: send prompt: %w", err)
	}
	if ref == nil || ref.ID() == "" {
		h.broker.removePending(record)
		return fmt.Errorf("question: prompt has no origin reference")
	}

	h.broker.mu.Lock()
	record.origin = ref
	h.broker.mu.Unlock()

	slog.Info("question prompt registered", "platform", record.platform, "channel_id", record.channelID)
	return nil
}

// HandleQuestionCallback is exported through the Router wrapper to prevent
// the linker from eliminating it as dead code.
func (b *questionBroker) HandleQuestionCallback(ctx context.Context, msg channel.IncomingMessage) error {
	token, qIdx, optIdx, skip, ok := parseQuestionCallback(msg.CallbackData)
	if !ok || msg.CallbackRef == nil {
		b.renderExpired(msg)
		return nil
	}

	b.mu.Lock()
	b.cleanupLocked(time.Now())
	record := b.records[token]
	if record == nil || record.origin == nil || record.origin.ID() != msg.CallbackRef.ID() || record.platform != msg.Platform || record.channelID != msg.ChannelID {
		b.mu.Unlock()
		b.renderExpired(msg)
		return nil
	}
	if record.state != questionPending {
		b.mu.Unlock()
		b.renderExpired(msg)
		return nil
	}
	record.state = questionHandling
	reply := record.reply
	b.mu.Unlock()

	terminal := questionSkippedLabel
	if skip {
		if err := record.client.RejectQuestion(ctx, record.requestID); err != nil {
			slog.Warn("question: reject failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
			b.resetPending(record)
			return b.retry(record, err)
		}
	} else {
		if qIdx >= len(record.questions) || optIdx >= len(record.questions[qIdx].Options) {
			b.resetPending(record)
			return b.retry(record, nil)
		}
		label := record.questions[qIdx].Options[optIdx].Label
		// Build one answer entry per question. Unanswered questions must be
		// an empty array, never nil: nil marshals to JSON null, and opencode
		// rejects any null entry in answers with HTTP 400
		// ("Expected QuestionAnswer, got null at [answers][i]").
		answers := make([][]string, len(record.questions))
		for i := range answers {
			answers[i] = []string{}
		}
		answers[qIdx] = []string{label}
		if err := record.client.AnswerQuestion(ctx, record.requestID, answers); err != nil {
			slog.Warn("question: answer failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
			b.resetPending(record)
			return b.retry(record, err)
		}
		if len(record.questions) > 1 {
			var skipped []string
			for i := range record.questions {
				if i != qIdx {
					skipped = append(skipped, strconv.Itoa(i+1))
				}
			}
			terminal = fmt.Sprintf("✅ Soal %d: %s — soal %s dilewati (ketuk opsi buat menjawab, atau Skip).", qIdx+1, label, strings.Join(skipped, ", "))
		} else {
			terminal = questionAnsweredLabel + ": " + label
		}
	}

	b.resolve(record)
	if reply != nil && record.origin != nil {
		if err := reply.EditWithButtons(record.origin, terminal, nil); err != nil {
			slog.Warn("question: terminal view failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
		}
	}
	return nil
}

func (b *questionBroker) retry(record *questionRecord, err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if record.state == questionHandling {
		record.state = questionPending
	}
	retryText := formatQuestionRetryMessage(err)
	if record.lastRetryText == retryText {
		return nil
	}
	record.lastRetryText = retryText
	if record.reply != nil && record.origin != nil {
		if editErr := record.reply.EditWithButtons(record.origin, retryText, questionButtons(record.token, record.questions)); editErr != nil {
			slog.Warn("question: retry view failed", "platform", record.platform, "channel_id", record.channelID, "error", editErr)
		}
	}
	return nil
}

func formatQuestionRetryMessage(err error) string {
	const fallback = "⚠️ Could not submit the answer. Try again."
	if err == nil {
		return fallback
	}

	var reason string
	switch {
	case errors.Is(err, relay.ErrNotFound):
		reason = "agent resource not found"
	case errors.Is(err, relay.ErrTimeout):
		reason = "agent request timed out"
	case errors.Is(err, relay.ErrUnreachable):
		reason = "agent unreachable"
	default:
		reason = err.Error()
		reason = strings.TrimPrefix(reason, "relay: answer question: ")
		reason = strings.TrimPrefix(reason, "relay: reject question: ")
		reason = strings.TrimPrefix(reason, "relay: ")
		reason = strings.TrimSpace(reason)
	}

	if reason == "" {
		return fallback
	}

	if strings.HasPrefix(reason, "{") || strings.Contains(reason, "{\"") {
		var js struct {
			Message string `json:"message"`
			Error   string `json:"error"`
			Detail  string `json:"detail"`
		}
		if jsonErr := json.Unmarshal([]byte(reason), &js); jsonErr == nil {
			if js.Message != "" {
				reason = js.Message
			} else if js.Error != "" {
				reason = js.Error
			} else if js.Detail != "" {
				reason = js.Detail
			} else {
				return fallback
			}
		} else {
			return fallback
		}
	}

	if len(reason) > 100 {
		reason = reason[:100] + "…"
	}

	return fmt.Sprintf("⚠️ Gagal kirim jawaban — %s. Coba lagi atau ketuk Skip.", reason)
}

func (b *questionBroker) resetPending(record *questionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if record.state == questionHandling {
		record.state = questionPending
	}
}

func (b *questionBroker) resolve(record *questionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	record.state = questionResolved
	delete(b.records, record.token)
}

func (b *questionBroker) removePending(record *questionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if record.state == questionPending {
		delete(b.records, record.token)
	}
}

func (b *questionBroker) renderExpired(msg channel.IncomingMessage) {
	if msg.CallbackRef == nil || msg.ReplyCtx == nil {
		return
	}
	if err := msg.ReplyCtx.EditWithButtons(msg.CallbackRef, questionExpiredMessage, nil); err != nil {
		slog.Warn("question: expired callback view failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
	}
}

func (b *questionBroker) cleanupLocked(now time.Time) {
	for token, record := range b.records {
		if now.After(record.expiresAt) {
			delete(b.records, token)
		}
	}
}

func questionPromptText(questions []relay.QuestionInfo) string {
	var sb strings.Builder
	sb.WriteString("❓ Agent has a question:\n\n")
	for i, q := range questions {
		if len(questions) > 1 {
			fmt.Fprintf(&sb, "**%d. %s**\n", i+1, q.Question)
		} else {
			sb.WriteString("**" + q.Question + "**\n")
		}
		for _, o := range q.Options {
			sb.WriteString("• " + o.Label)
			if o.Description != "" {
				sb.WriteString(" — " + o.Description)
			}
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

func questionButtons(token string, questions []relay.QuestionInfo) []channel.Button {
	var buttons []channel.Button
	for qIdx, q := range questions {
		for optIdx, o := range q.Options {
			value := "question:" + token + ":" + strconv.Itoa(qIdx) + ":" + strconv.Itoa(optIdx)
			buttons = append(buttons, channel.Button{Label: o.Label, Value: value, Row: qIdx + 1})
		}
	}
	buttons = append(buttons, channel.Button{Label: "❌ Skip", Value: "question:" + token + ":skip", Row: len(questions) + 1})
	return buttons
}

func parseQuestionCallback(data string) (token string, qIdx, optIdx int, skip bool, ok bool) {
	parts := strings.Split(data, ":")
	if len(parts) < 3 || parts[0] != "question" || parts[1] == "" {
		return "", 0, 0, false, false
	}
	if len(parts) == 3 && parts[2] == "skip" {
		return parts[1], 0, 0, true, true
	}
	if len(parts) != 4 {
		return "", 0, 0, false, false
	}
	q, errQ := strconv.Atoi(parts[2])
	o, errO := strconv.Atoi(parts[3])
	if errQ != nil || errO != nil {
		return "", 0, 0, false, false
	}
	return parts[1], q, o, false, true
}
