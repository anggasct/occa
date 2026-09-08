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
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
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

type questionAction uint8

const (
	questionActionAnswer questionAction = iota
	questionActionSkipOne
	questionActionCancel
	questionActionLegacySkip
)

const questionActionSkipAll = questionActionLegacySkip

type questionRecord struct {
	token         string
	client        relay.Client
	platform      string
	channelID     string
	sessionID     string
	requestID     string
	questions     []relay.QuestionInfo
	answers       [][]string
	currentIdx    int
	renderText    func(string) []string
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
	split     func(string) []string
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
		token:      token,
		client:     h.client,
		platform:   h.platform,
		channelID:  h.channelID,
		sessionID:  sessionID,
		requestID:  request.ID,
		questions:  request.Questions,
		answers:    make([][]string, len(request.Questions)),
		renderText: h.renderText,
		reply:      h.reply,
		state:      questionPending,
		createdAt:  time.Now(),
		expiresAt:  time.Now().Add(questionTombstoneTTL),
	}

	h.broker.mu.Lock()
	h.broker.cleanupLocked(time.Now())
	h.broker.records[token] = record
	h.broker.mu.Unlock()

	mode := "single"
	var textChunks []string
	var buttons []channel.Button
	if len(request.Questions) > 1 {
		mode = "wizard"
		rawText := questionStepText(request.Questions, 0)
		textChunks = h.renderText(rawText)
		if len(textChunks) > 1 {
			limit := render.DiscordLimit
			if h.platform == "telegram" {
				limit = render.TelegramLimit
			}
			textChunks = []string{render.Clamp(textChunks[0], limit)}
		}
		buttons = questionStepButtons(token, request.Questions, 0)
	} else {
		textChunks = h.renderText(questionPromptText(request.Questions))
		buttons = questionButtons(token, request.Questions)
	}
	var ref channel.MessageRef
	if len(textChunks) == 1 {
		ref, err = h.reply.SendWithButtons(textChunks[0], buttons)
	} else {
		for _, chunk := range textChunks {
			if _, err = h.reply.Send(chunk); err != nil {
				return h.failPrompt(ctx, record, err)
			}
		}
		actionChunks := h.renderText("❓ Choose an option:")
		ref, err = h.reply.SendWithButtons(actionChunks[0], buttons)
	}
	if err != nil {
		return h.failPrompt(ctx, record, err)
	}
	if ref == nil || ref.ID() == "" {
		return h.failPrompt(ctx, record, errors.New("prompt has no origin reference"))
	}

	h.broker.mu.Lock()
	record.origin = ref
	h.broker.mu.Unlock()

	slog.Info("question prompt registered", "platform", record.platform, "channel_id", record.channelID, "questions", len(request.Questions), "mode", mode)
	return nil
}

func (h *questionPromptHandler) renderText(text string) []string {
	if h.split != nil {
		chunks := h.split(text)
		if len(chunks) > 0 {
			return chunks
		}
	}
	if h.encode != nil {
		return []string{h.encode(text)}
	}
	return []string{text}
}

func (h *questionPromptHandler) failPrompt(ctx context.Context, record *questionRecord, sendErr error) error {
	h.broker.removePending(record)
	rejectErr := record.client.RejectQuestion(ctx, record.requestID)
	if rejectErr != nil {
		slog.Warn("question: reject after prompt failure failed", "platform", record.platform, "channel_id", record.channelID, "error", rejectErr)
	}
	if record.reply != nil {
		failureText := "⚠️ Could not show the question."
		if rejectErr == nil {
			failureText += " The request was stopped."
		} else {
			failureText += " Please check the agent status and try again."
		}
		if _, notifyErr := record.reply.Send(failureText); notifyErr != nil {
			slog.Warn("question: prompt failure notice failed", "platform", record.platform, "channel_id", record.channelID, "error", notifyErr)
		}
	}
	return fmt.Errorf("question: send prompt: %w", sendErr)
}

// HandleQuestionCallback is exported through the Router wrapper to prevent
// the linker from eliminating it as dead code.
func (b *questionBroker) HandleQuestionCallback(ctx context.Context, msg channel.IncomingMessage) error {
	token, qIdx, optIdx, action, ok := parseQuestionCallback(msg.CallbackData)
	if !ok || msg.CallbackRef == nil {
		b.renderExpired(msg)
		return nil
	}

	now := time.Now()
	b.mu.Lock()
	b.cleanupLocked(now)
	record := b.records[token]
	if record == nil || record.origin == nil || record.origin.ID() != msg.CallbackRef.ID() || record.platform != msg.Platform || record.channelID != msg.ChannelID {
		b.mu.Unlock()
		b.renderExpired(msg)
		return nil
	}
	if record.state == questionHandling {
		platform, channelID := record.platform, record.channelID
		b.mu.Unlock()
		slog.Debug("question: callback dropped during transition", "platform", platform, "channel_id", channelID)
		return nil
	}
	if record.state != questionPending {
		b.mu.Unlock()
		b.renderExpired(msg)
		return nil
	}
	record.state = questionHandling
	record.expiresAt = now.Add(questionTombstoneTTL)
	reply := record.reply
	wizard := len(record.questions) > 1
	b.mu.Unlock()

	if !wizard {
		return b.handleSingleCallback(ctx, record, reply, qIdx, optIdx, action)
	}
	return b.handleWizardCallback(ctx, record, reply, qIdx, optIdx, action)
}

func (b *questionBroker) handleSingleCallback(ctx context.Context, record *questionRecord, reply channel.ReplyContext, qIdx, optIdx int, action questionAction) error {
	terminal := questionSkippedLabel
	if action != questionActionAnswer {
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
		terminal = questionAnsweredLabel + ": " + label
	}

	b.resolve(record)
	if reply != nil && record.origin != nil {
		if err := reply.EditWithButtons(record.origin, terminal, nil); err != nil {
			slog.Warn("question: terminal view failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
		}
	}
	return nil
}

func (b *questionBroker) handleWizardCallback(ctx context.Context, record *questionRecord, reply channel.ReplyContext, qIdx, optIdx int, action questionAction) error {
	b.mu.Lock()
	idx := record.currentIdx
	questions := record.questions
	total := len(questions)
	b.mu.Unlock()

	if action == questionActionCancel || action == questionActionSkipAll {
		b.logWizardStep(record, idx, total, "cancel")
		if err := record.client.RejectQuestion(ctx, record.requestID); err != nil {
			slog.Warn("question: reject failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
			b.resetPending(record)
			return b.retry(record, err)
		}
		b.resolve(record)
		if reply != nil && record.origin != nil {
			if err := reply.EditWithButtons(record.origin, "❌ Question request cancelled.", nil); err != nil {
				slog.Warn("question: terminal view failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
			}
		}
		return nil
	}

	if qIdx != idx {
		b.resetPending(record)
		return nil
	}

	if action == questionActionSkipOne {
		b.stageAnswer(record, idx, []string{})
		b.logWizardStep(record, idx, total, "skip")
		return b.advanceOrSubmit(ctx, record, reply, idx)
	}

	if qIdx >= total || optIdx >= len(questions[qIdx].Options) {
		b.resetPending(record)
		return b.retry(record, nil)
	}
	label := questions[qIdx].Options[optIdx].Label
	b.stageAnswer(record, idx, []string{label})
	b.logWizardStep(record, idx, total, "answer")
	return b.advanceOrSubmit(ctx, record, reply, idx)
}

func (b *questionBroker) advanceOrSubmit(ctx context.Context, record *questionRecord, reply channel.ReplyContext, idx int) error {
	b.mu.Lock()
	questions := record.questions
	token := record.token
	total := len(questions)
	origin := record.origin
	b.mu.Unlock()

	if idx < total-1 {
		next := idx + 1
		text := b.renderStepText(record, next)
		buttons := questionStepButtons(token, questions, next)
		if reply != nil && origin != nil {
			if err := reply.EditWithButtons(origin, text, buttons); err != nil {
				slog.Warn("question: wizard advance failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
				b.resetPending(record)
				return nil
			}
		}
		b.mu.Lock()
		record.currentIdx = next
		record.state = questionPending
		b.mu.Unlock()
		return nil
	}

	answers := b.stagedAnswers(record)
	if err := record.client.AnswerQuestion(ctx, record.requestID, answers); err != nil {
		slog.Warn("question: answer failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
		b.resetPending(record)
		return b.retry(record, err)
	}
	answered := 0
	for _, a := range answers {
		if len(a) > 0 {
			answered++
		}
	}
	slog.Info("question wizard submitted", "platform", record.platform, "channel_id", record.channelID, "answered", answered, "skipped", total-answered)
	b.resolve(record)
	if reply != nil && origin != nil {
		summaryText := questionSummaryText(questions, answers)
		limit := render.DiscordLimit
		if record.platform == "telegram" {
			limit = render.TelegramLimit
		}
		if record.renderText != nil {
			chunks := record.renderText(summaryText)
			if len(chunks) > 0 {
				summaryText = render.Clamp(chunks[0], limit)
			}
		} else {
			summaryText = render.Clamp(summaryText, limit)
		}
		if err := reply.EditWithButtons(origin, summaryText, nil); err != nil {
			slog.Warn("question: terminal view failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
		}
	}
	return nil
}

func (b *questionBroker) renderStepText(record *questionRecord, idx int) string {
	rawText := questionStepText(record.questions, idx)
	limit := render.DiscordLimit
	if record.platform == "telegram" {
		limit = render.TelegramLimit
	}
	if record.renderText != nil {
		chunks := record.renderText(rawText)
		if len(chunks) > 0 {
			return render.Clamp(chunks[0], limit)
		}
	}
	return render.Clamp(rawText, limit)
}

func (b *questionBroker) stageAnswer(record *questionRecord, idx int, answer []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	record.answers[idx] = answer
}

func (b *questionBroker) stagedAnswers(record *questionRecord) [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([][]string, len(record.questions))
	for i := range record.questions {
		if record.answers[i] == nil {
			out[i] = []string{}
		} else {
			out[i] = record.answers[i]
		}
	}
	return out
}

func (b *questionBroker) logWizardStep(record *questionRecord, idx, total int, action string) {
	slog.Info("question wizard step", "platform", record.platform, "channel_id", record.channelID, "index", idx+1, "total", total, "action", action)
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
	if record.reply != nil && record.origin != nil {
		buttons := questionButtons(record.token, record.questions)
		if len(record.questions) > 1 {
			buttons = questionStepButtons(record.token, record.questions, record.currentIdx)
		}
		if editErr := record.reply.EditWithButtons(record.origin, retryText, buttons); editErr != nil {
			slog.Warn("question: retry view failed", "platform", record.platform, "channel_id", record.channelID, "error", editErr)
			return nil
		}
	}
	record.lastRetryText = retryText
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

	if idx := strings.Index(reason, "{"); idx >= 0 {
		var js struct {
			Message string `json:"message"`
			Error   string `json:"error"`
			Detail  string `json:"detail"`
		}
		if jsonErr := json.Unmarshal([]byte(reason[idx:]), &js); jsonErr == nil {
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

	reason = truncateRunes(reason, 100)

	return fmt.Sprintf("⚠️ Could not submit your answer — %s. Try again or tap Skip.", reason)
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	var count int
	var byteIdx int
	for idx := range s {
		if count == max {
			byteIdx = idx
			break
		}
		count++
	}
	return s[:byteIdx] + "…"
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

func questionStepText(questions []relay.QuestionInfo, idx int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "❓ Agent has a question (%d/%d):\n\n", idx+1, len(questions))
	q := questions[idx]
	sb.WriteString("**" + q.Question + "**\n")
	for _, o := range q.Options {
		sb.WriteString("• " + o.Label)
		if o.Description != "" {
			sb.WriteString(" — " + o.Description)
		}
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

func questionSummaryText(questions []relay.QuestionInfo, answers [][]string) string {
	var sb strings.Builder
	answered := 0
	for _, a := range answers {
		if len(a) > 0 {
			answered++
		}
	}
	fmt.Fprintf(&sb, "✅ Answered (%d/%d):", answered, len(questions))
	for i := range questions {
		sb.WriteString("\n")
		if i < len(answers) && len(answers[i]) > 0 {
			fmt.Fprintf(&sb, "%d. %s", i+1, answers[i][0])
		} else {
			fmt.Fprintf(&sb, "%d. skipped", i+1)
		}
	}
	return sb.String()
}

func questionButtons(token string, questions []relay.QuestionInfo) []channel.Button {
	var buttons []channel.Button
	buttonIndex := 0
	for qIdx, q := range questions {
		for optIdx := range q.Options {
			value := "question:" + token + ":" + strconv.Itoa(qIdx) + ":" + strconv.Itoa(optIdx)
			buttons = append(buttons, channel.Button{Label: fmt.Sprintf("Q%d · %d", qIdx+1, optIdx+1), Value: value, Row: buttonIndex/5 + 1})
			buttonIndex++
		}
	}
	buttons = append(buttons, channel.Button{Label: "❌ Skip", Value: "question:" + token + ":skip", Row: buttonIndex/5 + 1})
	return buttons
}

func questionStepButtons(token string, questions []relay.QuestionInfo, idx int) []channel.Button {
	var buttons []channel.Button
	numOpts := len(questions[idx].Options)
	for optIdx := range questions[idx].Options {
		value := "question:" + token + ":" + strconv.Itoa(idx) + ":" + strconv.Itoa(optIdx)
		buttons = append(buttons, channel.Button{
			Label: strconv.Itoa(optIdx + 1),
			Value: value,
			Row:   optIdx/5 + 1,
		})
	}
	actionRow := (numOpts-1)/5 + 2
	if numOpts == 0 {
		actionRow = 1
	}
	if actionRow > 5 {
		actionRow = 5
	}
	buttons = append(buttons, channel.Button{
		Label: "⏭ Skip question",
		Value: "question:" + token + ":" + strconv.Itoa(idx) + ":skipone",
		Row:   actionRow,
	})
	buttons = append(buttons, channel.Button{
		Label: "❌ Cancel",
		Value: "question:" + token + ":cancel",
		Row:   actionRow,
	})
	return buttons
}

func parseQuestionCallback(data string) (token string, qIdx, optIdx int, action questionAction, ok bool) {
	parts := strings.Split(data, ":")
	if len(parts) < 3 || parts[0] != "question" || parts[1] == "" {
		return "", 0, 0, questionActionAnswer, false
	}
	if len(parts) == 3 {
		switch parts[2] {
		case "skip":
			return parts[1], 0, 0, questionActionLegacySkip, true
		case "skipone":
			return parts[1], 0, 0, questionActionSkipOne, true
		case "cancel":
			return parts[1], 0, 0, questionActionCancel, true
		}
		return "", 0, 0, questionActionAnswer, false
	}
	if len(parts) != 4 {
		return "", 0, 0, questionActionAnswer, false
	}
	if parts[3] == "skipone" {
		q, errQ := strconv.Atoi(parts[2])
		if errQ != nil {
			return "", 0, 0, questionActionAnswer, false
		}
		return parts[1], q, 0, questionActionSkipOne, true
	}
	q, errQ := strconv.Atoi(parts[2])
	o, errO := strconv.Atoi(parts[3])
	if errQ != nil || errO != nil {
		return "", 0, 0, questionActionAnswer, false
	}
	return parts[1], q, o, questionActionAnswer, true
}
