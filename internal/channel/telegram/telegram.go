package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type Adapter struct {
	bot            *tgbotapi.BotAPI
	token          string
	menu           []channel.MenuCommand
	downloadClient *http.Client
}

const defaultDownloadTimeout = 60 * time.Second

func New(token string, menu []channel.MenuCommand) *Adapter {
	return &Adapter{token: token, menu: menu, downloadClient: &http.Client{Timeout: defaultDownloadTimeout}}
}

func (a *Adapter) Name() string { return "telegram" }

func (a *Adapter) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	bot, err := tgbotapi.NewBotAPI(a.token)
	if err != nil {
		return fmt.Errorf("telegram: init bot: %w", err)
	}
	a.bot = bot

	a.registerCommands()

	slog.Info("telegram adapter started", "bot_username", bot.Self.UserName)

	// Long-poll loop over the raw update JSON: the Telegram SDK predates
	// forum topics and drops message_thread_id when parsing updates, so the
	// raw payload is read in parallel to capture the topic id.
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		resp, err := bot.MakeRequest("getUpdates", tgbotapi.Params{
			"timeout": "60",
			"offset":  strconv.Itoa(offset),
		})
		if err != nil {
			slog.Warn("telegram: getUpdates failed, retrying", "error", err)
			time.Sleep(3 * time.Second)
			continue
		}

		var updates []tgbotapi.Update
		if err := json.Unmarshal(resp.Result, &updates); err != nil {
			slog.Warn("telegram: decode updates failed", "error", err)
			continue
		}
		var raw []rawUpdate
		if err := json.Unmarshal(resp.Result, &raw); err != nil {
			slog.Warn("telegram: decode raw updates failed", "error", err)
			continue
		}

		for i, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			threadID := int64(0)
			if i < len(raw) {
				threadID = raw[i].threadID()
			}
			if u.CallbackQuery != nil {
				handler(a.normalizeCallback(u, threadID))
				continue
			}
			if u.Message == nil {
				continue
			}
			handler(a.normalize(u, threadID))
		}
	}
}

// rawUpdate captures the update fields the SDK's Update type drops, most
// importantly message_thread_id (forum topics / private-chat topics).
type rawUpdate struct {
	Message *struct {
		MessageThreadID int64 `json:"message_thread_id"`
	} `json:"message"`
	CallbackQuery *struct {
		Message *struct {
			MessageThreadID int64 `json:"message_thread_id"`
		} `json:"message"`
	} `json:"callback_query"`
}

func (r rawUpdate) threadID() int64 {
	if r.Message != nil {
		return r.Message.MessageThreadID
	}
	if r.CallbackQuery != nil && r.CallbackQuery.Message != nil {
		return r.CallbackQuery.Message.MessageThreadID
	}
	return 0
}

// registerCommands populates Telegram's native "/" command menu. A failure
// here is logged and never blocks the adapter — the bot remains fully usable
// via typed commands regardless.
func (a *Adapter) registerCommands() {
	if len(a.menu) == 0 {
		return
	}
	commands := make([]tgbotapi.BotCommand, len(a.menu))
	for i, m := range a.menu {
		commands[i] = tgbotapi.BotCommand{Command: m.Alias, Description: sanitizeDescription(m.Description)}
	}
	if _, err := a.bot.Request(tgbotapi.NewSetMyCommands(commands...)); err != nil {
		slog.Warn("telegram: set commands failed", "error", err)
	}
}

// sanitizeCommandName maps an arbitrary command name (e.g. an agent's own
// command, which may contain characters Telegram rejects) into Telegram's
// allowed set: lowercase letters, digits, and underscores, 1-32 characters.
func sanitizeCommandName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	if len(s) > 32 {
		s = s[:32]
	}
	return s
}

// maxCommandDescriptionLen is Telegram's BotCommand.Description limit. A
// batch setMyCommands call fails atomically if any single description
// exceeds it, so every description must be capped before sending.
const maxCommandDescriptionLen = 256

func sanitizeDescription(desc string) string {
	r := []rune(desc)
	if len(r) <= maxCommandDescriptionLen {
		return desc
	}
	return string(r[:maxCommandDescriptionLen])
}

func (a *Adapter) Stop() error {
	if a.bot != nil {
		a.bot.StopReceivingUpdates()
	}
	return nil
}

func (a *Adapter) Notify(channelID string, text string) error {
	var chatID int64
	fmt.Sscanf(channelID, "%d", &chatID)
	for _, chunk := range render.Split(text, 4096) {
		msg := tgbotapi.NewMessage(chatID, chunk)
		msg.ParseMode = "HTML"
		msg.DisableWebPagePreview = true
		if _, err := a.bot.Send(msg); err != nil {
			return fmt.Errorf("telegram: notify: %w", err)
		}
	}
	return nil
}

func (a *Adapter) normalize(update tgbotapi.Update, threadID int64) channel.IncomingMessage {
	msg := update.Message
	chatID := fmt.Sprintf("%d", msg.Chat.ID)
	userID := fmt.Sprintf("%d", msg.From.ID)

	isMention := false
	if msg.Chat.IsGroup() || msg.Chat.IsSuperGroup() {
		for _, entity := range msg.Entities {
			if entity.Type == "mention" {
				mentioned := msg.Text[entity.Offset : entity.Offset+entity.Length]
				if mentioned == "@"+a.bot.Self.UserName {
					isMention = true
					break
				}
			}
		}
	} else {
		isMention = true
	}

	text := msg.Text
	if text == "" && msg.Caption != "" {
		text = msg.Caption
	}

	threadIDStr := ""
	if threadID != 0 {
		threadIDStr = fmt.Sprintf("%d", threadID)
	}

	return channel.IncomingMessage{
		Platform:    "telegram",
		ChannelID:   chatID,
		ThreadID:    threadIDStr,
		UserID:      userID,
		Text:        text,
		IsMention:   isMention,
		IsThread:    threadID != 0,
		Attachments: a.downloadAttachments(msg),
		ReplyCtx:    &replyContext{bot: a.bot, chatID: msg.Chat.ID, threadID: threadID},
	}
}

func (a *Adapter) normalizeCallback(update tgbotapi.Update, threadID int64) channel.IncomingMessage {
	cb := update.CallbackQuery
	chatID := ""
	var origin channel.MessageRef
	var numericChatID int64
	if cb.Message != nil {
		chatID = fmt.Sprintf("%d", cb.Message.Chat.ID)
		numericChatID = cb.Message.Chat.ID
		origin = messageRef{id: fmt.Sprintf("%d", cb.Message.MessageID)}
	}
	userID := fmt.Sprintf("%d", cb.From.ID)

	callback := tgbotapi.NewCallback(cb.ID, "")
	a.bot.Request(callback)

	threadIDStr := ""
	if threadID != 0 {
		threadIDStr = fmt.Sprintf("%d", threadID)
	}

	return channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    chatID,
		ThreadID:     threadIDStr,
		UserID:       userID,
		IsCallback:   true,
		CallbackData: cb.Data,
		CallbackRef:  origin,
		IsMention:    true,
		IsThread:     threadID != 0,
		ReplyCtx:     &replyContext{bot: a.bot, chatID: numericChatID, threadID: threadID},
	}
}

func (a *Adapter) downloadAttachments(msg *tgbotapi.Message) []channel.Attachment {
	var attachments []channel.Attachment

	if len(msg.Photo) > 0 {
		photo := msg.Photo[len(msg.Photo)-1]
		if att := a.downloadFile(photo.FileID, "photo.jpg", "image/jpeg"); att != nil {
			attachments = append(attachments, *att)
		}
	}

	if msg.Voice != nil {
		if att := a.downloadFile(msg.Voice.FileID, "voice.ogg", "audio/ogg"); att != nil {
			attachments = append(attachments, *att)
		}
	}

	if msg.Document != nil {
		mime := msg.Document.MimeType
		if mime == "" {
			mime = "application/octet-stream"
		}
		if att := a.downloadFile(msg.Document.FileID, msg.Document.FileName, mime); att != nil {
			attachments = append(attachments, *att)
		}
	}

	return attachments
}

const maxDownloadSize = 10 * 1024 * 1024

func (a *Adapter) downloadFile(fileID, filename, mimeType string) *channel.Attachment {
	file, err := a.bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		slog.Warn("telegram: get file failed", "file_id", fileID, "error", err)
		return nil
	}

	url, err := a.bot.GetFileDirectURL(file.FileID)
	if err != nil {
		slog.Warn("telegram: get file url failed", "file_id", fileID, "error", err)
		return nil
	}
	data, err := a.fetchFile(url)
	if err != nil {
		slog.Warn("telegram: download file failed", "file_id", fileID, "error", err)
		return nil
	}

	if filename == "" {
		filename = fileID
	}
	return &channel.Attachment{Filename: filename, MimeType: mimeType, Data: data}
}

// fetchFile downloads a file URL with a bounded client: a stalled CDN must
// fail the attachment instead of blocking the update loop.
func (a *Adapter) fetchFile(url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(io.LimitReader(resp.Body, maxDownloadSize+1))
}

type replyContext struct {
	bot      *tgbotapi.BotAPI
	chatID   int64
	threadID int64
}

// SetChatCommands registers a native command menu scoped to this one chat,
// overriding the global menu for it. Telegram's per-chat scope replaces
// (does not merge with) the default scope, so callers must pass the full
// desired menu, not just the delta.
func (rc *replyContext) SetChatCommands(commands []channel.MenuCommand) error {
	botCommands := make([]tgbotapi.BotCommand, len(commands))
	for i, m := range commands {
		botCommands[i] = tgbotapi.BotCommand{Command: sanitizeCommandName(m.Alias), Description: sanitizeDescription(m.Description)}
	}
	scope := tgbotapi.NewBotCommandScopeChat(rc.chatID)
	_, err := rc.bot.Request(tgbotapi.NewSetMyCommandsWithScope(scope, botCommands...))
	return err
}

func (rc *replyContext) SendTyping() error {
	params := tgbotapi.Params{
		"chat_id": strconv.FormatInt(rc.chatID, 10),
		"action":  "typing",
	}
	if rc.threadID != 0 {
		params["message_thread_id"] = strconv.FormatInt(rc.threadID, 10)
	}
	_, err := rc.bot.MakeRequest("sendChatAction", params)
	return err
}

// baseParams returns the common chat params for outbound messages.
func (rc *replyContext) baseParams() tgbotapi.Params {
	params := tgbotapi.Params{
		"chat_id":                  strconv.FormatInt(rc.chatID, 10),
		"parse_mode":               "HTML",
		"disable_web_page_preview": "true",
	}
	if rc.threadID != 0 {
		params["message_thread_id"] = strconv.FormatInt(rc.threadID, 10)
	}
	return params
}

func (rc *replyContext) Send(text string) (channel.MessageRef, error) {
	chunks := render.Split(text, 4096)
	var lastRef channel.MessageRef

	for _, chunk := range chunks {
		params := rc.baseParams()
		params["text"] = chunk
		sent, err := rc.request("sendMessage", params)
		if err != nil {
			return nil, fmt.Errorf("telegram: send: %w", err)
		}
		lastRef = messageRef{id: fmt.Sprintf("%d", sent.MessageID)}
	}
	return lastRef, nil
}

func (rc *replyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	params := rc.baseParams()
	params["text"] = text
	params["reply_markup"] = inlineKeyboardJSON(buttons)

	sent, err := rc.request("sendMessage", params)
	if err != nil {
		return nil, fmt.Errorf("telegram: send with buttons: %w", err)
	}
	return messageRef{id: fmt.Sprintf("%d", sent.MessageID)}, nil
}

func (rc *replyContext) Edit(ref channel.MessageRef, text string) error {
	msgID := 0
	fmt.Sscanf(ref.ID(), "%d", &msgID)

	params := tgbotapi.Params{
		"chat_id":                  strconv.FormatInt(rc.chatID, 10),
		"message_id":               strconv.Itoa(msgID),
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": "true",
	}
	return rc.requestSilent("editMessageText", params)
}

func (rc *replyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	msgID := 0
	refID := ref.ID()
	if _, err := fmt.Sscanf(refID, "%d", &msgID); err != nil {
		return fmt.Errorf("telegram: parse message id %q: %w", refID, err)
	}

	params := tgbotapi.Params{
		"chat_id":                  strconv.FormatInt(rc.chatID, 10),
		"message_id":               strconv.Itoa(msgID),
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": "true",
		"reply_markup":             inlineKeyboardJSON(buttons),
	}
	return rc.requestSilent("editMessageText", params)
}

// request calls a bot API method with retry-on-429 backoff and returns the
// parsed message result.
func (rc *replyContext) request(method string, params tgbotapi.Params) (tgbotapi.Message, error) {
	for {
		resp, err := rc.bot.MakeRequest(method, params)
		if err == nil {
			var msg tgbotapi.Message
			if err := json.Unmarshal(resp.Result, &msg); err != nil {
				return msg, fmt.Errorf("telegram: decode %s result: %w", method, err)
			}
			return msg, nil
		}
		retryAfter := extractRetryAfter(err)
		if retryAfter <= 0 {
			return tgbotapi.Message{}, err
		}
		slog.Warn("telegram: rate limited, retrying", "method", method, "retry_after", retryAfter)
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}
}

// requestSilent is request for methods whose result carries no message (e.g.
// edits, where a failed edit is a normal condition, not a stream failure).
func (rc *replyContext) requestSilent(method string, params tgbotapi.Params) error {
	for {
		_, err := rc.bot.MakeRequest(method, params)
		if err == nil {
			return nil
		}
		retryAfter := extractRetryAfter(err)
		if retryAfter <= 0 {
			return err
		}
		slog.Warn("telegram: rate limited, retrying", "method", method, "retry_after", retryAfter)
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}
}

func inlineKeyboard(buttons []channel.Button) tgbotapi.InlineKeyboardMarkup {
	rows := make([][]tgbotapi.InlineKeyboardButton, 0, len(buttons))
	for _, b := range buttons {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(b.Label, b.Value),
		))
	}
	return tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func inlineKeyboardJSON(buttons []channel.Button) string {
	data, err := json.Marshal(inlineKeyboard(buttons))
	if err != nil {
		return ""
	}
	return string(data)
}

func extractRetryAfter(err error) int {
	if apiErr, ok := err.(*tgbotapi.Error); ok {
		return int(apiErr.ResponseParameters.RetryAfter)
	}
	return 0
}

type messageRef struct {
	id string
}

func (m messageRef) ID() string { return m.id }

var (
	_ channel.Channel           = (*Adapter)(nil)
	_ channel.ReplyContext      = (*replyContext)(nil)
	_ channel.ChatCommandSetter = (*replyContext)(nil)
	_ channel.MessageRef        = messageRef{}
)
