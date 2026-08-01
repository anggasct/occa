package telegram

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	go func() {
		<-ctx.Done()
		bot.StopReceivingUpdates()
	}()

	for update := range updates {
		if update.CallbackQuery != nil {
			msg := a.normalizeCallback(update)
			handler(msg)
			continue
		}
		if update.Message == nil {
			continue
		}
		msg := a.normalize(update)
		handler(msg)
	}
	return nil
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
		commands[i] = tgbotapi.BotCommand{Command: m.Alias, Description: m.Description}
	}
	if _, err := a.bot.Request(tgbotapi.NewSetMyCommands(commands...)); err != nil {
		slog.Warn("telegram: set commands failed", "error", err)
	}
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

func (a *Adapter) normalize(update tgbotapi.Update) channel.IncomingMessage {
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

	return channel.IncomingMessage{
		Platform:    "telegram",
		ChannelID:   chatID,
		UserID:      userID,
		Text:        text,
		IsMention:   isMention,
		Attachments: a.downloadAttachments(msg),
		ReplyCtx:    &replyContext{bot: a.bot, chatID: msg.Chat.ID},
	}
}

func (a *Adapter) normalizeCallback(update tgbotapi.Update) channel.IncomingMessage {
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

	return channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    chatID,
		UserID:       userID,
		IsCallback:   true,
		CallbackData: cb.Data,
		CallbackRef:  origin,
		IsMention:    true,
		ReplyCtx:     &replyContext{bot: a.bot, chatID: numericChatID},
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
	bot    *tgbotapi.BotAPI
	chatID int64
}

func (rc *replyContext) SendTyping() error {
	msg := tgbotapi.NewChatAction(rc.chatID, tgbotapi.ChatTyping)
	_, err := rc.bot.Request(msg)
	return err
}

func (rc *replyContext) Send(text string) (channel.MessageRef, error) {
	chunks := render.Split(text, 4096)
	var lastRef channel.MessageRef

	for _, chunk := range chunks {
		msg := tgbotapi.NewMessage(rc.chatID, chunk)
		msg.ParseMode = "HTML"
		msg.DisableWebPagePreview = true

		sent, err := rc.sendWithRetry(msg)
		if err != nil {
			return nil, fmt.Errorf("telegram: send: %w", err)
		}
		lastRef = messageRef{id: fmt.Sprintf("%d", sent.MessageID)}
	}
	return lastRef, nil
}

func (rc *replyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	msg := tgbotapi.NewMessage(rc.chatID, text)
	msg.ParseMode = "HTML"
	msg.ReplyMarkup = inlineKeyboard(buttons)

	sent, err := rc.sendWithRetry(msg)
	if err != nil {
		return nil, fmt.Errorf("telegram: send with buttons: %w", err)
	}
	return messageRef{id: fmt.Sprintf("%d", sent.MessageID)}, nil
}

func (rc *replyContext) Edit(ref channel.MessageRef, text string) error {
	msgID := 0
	fmt.Sscanf(ref.ID(), "%d", &msgID)

	msg := tgbotapi.NewEditMessageText(rc.chatID, msgID, text)
	msg.ParseMode = "HTML"
	msg.DisableWebPagePreview = true

	for {
		_, err := rc.bot.Send(msg)
		if err == nil {
			return nil
		}
		retryAfter := extractRetryAfter(err)
		if retryAfter <= 0 {
			return err
		}
		slog.Warn("telegram: edit rate limited, retrying", "retry_after", retryAfter)
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}
}

func (rc *replyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	msgID := 0
	refID := ref.ID()
	if _, err := fmt.Sscanf(refID, "%d", &msgID); err != nil {
		return fmt.Errorf("telegram: parse message id %q: %w", refID, err)
	}

	msg := tgbotapi.NewEditMessageTextAndMarkup(rc.chatID, msgID, text, inlineKeyboard(buttons))
	msg.ParseMode = "HTML"
	msg.DisableWebPagePreview = true

	for {
		_, err := rc.bot.Send(msg)
		if err == nil {
			return nil
		}
		retryAfter := extractRetryAfter(err)
		if retryAfter <= 0 {
			return err
		}
		slog.Warn("telegram: edit rate limited, retrying", "retry_after", retryAfter)
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

func (rc *replyContext) sendWithRetry(msg tgbotapi.MessageConfig) (tgbotapi.Message, error) {
	for {
		sent, err := rc.bot.Send(msg)
		if err == nil {
			return sent, nil
		}
		retryAfter := extractRetryAfter(err)
		if retryAfter <= 0 {
			return sent, err
		}
		slog.Warn("telegram: rate limited, retrying", "retry_after", retryAfter)
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}
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
	_ channel.Channel      = (*Adapter)(nil)
	_ channel.ReplyContext = (*replyContext)(nil)
	_ channel.MessageRef   = messageRef{}
)
