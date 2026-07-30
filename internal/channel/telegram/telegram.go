package telegram

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/anggasct/occa/internal/channel"
)

type Adapter struct {
	bot  *tgbotapi.BotAPI
	token string
}

func New(token string) *Adapter {
	return &Adapter{token: token}
}

func (a *Adapter) Name() string { return "telegram" }

func (a *Adapter) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	bot, err := tgbotapi.NewBotAPI(a.token)
	if err != nil {
		return fmt.Errorf("telegram: init bot: %w", err)
	}
	a.bot = bot

	slog.Info("telegram adapter started", "bot_username", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	go func() {
		<-ctx.Done()
		bot.StopReceivingUpdates()
	}()

	for update := range updates {
		if update.Message == nil {
			continue
		}
		msg := a.normalize(update)
		handler(msg)
	}
	return nil
}

func (a *Adapter) Stop() error {
	if a.bot != nil {
		a.bot.StopReceivingUpdates()
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

func (a *Adapter) downloadAttachments(msg *tgbotapi.Message) []channel.Attachment {
	var attachments []channel.Attachment

	if len(msg.Photo) > 0 {
		photo := msg.Photo[len(msg.Photo)-1]
		if photo.FileSize > 0 && photo.FileSize > maxDownloadSize {
			return nil
		}
		if att := a.downloadFile(photo.FileID, "photo.jpg", "image/jpeg"); att != nil {
			attachments = append(attachments, *att)
		}
	}

	if msg.Document != nil {
		if msg.Document.FileSize > 0 && msg.Document.FileSize > maxDownloadSize {
			return attachments
		}
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
	resp, err := http.Get(url)
	if err != nil {
		slog.Warn("telegram: download file failed", "file_id", fileID, "error", err)
		return nil
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDownloadSize+1))
	if err != nil {
		slog.Warn("telegram: read file failed", "file_id", fileID, "error", err)
		return nil
	}
	if len(data) > maxDownloadSize {
		slog.Warn("telegram: file too large, skipping", "file_id", fileID, "size", len(data))
		return nil
	}

	if filename == "" {
		filename = fileID
	}
	return &channel.Attachment{Filename: filename, MimeType: mimeType, Data: data}
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
	chunks := splitMessage(text, 4096)
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

func splitMessage(text string, maxLen int) []string {
	if len(text) <= maxLen {
		return []string{text}
	}

	var chunks []string
	remaining := text
	for len(remaining) > 0 {
		if len(remaining) <= maxLen {
			chunks = append(chunks, remaining)
			break
		}

		breakAt := findBreakPoint(remaining, maxLen)
		chunks = append(chunks, remaining[:breakAt])
		remaining = strings.TrimLeft(remaining[breakAt:], "\n")
	}
	return chunks
}

func findBreakPoint(text string, maxLen int) int {
	if idx := strings.LastIndex(text[:maxLen], "\n\n"); idx > 0 {
		return idx
	}
	if idx := strings.LastIndex(text[:maxLen], "\n"); idx > 0 {
		return idx
	}
	if idx := strings.LastIndex(text[:maxLen], " "); idx > 0 {
		return idx
	}
	return maxLen
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
