package telegram

import (
	"context"
	"fmt"
	"log/slog"
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

	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: chatID,
		UserID:    userID,
		Text:      msg.Text,
		IsMention: isMention,
		ReplyCtx:  &replyContext{bot: a.bot, chatID: msg.Chat.ID},
	}
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

	_, err := rc.bot.Send(msg)
	return err
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
