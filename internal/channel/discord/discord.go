package discord

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

const maxDownloadSize = 10 * 1024 * 1024

type Adapter struct {
	session *discordgo.Session
	token   string
	botID   string
}

func New(token string) *Adapter {
	return &Adapter{token: token}
}

func (a *Adapter) Name() string { return "discord" }

func (a *Adapter) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	s, err := discordgo.New("Bot " + a.token)
	if err != nil {
		return fmt.Errorf("discord: init: %w", err)
	}
	a.session = s
	a.botID = s.State.User.ID

	s.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsMessageContent | discordgo.IntentsDirectMessages

	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Author.ID == a.botID || m.Author.Bot {
			return
		}
		msg := a.normalizeMessage(m.Message)
		handler(msg)
	})

	s.AddHandler(func(sess *discordgo.Session, i *discordgo.InteractionCreate) {
		if i.Type != discordgo.InteractionApplicationCommand {
			return
		}
		data := i.ApplicationCommandData()
		text := "/" + data.Name
		for _, opt := range data.Options {
			text += " " + fmt.Sprintf("%v", opt.Value)
		}

		var userID string
		if i.Member != nil {
			userID = i.Member.User.ID
		} else if i.User != nil {
			userID = i.User.ID
		}

		if err := sess.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		}); err != nil {
			slog.Warn("discord: defer interaction failed", "error", err)
		}

		msg := channel.IncomingMessage{
			Platform:  "discord",
			ChannelID: i.ChannelID,
			UserID:    userID,
			Text:      strings.TrimSpace(text),
			IsMention: true,
			ReplyCtx:  &replyContext{session: sess, channelID: i.ChannelID, interaction: i.Interaction},
		}
		handler(msg)
	})

	if err := s.Open(); err != nil {
		return fmt.Errorf("discord: open gateway: %w", err)
	}

	slog.Info("discord adapter started")

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return nil
}

func (a *Adapter) Stop() error {
	if a.session != nil {
		return a.session.Close()
	}
	return nil
}

func (a *Adapter) normalizeMessage(m *discordgo.Message) channel.IncomingMessage {
	isMention := false
	if m.GuildID == "" {
		isMention = true
	} else {
		for _, mention := range m.Mentions {
			if mention.ID == a.botID {
				isMention = true
				break
			}
		}
	}

	return channel.IncomingMessage{
		Platform:    "discord",
		ChannelID:   m.ChannelID,
		UserID:      m.Author.ID,
		Text:        m.Content,
		IsMention:   isMention,
		IsThread:    a.isThreadChannel(m.ChannelID),
		Attachments: a.downloadAttachments(m),
		ReplyCtx:    &replyContext{session: a.session, channelID: m.ChannelID},
	}
}

func (a *Adapter) downloadAttachments(m *discordgo.Message) []channel.Attachment {
	var attachments []channel.Attachment
	for _, da := range m.Attachments {
		if da.Size > maxDownloadSize {
			continue
		}
		resp, err := http.Get(da.URL)
		if err != nil {
			slog.Warn("discord: download attachment failed", "filename", da.Filename, "error", err)
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxDownloadSize+1))
		resp.Body.Close()
		if err != nil || len(data) > maxDownloadSize {
			slog.Warn("discord: attachment too large or read failed", "filename", da.Filename)
			continue
		}
		mime := da.ContentType
		if mime == "" {
			mime = "application/octet-stream"
		}
		attachments = append(attachments, channel.Attachment{
			Filename: da.Filename,
			MimeType: mime,
			Data:     data,
		})
	}
	return attachments
}

func (a *Adapter) isThreadChannel(channelID string) bool {
	ch, err := a.session.State.Channel(channelID)
	if err != nil {
		ch, err = a.session.Channel(channelID)
		if err != nil {
			return false
		}
	}
	switch ch.Type {
	case discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread,
		discordgo.ChannelTypeGuildNewsThread:
		return true
	}
	return false
}

type replyContext struct {
	session     *discordgo.Session
	channelID   string
	interaction *discordgo.Interaction
}

func (rc *replyContext) SendTyping() error {
	return rc.session.ChannelTyping(rc.channelID)
}

func (rc *replyContext) Send(text string) (channel.MessageRef, error) {
	if rc.interaction != nil {
		content := text
		msg, err := rc.session.InteractionResponseEdit(rc.interaction, &discordgo.WebhookEdit{
			Content: &content,
		})
		if err != nil {
			return nil, fmt.Errorf("discord: interaction response edit: %w", err)
		}
		rc.interaction = nil
		return messageRef{id: msg.ID}, nil
	}

	chunks := splitMessage(text, 2000)
	var lastRef channel.MessageRef

	for _, chunk := range chunks {
		msg, err := rc.session.ChannelMessageSend(rc.channelID, chunk)
		if err != nil {
			return nil, fmt.Errorf("discord: send: %w", err)
		}
		lastRef = messageRef{id: msg.ID}
	}
	return lastRef, nil
}

func (rc *replyContext) Edit(ref channel.MessageRef, text string) error {
	_, err := rc.session.ChannelMessageEdit(rc.channelID, ref.ID(), text)
	return err
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
