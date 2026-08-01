package discord

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const maxDownloadSize = 10 * 1024 * 1024

type Adapter struct {
	session        *discordgo.Session
	token          string
	botID          atomic.Value
	channelLookup  func(string) (*discordgo.Channel, error)
	downloadClient *http.Client
}

const defaultDownloadTimeout = 60 * time.Second

func New(token string) *Adapter {
	return &Adapter{token: token, downloadClient: &http.Client{Timeout: defaultDownloadTimeout}}
}

func (a *Adapter) Name() string { return "discord" }

func (a *Adapter) setBotID(id string) { a.botID.Store(id) }

// selfID is empty until the gateway delivers READY. Callers must treat an
// empty result as "not this bot" rather than as a match.
func (a *Adapter) selfID() string {
	id, _ := a.botID.Load().(string)
	return id
}

func (a *Adapter) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	s, err := discordgo.New("Bot " + a.token)
	if err != nil {
		return fmt.Errorf("discord: init: %w", err)
	}
	a.session = s
	a.configure(s, handler)

	if err := s.Open(); err != nil {
		return fmt.Errorf("discord: open gateway: %w", err)
	}

	slog.Info("discord adapter started")

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()

	return nil
}

// configure registers intents and gateway handlers. It must not read session
// state the gateway populates — State.User stays nil until READY arrives, so
// reading it here would panic before the connection is even open.
func (a *Adapter) configure(s *discordgo.Session, handler func(channel.IncomingMessage)) {
	s.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsMessageContent | discordgo.IntentsDirectMessages

	s.AddHandler(func(_ *discordgo.Session, r *discordgo.Ready) {
		a.onReady(r)
	})

	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		a.onMessage(m, handler)
	})

	s.AddHandler(func(sess *discordgo.Session, i *discordgo.InteractionCreate) {
		switch i.Type {
		case discordgo.InteractionMessageComponent:
			data := i.MessageComponentData()
			var origin channel.MessageRef
			if i.Message != nil {
				origin = messageRef{id: i.Message.ID}
			}
			var userID string
			if i.Member != nil {
				userID = i.Member.User.ID
			} else if i.User != nil {
				userID = i.User.ID
			}

			if err := sess.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseUpdateMessage,
			}); err != nil {
				slog.Warn("discord: defer component interaction failed", "error", err)
			}

			parentChannelID, isThread, scopeUnresolved := a.channelScope(i.Interaction.GuildID, i.ChannelID)
			msg := channel.IncomingMessage{
				Platform:               "discord",
				ChannelID:              i.ChannelID,
				ParentChannelID:        parentChannelID,
				ChannelScopeUnresolved: scopeUnresolved,
				UserID:                 userID,
				IsThread:               isThread,
				IsCallback:             true,
				CallbackData:           data.CustomID,
				CallbackRef:            origin,
				IsMention:              true,
				ReplyCtx:               &replyContext{session: sess, channelID: i.ChannelID, interaction: i.Interaction},
			}
			handler(msg)

		case discordgo.InteractionApplicationCommand:
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

			parentChannelID, isThread, scopeUnresolved := a.channelScope(i.Interaction.GuildID, i.ChannelID)
			msg := channel.IncomingMessage{
				Platform:               "discord",
				ChannelID:              i.ChannelID,
				ParentChannelID:        parentChannelID,
				ChannelScopeUnresolved: scopeUnresolved,
				UserID:                 userID,
				Text:                   strings.TrimSpace(text),
				IsMention:              true,
				IsThread:               isThread,
				ReplyCtx:               &replyContext{session: sess, channelID: i.ChannelID, interaction: i.Interaction},
			}
			handler(msg)
		}
	})
}

func (a *Adapter) onReady(r *discordgo.Ready) {
	if r == nil || r.User == nil {
		slog.Warn("discord: ready event carried no bot identity")
		return
	}
	a.setBotID(r.User.ID)
	slog.Info("discord adapter connected", "bot_id", r.User.ID)
}

func (a *Adapter) onMessage(m *discordgo.MessageCreate, handler func(channel.IncomingMessage)) {
	if m.Author.Bot {
		return
	}
	if self := a.selfID(); self != "" && m.Author.ID == self {
		return
	}
	handler(a.normalizeMessage(m.Message))
}

func (a *Adapter) Stop() error {
	if a.session != nil {
		return a.session.Close()
	}
	return nil
}

func (a *Adapter) Notify(channelID string, text string) error {
	for _, chunk := range render.Split(text, 2000) {
		if _, err := a.session.ChannelMessageSend(channelID, chunk); err != nil {
			return fmt.Errorf("discord: notify: %w", err)
		}
	}
	return nil
}

func (a *Adapter) normalizeMessage(m *discordgo.Message) channel.IncomingMessage {
	isMention := false
	if m.GuildID == "" {
		isMention = true
	} else {
		for _, mention := range m.Mentions {
			if self := a.selfID(); self != "" && mention.ID == self {
				isMention = true
				break
			}
		}
	}

	parentChannelID, isThread, scopeUnresolved := a.channelScope(m.GuildID, m.ChannelID)
	return channel.IncomingMessage{
		Platform:               "discord",
		ChannelID:              m.ChannelID,
		ParentChannelID:        parentChannelID,
		ChannelScopeUnresolved: scopeUnresolved,
		UserID:                 m.Author.ID,
		Text:                   m.Content,
		IsMention:              isMention,
		IsThread:               isThread,
		Attachments:            a.downloadAttachments(m),
		ReplyCtx:               &replyContext{session: a.session, channelID: m.ChannelID},
	}
}

func (a *Adapter) downloadAttachments(m *discordgo.Message) []channel.Attachment {
	var attachments []channel.Attachment
	for _, da := range m.Attachments {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, da.URL, nil)
		if err != nil {
			slog.Warn("discord: download attachment failed", "filename", da.Filename, "error", err)
			continue
		}
		resp, err := a.downloadClient.Do(req)
		if err != nil {
			slog.Warn("discord: download attachment failed", "filename", da.Filename, "error", err)
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxDownloadSize+1))
		resp.Body.Close()
		if err != nil {
			slog.Warn("discord: read attachment failed", "filename", da.Filename, "error", err)
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

func (a *Adapter) channelScope(guildID, channelID string) (string, bool, bool) {
	if guildID == "" {
		return "", false, false
	}
	ch, err := a.lookupChannel(channelID)
	if err != nil {
		slog.Warn("discord: channel scope lookup failed", "channel_id", channelID, "error", err)
		return "", false, true
	}
	if !isThreadType(ch.Type) {
		return "", false, false
	}
	if ch.ParentID == "" {
		slog.Warn("discord: thread parent missing", "channel_id", channelID)
		return "", true, true
	}
	return ch.ParentID, true, false
}

func (a *Adapter) lookupChannel(channelID string) (*discordgo.Channel, error) {
	if a.channelLookup != nil {
		return a.channelLookup(channelID)
	}
	ch, err := a.session.State.Channel(channelID)
	if err != nil {
		ch, err = a.session.Channel(channelID)
		if err != nil {
			return nil, fmt.Errorf("discord: lookup channel %s: %w", channelID, err)
		}
	}
	return ch, nil
}

func isThreadType(channelType discordgo.ChannelType) bool {
	switch channelType {
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

	chunks := render.Split(text, 2000)
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

func (rc *replyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	msg, err := rc.session.ChannelMessageSendComplex(rc.channelID, &discordgo.MessageSend{
		Content:    text,
		Components: componentRows(buttons),
	})
	if err != nil {
		return nil, fmt.Errorf("discord: send with buttons: %w", err)
	}
	return messageRef{id: msg.ID}, nil
}

func (rc *replyContext) Edit(ref channel.MessageRef, text string) error {
	_, err := rc.session.ChannelMessageEdit(rc.channelID, ref.ID(), text)
	return err
}

func (rc *replyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	content := text
	components := componentRows(buttons)
	_, err := rc.session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		ID:         ref.ID(),
		Channel:    rc.channelID,
		Content:    &content,
		Components: &components,
	})
	return err
}

func componentRows(buttons []channel.Button) []discordgo.MessageComponent {
	components := make([]discordgo.MessageComponent, 0, 1)
	if len(buttons) == 0 {
		return components
	}

	btns := make([]discordgo.MessageComponent, 0, len(buttons))
	for _, b := range buttons {
		btns = append(btns, discordgo.Button{
			Label:    b.Label,
			Style:    discordgo.SecondaryButton,
			CustomID: b.Value,
		})
	}
	return append(components, discordgo.ActionsRow{Components: btns})
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
