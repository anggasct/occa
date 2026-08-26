package discord

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const (
	maxDownloadSize        = 10 * 1024 * 1024
	maxButtonLabelRunes    = 80
	maxDiscordActionRows   = 5
	maxButtonsPerActionRow = 5
)

type ThreadPolicy func(channelID string) (bool, error)

type OwnedThreadCheck func(threadID string) (bool, error)

type TrustedBotSender struct {
	UserID     string
	ChannelIDs []string
}

type TrustedBotPolicy struct {
	TriggerRoleIDs    []string
	TrustedBotSenders []TrustedBotSender
}

type Adapter struct {
	session        *discordgo.Session
	token          string
	menu           []channel.MenuCommand
	triggerRoleIDs map[string]struct{}
	trustedBots    map[string]map[string]struct{}
	botID          atomic.Value
	appID          atomic.Value
	channelLookup  func(string) (*discordgo.Channel, error)
	downloadClient *http.Client
	autoThread     ThreadPolicy
	ownedThread    OwnedThreadCheck
	connected      atomic.Bool

	threadsMu sync.Mutex
	threads   map[string]struct{}
}

const defaultDownloadTimeout = 60 * time.Second

func New(token string, menu []channel.MenuCommand) *Adapter {
	return NewWithPolicy(token, menu, TrustedBotPolicy{})
}

func NewWithPolicy(token string, menu []channel.MenuCommand, policy TrustedBotPolicy) *Adapter {
	a := &Adapter{
		token:          token,
		menu:           menu,
		downloadClient: &http.Client{Timeout: defaultDownloadTimeout},
		triggerRoleIDs: make(map[string]struct{}, len(policy.TriggerRoleIDs)),
		trustedBots:    make(map[string]map[string]struct{}, len(policy.TrustedBotSenders)),
	}
	for _, roleID := range policy.TriggerRoleIDs {
		a.triggerRoleIDs[strings.TrimSpace(roleID)] = struct{}{}
	}
	for _, sender := range policy.TrustedBotSenders {
		channels := make(map[string]struct{}, len(sender.ChannelIDs))
		for _, channelID := range sender.ChannelIDs {
			channels[strings.TrimSpace(channelID)] = struct{}{}
		}
		a.trustedBots[strings.TrimSpace(sender.UserID)] = channels
	}
	return a
}

func (a *Adapter) Name() string { return "discord" }

func (a *Adapter) Connected() (bool, string) {
	if a.connected.Load() {
		return true, ""
	}
	return false, "gateway not connected"
}

func (a *Adapter) SetAutoThreadPolicy(policy ThreadPolicy) {
	a.autoThread = policy
}

func (a *Adapter) SetOwnedThreadCheck(check OwnedThreadCheck) {
	a.ownedThread = check
}

func (a *Adapter) trackThread(threadID string) {
	a.threadsMu.Lock()
	defer a.threadsMu.Unlock()
	if a.threads == nil {
		a.threads = make(map[string]struct{})
	}
	a.threads[threadID] = struct{}{}
}

func (a *Adapter) isTrackedThread(threadID string) bool {
	a.threadsMu.Lock()
	defer a.threadsMu.Unlock()
	_, ok := a.threads[threadID]
	return ok
}

func (a *Adapter) isOwnedThread(threadID string) bool {
	if a.isTrackedThread(threadID) {
		return true
	}
	if a.ownedThread == nil {
		return false
	}
	owned, err := a.ownedThread(threadID)
	if err != nil {
		slog.Warn("discord: owned-thread lookup failed", "thread_id", threadID, "error", err)
		return false
	}
	return owned
}

func (a *Adapter) setBotID(id string) { a.botID.Store(id) }

func (a *Adapter) setAppID(id string) { a.appID.Store(id) }

func (a *Adapter) appIDValue() string {
	id, _ := a.appID.Load().(string)
	return id
}

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
	a.connected.Store(true)

	slog.Info("discord adapter started")

	go func() {
		<-ctx.Done()
		a.connected.Store(false)
		_ = s.Close()
	}()

	return nil
}

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
			msgChannelID := i.ChannelID
			threadID := ""
			if isThread {
				threadID = i.ChannelID
			}
			if isThread && a.isOwnedThread(i.ChannelID) && parentChannelID != "" {
				msgChannelID = parentChannelID
			}
			msg := channel.IncomingMessage{
				Platform:               "discord",
				ChannelID:              msgChannelID,
				ParentChannelID:        parentChannelID,
				ChannelScopeUnresolved: scopeUnresolved,
				ThreadID:               threadID,
				UserID:                 userID,
				IsThread:               isThread,
				IsCallback:             true,
				CallbackData:           data.CustomID,
				CallbackRef:            origin,
				IsMention:              true,
				ReplyCtx:               &replyContext{session: sess, channelID: i.ChannelID, guildID: i.GuildID, appID: a.appIDValue(), interaction: i.Interaction},
			}
			handler(msg)

		case discordgo.InteractionApplicationCommand:
			a.handleApplicationCommandInteraction(sess, i, handler)
		}
	})
}

func (a *Adapter) handleApplicationCommandInteraction(sess *discordgo.Session, i *discordgo.InteractionCreate, handler func(channel.IncomingMessage)) {
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

	parentChannelID, isThread, scopeUnresolved := a.channelScope(i.GuildID, i.ChannelID)
	msgChannelID := i.ChannelID
	threadID := ""
	if isThread {
		threadID = i.ChannelID
	}
	if isThread && a.isOwnedThread(i.ChannelID) && parentChannelID != "" {
		msgChannelID = parentChannelID
	}
	msg := channel.IncomingMessage{
		Platform:               "discord",
		ChannelID:              msgChannelID,
		ParentChannelID:        parentChannelID,
		ChannelScopeUnresolved: scopeUnresolved,
		ThreadID:               threadID,
		UserID:                 userID,
		Text:                   strings.TrimSpace(text),
		IsMention:              true,
		IsThread:               isThread,
		ReplyCtx:               &replyContext{session: sess, channelID: i.ChannelID, guildID: i.GuildID, appID: a.appIDValue(), interaction: i.Interaction},
	}
	handler(msg)
}

func (a *Adapter) onReady(r *discordgo.Ready) {
	if r == nil || r.User == nil {
		slog.Warn("discord: ready event carried no bot identity")
		return
	}
	a.setBotID(r.User.ID)
	if r.Application != nil {
		a.setAppID(r.Application.ID)
	}
	slog.Info("discord adapter connected", "bot_id", r.User.ID)
	a.registerCommands(r)
}

func (a *Adapter) registerCommands(r *discordgo.Ready) {
	if len(a.menu) == 0 {
		return
	}
	if r.Application == nil || r.Application.ID == "" {
		slog.Warn("discord: set commands skipped — no application id in READY event")
		return
	}

	if _, err := a.session.ApplicationCommandBulkOverwrite(r.Application.ID, "", buildApplicationCommands(a.menu)); err != nil {
		slog.Warn("discord: set commands failed", "error", err)
	}
}

func buildApplicationCommands(menu []channel.MenuCommand) []*discordgo.ApplicationCommand {
	commands := make([]*discordgo.ApplicationCommand, len(menu))
	for i, m := range menu {
		cmd := &discordgo.ApplicationCommand{Name: sanitizeCommandName(m.Alias), Description: sanitizeDescription(m.Description)}
		if m.HasArgs {
			cmd.Options = []*discordgo.ApplicationCommandOption{{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "args",
				Description: "Arguments for this command",
			}}
		}
		commands[i] = cmd
	}
	return commands
}

func sanitizeCommandName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
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

const maxCommandDescriptionLen = 100

func sanitizeDescription(desc string) string {
	r := []rune(desc)
	if len(r) <= maxCommandDescriptionLen {
		return desc
	}
	return string(r[:maxCommandDescriptionLen])
}

func (a *Adapter) onMessage(m *discordgo.MessageCreate, handler func(channel.IncomingMessage)) {
	if m == nil || m.Message == nil || m.Author == nil {
		return
	}
	if self := a.selfID(); self != "" && m.Author.ID == self {
		return
	}
	if m.Author.Bot && !a.acceptsTrustedBotMessage(m.Message) {
		return
	}
	handler(a.normalizeMessage(m.Message))
}

func (a *Adapter) acceptsTrustedBotMessage(m *discordgo.Message) bool {
	channels, ok := a.trustedBots[m.Author.ID]
	if !ok {
		return false
	}
	if _, ok := channels[m.ChannelID]; ok {
		return a.hasOCCAMention(m)
	}

	parentChannelID, isThread, scopeUnresolved := a.channelScope(m.GuildID, m.ChannelID)
	if !isThread || scopeUnresolved || parentChannelID == "" || !a.isOwnedThread(m.ChannelID) {
		return false
	}
	if _, ok := channels[parentChannelID]; !ok {
		return false
	}
	return a.hasOCCAMention(m)
}

func (a *Adapter) hasOCCAMention(m *discordgo.Message) bool {
	self := a.selfID()
	if self != "" {
		for _, mention := range m.Mentions {
			if mention != nil && mention.ID == self {
				return true
			}
		}
	}
	for _, roleID := range m.MentionRoles {
		if _, ok := a.triggerRoleIDs[roleID]; ok {
			return true
		}
	}
	return false
}

func (a *Adapter) Stop() error {
	a.connected.Store(false)
	if a.session != nil {
		return a.session.Close()
	}
	return nil
}

func (a *Adapter) Notify(channelID string, text string) error {
	for _, chunk := range render.Split(text, 2000) {
		if _, err := a.session.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
			Content:         chunk,
			AllowedMentions: &discordgo.MessageAllowedMentions{},
		}); err != nil {
			return fmt.Errorf("discord: notify: %w", err)
		}
	}
	return nil
}

func (a *Adapter) DeleteMessage(channelID, messageID string) error {
	if a.session == nil {
		return fmt.Errorf("discord: delete message: session not ready")
	}
	err := a.session.ChannelMessageDelete(channelID, messageID)
	if restErr, ok := err.(*discordgo.RESTError); ok && restErr.Response != nil && restErr.Response.StatusCode == http.StatusNotFound {
		return channel.ErrMessageNotFound
	}
	return err
}

func (a *Adapter) normalizeMessage(m *discordgo.Message) channel.IncomingMessage {
	// The channel that hosts the user's triggering message. It is captured
	// before any auto-thread reassignment so reactions can target the source
	// message in its own channel (a read-receipt) even when the reply lands in
	// a freshly created thread.
	sourceChannelID := m.ChannelID

	isMention := m.GuildID == "" || a.hasOCCAMention(m)

	parentChannelID, isThread, scopeUnresolved := a.channelScope(m.GuildID, m.ChannelID)
	threadID := ""
	if isThread {
		threadID = m.ChannelID
	}
	replyChannelID := m.ChannelID

	if isMention && !isThread && m.GuildID != "" && a.autoThread != nil && a.session != nil {
		enabled, err := a.autoThread(m.ChannelID)
		if err != nil {
			slog.Warn("discord: auto-thread policy lookup failed", "channel_id", m.ChannelID, "error", err)
		} else if enabled {
			thread, err := a.session.MessageThreadStart(m.ChannelID, m.ID, threadName(m.Content), 1440)
			if err != nil {
				slog.Warn("discord: auto-thread creation failed", "channel_id", m.ChannelID, "error", err)
			} else {
				slog.Info("discord: auto-thread created", "channel_id", m.ChannelID, "thread_id", thread.ID, "thread_name", thread.Name)
				parentChannelID = m.ChannelID
				threadID = thread.ID
				replyChannelID = thread.ID
				isThread = true
				a.trackThread(thread.ID)
			}
		}
	}

	if isThread && threadID != "" && a.isOwnedThread(threadID) {
		isMention = true
		if parentChannelID != "" {
			m.ChannelID = parentChannelID
		}
	}

	return channel.IncomingMessage{
		Platform:               "discord",
		ChannelID:              m.ChannelID,
		ParentChannelID:        parentChannelID,
		ChannelScopeUnresolved: scopeUnresolved,
		ThreadID:               threadID,
		UserID:                 m.Author.ID,
		Text:                   m.Content,
		IsMention:              isMention,
		IsThread:               isThread,
		SourceRef:              messageRef{id: m.ID},
		Attachments:            a.downloadAttachments(m),
		ReplyCtx:               &replyContext{session: a.session, channelID: replyChannelID, reactionChannelID: sourceChannelID, guildID: m.GuildID, appID: a.appIDValue()},
	}
}

func threadName(content string) string {
	var b strings.Builder
	inToken := false
	runes := 0
	for _, r := range strings.TrimSpace(content) {
		switch {
		case r == '<':
			inToken = true
			continue
		case r == '>':
			inToken = false
			continue
		case inToken:
			continue
		case r == '\n' || strings.ContainsRune("`@#?:*{}|\\\"", r):
			continue
		}
		if runes >= 100 {
			break
		}
		b.WriteRune(r)
		runes++
	}
	name := strings.TrimSpace(b.String())
	if name == "" {
		return "OCCA chat"
	}
	return name
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
	if ch == nil {
		slog.Warn("discord: channel scope lookup returned no channel", "channel_id", channelID)
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
	session           *discordgo.Session
	channelID         string
	reactionChannelID string
	guildID           string
	appID             string
	interaction       *discordgo.Interaction
	currentReaction   map[string]string
}

func (rc *replyContext) SendTyping() error {
	return rc.session.ChannelTyping(rc.channelID)
}

func (rc *replyContext) SetReaction(ref channel.MessageRef, state channel.ReactionState) error {
	emoji := reactionEmoji(state)
	if rc.currentReaction == nil {
		rc.currentReaction = make(map[string]string)
	}
	// React on the source message's own channel (the read-receipt target) when
	// known, falling back to the reply channel so the auto-thread case reacts
	// on the parent channel where the user's message lives.
	reactionChannelID := rc.channelID
	if rc.reactionChannelID != "" {
		reactionChannelID = rc.reactionChannelID
	}
	if prev, ok := rc.currentReaction[ref.ID()]; ok {
		if prev == emoji {
			return nil
		}
		if err := rc.session.MessageReactionRemove(reactionChannelID, ref.ID(), prev, "@me"); err != nil {
			return fmt.Errorf("discord: remove reaction %s: %w", prev, err)
		}
	}
	if err := rc.session.MessageReactionAdd(reactionChannelID, ref.ID(), emoji); err != nil {
		return fmt.Errorf("discord: add reaction %s: %w", emoji, err)
	}
	rc.currentReaction[ref.ID()] = emoji
	return nil
}

func reactionEmoji(state channel.ReactionState) string {
	switch state {
	case channel.ReactionProcessing:
		return "👀"
	case channel.ReactionSuccess:
		return "✅"
	case channel.ReactionError:
		return "❌"
	}
	return ""
}

func (rc *replyContext) SetChatCommands(commands []channel.MenuCommand) error {
	if rc.guildID == "" || rc.appID == "" {
		return nil
	}
	_, err := rc.session.ApplicationCommandBulkOverwrite(rc.appID, rc.guildID, buildApplicationCommands(commands))
	return err
}

func (rc *replyContext) Send(text string) (channel.MessageRef, error) {
	if rc.interaction != nil {
		content := text
		msg, err := rc.session.InteractionResponseEdit(rc.interaction, &discordgo.WebhookEdit{
			Content:         &content,
			AllowedMentions: &discordgo.MessageAllowedMentions{},
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
		msg, err := rc.session.ChannelMessageSendComplex(rc.channelID, &discordgo.MessageSend{
			Content:         chunk,
			AllowedMentions: &discordgo.MessageAllowedMentions{},
		})
		if err != nil {
			return nil, fmt.Errorf("discord: send: %w", err)
		}
		lastRef = messageRef{id: msg.ID}
	}
	return lastRef, nil
}

func (rc *replyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	if rc.interaction != nil {
		// A deferred interaction response is still subject to Discord's
		// 2000-unit content limit, so clamp even though we carry buttons —
		// same as the channel-message path below.
		content := render.Clamp(text, render.DiscordLimit)
		components := componentRows(buttons)
		msg, err := rc.session.InteractionResponseEdit(rc.interaction, &discordgo.WebhookEdit{
			Content:         &content,
			Components:      &components,
			AllowedMentions: &discordgo.MessageAllowedMentions{},
		})
		if err != nil {
			return nil, fmt.Errorf("discord: interaction response edit: %w", err)
		}
		rc.interaction = nil
		return messageRef{id: msg.ID}, nil
	}

	msg, err := rc.session.ChannelMessageSendComplex(rc.channelID, &discordgo.MessageSend{
		Content:         render.Clamp(text, render.DiscordLimit),
		Components:      componentRows(buttons),
		AllowedMentions: &discordgo.MessageAllowedMentions{},
	})
	if err != nil {
		return nil, fmt.Errorf("discord: send with buttons: %w", err)
	}
	return messageRef{id: msg.ID}, nil
}

func (rc *replyContext) Edit(ref channel.MessageRef, text string) error {
	_, err := rc.session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		Channel:         rc.channelID,
		ID:              ref.ID(),
		Content:         &text,
		AllowedMentions: &discordgo.MessageAllowedMentions{},
	})
	return err
}

func (rc *replyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	content := render.Clamp(text, render.DiscordLimit)
	components := componentRows(buttons)
	_, err := rc.session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		ID:              ref.ID(),
		Channel:         rc.channelID,
		Content:         &content,
		Components:      &components,
		AllowedMentions: &discordgo.MessageAllowedMentions{},
	})
	return err
}

func componentRows(buttons []channel.Button) []discordgo.MessageComponent {
	components := make([]discordgo.MessageComponent, 0, 1)
	if len(buttons) == 0 {
		return components
	}

	var legacy []discordgo.MessageComponent
	grouped := make(map[int][]discordgo.MessageComponent)
	var order []int
	for _, b := range buttons {
		btn := discordgo.Button{
			Label:    safeButtonLabel(b.Label),
			Style:    discordgo.SecondaryButton,
			CustomID: b.Value,
		}
		if b.Row == 0 {
			legacy = append(legacy, btn)
			continue
		}
		if _, ok := grouped[b.Row]; !ok {
			order = append(order, b.Row)
		}
		grouped[b.Row] = append(grouped[b.Row], btn)
	}
	for _, r := range order {
		if len(components) >= maxDiscordActionRows {
			break
		}
		row := grouped[r]
		for len(row) > 0 && len(components) < maxDiscordActionRows {
			count := maxButtonsPerActionRow
			if len(row) < count {
				count = len(row)
			}
			components = append(components, discordgo.ActionsRow{Components: row[:count]})
			row = row[count:]
		}
	}
	for len(legacy) > 0 && len(components) < maxDiscordActionRows {
		count := maxButtonsPerActionRow
		if len(legacy) < count {
			count = len(legacy)
		}
		components = append(components, discordgo.ActionsRow{Components: legacy[:count]})
		legacy = legacy[count:]
	}
	return components
}

func safeButtonLabel(label string) string {
	runes := []rune(label)
	if len(runes) <= maxButtonLabelRunes {
		return label
	}
	return string(runes[:maxButtonLabelRunes-1]) + "…"
}

type messageRef struct {
	id string
}

func (m messageRef) ID() string { return m.id }

func (rc *replyContext) Delete(ref channel.MessageRef) error {
	return rc.session.ChannelMessageDelete(rc.channelID, ref.ID())
}

var (
	_ channel.Channel           = (*Adapter)(nil)
	_ channel.MessageDeleter    = (*Adapter)(nil)
	_ channel.ReplyContext      = (*replyContext)(nil)
	_ channel.MessageRemover    = (*replyContext)(nil)
	_ channel.ChatCommandSetter = (*replyContext)(nil)
	_ channel.ReactionSetter    = (*replyContext)(nil)
	_ channel.MessageRef        = messageRef{}
)
