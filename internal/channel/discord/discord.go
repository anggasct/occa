package discord

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const (
	maxButtonLabelRunes    = 80
	maxDiscordActionRows   = 5
	maxButtonsPerActionRow = 5
)

type ThreadPolicy func(channelID string) (bool, error)

type OwnedThreadCheck func(threadID string) (bool, error)

type AllowlistPolicy struct {
	AllowedSenderIDs []string
}

type Config struct {
	DownloadTimeout time.Duration
	MaxDownloadSize int64
}

type Adapter struct {
	session         *discordgo.Session
	token           string
	menu            []channel.MenuCommand
	allowedSenders  map[string]struct{}
	botID           atomic.Value
	appID           atomic.Value
	channelLookup   func(string) (*discordgo.Channel, error)
	downloadClient  *http.Client
	maxDownloadSize int64
	autoThread      ThreadPolicy
	ownedThread     OwnedThreadCheck
	connected       atomic.Bool

	threadsMu   sync.Mutex
	threads     map[string]struct{}
	parentCache map[string]string

	kindsMu     sync.Mutex
	channelKind map[string]discordgo.ChannelType

	dispatchMu       sync.Mutex
	seenDispatchTask map[string]time.Time

	taskThreadLookup     func(repo, branch string) (string, bool)
	taskThreadMissDetail func(repo, branch string) (string, string, string, bool, bool)
}

const maxSeenDispatchTasks = 1024

func New(token string, menu []channel.MenuCommand, cfg Config) *Adapter {
	return NewWithPolicy(token, menu, AllowlistPolicy{}, cfg)
}

func NewWithPolicy(token string, menu []channel.MenuCommand, policy AllowlistPolicy, cfg Config) *Adapter {
	a := &Adapter{
		token:           token,
		menu:            menu,
		downloadClient:  &http.Client{Timeout: cfg.DownloadTimeout},
		allowedSenders:  make(map[string]struct{}, len(policy.AllowedSenderIDs)),
		maxDownloadSize: cfg.MaxDownloadSize,
	}
	for _, id := range policy.AllowedSenderIDs {
		if id = strings.TrimSpace(id); id != "" {
			a.allowedSenders[id] = struct{}{}
		}
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

func (a *Adapter) SetTaskThreadLookup(fn func(repo, branch string) (string, bool)) {
	a.taskThreadLookup = fn
}

func (a *Adapter) SetTaskThreadMissDetail(fn func(repo, branch string) (string, string, string, bool, bool)) {
	a.taskThreadMissDetail = fn
}

func (a *Adapter) lookupTaskThread(repo, branch string) (string, bool) {
	if a.taskThreadLookup == nil {
		return "", false
	}
	threadID, ok := a.taskThreadLookup(strings.TrimSpace(repo), strings.TrimSpace(branch))
	if !ok || strings.TrimSpace(threadID) == "" {
		return "", false
	}
	return strings.TrimSpace(threadID), true
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

func (a *Adapter) ParentChannelOf(threadID string) (string, error) {
	if threadID == "" {
		return "", nil
	}
	a.threadsMu.Lock()
	if a.parentCache != nil {
		if parent, ok := a.parentCache[threadID]; ok {
			a.threadsMu.Unlock()
			if parent == "" {
				return "", channel.ErrThreadNotFound
			}
			return parent, nil
		}
	}
	a.threadsMu.Unlock()
	if a.channelLookup == nil && a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	ch, err := a.lookupChannel(threadID)
	if err != nil {
		var restErr *discordgo.RESTError
		if errors.As(err, &restErr) && restErr.Response != nil && restErr.Response.StatusCode == http.StatusNotFound {
			a.threadsMu.Lock()
			if a.parentCache == nil {
				a.parentCache = make(map[string]string)
			}
			a.parentCache[threadID] = ""
			a.threadsMu.Unlock()
			return "", channel.ErrThreadNotFound
		}
		return "", err
	}
	if ch == nil || ch.ParentID == "" {
		a.threadsMu.Lock()
		if a.parentCache == nil {
			a.parentCache = make(map[string]string)
		}
		a.parentCache[threadID] = ""
		a.threadsMu.Unlock()
		return "", channel.ErrThreadNotFound
	}
	a.threadsMu.Lock()
	if a.parentCache == nil {
		a.parentCache = make(map[string]string)
	}
	a.parentCache[threadID] = ch.ParentID
	a.threadsMu.Unlock()
	return ch.ParentID, nil
}

func (a *Adapter) GuildIDForChannel(channelID string) (string, error) {
	id := strings.TrimSpace(channelID)
	if id == "" {
		return "", nil
	}
	if strings.Contains(id, ":") {
		parts := strings.Split(id, ":")
		id = strings.TrimSpace(parts[len(parts)-1])
		if id == "" {
			return "", nil
		}
	}
	if a.channelLookup == nil && a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	ch, err := a.lookupChannel(id)
	if err != nil {
		return "", err
	}
	if ch == nil {
		return "", nil
	}
	if strings.TrimSpace(ch.GuildID) != "" {
		return ch.GuildID, nil
	}
	if strings.TrimSpace(ch.ParentID) == "" {
		return "", nil
	}
	parent, err := a.lookupChannel(ch.ParentID)
	if err != nil || parent == nil {
		return "", err
	}
	return parent.GuildID, nil
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
		if repo, branch, taskID, ok := channel.ParseDispatchSummary(m.Content); ok {
			if !a.tryClaimDispatchTask(taskID) {
				return
			}
			msg := a.normalizeMessage(m.Message)
			msg.IsMention = true
			msg.IsSelfDispatch = true
			if threadID, found := a.lookupTaskThread(repo, branch); found {
				parent := msg.ChannelID
				if parent == "" {
					parent = m.ChannelID
				}
				msg.ParentChannelID = parent
				msg.ThreadID = threadID
				msg.IsThread = true
				if rc, ok := msg.ReplyCtx.(*replyContext); ok && rc != nil {
					threadRc := &replyContext{
						session:           rc.session,
						channelID:         threadID,
						reactionChannelID: parent,
						guildID:           rc.guildID,
						appID:             rc.appID,
					}
					msg.ReplyCtx = a.taskThreadReply(threadRc, rc)
				}
				a.trackThread(threadID)
				slog.Info("discord: task thread claim hit", "repo", repo, "branch", branch, "thread_id", threadID, "task_id", taskID)
			} else {
				tried, sameKey, sameThread, hasSame, triedExists := "", "", "", false, false
				if a.taskThreadMissDetail != nil {
					tried, sameKey, sameThread, hasSame, triedExists = a.taskThreadMissDetail(repo, branch)
				}
				slog.Warn("discord: task thread unknown, self-dispatch will run top-level", "repo", repo, "branch", branch, "tried_key", tried, "same_branch_key", sameKey, "same_branch_thread", sameThread, "has_same_branch", hasSame, "tried_exists", triedExists, "task_id", taskID, "channel_id", m.ChannelID)
			}
			handler(msg)
		}
		return
	}
	if m.Author.Bot && !a.allowsBotSender(m.Author.ID) {
		return
	}
	if m.Author.Bot && !a.hasOCCAMention(m.Message) {
		return
	}
	handler(a.normalizeMessage(m.Message))
}

func (a *Adapter) tryClaimDispatchTask(taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	a.dispatchMu.Lock()
	defer a.dispatchMu.Unlock()
	if a.seenDispatchTask == nil {
		a.seenDispatchTask = make(map[string]time.Time)
	}
	now := time.Now()
	if _, ok := a.seenDispatchTask[taskID]; ok {
		return false
	}
	if len(a.seenDispatchTask) >= maxSeenDispatchTasks {
		oldest := ""
		var oldestAt time.Time
		first := true
		for id, claimedAt := range a.seenDispatchTask {
			if first || claimedAt.Before(oldestAt) {
				oldest, oldestAt, first = id, claimedAt, false
			}
		}
		if oldest != "" {
			delete(a.seenDispatchTask, oldest)
		}
	}
	a.seenDispatchTask[taskID] = now
	return true
}

func (a *Adapter) allowsBotSender(userID string) bool {
	_, ok := a.allowedSenders[strings.TrimSpace(userID)]
	return ok
}

func (a *Adapter) hasOCCAMention(m *discordgo.Message) bool {
	self := a.selfID()
	if self == "" {
		return false
	}
	for _, mention := range m.Mentions {
		if mention != nil && mention.ID == self {
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
	_, err := a.SendNotification(channelID, text)
	return err
}

func (a *Adapter) SendNotification(channelID, text string) (string, error) {
	if a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	if a.isForumChannel(channelID) {
		return a.sendForumPost(channelID, text, nil, "")
	}
	chunks := render.Split(text, 2000)
	if len(chunks) == 0 {
		return "", nil
	}
	var firstID string
	for _, chunk := range chunks {
		msg, err := a.session.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
			Content:         chunk,
			AllowedMentions: &discordgo.MessageAllowedMentions{},
			Flags:           discordgo.MessageFlagsSuppressEmbeds,
		})
		if err != nil {
			return "", fmt.Errorf("discord: send notification: %w", err)
		}
		if firstID == "" {
			firstID = msg.ID
		}
	}
	return firstID, nil
}

func (a *Adapter) EditNotification(channelID, messageID, text string) error {
	return a.editNotification(channelID, messageID, text, true)
}

// EditNotificationNoRetry surfaces a rate limit instead of sleeping through it.
// Background writers use it so their own cadence, not the HTTP client, backs off.
func (a *Adapter) EditNotificationNoRetry(channelID, messageID, text string) error {
	return a.editNotification(channelID, messageID, text, false)
}

func (a *Adapter) editNotification(channelID, messageID, text string, retry bool) error {
	if a.session == nil {
		return errors.New("discord: session not ready")
	}
	channelID, messageID = a.notificationTarget(channelID, messageID)
	chunks := render.Split(text, 2000)
	if len(chunks) == 0 {
		return nil
	}
	content := chunks[0]
	_, err := a.session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		Channel:         channelID,
		ID:              messageID,
		Content:         &content,
		AllowedMentions: &discordgo.MessageAllowedMentions{},
		Flags:           discordgo.MessageFlagsSuppressEmbeds,
	}, discordgo.WithRetryOnRatelimit(retry))
	if err != nil {
		return discordRateLimit(fmt.Errorf("discord: edit notification: %w", err))
	}
	return nil
}

func (a *Adapter) SendNotificationWithButtons(channelID, text string, buttons []channel.Button) (string, error) {
	if a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	if a.isForumChannel(channelID) {
		return a.sendForumPost(channelID, text, buttons, "")
	}
	content := render.Clamp(text, render.DiscordLimit)
	msg, err := a.session.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
		Content:         content,
		Components:      componentRows(buttons),
		AllowedMentions: &discordgo.MessageAllowedMentions{},
		Flags:           discordgo.MessageFlagsSuppressEmbeds,
	})
	if err != nil {
		return "", fmt.Errorf("discord: send notification with buttons: %w", err)
	}
	return msg.ID, nil
}

// SendNotificationMention posts a notification that can ping a single user.
// The escalation path of the product is the only caller: every other send
// keeps mentions suppressed. The mention token is rendered here: an allowed
// mention only permits a ping, it does not create one, so the body must
// carry "<@id>" for the resolved user.
func (a *Adapter) SendNotificationMention(channelID, text string, mentionUserID string) (string, error) {
	if a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	mentionUserID = strings.TrimSpace(mentionUserID)
	content := render.Clamp(text, render.DiscordLimit)
	if mentionUserID != "" {
		token := "<@" + mentionUserID + ">"
		if !strings.Contains(content, token) {
			content = render.Clamp(text, render.DiscordLimit-utf8.RuneCountInString(token)-1) + "\n" + token
		}
	}
	if a.isForumChannel(channelID) {
		return a.sendForumPost(channelID, content, nil, mentionUserID)
	}
	msg, err := a.session.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
		Content: content,
		AllowedMentions: &discordgo.MessageAllowedMentions{
			Users: []string{mentionUserID},
		},
		Flags: discordgo.MessageFlagsSuppressEmbeds,
	})
	if err != nil {
		return "", fmt.Errorf("discord: send notification mention: %w", err)
	}
	return msg.ID, nil
}

// ResolveMentionID maps a username or user id to a Discord user id. It is
// fail-closed: an unknown name resolves to nothing so the caller can post
// without a ping.
func (a *Adapter) ResolveMentionID(_ context.Context, name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	if isDiscordSnowflake(name) {
		return name, true
	}
	if a.session == nil {
		return "", false
	}
	for _, guild := range a.session.State.Guilds {
		if guild == nil {
			continue
		}
		members, err := a.session.GuildMembersSearch(guild.ID, name, 100)
		if err != nil {
			continue
		}
		for _, member := range members {
			if member == nil || member.User == nil {
				continue
			}
			if strings.EqualFold(member.User.Username, name) || strings.EqualFold(member.Nick, name) {
				return member.User.ID, true
			}
		}
	}
	return "", false
}

func isDiscordSnowflake(value string) bool {
	if len(value) < 17 || len(value) > 20 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (a *Adapter) EditNotificationWithButtons(channelID, messageID, text string, buttons []channel.Button) error {
	return a.editNotificationWithButtons(channelID, messageID, text, buttons, true)
}

func (a *Adapter) EditNotificationWithButtonsNoRetry(channelID, messageID, text string, buttons []channel.Button) error {
	return a.editNotificationWithButtons(channelID, messageID, text, buttons, false)
}

func (a *Adapter) editNotificationWithButtons(channelID, messageID, text string, buttons []channel.Button, retry bool) error {
	if a.session == nil {
		return errors.New("discord: session not ready")
	}
	channelID, messageID = a.notificationTarget(channelID, messageID)
	content := render.Clamp(text, render.DiscordLimit)
	components := componentRows(buttons)
	_, err := a.session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		Channel:         channelID,
		ID:              messageID,
		Content:         &content,
		Components:      &components,
		AllowedMentions: &discordgo.MessageAllowedMentions{},
		Flags:           discordgo.MessageFlagsSuppressEmbeds,
	}, discordgo.WithRetryOnRatelimit(retry))
	if err != nil {
		return discordRateLimit(fmt.Errorf("discord: edit notification with buttons: %w", err))
	}
	return nil
}

func discordRateLimit(err error) error {
	if err == nil {
		return nil
	}
	var limitErr *discordgo.RateLimitError
	if errors.As(err, &limitErr) {
		return fmt.Errorf("%w: %v", channel.ErrRateLimited, err)
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) && restErr.Response != nil && restErr.Response.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("%w: %v", channel.ErrRateLimited, err)
	}
	return err
}

func (a *Adapter) ReplyNotification(channelID, replyToMessageID, text string) (string, error) {
	if a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	// A reply aimed at a forum channel targets the post itself: the starter
	// message cannot be replied to from the channel, only from inside the post.
	if a.isForumChannel(channelID) && strings.TrimSpace(replyToMessageID) != "" {
		return a.SendNotification(replyToMessageID, text)
	}
	chunks := render.Split(text, 2000)
	if len(chunks) == 0 {
		return "", nil
	}
	var firstID string
	for _, chunk := range chunks {
		msg, err := a.session.ChannelMessageSendComplex(channelID, &discordgo.MessageSend{
			Content: chunk,
			Reference: &discordgo.MessageReference{
				MessageID: replyToMessageID,
				ChannelID: channelID,
			},
			AllowedMentions: &discordgo.MessageAllowedMentions{},
			Flags:           discordgo.MessageFlagsSuppressEmbeds,
		})
		if err != nil {
			return "", fmt.Errorf("discord: reply notification: %w", err)
		}
		if firstID == "" {
			firstID = msg.ID
		}
	}
	return firstID, nil
}

func (a *Adapter) StartThread(channelID, messageID, name string) (string, error) {
	if a.session == nil {
		return "", errors.New("discord: session not ready")
	}
	if a.isForumChannel(channelID) {
		postID := strings.TrimSpace(messageID)
		if postID == "" {
			return "", errors.New("discord: start thread: forum post id missing")
		}
		a.trackThread(postID)
		return postID, nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "thread"
	}
	if utf8.RuneCountInString(name) > 100 {
		runes := []rune(name)
		name = string(runes[:100])
	}
	thread, err := a.session.MessageThreadStart(channelID, messageID, name, 1440)
	if err != nil {
		return "", fmt.Errorf("discord: start thread: %w", err)
	}
	a.trackThread(thread.ID)
	return thread.ID, nil
}

// PinNotification pins a message in its channel so the operator finds it
// without scrolling the thread.
func (a *Adapter) PinNotification(channelID, messageID string) error {
	if a.session == nil {
		return errors.New("discord: session not ready")
	}
	channelID = strings.TrimSpace(channelID)
	messageID = strings.TrimSpace(messageID)
	if channelID == "" || messageID == "" {
		return errors.New("discord: pin notification: empty channel or message id")
	}
	channelID, messageID = a.notificationTarget(channelID, messageID)
	if err := a.session.ChannelMessagePin(channelID, messageID); err != nil {
		return fmt.Errorf("discord: pin notification: %w", err)
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
		data, err := io.ReadAll(io.LimitReader(resp.Body, a.maxDownloadSize+1))
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

// forumAutoArchiveMinutes keeps a delivery post visible for a week of
// inactivity; Discord archives forum posts afterwards without deleting them.
const forumAutoArchiveMinutes = 10080

// isForumChannel reports whether channelID is a forum channel. The answer is
// cached per channel; an unknown or unreachable channel reports false so every
// notification keeps the pre-forum behaviour.
func (a *Adapter) isForumChannel(channelID string) bool {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return false
	}
	a.kindsMu.Lock()
	if kind, ok := a.channelKind[channelID]; ok {
		a.kindsMu.Unlock()
		return kind == discordgo.ChannelTypeGuildForum
	}
	a.kindsMu.Unlock()
	kind, ok := a.cachedChannelType(channelID)
	if !ok {
		return false
	}
	a.kindsMu.Lock()
	if a.channelKind == nil {
		a.channelKind = make(map[string]discordgo.ChannelType)
	}
	a.channelKind[channelID] = kind
	a.kindsMu.Unlock()
	return kind == discordgo.ChannelTypeGuildForum
}

// cachedChannelType resolves a channel type without spending a REST call: the
// gateway state already carries every guild channel, and an unknown channel
// reports false so notification paths keep their pre-forum behaviour.
func (a *Adapter) cachedChannelType(channelID string) (discordgo.ChannelType, bool) {
	if a.channelLookup != nil {
		ch, err := a.channelLookup(channelID)
		if err != nil || ch == nil {
			return 0, false
		}
		return ch.Type, true
	}
	if a.session == nil {
		return 0, false
	}
	ch, err := a.session.State.Channel(channelID)
	if err != nil || ch == nil {
		return 0, false
	}
	return ch.Type, true
}

// notificationTarget maps a notification addressed at a forum channel onto the
// post that owns it. A forum post is a thread whose id doubles as its starter
// message id, and that starter message lives inside the post — never in the
// forum channel itself.
func (a *Adapter) notificationTarget(channelID, messageID string) (string, string) {
	if strings.TrimSpace(messageID) == "" || !a.isForumChannel(channelID) {
		return channelID, messageID
	}
	return messageID, messageID
}

// sendForumPost opens a forum post whose starter message carries the task
// surface (text plus buttons) and returns the post id. Downstream code keeps
// treating that id as both root message and thread; Discord resolves both roles
// for a forum post.
func (a *Adapter) sendForumPost(forumChannelID, text string, buttons []channel.Button, mentionUserID string) (string, error) {
	content := render.Clamp(text, render.DiscordLimit)
	allowed := &discordgo.MessageAllowedMentions{}
	if mentionUserID = strings.TrimSpace(mentionUserID); mentionUserID != "" {
		allowed.Users = []string{mentionUserID}
	}
	post, err := a.session.ForumThreadStartComplex(forumChannelID, &discordgo.ThreadStart{
		Name:                forumPostTitle(text),
		AutoArchiveDuration: forumAutoArchiveMinutes,
	}, &discordgo.MessageSend{
		Content:         content,
		Components:      componentRows(buttons),
		AllowedMentions: allowed,
		Flags:           discordgo.MessageFlagsSuppressEmbeds,
	})
	if err != nil {
		return "", fmt.Errorf("discord: create forum post: %w", err)
	}
	a.trackThread(post.ID)
	return post.ID, nil
}

// forumPostTitle derives a post title from the notification body: the first
// meaningful line, stripped of markdown emphasis, clipped to Discord's limit.
func forumPostTitle(text string) string {
	for _, raw := range strings.Split(text, "\n") {
		line := strings.Trim(strings.TrimSpace(raw), "*_`# ")
		if line == "" {
			continue
		}
		return clipTitle(line)
	}
	return "run"
}

func clipTitle(value string) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) > 100 {
		runes = runes[:100]
	}
	if len(runes) == 0 {
		return "run"
	}
	return string(runes)
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
		var btn discordgo.Button
		if strings.TrimSpace(b.URL) != "" {
			btn = discordgo.Button{
				Label: safeButtonLabel(b.Label),
				Style: discordgo.LinkButton,
				URL:   strings.TrimSpace(b.URL),
			}
		} else {
			btn = discordgo.Button{
				Label:    safeButtonLabel(b.Label),
				Style:    discordgo.SecondaryButton,
				CustomID: b.Value,
				Disabled: b.Disabled,
			}
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

func (a *Adapter) taskThreadReply(thread, parent *replyContext) channel.ReplyContext {
	if thread == nil {
		return parent
	}
	if parent == nil {
		return thread
	}
	return &taskThreadReplyContext{thread: thread, parent: parent}
}

type taskThreadReplyContext struct {
	thread *replyContext
	parent *replyContext
}

func (t *taskThreadReplyContext) SendTyping() error {
	if err := t.thread.SendTyping(); err != nil {
		slog.Warn("discord: task thread typing failed, falling back to parent", "thread_id", t.thread.channelID, "error", err)
		return t.parent.SendTyping()
	}
	return nil
}

func (t *taskThreadReplyContext) Send(text string) (channel.MessageRef, error) {
	ref, err := t.thread.Send(text)
	if err != nil {
		slog.Warn("discord: task thread send failed, falling back to parent", "thread_id", t.thread.channelID, "error", err)
		return t.parent.Send(text)
	}
	return ref, nil
}

func (t *taskThreadReplyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	ref, err := t.thread.SendWithButtons(text, buttons)
	if err != nil {
		slog.Warn("discord: task thread send failed, falling back to parent", "thread_id", t.thread.channelID, "error", err)
		return t.parent.SendWithButtons(text, buttons)
	}
	return ref, nil
}

func (t *taskThreadReplyContext) Edit(ref channel.MessageRef, text string) error {
	if err := t.thread.Edit(ref, text); err != nil {
		slog.Warn("discord: task thread edit failed, falling back to parent", "thread_id", t.thread.channelID, "error", err)
		return t.parent.Edit(ref, text)
	}
	return nil
}

func (t *taskThreadReplyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	if err := t.thread.EditWithButtons(ref, text, buttons); err != nil {
		slog.Warn("discord: task thread edit failed, falling back to parent", "thread_id", t.thread.channelID, "error", err)
		return t.parent.EditWithButtons(ref, text, buttons)
	}
	return nil
}

func (t *taskThreadReplyContext) Delete(ref channel.MessageRef) error {
	return t.thread.Delete(ref)
}

func (t *taskThreadReplyContext) SetReaction(ref channel.MessageRef, state channel.ReactionState) error {
	return t.thread.SetReaction(ref, state)
}

func (t *taskThreadReplyContext) SetChatCommands(commands []channel.MenuCommand) error {
	return t.thread.SetChatCommands(commands)
}

var (
	_ channel.Channel                  = (*Adapter)(nil)
	_ channel.MessageDeleter           = (*Adapter)(nil)
	_ channel.MessageSender            = (*Adapter)(nil)
	_ channel.MessageSenderWithButtons = (*Adapter)(nil)
	_ channel.MessageEditor            = (*Adapter)(nil)
	_ channel.MessageEditorWithButtons = (*Adapter)(nil)
	_ channel.MessageReplier           = (*Adapter)(nil)
	_ channel.ThreadStarter            = (*Adapter)(nil)
	_ channel.ThreadParentResolver     = (*Adapter)(nil)
	_ channel.GuildResolver            = (*Adapter)(nil)
	_ channel.ReplyContext             = (*replyContext)(nil)
	_ channel.MessageRemover           = (*replyContext)(nil)
	_ channel.ChatCommandSetter        = (*replyContext)(nil)
	_ channel.ReactionSetter           = (*replyContext)(nil)
	_ channel.ReplyContext             = (*taskThreadReplyContext)(nil)
	_ channel.MessageRemover           = (*taskThreadReplyContext)(nil)
	_ channel.ChatCommandSetter        = (*taskThreadReplyContext)(nil)
	_ channel.ReactionSetter           = (*taskThreadReplyContext)(nil)
	_ channel.MessageRef               = messageRef{}
)
