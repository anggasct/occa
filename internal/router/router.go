package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/store"
)

var ErrDenied = errors.New("access denied")

const (
	accessDeniedMessage = "⚠️ Access denied. Ask an admin to /allow you."
	accessVerifyMessage = "⚠️ Unable to verify access. Try again."
)

type Command struct {
	Name    string
	Admin   bool
	Handler func(ctx context.Context, msg channel.IncomingMessage, args string) (string, error)
}

// MenuCommands is the single source of truth for native command-menu
// registration on both platforms.
func (r *Router) MenuCommands() []channel.MenuCommand {
	return []channel.MenuCommand{
		{Alias: "help", Description: "Show available commands"},
		{Alias: "status", Description: "Agent health and session info"},
		{Alias: "session", Description: "Manage sessions: list, new, switch, or delete", HasArgs: true},
		{Alias: "reset", Description: "Clear current session and start fresh"},
		{Alias: "dir", Description: "View or set this channel's working directory", HasArgs: true},
		{Alias: "allow", Description: "Allow a user to use this bot", HasArgs: true},
		{Alias: "deny", Description: "Revoke a user's access to this bot", HasArgs: true},
		{Alias: "admin", Description: "Grant a user admin access", HasArgs: true},
		{Alias: "channel", Description: "View or set listen mode (mention, all, thread)", HasArgs: true},
		{Alias: "model", Description: "View or set the active model", HasArgs: true},
		{Alias: "variants", Description: "List and set model reasoning variants", HasArgs: true},
		{Alias: "schedules", Description: "View or delete scheduled tasks", HasArgs: true},
	}
}

// AgentInstance is a ready agent backend handle for a working directory.
type AgentInstance interface {
	Client() relay.Client
	End()
	PID() int
	Workdir() string
}

// InstanceProvider resolves an AgentInstance for a working directory.
type InstanceProvider interface {
	Instance(ctx context.Context, workdir string) (AgentInstance, error)
	ForceStop(workdir string)
}

type Router struct {
	commands       map[string]Command
	instances      InstanceProvider
	store          store.Store
	defaultWorkdir string
	adminID        string
	startedAt      time.Time
	sched          ScheduleStore
	tokenGen       TokenGenerator
	responses      *responseCoordinator
	permissions    *permissionBroker
	questions      *questionBroker
	modelBrowser   *modelBrowserBroker
	renderer       render.Renderer
}

type ScheduleStore interface {
	ListSchedules(ctx context.Context, platform, channelID string) ([]store.Schedule, error)
	RemoveSchedule(ctx context.Context, platform, channelID string, id int64) error
}

type TokenGenerator interface {
	Generate(platform, channelID string) (string, error)
}

func (r *Router) SetScheduler(s ScheduleStore) {
	r.sched = s
}

func (r *Router) SetTokenGenerator(t TokenGenerator) {
	r.tokenGen = t
}

func New(instances InstanceProvider, st store.Store, defaultWorkdir string, adminID string) *Router {
	r := &Router{
		commands:       make(map[string]Command),
		instances:      instances,
		store:          st,
		defaultWorkdir: defaultWorkdir,
		adminID:        adminID,
		startedAt:      time.Now(),
		responses:      newResponseCoordinator(),
		permissions:    newPermissionBroker(),
		questions:      newQuestionBroker(),
		modelBrowser:   newModelBrowserBroker(),
		renderer:       render.New(),
	}
	r.registerDefaults()
	return r
}

func (r *Router) Route(ctx context.Context, msg channel.IncomingMessage) error {
	isOcca := r.isOccaCommand(msg.Text)
	msg.Text = normalizeCommandAlias(msg.Text)
	inputKind := r.routeInputKind(msg, isOcca)
	if err := r.authorize(ctx, msg); err != nil {
		if errors.Is(err, ErrDenied) {
			slog.Info("access denied", "platform", msg.Platform, "channel_id", msg.ChannelID, "thread_id", msg.ThreadID, "user_id", msg.UserID, "input_kind", inputKind, "outcome", "denied")
			r.reply(msg, accessDeniedMessage)
			return nil
		}

		slog.Error("access verification failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "thread_id", msg.ThreadID, "user_id", msg.UserID, "input_kind", inputKind, "outcome", "error", "error", err)
		r.reply(msg, accessVerifyMessage)
		return nil
	}

	if msg.IsCallback {
		return r.handleCallback(ctx, msg)
	}

	if isOcca {
		return r.handleCommand(ctx, msg)
	}

	if !r.listenModeAllows(ctx, msg) {
		return nil
	}

	return r.passthrough(ctx, msg)
}

// normalizeCommandAlias rewrites legacy "/occa_name" and "/occa:name" aliases
// to the short canonical form "/name".
func normalizeCommandAlias(text string) string {
	if strings.HasPrefix(text, "/occa:") {
		return "/" + strings.TrimPrefix(text, "/occa:")
	}
	if strings.HasPrefix(text, "/occa_") {
		return "/" + strings.TrimPrefix(text, "/occa_")
	}
	return text
}

func (r *Router) isOccaCommand(text string) bool {
	if strings.HasPrefix(text, "/occa:") || strings.HasPrefix(text, "/occa_") {
		return true
	}
	if !strings.HasPrefix(text, "/") {
		return false
	}
	trimmed := strings.TrimPrefix(text, "/")
	parts := strings.SplitN(trimmed, " ", 2)
	name := parts[0]
	_, ok := r.commands[name]
	return ok
}

func (r *Router) routeInputKind(msg channel.IncomingMessage, isOcca bool) string {
	if msg.IsCallback {
		return "callback"
	}
	if isOcca {
		return "occa_command"
	}
	return "message"
}

// reply renders text for the destination platform before it reaches the
// adapter. Every outbound string goes through here so that a workdir path, a
// stored prompt, or an agent error containing markup characters is escaped
// rather than silently rejected by the platform's parser.
func (r *Router) reply(msg channel.IncomingMessage, text string) {
	if msg.ReplyCtx == nil {
		slog.Warn("reply has no reply context", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID)
		return
	}
	for _, chunk := range r.outbound(msg.Platform, text) {
		if _, err := msg.ReplyCtx.Send(chunk); err != nil {
			slog.Warn("failed to send reply", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "error", err)
			return
		}
	}
}

func (r *Router) outbound(platform, text string) []string {
	chunks, err := r.renderer.Render(text, render.PlatformFor(platform))
	if err != nil || len(chunks) == 0 {
		return []string{text}
	}
	return chunks
}

func (r *Router) inline(platform, text string) string {
	return strings.Join(r.outbound(platform, text), "\n")
}

func (r *Router) listenModeAllows(ctx context.Context, msg channel.IncomingMessage) bool {
	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err != nil || ch == nil {
		return msg.IsMention
	}

	switch ch.ListenMode {
	case "all":
		return true
	case "thread":
		return msg.IsThread || msg.IsMention
	default:
		return msg.IsMention
	}
}

func (r *Router) ensureAdminBootstrap(ctx context.Context, msg channel.IncomingMessage) error {
	if r.adminID == "" || msg.UserID != r.adminID {
		return nil
	}
	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, msg.ChannelID, msg.UserID)
	if err != nil {
		return fmt.Errorf("admin bootstrap lookup: %w", err)
	}
	if o == nil || o.Role != "admin" {
		if err := r.store.OverrideRepo().UpsertRole(ctx, msg.Platform, msg.ChannelID, msg.UserID, "admin"); err != nil {
			return fmt.Errorf("admin bootstrap persist: %w", err)
		}
		slog.Info("bootstrapped admin role for channel", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID)
	}
	return nil
}

func (r *Router) authorize(ctx context.Context, msg channel.IncomingMessage) error {
	if r.adminID != "" && msg.UserID == r.adminID {
		return r.ensureAdminBootstrap(ctx, msg)
	}
	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, msg.ChannelID, msg.UserID)
	if err != nil {
		return fmt.Errorf("authorize: %w", err)
	}
	if o == nil || (o.Role != "allow" && o.Role != "admin") {
		return ErrDenied
	}
	return nil
}

func (r *Router) isAdmin(ctx context.Context, msg channel.IncomingMessage) bool {
	if r.adminID != "" && msg.UserID == r.adminID {
		return true
	}
	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, msg.ChannelID, msg.UserID)
	if err != nil || o == nil {
		return false
	}
	return o.Role == "admin"
}

func (r *Router) handleCommand(ctx context.Context, msg channel.IncomingMessage) error {
	trimmed := strings.TrimPrefix(msg.Text, "/")
	parts := strings.SplitN(trimmed, " ", 2)
	name := parts[0]
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}

	cmd, ok := r.commands[name]
	if !ok {
		r.reply(msg, r.helpText())
		return nil
	}

	if cmd.Admin && !r.isAdmin(ctx, msg) {
		r.reply(msg, "⚠️ Admin access required.")
		return nil
	}

	reply, err := cmd.Handler(ctx, msg, args)
	if err != nil {
		if errors.Is(err, errReplied) {
			return nil
		}
		var replyErr *replyError
		if !errors.As(err, &replyErr) || replyErr.cause != nil {
			slog.Error("command failed", "command", name, "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "error", err)
		}
		message := "Command failed. Please try again."
		if replyErr != nil {
			message = replyErr.message
		}
		r.reply(msg, "⚠️ "+message)
		return nil
	}
	r.reply(msg, reply)
	return nil
}

// clientFor resolves the agent instance for a message's effective workdir.
func (r *Router) clientFor(ctx context.Context, msg channel.IncomingMessage) (AgentInstance, error) {
	workdir := r.effectiveWorkdir(ctx, msg.Platform, msg.ChannelID)
	return r.instances.Instance(ctx, workdir)
}

// conversationKey derives the session-key components (threadID, userID) from
// a message per the session-key policy: threads are shared per thread; other
// conversations are isolated per sender. DM chats resolve to the sender too —
// a private chat has a single sender, so the key stays per-chat in effect.
func conversationKey(msg channel.IncomingMessage) (threadID, userID string) {
	if msg.IsThread && msg.ThreadID != "" {
		return msg.ThreadID, ""
	}
	return "", msg.UserID
}

func (r *Router) passthrough(ctx context.Context, msg channel.IncomingMessage) error {
	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	taskCtx, cancel := context.WithCancel(ctx)
	if !r.responses.acquire(key, cancel) {
		cancel()
		r.reply(msg, busyResponseMessage)
		return nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		r.responses.release(key)
		r.reply(msg, "⚠️ Agent unreachable")
		return nil
	}

	resolver := relay.NewSessionResolver(r.store.SessionRepo(), inst.Client())
	sessionID, err := resolver.Resolve(ctx, msg.Platform, msg.ChannelID, threadID, userID, inst.PID())
	if err != nil {
		inst.End()
		r.responses.release(key)
		r.reply(msg, "⚠️ Agent unreachable")
		return nil
	}

	text := msg.Text
	var model *relay.ModelRef
	var attachments []relay.Attachment
	if !strings.HasPrefix(text, "/") && r.tokenGen != nil {
		token, err := r.tokenGen.Generate(msg.Platform, msg.ChannelID)
		if err != nil {
			slog.Error("failed to generate schedule token; message sent without it", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
		} else {
			text = text + "\n\n—\n<occa:schedule_token>" + token + "</occa:schedule_token> — OCCA internal metadata for scheduled-task attribution, not a credential, ignore"
		}
	}

	if !strings.HasPrefix(msg.Text, "/") {
		model, err = r.modelForMessage(ctx, msg)
		if err != nil {
			inst.End()
			r.responses.release(key)
			slog.Error("failed to resolve message model; message not sent", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "error", err)
			r.reply(msg, "⚠️ Unable to resolve model configuration. Message not sent.")
			return nil
		}
		attachments = make([]relay.Attachment, len(msg.Attachments))
		for i, a := range msg.Attachments {
			attachments[i] = relay.Attachment{Filename: a.Filename, MimeType: a.MimeType, Data: a.Data}
		}
	}

	msg.ReplyCtx.SendTyping()
	events, err := inst.Client().Events(taskCtx, sessionID)
	if err != nil || events == nil {
		cancel()
		inst.End()
		r.responses.release(key)
		r.reply(msg, "⚠️ Agent unreachable")
		return nil
	}

	dispatch := func(dispatchCtx context.Context) error {
		if strings.HasPrefix(msg.Text, "/") {
			return inst.Client().RunCommand(dispatchCtx, sessionID, msg.Text)
		}
		return inst.Client().SendMessage(dispatchCtx, sessionID, text, model, attachments)
	}

	go r.runResponse(taskCtx, cancel, key, msg, inst, sessionID, events, dispatch)
	return nil
}

func (r *Router) registerDefaults() {
	r.commands["help"] = Command{
		Name:    "help",
		Handler: r.handleHelp,
	}
	r.commands["status"] = Command{
		Name:    "status",
		Handler: r.handleStatus,
	}
	r.commands["session"] = Command{
		Name:    "session",
		Handler: r.handleSession,
	}
	r.commands["reset"] = Command{
		Name:    "reset",
		Handler: r.handleReset,
	}
	r.commands["dir"] = Command{
		Name:    "dir",
		Admin:   true,
		Handler: r.handleDir,
	}
	r.commands["allow"] = Command{
		Name:    "allow",
		Admin:   true,
		Handler: r.handleAllow,
	}
	r.commands["deny"] = Command{
		Name:    "deny",
		Admin:   true,
		Handler: r.handleDeny,
	}
	r.commands["admin"] = Command{
		Name:    "admin",
		Admin:   true,
		Handler: r.handleAdmin,
	}
	r.commands["channel"] = Command{
		Name:    "channel",
		Admin:   true,
		Handler: r.handleChannel,
	}
	r.commands["model"] = Command{
		Name:    "model",
		Handler: r.handleModel,
	}
	r.commands["variants"] = Command{
		Name:    "variants",
		Handler: r.handleVariants,
	}
	r.commands["schedules"] = Command{
		Name:    "schedules",
		Admin:   true,
		Handler: r.handleSchedules,
	}
}

// handleHelp lists OCCA's own commands and, best-effort, the connected
// agent's own commands. An unreachable agent or an empty/error result from
// ListCommands never fails the command — it just omits that section.
func (r *Router) handleHelp(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	text := r.helpText()

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return text, nil
	}
	defer inst.End()

	commands, err := inst.Client().ListCommands(ctx)
	if err != nil || len(commands) == 0 {
		return text, nil
	}

	text += "\n\nAgent commands:\n"
	for _, c := range commands {
		text += fmt.Sprintf("• /%s — %s\n", c.Name, c.Description)
	}
	return strings.TrimRight(text, "\n"), nil
}

func (r *Router) helpText() string {
	return "OCCA commands:\n" +
		"• /help — show this message\n" +
		"• /status — agent health + session info\n" +
		"• /session [list|new|switch <id>|delete <id>] — manage sessions\n" +
		"• /dir [path] — view or set this channel's working directory\n" +
		"• /channel [mention|all|thread] — view or set listen mode\n" +
		"• /model [channel] [provider/model-id[@variant]] — view or set model\n" +
		"• /variants [provider/model-id] — list and set model reasoning variants\n" +
		"• /schedules [delete <id>] — view or delete scheduled tasks\n" +
		"• /reset — clear current session and start fresh\n\n" +
		"(Legacy /occa: command aliases are also supported.)\n\n" +
		"All other messages and /commands are forwarded to the agent."
}

func (r *Router) handleStatus(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	start := time.Now()
	resolver := relay.NewSessionResolver(r.store.SessionRepo(), inst.Client())
	threadID, userID := conversationKey(msg)
	sessionID, err := resolver.Resolve(ctx, msg.Platform, msg.ChannelID, threadID, userID, inst.PID())
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	latency := time.Since(start).Truncate(time.Millisecond)

	uptime := time.Since(r.startedAt).Truncate(time.Second)
	workdir := r.effectiveWorkdir(ctx, msg.Platform, msg.ChannelID)

	return fmt.Sprintf("✅ Agent connected\nSession: %s\nUptime: %s\nWorkdir: %s\nLatency: %s",
		sessionID, uptime, workdir, latency), nil
}

func (r *Router) handleSession(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	sub := "list"
	if len(parts) > 0 {
		sub = parts[0]
	}

	switch sub {
	case "list":
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session list: %w", err)
		}
		if len(sessions) == 0 {
			return "No sessions yet.", nil
		}
		var sb strings.Builder
		sb.WriteString("Sessions:\n")
		for _, s := range sessions {
			marker := "  "
			if s.Active {
				marker = "→ "
			}
			sb.WriteString(fmt.Sprintf("%s%s (created %d)\n", marker, s.AgentSessionID, s.CreatedAt))
		}
		return strings.TrimRight(sb.String(), "\n"), nil

	case "new":
		threadID, userID := conversationKey(msg)
		r.responses.cancelResponse(responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID})

		inst, err := r.clientFor(ctx, msg)
		if err != nil {
			return "⚠️ Agent unreachable", nil
		}
		defer inst.End()
		sessionID, err := inst.Client().CreateSession(ctx)
		if err != nil {
			return "⚠️ Agent unreachable", nil
		}
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, threadID, userID, sessionID, inst.PID()); err != nil {
			return "", fmt.Errorf("session new: %w", err)
		}
		return fmt.Sprintf("✅ New session: %s", sessionID), nil

	case "switch":
		if len(parts) < 2 {
			return "Usage: /session switch <id>", nil
		}
		target := parts[1]
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}
		found := false
		for _, s := range sessions {
			if s.AgentSessionID == target {
				found = true
				break
			}
		}
		if !found {
			return "Session not found.", nil
		}
		threadID, userID := conversationKey(msg)
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, threadID, userID, target, 0); err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}
		return fmt.Sprintf("✅ Switched to: %s", target), nil

	case "delete":
		if len(parts) < 2 {
			return "Usage: /session delete <id>", nil
		}
		target := parts[1]
		sessions, err := r.store.SessionRepo().List(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("session delete: %w", err)
		}
		for _, s := range sessions {
			if s.AgentSessionID == target {
				if err := r.store.SessionRepo().Delete(ctx, s.ID); err != nil {
					return "", fmt.Errorf("session delete: %w", err)
				}
				return fmt.Sprintf("✅ Deleted: %s", target), nil
			}
		}
		return "Session not found.", nil

	default:
		return "Usage: /session [list|new|switch <id>|delete <id>]", nil
	}
}

func (r *Router) handleReset(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	threadID, userID := conversationKey(msg)
	r.responses.cancelResponse(responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID})

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	sessionID, err := inst.Client().CreateSession(ctx)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, threadID, userID, sessionID, inst.PID()); err != nil {
		return "", fmt.Errorf("reset: %w", err)
	}
	return fmt.Sprintf("✅ Session reset. New session: %s", sessionID), nil
}

func (r *Router) handleAllow(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	userID := strings.TrimSpace(args)
	if userID == "" {
		return "Usage: /allow <user_id>", nil
	}
	err := r.store.OverrideRepo().UpsertRole(ctx, msg.Platform, msg.ChannelID, userID, "allow")
	if err != nil {
		return "", fmt.Errorf("allow: %w", err)
	}
	return fmt.Sprintf("✅ Allowed user: %s", userID), nil
}

func (r *Router) handleDeny(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	userID := strings.TrimSpace(args)
	if userID == "" {
		return "Usage: /deny <user_id>", nil
	}

	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, msg.ChannelID, userID)
	if err != nil {
		return "", fmt.Errorf("deny: %w", err)
	}
	if o != nil && o.Role == "admin" {
		admins, err := r.countAdmins(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("deny: %w", err)
		}
		if admins <= 1 {
			return "⚠️ Cannot deny the last admin.", nil
		}
	}

	err = r.store.OverrideRepo().UpsertRole(ctx, msg.Platform, msg.ChannelID, userID, "deny")
	if err != nil {
		return "", fmt.Errorf("deny: %w", err)
	}
	return fmt.Sprintf("✅ Denied user: %s", userID), nil
}

func (r *Router) handleAdmin(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	userID := strings.TrimSpace(args)
	if userID == "" {
		return "Usage: /admin <user_id>", nil
	}
	err := r.store.OverrideRepo().UpsertRole(ctx, msg.Platform, msg.ChannelID, userID, "admin")
	if err != nil {
		return "", fmt.Errorf("admin: %w", err)
	}
	return fmt.Sprintf("✅ Granted admin: %s", userID), nil
}

func (r *Router) countAdmins(ctx context.Context, platform, channelID string) (int, error) {
	overrides, err := r.store.OverrideRepo().ListByChannel(ctx, platform, channelID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, o := range overrides {
		if o.Role == "admin" {
			count++
		}
	}
	return count, nil
}
