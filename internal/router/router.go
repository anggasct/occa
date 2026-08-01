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
	accessDeniedMessage = "⚠️ Access denied. Ask an admin to /occa:allow you."
	accessVerifyMessage = "⚠️ Unable to verify access. Try again."
)

type Command struct {
	Name    string
	Admin   bool
	Handler func(ctx context.Context, msg channel.IncomingMessage, args string) (string, error)
}

// AgentInstance is a ready agent backend handle for a working directory.
type AgentInstance interface {
	Client() relay.Client
	End()
}

// InstanceProvider resolves an AgentInstance for a working directory.
type InstanceProvider interface {
	Instance(ctx context.Context, workdir string) (AgentInstance, error)
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
	renderer       render.Renderer
}

type ScheduleStore interface {
	ListSchedules(ctx context.Context, platform, channelID string) ([]store.Schedule, error)
	RemoveSchedule(ctx context.Context, platform, channelID string, id int64) error
}

type TokenGenerator interface {
	Generate(platform, channelID string) string
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
		renderer:       render.New(),
	}
	r.registerDefaults()
	return r
}

func (r *Router) Route(ctx context.Context, msg channel.IncomingMessage) error {
	inputKind := routeInputKind(msg)
	if err := r.authorize(ctx, msg); err != nil {
		if errors.Is(err, ErrDenied) {
			slog.Info("access denied", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "input_kind", inputKind, "outcome", "denied")
			r.reply(msg, accessDeniedMessage)
			return nil
		}

		slog.Error("access verification failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "input_kind", inputKind, "outcome", "error", "error", err)
		r.reply(msg, accessVerifyMessage)
		return nil
	}

	if msg.IsCallback {
		return r.handleCallback(ctx, msg)
	}

	if strings.HasPrefix(msg.Text, "/occa:") {
		return r.handleCommand(ctx, msg)
	}

	if !r.listenModeAllows(ctx, msg) {
		return nil
	}

	return r.passthrough(ctx, msg)
}

func routeInputKind(msg channel.IncomingMessage) string {
	if msg.IsCallback {
		return "callback"
	}
	if strings.HasPrefix(msg.Text, "/occa:") {
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
	trimmed := strings.TrimPrefix(msg.Text, "/occa:")
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

func (r *Router) passthrough(ctx context.Context, msg channel.IncomingMessage) error {
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID}
	if !r.responses.acquire(key) {
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
	sessionID, err := resolver.Resolve(ctx, msg.Platform, msg.ChannelID)
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
		token := r.tokenGen.Generate(msg.Platform, msg.ChannelID)
		text = text + "\n\n—\nOCCA schedule token: " + token
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
	taskCtx, cancel := context.WithCancel(ctx)
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
		Handler: func(_ context.Context, _ channel.IncomingMessage, _ string) (string, error) { return r.helpText(), nil },
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
	r.commands["schedules"] = Command{
		Name:    "schedules",
		Admin:   true,
		Handler: r.handleSchedules,
	}
}

func (r *Router) helpText() string {
	return "OCCA commands:\n" +
		"• /occa:help — show this message\n" +
		"• /occa:status — agent health + session info\n" +
		"• /occa:session [list|new|switch <id>|delete <id>] — manage sessions\n" +
		"• /occa:dir [path] — view or set this channel's working directory\n" +
		"• /occa:channel [mention|all|thread] — view or set listen mode\n" +
		"• /occa:model [channel] [provider/model-id] — view or set model\n" +
		"• /occa:schedules [delete <id>] — view or delete scheduled tasks\n" +
		"• /occa:reset — clear current session and start fresh\n\n" +
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
	sessionID, err := resolver.Resolve(ctx, msg.Platform, msg.ChannelID)
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
		inst, err := r.clientFor(ctx, msg)
		if err != nil {
			return "⚠️ Agent unreachable", nil
		}
		defer inst.End()
		sessionID, err := inst.Client().CreateSession(ctx)
		if err != nil {
			return "⚠️ Agent unreachable", nil
		}
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, sessionID); err != nil {
			return "", fmt.Errorf("session new: %w", err)
		}
		return fmt.Sprintf("✅ New session: %s", sessionID), nil

	case "switch":
		if len(parts) < 2 {
			return "Usage: /occa:session switch <id>", nil
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
		if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, target); err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}
		return fmt.Sprintf("✅ Switched to: %s", target), nil

	case "delete":
		if len(parts) < 2 {
			return "Usage: /occa:session delete <id>", nil
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
		return "Usage: /occa:session [list|new|switch <id>|delete <id>]", nil
	}
}

func (r *Router) handleReset(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	sessionID, err := inst.Client().CreateSession(ctx)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, sessionID); err != nil {
		return "", fmt.Errorf("reset: %w", err)
	}
	return fmt.Sprintf("✅ Session reset. New session: %s", sessionID), nil
}

func (r *Router) handleAllow(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	userID := strings.TrimSpace(args)
	if userID == "" {
		return "Usage: /occa:allow <user_id>", nil
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
		return "Usage: /occa:deny <user_id>", nil
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
		return "Usage: /occa:admin <user_id>", nil
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
