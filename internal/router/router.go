package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/attribution"
	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/store"
)

var ErrDenied = errors.New("access denied")

const (
	maxPickerSessions   = 6
	maxPickerPages      = 5
	accessDeniedMessage = "⚠️ Access denied. Ask an admin to /allow you."
	accessVerifyMessage = "⚠️ Unable to verify access. Try again."
)

type Command struct {
	Name    string
	Admin   bool
	Handler func(ctx context.Context, msg channel.IncomingMessage, args string) (string, error)
}

func (r *Router) MenuCommands() []channel.MenuCommand {
	return []channel.MenuCommand{
		{Alias: "help", Description: "Show available commands"},
		{Alias: "status", Description: "Agent health and session info"},
		{Alias: "session", Description: "Manage sessions: new, switch, or delete", HasArgs: true},
		{Alias: "stop", Description: "Stop the running response (session kept)"},
		{Alias: "steer", Description: "Stop and redirect the agent (session kept)", HasArgs: true},
		{Alias: "compact", Description: "Compact the current session context"},
		{Alias: "undo", Description: "Undo the last turn (message + file changes)"},
		{Alias: "redo", Description: "Restore a reverted turn"},
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

type AgentInstance interface {
	Client() relay.Client
	End()
	PID() int
	Workdir() string
}

type InstanceProvider interface {
	Instance(ctx context.Context, workdir string) (AgentInstance, error)
	ForceStop(workdir string)
}

type Router struct {
	commands               map[string]Command
	instances              InstanceProvider
	store                  store.Store
	defaultWorkdir         string
	adminID                string
	startedAt              time.Time
	sched                  ScheduleStore
	attrib                 *attribution.Store
	responses              *responseCoordinator
	permissions            *permissionBroker
	questions              *questionBroker
	modelBrowser           *modelBrowserBroker
	renderer               render.Renderer
	streamerNoEventTimeout time.Duration
}

type ScheduleStore interface {
	ListSchedules(ctx context.Context, platform, channelID string) ([]store.Schedule, error)
	RemoveSchedule(ctx context.Context, platform, channelID string, id int64) error
}

func (r *Router) SetScheduler(s ScheduleStore) {
	r.sched = s
}

func (r *Router) SetAttributionStore(s *attribution.Store) {
	r.attrib = s
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
	// Drop messages when the surrounding context is already canceled (process
	// shutdown in progress): any store/agent work would only fail with
	// "context canceled" and emit misleading WARNs while the channel adapters
	// are being torn down.
	if err := ctx.Err(); err != nil {
		return nil
	}
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

	if isOwnedThreadMessage(msg) {
		if err := r.ensureThreadConfig(ctx, msg); err != nil {
			slog.Warn("router: materialize thread config failed", "platform", msg.Platform, "thread_id", msg.ThreadID, "error", err)
		}
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
	mode, err := r.effectiveListenMode(ctx, msg)
	if err != nil {
		slog.Warn("router: listen mode resolution failed; message not processed", "platform", msg.Platform, "channel_id", msg.ChannelID, "thread_id", msg.ThreadID, "error", err)
		return false
	}
	switch mode {
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

func (r *Router) clientFor(ctx context.Context, msg channel.IncomingMessage) (AgentInstance, error) {
	workdir, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("router: resolve workdir: %w", err)
	}
	return r.instances.Instance(ctx, workdir)
}

func conversationKey(msg channel.IncomingMessage) (threadID, userID string) {
	if msg.IsThread && msg.ThreadID != "" {
		return msg.ThreadID, ""
	}
	return "", msg.UserID
}

func (r *Router) passthrough(ctx context.Context, msg channel.IncomingMessage) error {
	// Shutdown race guard: when the root context is already canceled (process
	// shut down mid-drain), do not acquire a response slot or touch the store
	// with a dead context — drop the message quietly.
	if err := ctx.Err(); err != nil {
		return nil
	}
	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	taskCtx, cancel := context.WithCancel(ctx)
	if !r.responses.acquire(key, cancel) {
		cancel()
		depth, ok := r.responses.enqueue(key, ctx, msg)
		if ok {
			r.reply(msg, fmt.Sprintf("⏳ Queued — %d message(s) will run after the current response finishes.", depth))
			return nil
		}
		r.reply(msg, busyResponseMessage)
		return nil
	}

	if err := r.executePassthrough(taskCtx, cancel, key, msg); err != nil {
		// The canceled-after-acquire guard already released the slot and
		// handled the queue; a canceled drop is not a routing failure, so
		// keep shutdown quiet instead of logging a "route error".
		if errors.Is(err, errPassthroughCanceled) {
			return nil
		}
		return err
	}
	return nil
}

func (r *Router) dispatchDrained(key responseKey, drained []queuedMessage) {
	for i, qmsg := range drained {
		if r.passthroughQueued(qmsg) {
			if len(drained[i+1:]) > 0 {
				r.responses.requeuePrefix(key, drained[i+1:])
			}
			return
		}
	}
}

func (r *Router) passthroughQueued(qmsg queuedMessage) bool {
	// Shutdown race guard: a queued message whose context is already canceled
	// (process shutting down) must not be dispatched — the store/agent work
	// would only fail with "context canceled" and emit misleading WARNs.
	if err := qmsg.ctx.Err(); err != nil {
		return false
	}
	threadID, userID := conversationKey(qmsg.msg)
	key := responseKey{platform: qmsg.msg.Platform, channelID: qmsg.msg.ChannelID, threadID: threadID, userID: userID}
	taskCtx, cancel := context.WithCancel(qmsg.ctx)
	if !r.responses.acquire(key, cancel) {
		cancel()
		return false
	}
	if err := r.executePassthrough(taskCtx, cancel, key, qmsg.msg); err != nil {
		if errors.Is(err, errPassthroughCanceled) {
			// The message was dropped by the canceled-after-acquire guard, not
			// dispatched. Report it as not dispatched so the caller can
			// continue with the remaining FIFO entries instead of treating the
			// nil return as a successful dispatch.
			return false
		}
		slog.Error("queued message dispatch failed", "platform", key.platform, "channel_id", key.channelID, "user_id", key.userID, "error", err)
	}
	return true
}

func (r *Router) executePassthrough(taskCtx context.Context, cancel context.CancelFunc, key responseKey, msg channel.IncomingMessage) error {
	ctx := taskCtx
	threadID, userID := key.threadID, key.userID

	// Shutdown race guard (defense in depth): if the task context was canceled
	// between acquire and here (e.g. process shutdown began), release the slot
	// and drop the message instead of running store/agent work that will fail
	// with "context canceled". Another request may have enqueued in that
	// window, so mirror normal response finalization: drain the FIFO queue and
	// redispatch any entry whose context can still run. Entries with canceled
	// contexts are intentionally discarded (their work would only fail with
	// "context canceled" during shutdown) — nothing is left stranded.
	if err := ctx.Err(); err != nil {
		cancel()
		r.responses.release(key)
		r.dispatchDrained(key, r.responses.drain(key))
		return errPassthroughCanceled
	}

	if isOwnedThreadMessage(msg) {
		if err := r.ensureThreadConfig(ctx, msg); err != nil {
			slog.Warn("router: materialize thread config at session activation failed", "platform", msg.Platform, "thread_id", msg.ThreadID, "error", err)
		}
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		r.responses.release(key)
		r.reply(msg, "⚠️ Agent unreachable")
		r.dispatchDrained(key, r.responses.drain(key))
		return nil
	}

	preResolveActiveID, _, _ := r.store.SessionRepo().Active(ctx, msg.Platform, msg.ChannelID, threadID, userID)

	resolver := relay.NewSessionResolver(r.store.SessionRepo(), inst.Client())
	sessionID, err := resolver.Resolve(ctx, msg.Platform, msg.ChannelID, threadID, userID, inst.PID())
	if err != nil {
		inst.End()
		r.responses.release(key)
		r.reply(msg, "⚠️ Agent unreachable")
		r.dispatchDrained(key, r.responses.drain(key))
		return nil
	}

	if preResolveActiveID == "" {
		title := strings.Join(strings.Fields(msg.Text), " ")
		title = truncateRunes(title, 60)
		if title != "" {
			sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID)
			if err == nil {
				for _, s := range sessions {
					if s.AgentSessionID == sessionID {
						if setErr := r.store.SessionRepo().SetTitle(ctx, s.ID, title); setErr != nil {
							slog.Warn("router: set session title failed", "session_id", sessionID, "error", setErr)
						}
						break
					}
				}
			}
		}
	}

	text := msg.Text
	var model *relay.ModelRef
	var attachments []relay.Attachment

	if !strings.HasPrefix(msg.Text, "/") {
		model, err = r.modelForMessage(ctx, msg)
		if err != nil {
			inst.End()
			r.responses.release(key)
			slog.Error("failed to resolve message model; message not sent", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "error", err)
			r.reply(msg, "⚠️ Unable to resolve model configuration. Message not sent.")
			r.dispatchDrained(key, r.responses.drain(key))
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
		r.dispatchDrained(key, r.responses.drain(key))
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
	r.commands["stop"] = Command{
		Name:    "stop",
		Handler: r.handleStop,
	}
	r.commands["steer"] = Command{
		Name:    "steer",
		Handler: r.handleSteer,
	}
	r.commands["compact"] = Command{
		Name:    "compact",
		Handler: r.handleCompact,
	}
	r.commands["undo"] = Command{
		Name:    "undo",
		Handler: r.handleUndo,
	}
	r.commands["redo"] = Command{
		Name:    "redo",
		Handler: r.handleRedo,
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
		"• /session [new|switch <id|#|title>|delete <id>] — manage sessions\n" +
		"• /stop — stop the running response (session kept)\n" +
		"• /steer <direction> — stop and redirect the agent (session kept)\n" +
		"• /compact — compact the current session context\n" +
		"• /undo — undo the last turn (message + file changes)\n" +
		"• /redo — restore a reverted turn\n" +
		"• /dir [path] — view or set working directory (per location)\n" +
		"• /channel [mention|all|thread] — view or set listen mode (per location)\n" +
		"• /model [provider/model-id[@variant]] — view or set model (per location)\n" +
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

	var modelLine, contextLine string
	if sessInfo, err := inst.Client().GetSession(ctx, sessionID); err != nil {
		slog.Warn("router: status get session failed", "session_id", sessionID, "error", err)
	} else if sessInfo != nil {
		var providerID, modelID, variant string
		if sessInfo.Model.ProviderID != "" && sessInfo.Model.ID != "" {
			providerID = sessInfo.Model.ProviderID
			modelID = sessInfo.Model.ID
			variant = sessInfo.Model.Variant
		} else if effModel, effErr := r.effectiveModel(ctx, msg); effErr == nil && effModel != nil {
			providerID = effModel.ProviderID
			modelID = effModel.ID
			variant = effModel.Variant
		}
		if providerID != "" && modelID != "" {
			modelName := providerID + "/" + modelID
			if variant != "" {
				modelName += "@" + variant
			}
			modelLine = "\nModel: " + modelName
		}
		inputK := float64(sessInfo.Tokens.Input) / 1000.0
		cachePart := ""
		if sessInfo.Tokens.CacheRead > 0 {
			cachePart = " · cache read: " + formatMetric(sessInfo.Tokens.CacheRead)
		}
		contextLine = fmt.Sprintf("\nInput: %.1fk tokens (cumulative)%s · cost: $%.2f", inputK, cachePart, sessInfo.Cost)
	}

	uptime := time.Since(r.startedAt).Truncate(time.Second)
	workdir, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("router: status workdir: %w", err)
	}

	sessionLine := sessionID
	if sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID); err == nil {
		for _, s := range sessions {
			if s.Active && s.AgentSessionID == sessionID && s.Title != "" {
				sessionLine = fmt.Sprintf("%s (%s)", s.Title, sessionID)
				break
			}
		}
	}

	status := fmt.Sprintf("✅ Agent connected\nSession: %s%s%s\nUptime: %s\nWorkdir: %s\nLatency: %s",
		sessionLine, modelLine, contextLine, uptime, workdir, latency)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	if qLen := r.responses.queueDepth(key); qLen > 0 {
		status += fmt.Sprintf("\nQueue: %d message(s)", qLen)
	}
	return status, nil
}

// formatMetric renders a raw token count in a human-friendly k/M suffix
// (e.g. 19400000 -> "19.4M", 12000 -> "12.0k"), used by the /status cache readout.
func formatMetric(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000.0)
	}
	return fmt.Sprintf("%.1fk", float64(n)/1000.0)
}

func relativeAge(createdAt int64) string {
	if createdAt <= 0 {
		return "0m ago"
	}
	d := time.Since(time.Unix(createdAt, 0))
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return "0m ago"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	return fmt.Sprintf("%dd ago", days)
}

func sessionPickerTotalPages(totalSessions int) int {
	if totalSessions <= 0 {
		return 1
	}
	pages := (totalSessions + maxPickerSessions - 1) / maxPickerSessions
	if pages > maxPickerPages {
		pages = maxPickerPages
	}
	return pages
}

func sessionPickerPageBounds(totalSessions, page int) (start int, end int, clampedPage int) {
	totalPages := sessionPickerTotalPages(totalSessions)
	clampedPage = page
	if clampedPage < 1 {
		clampedPage = 1
	}
	if clampedPage > totalPages {
		clampedPage = totalPages
	}

	if totalSessions <= 0 {
		return 0, 0, clampedPage
	}

	start = (clampedPage - 1) * maxPickerSessions
	if start > totalSessions {
		start = totalSessions
	}
	end = start + maxPickerSessions
	if end > totalSessions {
		end = totalSessions
	}
	return start, end, clampedPage
}

func (r *Router) buildSessionPickerPage(ctx context.Context, msg channel.IncomingMessage, page int, headerOverride ...string) (string, []channel.Button, error) {
	threadID, userID := conversationKey(msg)
	sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	if err != nil {
		return "", nil, fmt.Errorf("session picker: %w", err)
	}
	if len(sessions) == 0 {
		return "No sessions yet.", nil, nil
	}

	totalPages := sessionPickerTotalPages(len(sessions))
	start, end, clampedPage := sessionPickerPageBounds(len(sessions), page)

	header := "Sessions:"
	if len(headerOverride) > 0 && headerOverride[0] != "" {
		header = headerOverride[0]
	} else {
		header = fmt.Sprintf("Page %d/%d · Sessions", clampedPage, totalPages)
	}

	var sb strings.Builder
	sb.WriteString(header)
	sb.WriteString("\n")

	var buttons []channel.Button
	for i := start; i < end; i++ {
		s := sessions[i]
		num := i + 1
		marker := "  "
		if s.Active {
			marker = "→ "
		}
		label := s.AgentSessionID
		if s.Title != "" {
			label = s.Title
		}
		age := relativeAge(s.CreatedAt)
		fmt.Fprintf(&sb, "%d. %s%s (%s)\n", num, marker, label, age)

		buttons = append(buttons, channel.Button{
			Label: fmt.Sprintf("%d", num),
			Value: "switch:" + s.AgentSessionID,
			Row:   1,
		})
	}

	if clampedPage > 1 {
		buttons = append(buttons, channel.Button{
			Label: "◀️ Prev",
			Value: fmt.Sprintf("spage:%d", clampedPage-1),
			Row:   2,
		})
	}
	if clampedPage < totalPages {
		buttons = append(buttons, channel.Button{
			Label: "Next ▶️",
			Value: fmt.Sprintf("spage:%d", clampedPage+1),
			Row:   2,
		})
	}

	text := strings.TrimRight(sb.String(), "\n")
	return text, buttons, nil
}

func (r *Router) renderSessionPickerPage(ctx context.Context, msg channel.IncomingMessage, page int, headerOverride ...string) (string, error) {
	text, buttons, err := r.buildSessionPickerPage(ctx, msg, page, headerOverride...)
	if err != nil {
		return "", err
	}
	if msg.ReplyCtx != nil {
		if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
			return "", err
		}
		return "", errReplied
	}
	return text, nil
}

func (r *Router) renderSessionPicker(ctx context.Context, msg channel.IncomingMessage, headerOverride ...string) (string, error) {
	return r.renderSessionPickerPage(ctx, msg, 1, headerOverride...)
}

func (r *Router) switchSession(ctx context.Context, msg channel.IncomingMessage, targetSession *store.Session) (string, error) {
	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	r.responses.cancelResponse(key)
	drained := r.responses.drain(key)

	if err := r.store.SessionRepo().SetActive(ctx, msg.Platform, msg.ChannelID, threadID, userID, targetSession.AgentSessionID, 0); err != nil {
		return "", fmt.Errorf("session switch: %w", err)
	}

	var reply string
	if targetSession.Title != "" {
		reply = fmt.Sprintf("✅ Switched to %s (%s)", targetSession.Title, targetSession.AgentSessionID)
	} else {
		reply = fmt.Sprintf("✅ Switched to: %s", targetSession.AgentSessionID)
	}

	if len(drained) > 0 {
		reply += fmt.Sprintf("\nCleared %d queued message(s) from the previous session.", len(drained))
	}
	return reply, nil
}

func (r *Router) handleSwitchCallback(ctx context.Context, msg channel.IncomingMessage) error {
	targetID := strings.TrimPrefix(msg.CallbackData, "switch:")
	threadID, userID := conversationKey(msg)
	sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	if err != nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "Session not found.", nil)
		}
		return nil
	}
	var targetSession *store.Session
	for i := range sessions {
		if sessions[i].AgentSessionID == targetID {
			targetSession = &sessions[i]
			break
		}
	}
	if targetSession == nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "Session not found.", nil)
		}
		r.reply(msg, "Session not found.")
		return nil
	}

	replyText, err := r.switchSession(ctx, msg, targetSession)
	if err != nil {
		return err
	}

	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, replyText, nil)
	}
	r.reply(msg, replyText)
	return nil
}

func (r *Router) handleSessionPageCallback(ctx context.Context, msg channel.IncomingMessage) error {
	pageStr := strings.TrimPrefix(msg.CallbackData, "spage:")
	page, err := strconv.Atoi(pageStr)
	if err != nil {
		page = 1
	}

	text, buttons, err := r.buildSessionPickerPage(ctx, msg, page)
	if err != nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "Error loading sessions.", nil)
		}
		return err
	}

	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	r.reply(msg, text)
	return nil
}

func (r *Router) handleSession(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 {
		return r.renderSessionPicker(ctx, msg)
	}

	sub := parts[0]
	if page, err := strconv.Atoi(sub); err == nil && page >= 1 {
		return r.renderSessionPickerPage(ctx, msg, page)
	}

	switch sub {
	case "new":
		threadID, userID := conversationKey(msg)
		key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
		r.responses.cancelResponse(key)
		r.responses.drain(key)

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
			return "Usage: /session switch <id|#|title>", nil
		}
		target := strings.Join(parts[1:], " ")
		threadID, userID := conversationKey(msg)
		sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID)
		if err != nil {
			return "", fmt.Errorf("session switch: %w", err)
		}

		var matched *store.Session
		for i := range sessions {
			if sessions[i].AgentSessionID == target {
				matched = &sessions[i]
				break
			}
		}

		if matched == nil {
			if num, err := strconv.Atoi(target); err == nil && num >= 1 {
				maxBrowsable := len(sessions)
				if maxBrowsable > maxPickerPages*maxPickerSessions {
					maxBrowsable = maxPickerPages * maxPickerSessions
				}
				if num <= maxBrowsable {
					matched = &sessions[num-1]
				}
			}
		}

		if matched == nil {
			lowerTarget := strings.ToLower(target)
			var titleMatches []store.Session
			for _, s := range sessions {
				if s.Title != "" && strings.Contains(strings.ToLower(s.Title), lowerTarget) {
					titleMatches = append(titleMatches, s)
				}
			}
			if len(titleMatches) == 1 {
				matched = &titleMatches[0]
			} else if len(titleMatches) > 1 {
				header := fmt.Sprintf("Multiple sessions match %q — pick one:", target)
				return r.renderSessionPicker(ctx, msg, header)
			}
		}

		if matched == nil {
			return "Session not found.", nil
		}

		return r.switchSession(ctx, msg, matched)

	case "delete":
		if len(parts) < 2 {
			return "Usage: /session delete <id>", nil
		}
		target := parts[1]
		threadID, userID := conversationKey(msg)
		sessions, err := r.store.SessionRepo().ListConversation(ctx, msg.Platform, msg.ChannelID, threadID, userID)
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
		return "Usage: /session [new|switch <id|#|title>|delete <id>]", nil
	}
}

func (r *Router) handleReset(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	r.responses.cancelResponse(key)
	r.responses.drain(key)

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

func (r *Router) resolveActiveSession(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	threadID, userID := conversationKey(msg)
	sessionID, _, err := r.store.SessionRepo().Active(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	return sessionID, err
}

func (r *Router) handleStop(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	r.responses.cancelResponse(key)

	sessionID, err := r.resolveActiveSession(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("stop: %w", err)
	}
	if sessionID == "" {
		return "✅ Nothing running to stop (no active session).", nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	if err := inst.Client().AbortSession(ctx, sessionID); err != nil {
		if errors.Is(err, relay.ErrNotFound) {
			slog.Warn("stop abort: session not found on agent", "session_id", sessionID, "error", err)
			return "✅ Stopped. Your conversation is kept — send a message to continue.", nil
		}
		return "", fmt.Errorf("stop abort: %w", err)
	}
	return "✅ Stopped. Your conversation is kept — send a message to continue.", nil
}

func (r *Router) handleSteer(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	direction := strings.TrimSpace(args)
	if direction == "" {
		return "Usage: /steer <direction>", nil
	}

	threadID, userID := conversationKey(msg)
	key := responseKey{platform: msg.Platform, channelID: msg.ChannelID, threadID: threadID, userID: userID}
	r.responses.cancelResponse(key)

	sessionID, err := r.resolveActiveSession(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("steer: %w", err)
	}

	if sessionID != "" {
		inst, err := r.clientFor(ctx, msg)
		if err != nil {
			return "⚠️ Agent unreachable", nil
		}
		if err := inst.Client().AbortSession(ctx, sessionID); err != nil {
			slog.Warn("steer abort failed", "session_id", sessionID, "error", err)
		}
		inst.End()
	}

	prompt := direction
	if sessionID != "" {
		prompt = "New direction (previous task cancelled): " + direction
	}

	steerMsg := msg
	steerMsg.Text = prompt
	if err := r.passthrough(ctx, steerMsg); err != nil {
		return "", fmt.Errorf("steer: %w", err)
	}
	return "", errReplied
}

func (r *Router) handleCompact(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	sessionID, err := r.resolveActiveSession(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("compact: %w", err)
	}
	if sessionID == "" {
		return "⚠️ No active session to compact — start a conversation first.", nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	var providerID, modelID string
	sessInfo, err := inst.Client().GetSession(ctx, sessionID)
	if err == nil && sessInfo != nil && sessInfo.Model.ProviderID != "" && sessInfo.Model.ID != "" {
		providerID = sessInfo.Model.ProviderID
		modelID = sessInfo.Model.ID
	} else {
		effModel, effErr := r.modelForMessage(ctx, msg)
		if effErr == nil && effModel != nil {
			providerID = effModel.ProviderID
			modelID = effModel.ID
		}
	}

	if providerID == "" || modelID == "" {
		return "⚠️ Unable to resolve model configuration for compact.", nil
	}

	if err := inst.Client().SummarizeSession(ctx, sessionID, providerID, modelID); err != nil {
		return fmt.Sprintf("⚠️ Failed to compact session: %v", err), nil
	}
	return "✅ Session compacted — context summarized.", nil
}

func (r *Router) handleUndo(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	sessionID, err := r.resolveActiveSession(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("undo: %w", err)
	}
	if sessionID == "" {
		return "⚠️ Nothing to undo (no active session).", nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	messages, err := inst.Client().ListMessages(ctx, sessionID)
	if err != nil {
		return fmt.Sprintf("⚠️ Failed to list messages: %v", err), nil
	}

	var lastUserMsg *relay.MessageInfo
	for i := range messages {
		if messages[i].Role == "user" {
			if lastUserMsg == nil || messages[i].Created >= lastUserMsg.Created {
				lastUserMsg = &messages[i]
			}
		}
	}
	if lastUserMsg == nil {
		return "⚠️ Nothing to undo yet.", nil
	}

	if err := inst.Client().RevertMessage(ctx, sessionID, lastUserMsg.ID); err != nil {
		return fmt.Sprintf("⚠️ Failed to undo last turn: %v", err), nil
	}
	return "✅ Last turn undone (message + file changes reverted).", nil
}

func (r *Router) handleRedo(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	sessionID, err := r.resolveActiveSession(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("redo: %w", err)
	}
	if sessionID == "" {
		return "⚠️ No active session — start a conversation first.", nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	if err := inst.Client().UnrevertSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("⚠️ Failed to restore turn: %v", err), nil
	}
	return "✅ Reverted turns restored.", nil
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
