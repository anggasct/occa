package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/anggasct/occa/internal/attribution"
	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/channel/discord"
	"github.com/anggasct/occa/internal/channel/telegram"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/health"
	"github.com/anggasct/occa/internal/logging"
	"github.com/anggasct/occa/internal/loop"
	"github.com/anggasct/occa/internal/mcpserver"
	"github.com/anggasct/occa/internal/process"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/router"
	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/anggasct/occa/internal/webhook"
)

type managerProvider struct{ m *process.Manager }

func (p managerProvider) Instance(ctx context.Context, workdir string) (router.AgentInstance, error) {
	return p.m.Instance(ctx, workdir)
}

func (p managerProvider) ForceStop(workdir string) {
	p.m.ForceStop(workdir)
}

type agentProbe struct {
	manager *process.Manager
	workdir string
}

func (p agentProbe) Running(_ context.Context) (int, bool, error) {
	pid, ok := p.manager.Running(p.workdir)
	return pid, ok, nil
}

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

// resolveWebhookWorkdir selects the agent instance workdir for a webhook
// delivery: a worktree lease wins outright; without one the endpoint
// channel's own workdir isolates different repositories' reviewer turns on
// separate agent instances, and the agent default workdir is the last
// fallback. A channel lookup that fails, misses, or carries no workdir
// falls back to the default — never fails the delivery.
func resolveWebhookWorkdir(ctx context.Context, channels channelStore, defaultWorkdir, platform, channelID string, leaseWorkdir string) string {
	if leaseWorkdir != "" {
		return leaseWorkdir
	}
	chRow, err := channels.Get(ctx, platform, channelID)
	switch {
	case err != nil:
		slog.Warn("webhook: channel workdir lookup failed; using default workdir",
			"platform", platform, "channel_id", channelID, "error", err)
	case chRow == nil:
		slog.Warn("webhook: channel row not found; using default workdir",
			"platform", platform, "channel_id", channelID)
	case chRow.Workdir != "":
		slog.Info("webhook: resolved channel workdir",
			"platform", platform, "channel_id", channelID, "workdir", chRow.Workdir)
		return chRow.Workdir
	default:
		slog.Warn("webhook: channel workdir empty; using default workdir",
			"platform", platform, "channel_id", channelID)
	}
	return defaultWorkdir
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "db" {
		os.Exit(runDBCommand(os.Args[2:]))
	}
	if len(os.Args) >= 3 && os.Args[1] == "webhooks" && os.Args[2] == "check-config" {
		os.Exit(runWebhooksCheckConfig(os.Args[3:]))
	}

	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file (default ~/.occa/config.yaml)")
	flag.StringVar(&configPath, "c", "", "path to config file (shorthand)")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa: %v\n", err)
		os.Exit(1)
	}

	telegramToken := os.Getenv("OCCA_TELEGRAM_TOKEN")
	discordToken := os.Getenv("OCCA_DISCORD_TOKEN")
	if telegramToken == "" && discordToken == "" {
		fmt.Fprintln(os.Stderr, "occa: at least one of OCCA_TELEGRAM_TOKEN or OCCA_DISCORD_TOKEN must be set")
		os.Exit(1)
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(logging.NewRedactHandler(handler, telegramToken, discordToken)))

	db, dbLock, err := openStoreWithLock(cfg.Database.Path, cfg.Agent.DefaultWorkdir, store.UsageRetention{
		Retention: cfg.Runtime.Store.UsageRetention,
		MaxRows:   cfg.Runtime.Store.UsageMaxRows,
	}, cfg.Runtime.Store.RecoveryEventRetention)
	if err != nil {
		slog.Error("failed to lock or open store", "error", err)
		os.Exit(1)
	}
	defer func() {
		_ = db.Close()
		_ = dbLock.Unlock()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	manager, err := process.DefaultManager(cfg.Agent, process.Config{
		ReadinessTimeout:   cfg.Runtime.Process.ReadinessTimeout,
		StopGrace:          cfg.Runtime.Process.StopGrace,
		ControlTimeout:     cfg.Runtime.Process.ControlTimeout,
		ClientTimeout:      cfg.Runtime.Relay.ClientTimeout,
		MaxAttachmentBytes: int64(cfg.Runtime.Relay.MaxAttachmentSize),
		MaxEventLineBytes:  cfg.Runtime.Relay.MaxEventLineBytes,
	})
	if err != nil {
		slog.Error("failed to start process manager", "error", err)
		os.Exit(1)
	}
	defer func() { _ = manager.Close() }()

	reaped, err := manager.ReapOrphans(ctx)
	slog.Info("agent orphan sweep complete", "reaped", reaped, "error", err)

	rt := router.NewWithAllowlists(managerProvider{manager}, db, cfg.Agent.DefaultWorkdir, cfg.AdminID, cfg.Discord.AllowedSenderIDs, cfg.Telegram.AllowedSenderIDs, router.Config{
		ContextStaleAfter:      cfg.Runtime.Router.ContextStaleAfter,
		ProgressQuietThreshold: cfg.Runtime.Router.ProgressQuietThreshold,
		MaxQueuedMessages:      cfg.Runtime.Router.MaxQueuedMessages,
		MaxPickerSessions:      cfg.Runtime.Router.MaxPickerSessions,
		MaxPickerPages:         cfg.Runtime.Router.MaxPickerPages,
		ModelBrowserTTL:        cfg.Runtime.Router.ModelBrowserTTL,
		ModelBrowserPage:       cfg.Runtime.Router.ModelBrowserPage,
		ModelBrowserNavRows:    cfg.Runtime.Router.ModelBrowserNavRows,
		ModelBrowserCap:        cfg.Runtime.Router.ModelBrowserCap,
		AgentBrowserTTL:        cfg.Runtime.Router.AgentBrowserTTL,
		AgentBrowserCap:        cfg.Runtime.Router.AgentBrowserCap,
		QuestionTombstoneTTL:   cfg.Runtime.Router.QuestionTombstoneTTL,
		PermissionTombstoneTTL: cfg.Runtime.Router.PermissionTombstoneTTL,
		RecoveryBudget:         cfg.Runtime.Router.RecoveryBudget,
		RecoveryBaseBackoff:    cfg.Runtime.Router.RecoveryBaseBackoff,
		RecoveryMaxBackoff:     cfg.Runtime.Router.RecoveryMaxBackoff,
		UsagePageSize:          cfg.Runtime.Router.UsagePageSize,
		UsageDefaultWindow:     cfg.Runtime.Router.UsageDefaultWindow,
		StreamerNoEventTimeout: cfg.Runtime.Relay.NoEventTimeout,
	})

	if strings.TrimSpace(cfg.Webhooks.Dispatch.Token) != "" {
		dispatchPath := strings.TrimSpace(cfg.Webhooks.Dispatch.Path)
		if dispatchPath == "" {
			dispatchPath = "/dispatch"
		}
		bind := strings.TrimSpace(cfg.Webhooks.Bind)
		if bind == "" {
			bind = "127.0.0.1:8787"
		}
		rt.SetDispatchConfig(cfg.Webhooks.Dispatch.Token, "http://"+bind+dispatchPath)
	}

	if publicBase := strings.TrimSpace(os.Getenv("OCCA_PUBLIC_URL")); publicBase != "" {
		cfgPath := strings.TrimSpace(configPath)
		if cfgPath == "" {
			if def, err := config.DefaultConfigPath(); err == nil {
				cfgPath = def
			}
		}
		if cfgPath != "" {
			rt.SetConnectRepoConfig(cfgPath)
			if deps := router.NewConnectRepoGHDeps(publicBase, cfgPath); deps != nil {
				rt.SetConnectRepoDeps(deps)
			}
		}
	}

	discoverAgent(ctx, manager, cfg.Agent.DefaultWorkdir, cfg.Runtime.Relay.DiscoveryTimeout)

	menu := rt.MenuCommands()
	var channels []channel.Channel
	if telegramToken != "" {
		channels = append(channels, telegram.New(telegramToken, menu, telegram.Config{
			DownloadTimeout: cfg.Runtime.Channels.Telegram.DownloadTimeout,
			MaxDownloadSize: int64(cfg.Runtime.Channels.Telegram.MaxDownloadSize),
			InitTimeout:     cfg.Runtime.Channels.Telegram.InitTimeout,
			InitAttempts:    cfg.Runtime.Channels.Telegram.InitAttempts,
		}))
	}
	if discordToken != "" {
		da := discord.NewWithPolicy(discordToken, menu, discord.AllowlistPolicy{AllowedSenderIDs: cfg.Discord.AllowedSenderIDs}, discord.Config{
			DownloadTimeout: cfg.Runtime.Channels.Discord.DownloadTimeout,
			MaxDownloadSize: int64(cfg.Runtime.Channels.Discord.MaxDownloadSize),
		})
		da.SetAutoThreadPolicy(func(channelID string) (bool, error) {
			ch, err := db.ChannelRepo().Get(context.Background(), "discord", channelID)
			if err != nil {
				return false, err
			}
			if ch == nil {
				return true, nil
			}
			return ch.AutoThread, nil
		})
		da.SetOwnedThreadCheck(func(threadID string) (bool, error) {
			channelID, err := db.SessionRepo().ThreadChannel(context.Background(), "discord", threadID)
			if err != nil {
				return false, err
			}
			return channelID != "" && channelID != threadID, nil
		})
		rt.SetThreadParentResolver(da.ParentChannelOf)
		channels = append(channels, da)
	}

	executor := func(ctx context.Context, platform, channelID, prompt string) {
		var adapter channel.Channel
		for _, ch := range channels {
			if ch.Name() == platform {
				adapter = ch
				break
			}
		}
		if adapter == nil {
			slog.Warn("scheduler: no channel adapter", "platform", platform)
			return
		}

		workdir := cfg.Agent.DefaultWorkdir
		chRow, err := db.ChannelRepo().Get(ctx, platform, channelID)
		if err == nil && chRow != nil && chRow.Workdir != "" {
			workdir = chRow.Workdir
		}

		inst, err := manager.Instance(ctx, workdir)
		if err != nil {
			notify(adapter, channelID, "⚠️ Scheduled task failed: agent unreachable")
			return
		}
		defer inst.End()

		resolver := relay.NewSessionResolver(db.SessionRepo(), inst.Client())
		sessionID, err := resolver.Resolve(ctx, platform, channelID, "", "", inst.PID())
		if err != nil {
			notify(adapter, channelID, "⚠️ Scheduled task failed: session error")
			return
		}

		notify(adapter, channelID, "⏰ Running: "+prompt)

		if err := inst.Client().SendMessage(ctx, sessionID, prompt, nil, nil); err != nil {
			notify(adapter, channelID, "⚠️ Scheduled task failed: "+err.Error())
			return
		}

		events, err := inst.Client().Events(ctx, sessionID)
		if err != nil {
			notify(adapter, channelID, "⚠️ Scheduled task failed: events stream error")
			return
		}

		var buf strings.Builder
		for ev := range events {
			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
			case "done":
				result := buf.String()
				if result == "" {
					result = "(no output)"
				}
				notify(adapter, channelID, "✅ "+result)
				return
			case "error":
				notify(adapter, channelID, "⚠️ Scheduled task error: "+ev.Delta)
				return
			}
		}
	}

	sched := scheduler.New(db.ScheduleRepo(), executor, cfg.Runtime.Scheduler.StopGrace)
	if err := sched.Start(ctx); err != nil {
		// Do not serve scheduling (or the rest of the app) after a failed
		// pending-row sweep / schedule load: a half-initialized scheduler
		// would silently drop or misfire jobs.
		fmt.Fprintf(os.Stderr, "occa: scheduler start: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sched.Stop() }()

	loopExecutor := func(execCtx context.Context, conv loop.Conversation, prompt string) (string, error) {
		var adapter channel.Channel
		for _, ch := range channels {
			if ch.Name() == conv.Platform {
				adapter = ch
				break
			}
		}
		if adapter == nil {
			return "", errors.New("channel adapter unavailable")
		}
		workdir := cfg.Agent.DefaultWorkdir
		chRow, err := db.ChannelRepo().Get(execCtx, conv.Platform, conv.ChannelID)
		if err == nil && chRow != nil && chRow.Workdir != "" {
			workdir = chRow.Workdir
		}
		inst, err := manager.Instance(execCtx, workdir)
		if err != nil {
			return "", errors.New("agent unreachable")
		}
		defer inst.End()
		resolver := relay.NewSessionResolver(db.SessionRepo(), inst.Client())
		sessionID, err := resolver.Resolve(execCtx, conv.Platform, conv.ChannelID, conv.ThreadID, conv.UserID, inst.PID())
		if err != nil {
			return "", errors.New("session error")
		}
		if err := inst.Client().SendMessage(execCtx, sessionID, prompt, nil, nil); err != nil {
			return "", errors.New("agent request error")
		}
		events, err := inst.Client().Events(execCtx, sessionID)
		if err != nil {
			return "", errors.New("events stream error")
		}
		var buf strings.Builder
		for ev := range events {
			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
			case "done":
				return buf.String(), nil
			case "error", "stream_error":
				return "", errors.New("agent response error")
			}
		}
		return "", errors.New("events stream error")
	}
	loopNotify := func(conv loop.Conversation, text string) {
		target := conv.ChannelID
		if conv.Platform == "discord" && conv.ThreadID != "" {
			target = conv.ThreadID
		}
		for _, ch := range channels {
			if ch.Name() == conv.Platform {
				notify(ch, target, text)
				return
			}
		}
		slog.Warn("loop: no channel adapter", "platform", conv.Platform)
	}
	looper := loop.New(loopExecutor, loopNotify, rt.LoopBusy, loop.Config{
		MinInterval:        cfg.Runtime.Loop.MinInterval,
		MaxInterval:        cfg.Runtime.Loop.MaxInterval,
		MinDuration:        cfg.Runtime.Loop.MinDuration,
		MaxDuration:        cfg.Runtime.Loop.MaxDuration,
		IterationTimeout:   cfg.Runtime.Loop.IterationTimeout,
		MaxWallAge:         cfg.Runtime.Loop.MaxWallAge,
		MinCount:           cfg.Runtime.Loop.MinCount,
		MaxCount:           cfg.Runtime.Loop.MaxCount,
		MaxPromptRunes:     cfg.Runtime.Loop.MaxPromptRunes,
		MaxPerConversation: cfg.Runtime.Loop.MaxPerConversation,
		MaxGlobal:          cfg.Runtime.Loop.MaxGlobal,
	})
	rt.SetLooper(looper)
	defer looper.StopAll()

	attrib := attribution.NewStore(cfg.Runtime.Router.AttributionTTL)
	mcpSrv := mcpserver.New(sched, attrib, mcpserver.Config{
		ReadHeaderTimeout: cfg.Runtime.MCP.ReadHeaderTimeout,
		ReadTimeout:       cfg.Runtime.MCP.ReadTimeout,
		WriteTimeout:      cfg.Runtime.MCP.WriteTimeout,
		IdleTimeout:       cfg.Runtime.MCP.IdleTimeout,
	})
	if err := mcpSrv.Start(ctx); err != nil {
		slog.Error("failed to start mcp server", "error", err)
	}
	defer mcpSrv.Stop()

	rt.SetScheduler(sched)
	rt.SetAttributionStore(attrib)

	registerMCP(ctx, manager, mcpSrv, cfg.Agent.DefaultWorkdir)

	var webhookSrv *webhook.Server
	if len(cfg.Webhooks.Endpoints) > 0 {
		taskRegistry := webhook.NewTaskThreadRegistry()
		taskRegistry.SetStatusConfig(cfg.Webhooks.StatusIcons, cfg.Webhooks.StatusColors)
		for _, ch := range channels {
			if da, ok := ch.(*discord.Adapter); ok {
				registry := taskRegistry
				pollInterval := cfg.Runtime.Channels.Discord.TaskThreadPollInterval
				pollAttempts := cfg.Runtime.Channels.Discord.TaskThreadPollAttempts
				da.SetTaskThreadLookup(func(repo, branch string) (string, bool) {
					if rec, ok := registry.ResolveTaskThread(repo, branch, "claim"); ok && strings.TrimSpace(rec.ThreadID) != "" {
						return rec.ThreadID, true
					} else if !ok {
						return "", false
					}
					for i := 0; i < pollAttempts; i++ {
						time.Sleep(pollInterval)
						if rec, ok := registry.ResolveTaskThread(repo, branch, "claim"); ok && strings.TrimSpace(rec.ThreadID) != "" {
							return rec.ThreadID, true
						}
					}
					return "", false
				})
				da.SetTaskThreadMissDetail(func(repo, branch string) (string, string, string, bool, bool) {
					return registry.MissDetail(repo, branch)
				})
			}
		}
		webhookExecutor := newWebhookExecutor(channels, managerProvider{m: manager}, db.ChannelRepo(), cfg.Agent.DefaultWorkdir, relay.Config{
			NoEventTimeout:      cfg.Runtime.Relay.NoEventTimeout,
			WebhookAbortTimeout: cfg.Runtime.Relay.WebhookAbortTimeout,
			VerifyTimeout:       cfg.Runtime.Relay.VerifyTimeout,
		}, webhook.TaskOptions{Registry: taskRegistry, Icons: cfg.Webhooks.StatusIcons, Colors: cfg.Webhooks.StatusColors})

		webhookSrv = webhook.New(cfg.Webhooks, webhookExecutor, db.WebhookDeliveryRepo())
		webhookSrv.SetTaskRegistry(taskRegistry)
		webhookSrv.SetChannelStore(db.ChannelRepo())
		webhookSrv.SetStallFreshness(cfg.Runtime.Relay.StallFreshness)
		webhookSrv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					notify(ch, channelID, text)
					return nil
				}
			}
			return errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					return notifyEdit(ch, channelID, messageID, text)
				}
			}
			return errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetSender(func(ctx context.Context, platform, channelID, text string) (string, error) {
			for _, ch := range channels {
				if ch.Name() == platform {
					return notifySend(ch, channelID, text)
				}
			}
			return "", errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetSenderWithButtons(func(ctx context.Context, platform, channelID, text string, buttons []channel.Button) (string, error) {
			for _, ch := range channels {
				if ch.Name() == platform {
					return notifySendWithButtons(ch, channelID, text, buttons)
				}
			}
			return "", errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetEditorWithButtons(func(ctx context.Context, platform, channelID, messageID, text string, buttons []channel.Button) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					return notifyEditWithButtons(ch, channelID, messageID, text, buttons)
				}
			}
			return errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetThreadStarter(func(ctx context.Context, platform, channelID, messageID, name string) (string, error) {
			for _, ch := range channels {
				if ch.Name() == platform {
					if starter, ok := ch.(channel.ThreadStarter); ok {
						return starter.StartThread(channelID, messageID, name)
					}
					return "", errors.New("channel does not support threads")
				}
			}
			return "", errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetGuildResolver(func(ctx context.Context, platform, channelID string) string {
			for _, ch := range channels {
				if ch.Name() == platform {
					return guildIDForButtons(ch, channelID, "")
				}
			}
			return ""
		})
		rt.SetRootCardEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					return notifyEdit(ch, channelID, messageID, text)
				}
			}
			return errors.New("channel adapter unavailable")
		})
		webhookSrv.SetWorkspaceResolver(webhook.NewWorkspaceManagerWithTTL(cfg.Webhooks.Runtime.IsolatedWorkspaceTTL))
		webhookSrv.SetSessionStore(db.SessionRepo())
		if err := webhookSrv.Start(ctx); err != nil {
			slog.Error("failed to start webhook server", "error", err)
		}
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := webhookSrv.Stop(stopCtx); err != nil {
				slog.Warn("webhook: shutdown failed", "error", err)
			}
		}()
	}

	var channelProbes []health.Channel
	for _, ch := range channels {
		if hp, ok := ch.(health.Channel); ok {
			channelProbes = append(channelProbes, hp)
		}
	}
	healthReporter := health.New(
		health.WithStore(db),
		health.WithAgent(agentProbe{manager: manager, workdir: cfg.Agent.DefaultWorkdir}),
		health.WithChannels(channelProbes...),
		health.WithWebhook(webhookSrv),
		health.WithVersion(version),
		health.WithExpectedSchema(store.SchemaVersion),
		health.WithProbeTimeout(cfg.Runtime.Health.ProbeTimeout),
		health.WithLastError(health.NewLastError(logging.NewStringScrubber(healthSecrets(cfg, telegramToken, discordToken)...))),
	)
	rt.SetHealthReporter(healthReporter)

	for _, ch := range channels {
		go runChannel(ctx, ch, rt)
	}

	go func() {
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return
		}
		swept, err := router.SweepStaleProgressNotices(ctx, db.ProgressNoticeRepo(), func(platform string) channel.MessageDeleter {
			for _, ch := range channels {
				if ch.Name() == platform {
					if d, ok := ch.(channel.MessageDeleter); ok {
						return d
					}
				}
			}
			return nil
		})
		slog.Info("progress notice sweep complete", "swept", swept, "error", err)
	}()

	slog.Info("occa started", "default_workdir", cfg.Agent.DefaultWorkdir, "max_instances", cfg.Agent.MaxInstances)

	<-ctx.Done()
	slog.Info("shutting down")
	for _, ch := range channels {
		_ = ch.Stop()
	}
}

func healthSecrets(cfg config.Config, telegramToken, discordToken string) []string {
	secrets := []string{telegramToken, discordToken}
	for _, endpoint := range cfg.Webhooks.Endpoints {
		secrets = append(secrets, endpoint.Secret)
	}
	return secrets
}

func openStoreWithLock(dbPath, defaultWorkdir string, usage store.UsageRetention, recoveryRetention time.Duration) (*store.SQLiteStore, *store.DBLock, error) {
	dbLock, err := store.LockDB(dbPath)
	if err != nil {
		return nil, nil, err
	}
	db, err := store.OpenWithRetention(dbPath, defaultWorkdir, usage, recoveryRetention)
	if err != nil {
		_ = dbLock.Unlock()
		return nil, nil, err
	}
	return db, dbLock, nil
}

var outboundRenderer = render.New()

func notify(ch channel.Channel, channelID, text string) {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	for _, chunk := range chunks {
		if err := ch.Notify(channelID, chunk); err != nil {
			slog.Warn("notification failed", "platform", ch.Name(), "channel_id", channelID, "error", err)
			return
		}
	}
}

func notifyWebhook(ch channel.Channel, channelID, text string) {
	notify(ch, channelID, webhook.FormatWebhookMessage(text))
}

func notifySend(ch channel.Channel, channelID, text string) (string, error) {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	if sender, ok := ch.(channel.MessageSender); ok {
		return sender.SendNotification(channelID, chunks[0])
	}
	if err := ch.Notify(channelID, chunks[0]); err != nil {
		return "", err
	}
	return "", nil
}

func notifySendWithButtons(ch channel.Channel, channelID, text string, buttons []channel.Button) (string, error) {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	if len(buttons) > 0 {
		if sender, ok := ch.(channel.MessageSenderWithButtons); ok {
			return sender.SendNotificationWithButtons(channelID, chunks[0], buttons)
		}
	}
	return notifySend(ch, channelID, chunks[0])
}

func notifyEdit(ch channel.Channel, channelID, messageID, text string) error {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	if editor, ok := ch.(channel.MessageEditor); ok {
		return editor.EditNotification(channelID, messageID, chunks[0])
	}
	return errors.New("channel does not support editing")
}

func notifyEditWithButtons(ch channel.Channel, channelID, messageID, text string, buttons []channel.Button) error {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	if len(buttons) > 0 {
		if editor, ok := ch.(channel.MessageEditorWithButtons); ok {
			return editor.EditNotificationWithButtons(channelID, messageID, chunks[0], buttons)
		}
	}
	return notifyEdit(ch, channelID, messageID, chunks[0])
}

func guildIDForButtons(ch channel.Channel, channelID, threadID string) string {
	if !strings.EqualFold(strings.TrimSpace(ch.Name()), "discord") {
		return ""
	}
	resolver, ok := ch.(channel.GuildResolver)
	if !ok {
		return ""
	}
	for _, id := range []string{threadID, channelID} {
		id = strings.TrimSpace(id)
		if id == "" || strings.Contains(id, "://") || strings.Contains(id, ":") {
			continue
		}
		if guild, err := resolver.GuildIDForChannel(id); err == nil && strings.TrimSpace(guild) != "" {
			return strings.TrimSpace(guild)
		}
	}
	return ""
}

func refreshThreadBannerAndStatus(ch channel.Channel, reg *webhook.TaskThreadRegistry, platform, channelID, threadID, repo, branch string, workCtx *webhook.WebhookWorkContext, lastEvent string) {
	if reg == nil || strings.TrimSpace(threadID) == "" || strings.TrimSpace(repo) == "" || strings.TrimSpace(branch) == "" {
		return
	}
	workflow := ""
	deliveryID := ""
	if workCtx != nil {
		workflow = workCtx.Workflow
		deliveryID = workCtx.DeliveryID
	}
	phase, icon := webhook.ThreadPhaseForWorkflow(workflow)
	if rec, ok := reg.Lookup(repo, branch); ok {
		prev := strings.TrimSpace(rec.Phase)
		if prev == "" || prev != phase {
			banner := webhook.FormatPhaseBanner(workflow, lastEvent)
			target := threadID
			if platform == "telegram" && !strings.Contains(channelID, ":") {
				target = channelID + ":" + threadID
			}
			if strings.TrimSpace(banner) != "" {
				_, _ = notifySend(ch, target, banner)
			}
		}
	} else {
		banner := webhook.FormatPhaseBanner(workflow, lastEvent)
		target := threadID
		if platform == "telegram" && !strings.Contains(channelID, ":") {
			target = channelID + ":" + threadID
		}
		if strings.TrimSpace(banner) != "" {
			_, _ = notifySend(ch, target, banner)
		}
	}
	reg.SetPhase(repo, branch, phase)
	stepRun := strings.TrimSpace(workflow)
	if stepRun == "" {
		stepRun = "run"
	}
	if len(strings.TrimSpace(deliveryID)) >= 8 {
		stepRun += " run " + strings.TrimSpace(deliveryID)[:8]
	} else if strings.TrimSpace(deliveryID) != "" {
		stepRun += " run " + strings.TrimSpace(deliveryID)
	}
	next := ""
	switch phase {
	case "REVIEW":
		next = "fix or merge gate"
	case "FIX":
		next = "push + re-review"
	case "MERGE":
		next = "merge or fix"
	}
	text := webhook.ThreadStatusText(phase, icon, stepRun, lastEvent, next)
	target := threadID
	if platform == "telegram" && !strings.Contains(channelID, ":") {
		target = channelID + ":" + threadID
	}
	if rec, ok := reg.Lookup(repo, branch); ok && strings.TrimSpace(rec.StatusMessageID) != "" {
		if err := notifyEdit(ch, target, strings.TrimSpace(rec.StatusMessageID), text); err == nil {
			return
		}
	}
	if msgID, err := notifySend(ch, target, text); err == nil && strings.TrimSpace(msgID) != "" {
		reg.SetStatusMessage(repo, branch, msgID)
	}
}

func notifyReply(ch channel.Channel, channelID, replyToMessageID, text string) (string, error) {
	chunks, err := outboundRenderer.Render(text, render.PlatformFor(ch.Name()))
	if err != nil || len(chunks) == 0 {
		chunks = []string{text}
	}
	if replier, ok := ch.(channel.MessageReplier); ok && replyToMessageID != "" {
		var firstID string
		for _, chunk := range chunks {
			id, err := replier.ReplyNotification(channelID, replyToMessageID, chunk)
			if err != nil {
				slog.Warn("notification reply failed; falling back to notify", "platform", ch.Name(), "channel_id", channelID, "error", err)
				break
			}
			if firstID == "" {
				firstID = id
			}
		}
		if firstID != "" {
			return firstID, nil
		}
	}
	for _, chunk := range chunks {
		if err := ch.Notify(channelID, chunk); err != nil {
			return "", err
		}
	}
	return "", nil
}

type agentManager interface {
	Instance(ctx context.Context, workdir string) (router.AgentInstance, error)
}

type channelStore interface {
	Get(ctx context.Context, platform, channelID string) (*store.Channel, error)
}

func newWebhookExecutor(channels []channel.Channel, manager agentManager, channelRepo channelStore, defaultWorkdir string, relayCfg relay.Config, opts ...webhook.TaskOptions) webhook.Executor {
	return func(ctx context.Context, platform, channelID, prompt string, workCtx *webhook.WebhookWorkContext) error {
		if workCtx == nil {
			workCtx = &webhook.WebhookWorkContext{}
		}
		var taskOpts webhook.TaskOptions
		if len(opts) > 0 {
			taskOpts = opts[0]
		}
		for _, ch := range channels {
			if ch.Name() == platform {
				targetChannelID := channelID
				useThread := workCtx.Thread
				useProgressCard := workCtx.ProgressCard

				if platform == "telegram" && workCtx.ThreadID != "" {
					if !strings.Contains(targetChannelID, ":") {
						targetChannelID = targetChannelID + ":" + workCtx.ThreadID
					}
				}

				if taskOpts.Registry != nil {
					if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
						caller := workCtx.Workflow
						if caller == "" {
							caller = "executor"
						}
						if caller == "fix" {
							if evt, _ := workCtx.Envelope["event_type"].(string); evt == "issue_comment" {
								caller = "review-fix"
							}
						}
						if _, _, rec, ok := taskOpts.Registry.ResolveForDelivery(workCtx.Key, workCtx.Envelope, caller); ok {
							if rec.ThreadID != "" {
								workCtx.ThreadID = rec.ThreadID
							}
							if rec.RootMessageID != "" {
								workCtx.RootMessageID = rec.RootMessageID
							}
							if rec.ThreadID != "" {
								if platform == "discord" {
									targetChannelID = rec.ThreadID
								} else if platform == "telegram" && !strings.Contains(targetChannelID, ":") {
									targetChannelID = targetChannelID + ":" + rec.ThreadID
								}
							}
						}
					}
				}
				taskStatus := webhook.StatusForWorkflow(workCtx.Workflow)

				sender, _ := ch.(channel.MessageSender)
				editor, _ := ch.(channel.MessageEditor)
				starter, _ := ch.(channel.ThreadStarter)

				reused := workCtx.RootMessageID != ""
				if reused && taskOpts.Registry != nil {
					if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
						if _, ok := taskOpts.Registry.ResolveTaskThread(repo, branch, "executor"); ok {
							buttons := webhook.TaskParentButtons(platform, guildIDForButtons(ch, channelID, workCtx.ThreadID), workCtx.Envelope, repo, branch, workCtx.ThreadID)
							header := webhook.TaskCardHeaderWithButtons(workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons, platform, buttons)
							if header != "" {
								if err := notifyEditWithButtons(ch, channelID, workCtx.RootMessageID, webhook.FormatWebhookMessage(header), buttons); err != nil {
									slog.Warn("webhook: failed to edit task parent, continuing in thread", "channel_id", channelID, "error", err)
								}
							}
							if strings.TrimSpace(workCtx.ThreadID) != "" {
								refreshThreadBannerAndStatus(ch, taskOpts.Registry, platform, channelID, workCtx.ThreadID, repo, branch, workCtx, "run started")
							}
						}
					}
				}
				if reused && workCtx.ThreadID == "" && useThread && starter != nil {
					threadName := webhook.FormatThreadName(workCtx.Envelope, workCtx.Workflow)
					threadID, err := starter.StartThread(channelID, workCtx.RootMessageID, threadName)
					if err != nil {
						slog.Warn("webhook: thread creation on known parent failed", "channel_id", channelID, "error", err)
					} else if strings.TrimSpace(threadID) != "" {
						workCtx.ThreadID = threadID
						targetChannelID = threadID
						if taskOpts.Registry != nil {
							if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
								taskOpts.Registry.Upsert(repo, branch, workCtx.RootMessageID, threadID, taskStatus)
							}
						}
					}
				}

				if !reused && useThread && starter != nil && sender != nil {
					rootCard := webhook.FormatRootCard(workCtx.Envelope, workCtx.Workflow, "RUNNING", "", "", platform)
					repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope)
					buttons := webhook.TaskParentButtons(platform, guildIDForButtons(ch, channelID, ""), workCtx.Envelope, repo, branch, "")
					rootCard = webhook.WithTaskHeaderWithButtons(rootCard, workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons, platform, buttons)
					rootMsgID, err := notifySendWithButtons(ch, channelID, webhook.FormatWebhookMessage(rootCard), buttons)
					if err != nil {
						slog.Warn("webhook: failed to send root card; falling back to channel", "channel_id", channelID, "error", err)
					} else {
						if taskOpts.Registry != nil {
							if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
								taskOpts.Registry.Upsert(repo, branch, rootMsgID, "", taskStatus)
							}
						}
						threadName := webhook.FormatThreadName(workCtx.Envelope, workCtx.Workflow)
						threadID, err := starter.StartThread(channelID, rootMsgID, threadName)
						if err != nil {
							slog.Warn("webhook: thread creation failed; falling back to channel", "channel_id", channelID, "error", err)
						} else {
							workCtx.RootMessageID = rootMsgID
							workCtx.ThreadID = threadID
							targetChannelID = threadID
							if taskOpts.Registry != nil {
								if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
									taskOpts.Registry.Upsert(repo, branch, rootMsgID, threadID, taskStatus)
									threadButtons := webhook.TaskParentButtons(platform, guildIDForButtons(ch, channelID, threadID), workCtx.Envelope, repo, branch, threadID)
									_ = notifyEditWithButtons(ch, channelID, rootMsgID, webhook.FormatWebhookMessage(rootCard), threadButtons)
									opener := webhook.FormatThreadOpener(workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons)
									if strings.TrimSpace(opener) != "" {
										_, _ = notifySend(ch, targetChannelID, opener)
									}
									refreshThreadBannerAndStatus(ch, taskOpts.Registry, platform, channelID, threadID, repo, branch, workCtx, "dispatched")
								}
							}
						}
					}
				} else if platform == "telegram" && useProgressCard && sender != nil {
					rootCard := webhook.FormatRootCard(workCtx.Envelope, workCtx.Workflow, "RUNNING", "", workCtx.ThreadID, platform)
					rootCard = webhook.WithTaskHeaderWithButtons(rootCard, workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons, platform, nil)
					rootMsgID, err := notifySend(ch, targetChannelID, webhook.FormatWebhookMessage(rootCard))
					if err != nil {
						slog.Warn("webhook: failed to send telegram root card; falling back to channel", "channel_id", targetChannelID, "error", err)
					} else {
						workCtx.RootMessageID = rootMsgID
					}
				}

				if workCtx.RootMessageID == "" {
					notifyWebhook(ch, targetChannelID, "📨 Webhook: analyzing...")
				}

				workdir := resolveWebhookWorkdir(ctx, channelRepo, defaultWorkdir, platform, channelID, workCtx.Worktree)

				inst, err := manager.Instance(ctx, workdir)
				if err != nil {
					notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: agent unreachable")
					return errors.New("webhook agent unavailable")
				}
				defer inst.End()

				var streamer *relay.Streamer
				if workCtx.RootMessageID != "" && sender != nil && editor != nil {
					sink := webhook.NewWebhookSink(targetChannelID, platform,
						func(ctx context.Context, chID, text string) (string, error) {
							return notifySend(ch, chID, text)
						},
						func(ctx context.Context, chID, msgID, text string) error {
							return notifyEdit(ch, chID, msgID, text)
						},
					)
					streamer = relay.NewStreamerWithSink(sink, outboundRenderer, render.PlatformFor(platform), relayCfg.NoEventTimeout)
				}

				turn := relay.WebhookTurn{
					Client:        inst.Client(),
					Prompt:        prompt,
					Model:         workCtx.Model,
					Platform:      platform,
					ChannelID:     channelID,
					DeliveryID:    workCtx.DeliveryID,
					ExecutionKey:  workCtx.Key.String(),
					Attempt:       workCtx.Attempt,
					AbortTimeout:  relayCfg.WebhookAbortTimeout,
					VerifyTimeout: relayCfg.VerifyTimeout,
					Streamer:      streamer,
					OnEvent: func(ev relay.Event) {
						if workCtx.Heartbeat == nil {
							return
						}
						switch ev.Type {
						case relay.EventDelta, relay.EventTool, relay.EventReasoning, relay.EventSegment:
							workCtx.Heartbeat()
						}
					},
				}
				result, err := turn.Run(ctx)

				workCtx.SessionID = result.SessionID
				workCtx.SessionAborted = result.Aborted
				workCtx.SessionAbortOK = result.AbortOK
				workCtx.AgentPID = inst.PID()
				workCtx.Progress = result.Progress

				elapsed := time.Duration(0)
				if !workCtx.StartTime.IsZero() {
					elapsed = time.Since(workCtx.StartTime)
				}

				if err != nil {
					if workCtx.RootMessageID != "" {
						if workCtx.ThreadID != "" {
							slim := webhook.FormatThreadSlimCard(workCtx.Envelope, workCtx.Workflow, webhook.TaskFailed, taskOpts.Icons)
							_, _ = notifySend(ch, targetChannelID, slim)
							if taskOpts.Registry != nil {
								if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
									refreshThreadBannerAndStatus(ch, taskOpts.Registry, platform, channelID, workCtx.ThreadID, repo, branch, workCtx, webhook.FormatGateOutcome(workCtx.Envelope, workCtx.Workflow, "FAILED"))
								}
							}
						} else if platform == "discord" {
							baseCard := webhook.FormatTerminalCard(workCtx.Envelope, workCtx.Workflow, "FAILED", err.Error(), workCtx.ThreadID, platform, elapsed)
							terminalCard := webhook.WithTaskHeader(baseCard, workCtx.Envelope, workCtx.Workflow, webhook.TaskFailed, taskOpts.Icons)
							if editErr := notifyEdit(ch, channelID, workCtx.RootMessageID, webhook.FormatWebhookMessage(terminalCard)); editErr != nil {
								slog.Warn("webhook: failed to update task parent with failure", "channel_id", channelID, "error", editErr)
							}
						} else {
							baseCard := webhook.FormatTerminalCard(workCtx.Envelope, workCtx.Workflow, "FAILED", err.Error(), workCtx.ThreadID, platform, elapsed)
							terminalCard := webhook.WithTaskHeaderWithButtons(baseCard, workCtx.Envelope, workCtx.Workflow, webhook.TaskFailed, taskOpts.Icons, platform, nil)
							_, _ = notifySend(ch, targetChannelID, webhook.FormatWebhookMessage(terminalCard))
						}
					} else {
						switch {
						case errors.Is(err, relay.ErrWebhookSessionCreate):
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: session error")
						case errors.Is(err, relay.ErrWebhookPrompt):
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: agent request error")
						case errors.Is(err, relay.ErrWebhookEventStream):
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: events error")
						case errors.Is(err, relay.ErrWebhookAgentResponse):
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: agent response error")
						case errors.Is(err, relay.ErrWebhookResponseIncomplete):
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: incomplete response")
						default:
							notifyWebhook(ch, targetChannelID, "⚠️ Webhook analysis failed: agent error")
						}
					}
					return err
				}

				if workCtx.RootMessageID != "" {
					if workCtx.ThreadID != "" {
						slim := webhook.FormatThreadSlimCard(workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons)
						_, _ = notifySend(ch, targetChannelID, slim)
						if taskOpts.Registry != nil {
							if repo, branch := webhook.TaskRepoBranch(workCtx.Key, workCtx.Envelope); repo != "" && branch != "" {
								refreshThreadBannerAndStatus(ch, taskOpts.Registry, platform, channelID, workCtx.ThreadID, repo, branch, workCtx, webhook.FormatGateOutcome(workCtx.Envelope, workCtx.Workflow, "COMPLETED"))
							}
						}
					} else if platform == "discord" {
						baseCard := webhook.FormatTerminalCard(workCtx.Envelope, workCtx.Workflow, "COMPLETED", "", workCtx.ThreadID, platform, elapsed)
						terminalCard := webhook.WithTaskHeader(baseCard, workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons)
						if editErr := notifyEdit(ch, channelID, workCtx.RootMessageID, webhook.FormatWebhookMessage(terminalCard)); editErr != nil {
							slog.Warn("webhook: failed to update task parent with completion", "channel_id", channelID, "error", editErr)
						}
					} else {
						baseCard := webhook.FormatTerminalCard(workCtx.Envelope, workCtx.Workflow, "COMPLETED", "", workCtx.ThreadID, platform, elapsed)
						terminalCard := webhook.WithTaskHeaderWithButtons(baseCard, workCtx.Envelope, workCtx.Workflow, taskStatus, taskOpts.Icons, platform, nil)
						_, _ = notifySend(ch, targetChannelID, webhook.FormatWebhookMessage(terminalCard))
					}
				} else {
					output := result.Output
					if output == "" {
						output = "(no output)"
					}
					notify(ch, targetChannelID, output)
				}
				return nil
			}
		}
		slog.Warn("webhook: no channel adapter", "platform", platform, "channel_id", channelID)
		return errors.New("webhook channel adapter unavailable")
	}
}

type messageRouter interface {
	Route(ctx context.Context, msg channel.IncomingMessage) error
}

func runChannel(ctx context.Context, c channel.Channel, rt messageRouter) {
	defer func() {
		if v := recover(); v != nil {
			slog.Error("channel panicked", "platform", c.Name(), "panic", v, "stack", string(debug.Stack()))
		}
	}()

	slog.Info("starting channel", "platform", c.Name())
	if err := c.Start(ctx, func(msg channel.IncomingMessage) {
		go func() {
			if err := rt.Route(ctx, msg); err != nil {
				slog.Error("route error", "platform", msg.Platform, "error", err)
			}
		}()
	}); err != nil {
		slog.Error("channel error", "platform", c.Name(), "error", err)
	}
}

func discoverAgent(ctx context.Context, manager *process.Manager, workdir string, discoveryTimeout time.Duration) {
	discoverCtx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()

	inst, err := manager.Instance(discoverCtx, workdir)
	if err != nil {
		slog.Warn("agent unreachable at startup — will retry per message", "error", err)
		return
	}
	defer inst.End()

	doc, err := relay.Discover(discoverCtx, inst.Addr(), discoveryTimeout)
	if err != nil {
		slog.Warn("agent discovery failed — will retry per message", "error", err)
		return
	}

	slog.Info("agent connected", "version", doc.Info.Version)
	if missing := doc.MissingEndpoints(); len(missing) > 0 {
		slog.Warn("agent missing expected endpoints", "endpoints", missing)
	}
}

func registerMCP(ctx context.Context, manager *process.Manager, mcpSrv *mcpserver.Server, workdir string) {
	regCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	inst, err := manager.Instance(regCtx, workdir)
	if err != nil {
		slog.Warn("mcp self-registration skipped — agent unreachable", "error", err)
		return
	}
	defer inst.End()

	httpClient, ok := inst.Client().(*relay.HTTPClient)
	if !ok {
		slog.Warn("mcp self-registration skipped — not an HTTP client")
		return
	}

	err = httpClient.RegisterMCP(regCtx, "occa", relay.McpConfig{
		Type: "remote",
		URL:  mcpSrv.URL(),
	})
	if err != nil {
		slog.Warn("mcp self-registration failed", "error", err)
		return
	}
	slog.Info("mcp self-registered", "url", mcpSrv.URL())
}
