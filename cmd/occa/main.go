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

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "db" {
		os.Exit(runDBCommand(os.Args[2:]))
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

	db, dbLock, err := openStoreWithLock(cfg.Database.Path, cfg.Agent.DefaultWorkdir)
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

	manager, err := process.DefaultManager(cfg.Agent)
	if err != nil {
		slog.Error("failed to start process manager", "error", err)
		os.Exit(1)
	}
	defer func() { _ = manager.Close() }()

	reaped, err := manager.ReapOrphans(ctx)
	slog.Info("agent orphan sweep complete", "reaped", reaped, "error", err)

	rt := router.New(managerProvider{manager}, db, cfg.Agent.DefaultWorkdir, cfg.AdminID)

	discoverAgent(ctx, manager, cfg.Agent.DefaultWorkdir)

	menu := rt.MenuCommands()
	var channels []channel.Channel
	if telegramToken != "" {
		channels = append(channels, telegram.New(telegramToken, menu))
	}
	if discordToken != "" {
		da := discord.New(discordToken, menu)
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

	sched := scheduler.New(db.ScheduleRepo(), executor)
	if err := sched.Start(ctx); err != nil {
		// Do not serve scheduling (or the rest of the app) after a failed
		// pending-row sweep / schedule load: a half-initialized scheduler
		// would silently drop or misfire jobs.
		fmt.Fprintf(os.Stderr, "occa: scheduler start: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sched.Stop() }()

	attrib := attribution.NewStore()
	mcpSrv := mcpserver.New(sched, attrib)
	if err := mcpSrv.Start(ctx); err != nil {
		slog.Error("failed to start mcp server", "error", err)
	}
	defer mcpSrv.Stop()

	rt.SetScheduler(sched)
	rt.SetAttributionStore(attrib)

	registerMCP(ctx, manager, mcpSrv, cfg.Agent.DefaultWorkdir)

	var webhookSrv *webhook.Server
	if len(cfg.Webhooks.Endpoints) > 0 {
		webhookExecutor := func(ctx context.Context, platform, channelID, prompt string, workCtx webhook.WebhookWorkContext) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					send := func(text string) { notify(ch, channelID, text) }

					send("📨 Webhook: analyzing...")
					workdir := cfg.Agent.DefaultWorkdir
					if workCtx.Worktree != "" {
						workdir = workCtx.Worktree
					}

					inst, err := manager.Instance(ctx, workdir)
					if err != nil {
						send("⚠️ Webhook analysis failed: agent unreachable")
						return errors.New("webhook agent unavailable")
					}
					defer inst.End()

					resolver := relay.NewSessionResolver(db.SessionRepo(), inst.Client())
					sessionID, err := resolver.ResolveWithPermission(ctx, platform, channelID, workCtx.SessionKey, "", inst.PID(), workCtx.PermissionRuleset)
					if err != nil {
						send("⚠️ Webhook analysis failed: session error")
						return errors.New("webhook session unavailable")
					}
					if len(workCtx.PermissionRuleset) > 0 {
						permissionClient, ok := inst.Client().(interface {
							SetSessionPermission(context.Context, string, relay.PermissionRuleset) error
						})
						if !ok {
							send("⚠️ Webhook analysis failed: permission policy unsupported")
							return errors.New("webhook permission policy unsupported")
						}
						if err := permissionClient.SetSessionPermission(ctx, sessionID, workCtx.PermissionRuleset); err != nil {
							send("⚠️ Webhook analysis failed: permission policy error")
							return errors.New("webhook permission policy failed")
						}
					}

					if err := inst.Client().SendMessage(ctx, sessionID, prompt, workCtx.Model, nil); err != nil {
						send("⚠️ Webhook analysis failed: agent request error")
						return errors.New("webhook agent request failed")
					}

					events, err := inst.Client().Events(ctx, sessionID)
					if err != nil {
						send("⚠️ Webhook analysis failed: events error")
						return errors.New("webhook event stream failed")
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
							send(result)
							return nil
						case "error":
							send("⚠️ Webhook analysis failed: agent response error")
							return errors.New("webhook agent response failed")
						case "permission_asked":
							if ev.Permission != nil {
								_ = inst.Client().ReplyPermission(ctx, ev.Permission.ID, relay.PermissionReject)
							}
							send("⚠️ Webhook analysis failed: permission denied")
							return errors.New("webhook permission denied")
						}
					}
					send("⚠️ Webhook analysis failed: incomplete response")
					return errors.New("webhook response incomplete")
				}
			}
			slog.Warn("webhook: no channel adapter", "platform", platform, "channel_id", channelID)
			return errors.New("webhook channel adapter unavailable")
		}

		webhookSrv = webhook.New(cfg.Webhooks, webhookExecutor, db.WebhookDeliveryRepo())
		webhookSrv.SetChannelStore(db.ChannelRepo())
		webhookSrv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			for _, ch := range channels {
				if ch.Name() == platform {
					notify(ch, channelID, text)
					return nil
				}
			}
			return errors.New("webhook channel adapter unavailable")
		})
		webhookSrv.SetWorktreeResolver(webhook.NewGitWorktreeResolver(""))
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

func openStoreWithLock(dbPath, defaultWorkdir string) (*store.SQLiteStore, *store.DBLock, error) {
	dbLock, err := store.LockDB(dbPath)
	if err != nil {
		return nil, nil, err
	}
	db, err := store.OpenWithDefaultWorkdir(dbPath, defaultWorkdir)
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

func discoverAgent(ctx context.Context, manager *process.Manager, workdir string) {
	discoverCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	inst, err := manager.Instance(discoverCtx, workdir)
	if err != nil {
		slog.Warn("agent unreachable at startup — will retry per message", "error", err)
		return
	}
	defer inst.End()

	doc, err := relay.Discover(discoverCtx, inst.Addr())
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
