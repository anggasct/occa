package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/channel/discord"
	"github.com/anggasct/occa/internal/channel/telegram"
	"github.com/anggasct/occa/internal/config"
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

// managerProvider adapts *process.Manager (concrete Instance) to the
// router.InstanceProvider interface (AgentInstance).
type managerProvider struct{ m *process.Manager }

func (p managerProvider) Instance(ctx context.Context, workdir string) (router.AgentInstance, error) {
	return p.m.Instance(ctx, workdir)
}

func (p managerProvider) ForceStop(workdir string) {
	p.m.ForceStop(workdir)
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file (default ~/.occa/config.yaml)")
	flag.StringVar(&configPath, "c", "", "path to config file (shorthand)")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa: %v\n", err)
		os.Exit(1)
	}

	// Secrets are env-only (never in the config file). Read before the
	// logger is built so every log line, including startup errors, is
	// covered by redaction from the start.
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

	db, err := store.Open(cfg.Database.Path)
	if err != nil {
		slog.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	manager, err := process.DefaultManager(cfg.Agent)
	if err != nil {
		slog.Error("failed to start process manager", "error", err)
		os.Exit(1)
	}
	defer manager.Close()

	rt := router.New(managerProvider{manager}, db, cfg.Agent.DefaultWorkdir, cfg.AdminID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
		slog.Error("failed to start scheduler", "error", err)
	}
	defer func() { _ = sched.Stop() }()

	tokens := mcpserver.NewTokenStore()
	mcpSrv := mcpserver.New(sched, tokens)
	if err := mcpSrv.Start(ctx); err != nil {
		slog.Error("failed to start mcp server", "error", err)
	}
	defer mcpSrv.Stop()

	rt.SetScheduler(sched)
	rt.SetTokenGenerator(tokens)

	registerMCP(ctx, manager, mcpSrv, cfg.Agent.DefaultWorkdir)

	if len(cfg.Webhooks.Endpoints) > 0 {
		webhookExecutor := func(ctx context.Context, platform, channelID, prompt string) {
			for _, ch := range channels {
				if ch.Name() == platform {
					send := func(text string) { notify(ch, channelID, text) }

					send("📨 Webhook: analyzing...")
					inst, err := manager.Instance(ctx, cfg.Agent.DefaultWorkdir)
					if err != nil {
						send("⚠️ Webhook analysis failed: agent unreachable")
						return
					}
					defer inst.End()

					resolver := relay.NewSessionResolver(db.SessionRepo(), inst.Client())
					sessionID, err := resolver.Resolve(ctx, platform, channelID, "", "", inst.PID())
					if err != nil {
						send("⚠️ Webhook analysis failed: session error")
						return
					}

					if err := inst.Client().SendMessage(ctx, sessionID, prompt, nil, nil); err != nil {
						send("⚠️ Webhook analysis failed: " + err.Error())
						return
					}

					events, err := inst.Client().Events(ctx, sessionID)
					if err != nil {
						send("⚠️ Webhook analysis failed: events error")
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
							send(result)
							return
						case "error":
							send("⚠️ " + ev.Delta)
							return
						}
					}
					return
				}
			}
			slog.Warn("webhook: no channel adapter", "platform", platform, "channel_id", channelID)
		}

		webhookSrv := webhook.New(cfg.Webhooks, webhookExecutor)
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

	for _, ch := range channels {
		go runChannel(ctx, ch, rt)
	}

	slog.Info("occa started", "default_workdir", cfg.Agent.DefaultWorkdir, "max_instances", cfg.Agent.MaxInstances)

	<-ctx.Done()
	slog.Info("shutting down")
	for _, ch := range channels {
		ch.Stop()
	}
}

var outboundRenderer = render.New()

// notify renders operator-facing text for the destination platform before it
// reaches the adapter, so a prompt or agent result containing markup
// characters is escaped rather than rejected by the platform.
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

// runChannel supervises one adapter. A panic here would otherwise take the
// whole process down and with it every other connected platform.
func runChannel(ctx context.Context, c channel.Channel, rt messageRouter) {
	defer func() {
		if v := recover(); v != nil {
			slog.Error("channel panicked", "platform", c.Name(), "panic", v, "stack", string(debug.Stack()))
		}
	}()

	slog.Info("starting channel", "platform", c.Name())
	if err := c.Start(ctx, func(msg channel.IncomingMessage) {
		// Route in a goroutine: a long-running response must never block the
		// adapter's update loop, or callbacks (permission buttons, question
		// options) would never be fetched while a response is in flight.
		// Per-conversation serialization is the response coordinator's job.
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
