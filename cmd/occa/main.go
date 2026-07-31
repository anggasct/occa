package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/channel/discord"
	"github.com/anggasct/occa/internal/channel/telegram"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/mcpserver"
	"github.com/anggasct/occa/internal/process"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/router"
	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
)

// managerProvider adapts *process.Manager (concrete Instance) to the
// router.InstanceProvider interface (AgentInstance).
type managerProvider struct{ m *process.Manager }

func (p managerProvider) Instance(ctx context.Context, workdir string) (router.AgentInstance, error) {
	return p.m.Instance(ctx, workdir)
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

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))

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

	// Secrets are env-only (never in the config file).
	telegramToken := os.Getenv("OCCA_TELEGRAM_TOKEN")
	discordToken := os.Getenv("OCCA_DISCORD_TOKEN")
	if telegramToken == "" && discordToken == "" {
		slog.Error("at least one of OCCA_TELEGRAM_TOKEN or OCCA_DISCORD_TOKEN must be set")
		os.Exit(1)
	}

	var channels []channel.Channel
	if telegramToken != "" {
		channels = append(channels, telegram.New(telegramToken))
	}
	if discordToken != "" {
		channels = append(channels, discord.New(discordToken))
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
			adapter.Notify(channelID, "⚠️ Scheduled task failed: agent unreachable")
			return
		}
		defer inst.End()

		resolver := relay.NewSessionResolver(db.SessionRepo(), inst.Client())
		sessionID, err := resolver.Resolve(ctx, platform, channelID)
		if err != nil {
			adapter.Notify(channelID, "⚠️ Scheduled task failed: session error")
			return
		}

		adapter.Notify(channelID, "⏰ Running: "+prompt)

		if err := inst.Client().SendMessage(ctx, sessionID, prompt, nil); err != nil {
			adapter.Notify(channelID, "⚠️ Scheduled task failed: "+err.Error())
			return
		}

		events, err := inst.Client().Events(ctx, sessionID)
		if err != nil {
			adapter.Notify(channelID, "⚠️ Scheduled task failed: events stream error")
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
				adapter.Notify(channelID, "✅ "+result)
				return
			case "error":
				adapter.Notify(channelID, "⚠️ Scheduled task error: "+ev.Delta)
				return
			}
		}
	}

	sched := scheduler.New(db.ScheduleRepo(), executor)
	if err := sched.Start(ctx); err != nil {
		slog.Error("failed to start scheduler", "error", err)
	}
	defer func() { _ = sched.Stop() }()

	mcpSrv := mcpserver.New(sched)
	if err := mcpSrv.Start(ctx); err != nil {
		slog.Error("failed to start mcp server", "error", err)
	}
	defer mcpSrv.Stop()

	rt.SetScheduler(sched)
	rt.SetMCPContextSetter(mcpSrv)

	registerMCP(ctx, manager, mcpSrv, cfg.Agent.DefaultWorkdir)

	for _, ch := range channels {
		go func(c channel.Channel) {
			slog.Info("starting channel", "platform", c.Name())
			if err := c.Start(ctx, func(msg channel.IncomingMessage) {
				if err := rt.Route(ctx, msg); err != nil {
					slog.Error("route error", "platform", msg.Platform, "error", err)
				}
			}); err != nil {
				slog.Error("channel error", "platform", c.Name(), "error", err)
			}
		}(ch)
	}

	slog.Info("occa started", "default_workdir", cfg.Agent.DefaultWorkdir, "max_instances", cfg.Agent.MaxInstances)

	<-ctx.Done()
	slog.Info("shutting down")
	for _, ch := range channels {
		ch.Stop()
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
