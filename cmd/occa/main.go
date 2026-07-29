package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/channel/discord"
	"github.com/anggasct/occa/internal/channel/telegram"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/process"
	"github.com/anggasct/occa/internal/router"
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

	rt := router.New(managerProvider{manager}, db, cfg.Agent.DefaultWorkdir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
