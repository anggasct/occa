package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/channel/discord"
	"github.com/anggasct/occa/internal/channel/telegram"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/router"
	"github.com/anggasct/occa/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa: %v\n", err)
		os.Exit(1)
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if cfg.LogFormat == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))

	db, err := store.Open(cfg.DBPath)
	if err != nil {
		slog.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	relayClient := relay.NewHTTPClient(cfg.AgentAddr)
	resolver := relay.NewSessionResolver(db.SessionRepo(), relayClient)
	renderer := render.New()
	rt := router.New(relayClient, db, resolver)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var channels []channel.Channel

	if cfg.TelegramToken != "" {
		tg := telegram.New(cfg.TelegramToken)
		channels = append(channels, tg)
	}
	if cfg.DiscordToken != "" {
		dc := discord.New(cfg.DiscordToken)
		channels = append(channels, dc)
	}

	_ = renderer

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

	slog.Info("occa started", "agent_addr", cfg.AgentAddr)

	<-ctx.Done()
	slog.Info("shutting down")
	for _, ch := range channels {
		ch.Stop()
	}
}
