package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
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

	_ = relay.NewHTTPClient(cfg.OpenCodeAddr)

	slog.Info("occa started", "opencode_addr", cfg.OpenCodeAddr)

	select {}
}
