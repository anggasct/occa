package config

import (
	"fmt"
	"os"
)

type Config struct {
	OpenCodeAddr   string
	TelegramToken  string
	DiscordToken   string
	AdminID        string
	DBPath         string
	LogFormat      string
}

func Load() (Config, error) {
	cfg := Config{
		OpenCodeAddr: envOrDefault("OCCA_OPENCODE_ADDR", "http://127.0.0.1:4096"),
		TelegramToken: os.Getenv("OCCA_TELEGRAM_TOKEN"),
		DiscordToken:  os.Getenv("OCCA_DISCORD_TOKEN"),
		AdminID:       os.Getenv("OCCA_ADMIN_ID"),
		DBPath:        envOrDefault("OCCA_DB_PATH", "./occa.db"),
		LogFormat:     envOrDefault("OCCA_LOG_FORMAT", "text"),
	}

	if cfg.TelegramToken == "" && cfg.DiscordToken == "" {
		return cfg, fmt.Errorf("config: at least one of OCCA_TELEGRAM_TOKEN or OCCA_DISCORD_TOKEN must be set")
	}

	return cfg, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
