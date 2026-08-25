package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	AdminID  string
	Agent    AgentConfig    `yaml:"agent"`
	Database DatabaseConfig `yaml:"database"`
	Logging  LoggingConfig  `yaml:"logging"`
	Webhooks WebhookConfig  `yaml:"webhooks"`
}

type AgentConfig struct {
	Binary         string        `yaml:"binary"`
	PortRange      string        `yaml:"port_range"`
	MaxInstances   int           `yaml:"max_instances"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
	DefaultWorkdir string        `yaml:"default_workdir"`
	AutoInstall    bool          `yaml:"auto_install"`
}

type DatabaseConfig struct {
	Path string `yaml:"path"`
}

type LoggingConfig struct {
	Format string `yaml:"format"`
}

type WebhookConfig struct {
	Bind      string           `yaml:"bind"`
	Endpoints []EndpointConfig `yaml:"endpoints"`
}

type EndpointConfig struct {
	Name       string   `yaml:"name"`
	Path       string   `yaml:"path"`
	Auth       string   `yaml:"auth,omitempty"`
	Secret     string   `yaml:"secret"`
	Platform   string   `yaml:"platform"`
	ChannelID  string   `yaml:"channel_id"`
	Prompt     string   `yaml:"prompt"`
	SkipEvents []string `yaml:"skip_events,omitempty"`
}

type fileConfig struct {
	Agent struct {
		Binary         string `yaml:"binary"`
		PortRange      string `yaml:"port_range"`
		MaxInstances   int    `yaml:"max_instances"`
		IdleTimeout    string `yaml:"idle_timeout"`
		DefaultWorkdir string `yaml:"default_workdir"`
		AutoInstall    bool   `yaml:"auto_install"`
	} `yaml:"agent"`
	Database struct {
		Path string `yaml:"path"`
	} `yaml:"database"`
	Logging struct {
		Format string `yaml:"format"`
	} `yaml:"logging"`
	Webhooks WebhookConfig `yaml:"webhooks"`
}

const defaultConfigTemplate = `agent:
  binary: opencode
  port_range: 4096-4116
  max_instances: 5
  idle_timeout: 20m
  default_workdir: "~"
  auto_install: false
database:
  path: ~/.occa/occa.db
logging:
  format: text
`

func DefaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: home dir: %w", err)
	}
	return filepath.Join(home, ".occa", "config.yaml"), nil
}

func Load(configPath string) (Config, error) {
	fc, err := loadFileConfig(configPath)
	if err != nil {
		return Config{}, err
	}
	adminID := os.Getenv("OCCA_ADMIN_ID")
	if adminID == "" {
		return Config{}, fmt.Errorf("config: OCCA_ADMIN_ID must be set")
	}
	return build(fc, adminID)
}

// DBPath resolves the configured database path without requiring the bot or
// admin environment variables, so operator subcommands (db backup/restore)
// can locate the database from config alone.
func DBPath(configPath string) (string, error) {
	fc, err := loadFileConfig(configPath)
	if err != nil {
		return "", err
	}
	return expandHome(fc.Database.Path)
}

func loadFileConfig(configPath string) (fileConfig, error) {
	explicit := configPath != ""
	if configPath == "" {
		p, err := DefaultConfigPath()
		if err != nil {
			return fileConfig{}, err
		}
		configPath = p
	}

	fc := defaultFileConfig()

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return fileConfig{}, fmt.Errorf("config: read %s: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, &fc); err != nil {
			return fileConfig{}, fmt.Errorf("config: parse %s: %w", configPath, err)
		}
	} else if explicit {
		return fileConfig{}, fmt.Errorf("config: file not found: %s", configPath)
	} else if err := bootstrap(configPath); err != nil {
		return fileConfig{}, err
	}

	if err := applyEnv(&fc); err != nil {
		return fileConfig{}, err
	}
	return fc, nil
}

func defaultFileConfig() fileConfig {
	var fc fileConfig
	fc.Agent.Binary = "opencode"
	fc.Agent.PortRange = "4096-4116"
	fc.Agent.MaxInstances = 5
	fc.Agent.IdleTimeout = "20m"
	fc.Agent.DefaultWorkdir = "~"
	fc.Database.Path = "~/.occa/occa.db"
	fc.Logging.Format = "text"
	return fc
}

func bootstrap(configPath string) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return fmt.Errorf("config: create dir: %w", err)
	}
	if err := os.WriteFile(configPath, []byte(defaultConfigTemplate), 0o644); err != nil {
		return fmt.Errorf("config: write default config: %w", err)
	}
	return nil
}

func applyEnv(fc *fileConfig) error {
	if v := os.Getenv("OCCA_AGENT_BINARY"); v != "" {
		fc.Agent.Binary = v
	}
	if v := os.Getenv("OCCA_AGENT_PORT_RANGE"); v != "" {
		fc.Agent.PortRange = v
	}
	if v := os.Getenv("OCCA_AGENT_MAX_INSTANCES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: OCCA_AGENT_MAX_INSTANCES: %w", err)
		}
		fc.Agent.MaxInstances = n
	}
	if v := os.Getenv("OCCA_AGENT_IDLE_TIMEOUT"); v != "" {
		fc.Agent.IdleTimeout = v
	}
	if v := os.Getenv("OCCA_AGENT_WORKDIR"); v != "" {
		fc.Agent.DefaultWorkdir = v
	}
	if v := os.Getenv("OCCA_AGENT_AUTO_INSTALL"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("config: OCCA_AGENT_AUTO_INSTALL: %w", err)
		}
		fc.Agent.AutoInstall = b
	}
	if v := os.Getenv("OCCA_DB_PATH"); v != "" {
		fc.Database.Path = v
	}
	if v := os.Getenv("OCCA_LOG_FORMAT"); v != "" {
		fc.Logging.Format = v
	}
	return nil
}

func build(fc fileConfig, adminID string) (Config, error) {
	idle, err := time.ParseDuration(fc.Agent.IdleTimeout)
	if err != nil {
		return Config{}, fmt.Errorf("config: agent.idle_timeout: %w", err)
	}
	if fc.Agent.MaxInstances <= 0 {
		return Config{}, fmt.Errorf("config: agent.max_instances must be > 0")
	}
	if fc.Logging.Format != "text" && fc.Logging.Format != "json" {
		return Config{}, fmt.Errorf("config: logging.format must be \"text\" or \"json\"")
	}

	workdir, err := expandHome(fc.Agent.DefaultWorkdir)
	if err != nil {
		return Config{}, err
	}
	dbPath, err := expandHome(fc.Database.Path)
	if err != nil {
		return Config{}, err
	}

	if len(fc.Webhooks.Endpoints) > 0 {
		if fc.Webhooks.Bind == "" {
			fc.Webhooks.Bind = "127.0.0.1:8787"
		}
		if !isLoopbackBind(fc.Webhooks.Bind) {
			return Config{}, fmt.Errorf("config: webhooks.bind must be a loopback host:port (127.0.0.1, localhost, or ::1), got %q", fc.Webhooks.Bind)
		}
		paths := make(map[string]struct{}, len(fc.Webhooks.Endpoints))
		for i := range fc.Webhooks.Endpoints {
			endpoint := &fc.Webhooks.Endpoints[i]
			endpoint.Auth = strings.TrimSpace(strings.ToLower(endpoint.Auth))
			switch endpoint.Auth {
			case "", "legacy_bearer", "github_hmac_sha256":
			default:
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].auth is unsupported: %q (supported: \"github_hmac_sha256\", \"legacy_bearer\")", i, endpoint.Auth)
			}

			if strings.TrimSpace(endpoint.Secret) == "" {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].secret must not be empty", i)
			}
			if _, exists := paths[endpoint.Path]; exists {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].path duplicates %q", i, endpoint.Path)
			}
			paths[endpoint.Path] = struct{}{}
		}
	}

	return Config{
		AdminID: adminID,
		Agent: AgentConfig{
			Binary:         fc.Agent.Binary,
			PortRange:      fc.Agent.PortRange,
			MaxInstances:   fc.Agent.MaxInstances,
			IdleTimeout:    idle,
			DefaultWorkdir: workdir,
			AutoInstall:    fc.Agent.AutoInstall,
		},
		Database: DatabaseConfig{Path: dbPath},
		Logging:  LoggingConfig{Format: fc.Logging.Format},
		Webhooks: fc.Webhooks,
	}, nil
}

func isLoopbackBind(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return false
	}
	switch strings.ToLower(host) {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	return false
}

func expandHome(path string) (string, error) {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: home dir: %w", err)
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~")), nil
}
