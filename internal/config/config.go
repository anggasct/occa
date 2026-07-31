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

// Config is OCCA's resolved runtime configuration. Secrets (bot tokens) are
// deliberately not part of Config — they are read from the environment only.
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
	Name      string `yaml:"name"`
	Path      string `yaml:"path"`
	Secret    string `yaml:"secret"`
	Platform  string `yaml:"platform"`
	ChannelID string `yaml:"channel_id"`
	Prompt    string `yaml:"prompt"`
}

// fileConfig mirrors Config for YAML parsing, holding durations as strings so
// values like "20m" decode cleanly; build() converts them to time.Duration.
type fileConfig struct {
	Agent struct {
		Binary         string `yaml:"binary"`
		PortRange      string `yaml:"port_range"`
		MaxInstances   int    `yaml:"max_instances"`
		IdleTimeout    string `yaml:"idle_timeout"`
		DefaultWorkdir string `yaml:"default_workdir"`
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
database:
  path: ~/.occa/occa.db
logging:
  format: text
`

// DefaultConfigPath returns ~/.occa/config.yaml.
func DefaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: home dir: %w", err)
	}
	return filepath.Join(home, ".occa", "config.yaml"), nil
}

// Load resolves configuration with precedence: defaults <- YAML file <- env.
// An empty configPath uses ~/.occa/config.yaml, bootstrapping it (and ~/.occa/)
// when missing. An explicit non-existent configPath is an error.
func Load(configPath string) (Config, error) {
	explicit := configPath != ""
	if configPath == "" {
		p, err := DefaultConfigPath()
		if err != nil {
			return Config{}, err
		}
		configPath = p
	}

	fc := defaultFileConfig()

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return Config{}, fmt.Errorf("config: read %s: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, &fc); err != nil {
			return Config{}, fmt.Errorf("config: parse %s: %w", configPath, err)
		}
	} else if explicit {
		return Config{}, fmt.Errorf("config: file not found: %s", configPath)
	} else if err := bootstrap(configPath); err != nil {
		return Config{}, err
	}

	if err := applyEnv(&fc); err != nil {
		return Config{}, err
	}
	adminID := os.Getenv("OCCA_ADMIN_ID")
	if adminID == "" {
		return Config{}, fmt.Errorf("config: OCCA_ADMIN_ID must be set")
	}
	return build(fc, adminID)
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
			return Config{}, fmt.Errorf("config: webhooks.bind must be a loopback address (127.0.0.1, localhost, or ::1), got %q", fc.Webhooks.Bind)
		}
		for i, endpoint := range fc.Webhooks.Endpoints {
			if strings.TrimSpace(endpoint.Secret) == "" {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].secret must not be empty", i)
			}
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
		},
		Database: DatabaseConfig{Path: dbPath},
		Logging:  LoggingConfig{Format: fc.Logging.Format},
		Webhooks: fc.Webhooks,
	}, nil
}

func isLoopbackBind(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	switch strings.ToLower(host) {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	return false
}

// expandHome expands a leading "~" to the user's home directory.
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
