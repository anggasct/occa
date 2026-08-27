package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

type Config struct {
	AdminID  string
	Agent    AgentConfig    `yaml:"agent"`
	Database DatabaseConfig `yaml:"database"`
	Discord  DiscordConfig  `yaml:"discord"`
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

type DiscordConfig struct {
	TriggerRoleIDs    []string                 `yaml:"trigger_role_ids"`
	TrustedBotSenders []TrustedBotSenderConfig `yaml:"trusted_bot_senders"`
}

type TrustedBotSenderConfig struct {
	UserID     string   `yaml:"user_id"`
	ChannelIDs []string `yaml:"channel_ids"`
}

type LoggingConfig struct {
	Format string `yaml:"format"`
}

type WebhookConfig struct {
	Bind      string           `yaml:"bind"`
	Endpoints []EndpointConfig `yaml:"endpoints"`
}

type EndpointConfig struct {
	Name       string            `yaml:"name"`
	Path       string            `yaml:"path"`
	Auth       string            `yaml:"auth,omitempty"`
	Secret     string            `yaml:"secret"`
	Workflow   string            `yaml:"workflow,omitempty"`
	Platform   string            `yaml:"platform"`
	ChannelID  string            `yaml:"channel_id"`
	Prompt     string            `yaml:"prompt"`
	PromptFile string            `yaml:"prompt_file,omitempty"`
	SkipEvents []string          `yaml:"skip_events,omitempty"`
	Repository string            `yaml:"repository,omitempty"`
	Workspace  EndpointWorkspace `yaml:"workspace"`
}

type EndpointWorkspace struct {
	Type string `yaml:"type"`
	Path string `yaml:"path,omitempty"`
	Mode string `yaml:"mode,omitempty"`
}

const (
	WorkspaceTypeNone = "none"
	WorkspaceTypeGit  = "git"

	WorkspaceModeIsolated = "isolated"
	WorkspaceModeMutable  = "mutable"
)

func CanonicalRepository(repo string) (string, error) {
	s := strings.TrimSpace(strings.ToLower(repo))
	if s == "" {
		return "", fmt.Errorf("config: empty repository identity")
	}
	for _, prefix := range []string{"https://", "http://", "ssh://", "git://"} {
		s = strings.TrimPrefix(s, prefix)
	}
	if at := strings.Index(s, "@"); at >= 0 && !strings.Contains(s[:at], "/") {
		s = s[at+1:]
	}
	s = strings.ReplaceAll(s, ":", "/")
	s = strings.TrimSuffix(s, ".git")
	s = strings.TrimSuffix(s, "/")
	parts := strings.Split(s, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return "", fmt.Errorf("config: repository identity %q must be owner/repo or host/owner/repo", repo)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("config: invalid repository identity component in %q", repo)
		}
		for _, r := range part {
			if !isAllowedRepositoryChar(r) {
				return "", fmt.Errorf("config: invalid character %q in repository identity %q", r, repo)
			}
		}
	}
	return strings.Join(parts, "/"), nil
}

func isAllowedRepositoryChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
}

func RepositoryPath(identity string) string {
	parts := strings.Split(identity, "/")
	if len(parts) == 3 {
		return strings.Join(parts[1:], "/")
	}
	return identity
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
	Discord DiscordConfig `yaml:"discord"`
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
	if configPath == "" {
		configPath, err = DefaultConfigPath()
		if err != nil {
			return Config{}, err
		}
	}
	return build(fc, adminID, filepath.Dir(configPath))
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

func build(fc fileConfig, adminID, configDir string) (Config, error) {
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
	if err := validateDiscordPolicy(&fc.Discord); err != nil {
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
			endpoint.Workflow = strings.TrimSpace(strings.ToLower(endpoint.Workflow))
			if err := validateEndpointPath(endpoint.Path); err != nil {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].path: %w", i, err)
			}
			if err := validateEndpointWorkspace(endpoint, configDir); err != nil {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d] (%s): %w", i, endpoint.Name, err)
			}
			if endpoint.Workflow != "" {
				switch endpoint.Workflow {
				case "github_reviewer", "github_fix", "github_merge", "github_merged":
				default:
					return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].workflow is unsupported: %q", i, endpoint.Workflow)
				}
			}
			switch endpoint.Auth {
			case "", "legacy_bearer", "github_hmac_sha256":
			default:
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].auth is unsupported: %q (supported: \"github_hmac_sha256\", \"legacy_bearer\")", i, endpoint.Auth)
			}

			if strings.TrimSpace(endpoint.Secret) == "" {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].secret must not be empty", i)
			}
			if strings.TrimSpace(endpoint.Prompt) != "" && strings.TrimSpace(endpoint.PromptFile) != "" {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d] must not define both prompt and prompt_file", i)
			}
			if strings.TrimSpace(endpoint.PromptFile) != "" {
				prompt, err := loadPromptFile(configDir, endpoint.PromptFile)
				if err != nil {
					return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].prompt_file: %w", i, err)
				}
				endpoint.Prompt = prompt
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
		Discord:  fc.Discord,
		Logging:  LoggingConfig{Format: fc.Logging.Format},
		Webhooks: fc.Webhooks,
	}, nil
}

func validateDiscordPolicy(policy *DiscordConfig) error {
	roleIDs := make(map[string]struct{}, len(policy.TriggerRoleIDs))
	for i := range policy.TriggerRoleIDs {
		roleID := strings.TrimSpace(policy.TriggerRoleIDs[i])
		if roleID == "" {
			return fmt.Errorf("config: discord.trigger_role_ids[%d] must not be empty", i)
		}
		if _, exists := roleIDs[roleID]; exists {
			return fmt.Errorf("config: discord.trigger_role_ids[%d] duplicates %q", i, roleID)
		}
		policy.TriggerRoleIDs[i] = roleID
		roleIDs[roleID] = struct{}{}
	}

	senderIDs := make(map[string]struct{}, len(policy.TrustedBotSenders))
	for i := range policy.TrustedBotSenders {
		sender := &policy.TrustedBotSenders[i]
		sender.UserID = strings.TrimSpace(sender.UserID)
		if sender.UserID == "" {
			return fmt.Errorf("config: discord.trusted_bot_senders[%d].user_id must not be empty", i)
		}
		if _, exists := senderIDs[sender.UserID]; exists {
			return fmt.Errorf("config: discord.trusted_bot_senders[%d].user_id duplicates %q", i, sender.UserID)
		}
		senderIDs[sender.UserID] = struct{}{}
		if len(sender.ChannelIDs) == 0 {
			return fmt.Errorf("config: discord.trusted_bot_senders[%d].channel_ids must not be empty", i)
		}

		channelIDs := make(map[string]struct{}, len(sender.ChannelIDs))
		for j := range sender.ChannelIDs {
			channelID := strings.TrimSpace(sender.ChannelIDs[j])
			if channelID == "" {
				return fmt.Errorf("config: discord.trusted_bot_senders[%d].channel_ids[%d] must not be empty", i, j)
			}
			if _, exists := channelIDs[channelID]; exists {
				return fmt.Errorf("config: discord.trusted_bot_senders[%d].channel_ids[%d] duplicates %q", i, j, channelID)
			}
			sender.ChannelIDs[j] = channelID
			channelIDs[channelID] = struct{}{}
		}
	}
	return nil
}

func validateEndpointPath(p string) error {
	if p == "" {
		return fmt.Errorf("must not be empty")
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf("must begin with /")
	}
	if p == "/occa" || strings.HasPrefix(p, "/occa/") {
		return fmt.Errorf("must not include the deployment ingress prefix /occa; configure the suffix path only")
	}
	if strings.ContainsAny(p, "?#") {
		return fmt.Errorf("must not contain query or fragment components")
	}
	segments := strings.Split(strings.Trim(p, "/"), "/")
	for _, seg := range segments {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("must not contain empty or traversal segments")
		}
	}
	return nil
}

func validateEndpointWorkspace(endpoint *EndpointConfig, configDir string) error {
	ws := &endpoint.Workspace
	ws.Type = strings.TrimSpace(strings.ToLower(ws.Type))
	ws.Mode = strings.TrimSpace(strings.ToLower(ws.Mode))
	ws.Path = strings.TrimSpace(ws.Path)

	switch ws.Type {
	case WorkspaceTypeNone:
		if ws.Path != "" || ws.Mode != "" {
			return fmt.Errorf("workspace path and mode must be empty when workspace.type is none")
		}
		return nil
	case WorkspaceTypeGit:
	default:
		return fmt.Errorf("workspace.type is required and must be %q or %q", WorkspaceTypeNone, WorkspaceTypeGit)
	}

	if ws.Path == "" {
		return fmt.Errorf("workspace.path is required when workspace.type is git")
	}
	if !filepath.IsAbs(ws.Path) {
		ws.Path = filepath.Join(configDir, ws.Path)
	}
	ws.Path = filepath.Clean(ws.Path)
	if strings.HasPrefix(ws.Path, "..") || !filepath.IsAbs(ws.Path) {
		return fmt.Errorf("workspace.path %q must resolve to an absolute path", ws.Path)
	}

	switch ws.Mode {
	case WorkspaceModeIsolated, WorkspaceModeMutable:
	default:
		return fmt.Errorf("workspace.mode is required and must be %q or %q when workspace.type is git", WorkspaceModeIsolated, WorkspaceModeMutable)
	}

	if strings.TrimSpace(endpoint.Repository) == "" {
		return fmt.Errorf("repository binding is required when workspace.type is git")
	}
	canonical, err := CanonicalRepository(endpoint.Repository)
	if err != nil {
		return err
	}
	endpoint.Repository = canonical
	return nil
}

func loadPromptFile(configDir, promptFile string) (string, error) {
	if filepath.IsAbs(promptFile) {
		return "", fmt.Errorf("absolute paths are forbidden: %q", promptFile)
	}
	cleanRelative := filepath.Clean(promptFile)
	if cleanRelative == "." || cleanRelative == ".." || strings.HasPrefix(cleanRelative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes config directory: %q", promptFile)
	}

	baseDir, err := filepath.Abs(configDir)
	if err != nil {
		return "", fmt.Errorf("resolve config directory: %w", err)
	}
	baseDir, err = filepath.EvalSymlinks(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve config directory: %w", err)
	}
	candidate := filepath.Join(baseDir, cleanRelative)
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("read prompt file: %w", err)
	}
	relative, err := filepath.Rel(baseDir, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes config directory: %q", promptFile)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("stat prompt file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("prompt file is not a regular file")
	}
	if info.Mode().Perm()&0444 == 0 {
		return "", fmt.Errorf("prompt file is unreadable")
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		return "", err
	}
	if len(data) == 0 || strings.TrimSpace(string(data)) == "" {
		return "", fmt.Errorf("file is empty")
	}
	if !utf8.Valid(data) {
		return "", fmt.Errorf("file is not valid UTF-8")
	}
	return string(data), nil
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
