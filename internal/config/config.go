package config

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/anggasct/occa/internal/relay"
)

type Config struct {
	AdminID  string
	Agent    AgentConfig    `yaml:"agent"`
	Database DatabaseConfig `yaml:"database"`
	Discord  DiscordConfig  `yaml:"discord"`
	Telegram TelegramConfig `yaml:"telegram"`
	Logging  LoggingConfig  `yaml:"logging"`
	Runtime  RuntimeConfig  `yaml:"runtime"`
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
	AllowedSenderIDs []string `yaml:"allowed_sender_ids"`
}

type TelegramConfig struct {
	AllowedSenderIDs []string `yaml:"allowed_sender_ids"`
}

type LoggingConfig struct {
	Format string `yaml:"format"`
}

type WebhookConfig struct {
	Bind      string           `yaml:"bind"`
	Policy    WebhookPolicy    `yaml:"policy"`
	Runtime   WebhookRuntime   `yaml:"runtime"`
	Endpoints []EndpointConfig `yaml:"endpoints"`
}

type EndpointConfig struct {
	Name           string            `yaml:"name"`
	Path           string            `yaml:"path"`
	Auth           string            `yaml:"auth,omitempty"`
	Secret         string            `yaml:"secret"`
	Workflow       string            `yaml:"workflow,omitempty"`
	Platform       string            `yaml:"platform"`
	ChannelID      string            `yaml:"channel_id"`
	ThreadID       string            `yaml:"thread_id,omitempty"`
	Prompt         string            `yaml:"prompt"`
	PromptFile     string            `yaml:"prompt_file,omitempty"`
	SkipEvents     []string          `yaml:"skip_events,omitempty"`
	Repository     string            `yaml:"repository,omitempty"`
	Workspace      EndpointWorkspace `yaml:"workspace"`
	Thread         bool              `yaml:"thread,omitempty"`
	ProgressCard   bool              `yaml:"progress_card,omitempty"`
	Model          string            `yaml:"model,omitempty"`
	CommentTrigger []string          `yaml:"comment_trigger,omitempty"`
	Admit          []AdmitRule       `yaml:"admit"`
	Limits         *EndpointLimits   `yaml:"limits,omitempty"`
	VerifyEndpoint string            `yaml:"verify_endpoint,omitempty"`
}

func (e *EndpointConfig) UnmarshalYAML(node *yaml.Node) error {
	type rawEndpoint struct {
		Name           string            `yaml:"name"`
		Path           string            `yaml:"path"`
		Auth           string            `yaml:"auth,omitempty"`
		Secret         string            `yaml:"secret"`
		Workflow       string            `yaml:"workflow,omitempty"`
		Platform       string            `yaml:"platform"`
		ChannelID      string            `yaml:"channel_id"`
		ThreadID       string            `yaml:"thread_id,omitempty"`
		Prompt         string            `yaml:"prompt"`
		PromptFile     string            `yaml:"prompt_file,omitempty"`
		SkipEvents     []string          `yaml:"skip_events,omitempty"`
		Repository     string            `yaml:"repository,omitempty"`
		Workspace      EndpointWorkspace `yaml:"workspace"`
		Thread         bool              `yaml:"thread,omitempty"`
		ProgressCard   *bool             `yaml:"progress_card,omitempty"`
		Model          string            `yaml:"model,omitempty"`
		CommentTrigger []string          `yaml:"comment_trigger,omitempty"`
		Admit          []AdmitRule       `yaml:"admit"`
		Limits         *EndpointLimits   `yaml:"limits,omitempty"`
		VerifyEndpoint string            `yaml:"verify_endpoint,omitempty"`
	}
	var raw rawEndpoint
	dec := yaml.NewDecoder(bytes.NewReader(mustEncodeNode(node)))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	e.Name = raw.Name
	e.Path = raw.Path
	e.Auth = raw.Auth
	e.Secret = raw.Secret
	e.Workflow = raw.Workflow
	e.Platform = raw.Platform
	e.ChannelID = raw.ChannelID
	e.ThreadID = raw.ThreadID
	e.Prompt = raw.Prompt
	e.PromptFile = raw.PromptFile
	e.SkipEvents = raw.SkipEvents
	e.Repository = raw.Repository
	e.Workspace = raw.Workspace
	e.Thread = raw.Thread
	e.Model = strings.TrimSpace(raw.Model)
	e.CommentTrigger = raw.CommentTrigger
	e.Admit = raw.Admit
	e.Limits = raw.Limits
	e.VerifyEndpoint = strings.TrimSpace(raw.VerifyEndpoint)
	if raw.ProgressCard != nil {
		e.ProgressCard = *raw.ProgressCard
	} else {
		e.ProgressCard = raw.Thread
	}
	return nil
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
	Discord struct {
		AllowedSenderIDs  []string `yaml:"allowed_sender_ids"`
		TriggerRoleIDs    []string `yaml:"trigger_role_ids"`
		TrustedBotSenders []struct {
			UserID     string   `yaml:"user_id"`
			ChannelIDs []string `yaml:"channel_ids"`
		} `yaml:"trusted_bot_senders"`
	} `yaml:"discord"`
	Telegram TelegramConfig `yaml:"telegram"`
	Logging  struct {
		Format string `yaml:"format"`
	} `yaml:"logging"`
	Runtime  RuntimeConfig `yaml:"runtime"`
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
runtime:
  loop:
    min_interval: 30s
    max_interval: 1h
    max_duration: 4h
    min_duration: 1m
    iteration_timeout: 10m
    max_wall_age: 4h
    min_count: 2
    max_count: 60
    max_prompt_runes: 1000
    max_per_conversation: 1
    max_global: 20
  relay:
    discovery_timeout: 5s
    client_timeout: 3m
    max_attachment_size: 10MB
    max_event_line_bytes: 1114112
    webhook_abort_timeout: 5s
    verify_timeout: 15s
    stall_freshness: 2m
    no_event_timeout: 15m
  router:
    context_stale_after: 15m
    progress_quiet_threshold: 90s
    max_queued_messages: 5
    max_picker_sessions: 6
    max_picker_pages: 5
    model_browser_ttl: 30m
    model_browser_page: 10
    model_browser_nav_rows: 100
    model_browser_cap: 1000
    agent_browser_ttl: 30m
    agent_browser_cap: 256
    question_tombstone_ttl: 10m
    permission_tombstone_ttl: 10m
    attribution_ttl: 30s
    recovery_budget: 60s
    recovery_base_backoff: 10s
    recovery_max_backoff: 40s
    usage_page_size: 5
    usage_default_window: 168h
  channels:
    discord:
      download_timeout: 60s
      max_download_size: 10MB
    telegram:
      download_timeout: 60s
      max_download_size: 10MB
      init_timeout: 15s
      init_attempts: 3
  mcp:
    read_header_timeout: 10s
    read_timeout: 30s
    write_timeout: 5m
    idle_timeout: 2m
  process:
    readiness_timeout: 30s
    stop_grace: 5s
    control_timeout: 2s
  scheduler:
    stop_grace: 5s
  health:
    probe_timeout: 1500ms
  store:
    usage_retention: 2160h
    usage_max_rows: 100000
    recovery_event_retention: 720h
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
	adminID := strings.TrimSpace(os.Getenv("OCCA_ADMIN_ID"))
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
		if len(bytes.TrimSpace(data)) > 0 {
			dec := yaml.NewDecoder(bytes.NewReader(data))
			dec.KnownFields(true)
			if err := dec.Decode(&fc); err != nil {
				return fileConfig{}, fmt.Errorf("config: parse %s: %w", configPath, err)
			}
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
	discordIDs, err := validateSenderAllowlist("discord", fc.Discord.AllowedSenderIDs)
	if err != nil {
		return Config{}, err
	}
	if len(fc.Discord.TriggerRoleIDs) > 0 || len(fc.Discord.TrustedBotSenders) > 0 {
		return Config{}, fmt.Errorf("config: discord.trigger_role_ids and discord.trusted_bot_senders were replaced by discord.allowed_sender_ids")
	}
	telegramIDs, err := validateSenderAllowlist("telegram", fc.Telegram.AllowedSenderIDs)
	if err != nil {
		return Config{}, err
	}
	if len(discordIDs) == 0 && len(telegramIDs) == 0 && adminID == "" {
		return Config{}, fmt.Errorf("config: allowed_sender_ids is empty on every platform and OCCA_ADMIN_ID is unset; nobody could reach OCCA")
	}
	if adminID != "" {
		slog.Warn("config: OCCA_ADMIN_ID is a deprecated any-platform allowlist alias")
	}

	runtimeCfg, err := validateRuntime(fc.Runtime)
	if err != nil {
		return Config{}, err
	}
	fc.Runtime = runtimeCfg

	if len(fc.Webhooks.Endpoints) > 0 {
		if fc.Webhooks.Bind == "" {
			fc.Webhooks.Bind = "127.0.0.1:8787"
		}
		if !isLoopbackBind(fc.Webhooks.Bind) {
			return Config{}, fmt.Errorf("config: webhooks.bind must be a loopback host:port (127.0.0.1, localhost, or ::1), got %q", fc.Webhooks.Bind)
		}
		runtime, err := validateWebhookRuntime(fc.Webhooks.Runtime)
		if err != nil {
			return Config{}, err
		}
		fc.Webhooks.Runtime = runtime
		policy, err := validateWebhookPolicy(fc.Webhooks.Policy)
		if err != nil {
			return Config{}, err
		}
		fc.Webhooks.Policy = policy
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
				case "review", "fix", "merge", "merged", "custom":
				default:
					return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].workflow is unsupported: %q", i, endpoint.Workflow)
				}
			}
			if len(endpoint.SkipEvents) > 0 {
				return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].skip_events is removed; express the exclusion with admit rules", i)
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
			if strings.TrimSpace(endpoint.Model) != "" {
				if _, err := relay.ParseModelRef(strings.TrimSpace(endpoint.Model)); err != nil {
					return Config{}, fmt.Errorf("config: webhooks.endpoints[%d].model is invalid: %w", i, err)
				}
			}
			var triggers []string
			for _, t := range endpoint.CommentTrigger {
				t = strings.TrimSpace(t)
				if t != "" {
					triggers = append(triggers, t)
				}
			}
			endpoint.CommentTrigger = triggers
			if err := validateAdmitRules(endpoint.Name, endpoint.Admit, fc.Webhooks.Policy); err != nil {
				return Config{}, err
			}
			if err := validateEndpointLimits(endpoint.Name, endpoint.Limits, endpoint.Admit); err != nil {
				return Config{}, err
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
		Discord:  DiscordConfig{AllowedSenderIDs: discordIDs},
		Telegram: TelegramConfig{AllowedSenderIDs: telegramIDs},
		Logging:  LoggingConfig{Format: fc.Logging.Format},
		Runtime:  fc.Runtime,
		Webhooks: fc.Webhooks,
	}, nil
}

func validateSenderAllowlist(platform string, ids []string) ([]string, error) {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for i := range ids {
		id := strings.TrimSpace(ids[i])
		if id == "" {
			return nil, fmt.Errorf("config: %s.allowed_sender_ids[%d] must not be empty", platform, i)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("config: %s.allowed_sender_ids[%d] duplicates %q", platform, i, id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
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
