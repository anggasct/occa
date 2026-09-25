package config

import (
	"fmt"
	"time"
)

type RuntimeConfig struct {
	Loop      LoopRuntime      `yaml:"loop"`
	Relay     RelayRuntime     `yaml:"relay"`
	Router    RouterRuntime    `yaml:"router"`
	Channels  ChannelsRuntime  `yaml:"channels"`
	MCP       MCPRuntime       `yaml:"mcp"`
	Process   ProcessRuntime   `yaml:"process"`
	Scheduler SchedulerRuntime `yaml:"scheduler"`
	Health    HealthRuntime    `yaml:"health"`
	Store     StoreRuntime     `yaml:"store"`
}

type LoopRuntime struct {
	MinInterval        time.Duration `yaml:"min_interval"`
	MaxInterval        time.Duration `yaml:"max_interval"`
	MaxDuration        time.Duration `yaml:"max_duration"`
	MinDuration        time.Duration `yaml:"min_duration"`
	IterationTimeout   time.Duration `yaml:"iteration_timeout"`
	MaxWallAge         time.Duration `yaml:"max_wall_age"`
	MinCount           int           `yaml:"min_count"`
	MaxCount           int           `yaml:"max_count"`
	MaxPromptRunes     int           `yaml:"max_prompt_runes"`
	MaxPerConversation int           `yaml:"max_per_conversation"`
	MaxGlobal          int           `yaml:"max_global"`
}

type RelayRuntime struct {
	DiscoveryTimeout    time.Duration `yaml:"discovery_timeout"`
	ClientTimeout       time.Duration `yaml:"client_timeout"`
	MaxAttachmentSize   byteSize      `yaml:"max_attachment_size"`
	MaxEventLineBytes   int           `yaml:"max_event_line_bytes"`
	WebhookAbortTimeout time.Duration `yaml:"webhook_abort_timeout"`
	VerifyTimeout       time.Duration `yaml:"verify_timeout"`
	StallFreshness      time.Duration `yaml:"stall_freshness"`
	NoEventTimeout      time.Duration `yaml:"no_event_timeout"`
}

type RouterRuntime struct {
	ContextStaleAfter      time.Duration `yaml:"context_stale_after"`
	ProgressQuietThreshold time.Duration `yaml:"progress_quiet_threshold"`
	MaxQueuedMessages      int           `yaml:"max_queued_messages"`
	MaxPickerSessions      int           `yaml:"max_picker_sessions"`
	MaxPickerPages         int           `yaml:"max_picker_pages"`
	ModelBrowserTTL        time.Duration `yaml:"model_browser_ttl"`
	ModelBrowserPage       int           `yaml:"model_browser_page"`
	ModelBrowserNavRows    int           `yaml:"model_browser_nav_rows"`
	ModelBrowserCap        int           `yaml:"model_browser_cap"`
	AgentBrowserTTL        time.Duration `yaml:"agent_browser_ttl"`
	AgentBrowserCap        int           `yaml:"agent_browser_cap"`
	QuestionTombstoneTTL   time.Duration `yaml:"question_tombstone_ttl"`
	PermissionTombstoneTTL time.Duration `yaml:"permission_tombstone_ttl"`
	AttributionTTL         time.Duration `yaml:"attribution_ttl"`
	RecoveryBudget         time.Duration `yaml:"recovery_budget"`
	RecoveryBaseBackoff    time.Duration `yaml:"recovery_base_backoff"`
	RecoveryMaxBackoff     time.Duration `yaml:"recovery_max_backoff"`
	UsagePageSize          int           `yaml:"usage_page_size"`
	UsageDefaultWindow     time.Duration `yaml:"usage_default_window"`
}

type ChannelsRuntime struct {
	Discord  DiscordChannelRuntime  `yaml:"discord"`
	Telegram TelegramChannelRuntime `yaml:"telegram"`
}

type DiscordChannelRuntime struct {
	DownloadTimeout time.Duration `yaml:"download_timeout"`
	MaxDownloadSize byteSize      `yaml:"max_download_size"`
}

type TelegramChannelRuntime struct {
	DownloadTimeout time.Duration `yaml:"download_timeout"`
	MaxDownloadSize byteSize      `yaml:"max_download_size"`
	InitTimeout     time.Duration `yaml:"init_timeout"`
	InitAttempts    int           `yaml:"init_attempts"`
}

type MCPRuntime struct {
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
}

type ProcessRuntime struct {
	ReadinessTimeout time.Duration `yaml:"readiness_timeout"`
	StopGrace        time.Duration `yaml:"stop_grace"`
	ControlTimeout   time.Duration `yaml:"control_timeout"`
}

type SchedulerRuntime struct {
	StopGrace time.Duration `yaml:"stop_grace"`
}

type HealthRuntime struct {
	ProbeTimeout time.Duration `yaml:"probe_timeout"`
}

type StoreRuntime struct {
	UsageRetention         time.Duration `yaml:"usage_retention"`
	UsageMaxRows           int           `yaml:"usage_max_rows"`
	RecoveryEventRetention time.Duration `yaml:"recovery_event_retention"`
}

func validateRuntime(rt RuntimeConfig) (RuntimeConfig, error) {
	missing := func(block, key string) error {
		return fmt.Errorf("config: runtime.%s.%s is required", block, key)
	}
	positive := func(block, key string, d time.Duration) error {
		if d <= 0 {
			return missing(block, key)
		}
		return nil
	}
	count := func(block, key string, n int) error {
		if n <= 0 {
			return missing(block, key)
		}
		return nil
	}
	l := rt.Loop
	if err := positive("loop", "min_interval", l.MinInterval); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("loop", "max_interval", l.MaxInterval); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("loop", "max_duration", l.MaxDuration); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("loop", "min_duration", l.MinDuration); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("loop", "iteration_timeout", l.IterationTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("loop", "max_wall_age", l.MaxWallAge); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("loop", "min_count", l.MinCount); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("loop", "max_count", l.MaxCount); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("loop", "max_prompt_runes", l.MaxPromptRunes); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("loop", "max_per_conversation", l.MaxPerConversation); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("loop", "max_global", l.MaxGlobal); err != nil {
		return RuntimeConfig{}, err
	}
	if l.MinInterval > l.MaxDuration {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.loop.min_interval must be <= max_duration")
	}
	if l.MinCount > l.MaxCount {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.loop.min_count must be <= max_count")
	}
	r := rt.Relay
	if err := positive("relay", "discovery_timeout", r.DiscoveryTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("relay", "client_timeout", r.ClientTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if r.MaxAttachmentSize <= 0 {
		return RuntimeConfig{}, missing("relay", "max_attachment_size")
	}
	if err := count("relay", "max_event_line_bytes", r.MaxEventLineBytes); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("relay", "webhook_abort_timeout", r.WebhookAbortTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("relay", "verify_timeout", r.VerifyTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("relay", "stall_freshness", r.StallFreshness); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("relay", "no_event_timeout", r.NoEventTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	ro := rt.Router
	if err := positive("router", "context_stale_after", ro.ContextStaleAfter); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "progress_quiet_threshold", ro.ProgressQuietThreshold); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "max_queued_messages", ro.MaxQueuedMessages); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "max_picker_sessions", ro.MaxPickerSessions); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "max_picker_pages", ro.MaxPickerPages); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "model_browser_ttl", ro.ModelBrowserTTL); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "model_browser_page", ro.ModelBrowserPage); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "model_browser_nav_rows", ro.ModelBrowserNavRows); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "model_browser_cap", ro.ModelBrowserCap); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "agent_browser_ttl", ro.AgentBrowserTTL); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("router", "agent_browser_cap", ro.AgentBrowserCap); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "question_tombstone_ttl", ro.QuestionTombstoneTTL); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "permission_tombstone_ttl", ro.PermissionTombstoneTTL); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "attribution_ttl", ro.AttributionTTL); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "recovery_budget", ro.RecoveryBudget); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "recovery_base_backoff", ro.RecoveryBaseBackoff); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "recovery_max_backoff", ro.RecoveryMaxBackoff); err != nil {
		return RuntimeConfig{}, err
	}
	if ro.RecoveryBaseBackoff > ro.RecoveryMaxBackoff {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.router.recovery_base_backoff must be <= recovery_max_backoff")
	}
	if err := count("router", "usage_page_size", ro.UsagePageSize); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("router", "usage_default_window", ro.UsageDefaultWindow); err != nil {
		return RuntimeConfig{}, err
	}
	ch := rt.Channels
	if err := positive("channels.discord", "download_timeout", ch.Discord.DownloadTimeout); err != nil {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.discord.download_timeout is required")
	}
	if ch.Discord.MaxDownloadSize <= 0 {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.discord.max_download_size is required")
	}
	if err := positive("channels.telegram", "download_timeout", ch.Telegram.DownloadTimeout); err != nil {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.telegram.download_timeout is required")
	}
	if ch.Telegram.MaxDownloadSize <= 0 {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.telegram.max_download_size is required")
	}
	if err := positive("channels.telegram", "init_timeout", ch.Telegram.InitTimeout); err != nil {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.telegram.init_timeout is required")
	}
	if err := count("channels.telegram", "init_attempts", ch.Telegram.InitAttempts); err != nil {
		return RuntimeConfig{}, fmt.Errorf("config: runtime.channels.telegram.init_attempts is required")
	}
	m := rt.MCP
	if err := positive("mcp", "read_header_timeout", m.ReadHeaderTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("mcp", "read_timeout", m.ReadTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("mcp", "write_timeout", m.WriteTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("mcp", "idle_timeout", m.IdleTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	p := rt.Process
	if err := positive("process", "readiness_timeout", p.ReadinessTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("process", "stop_grace", p.StopGrace); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("process", "control_timeout", p.ControlTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("scheduler", "stop_grace", rt.Scheduler.StopGrace); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("health", "probe_timeout", rt.Health.ProbeTimeout); err != nil {
		return RuntimeConfig{}, err
	}
	s := rt.Store
	if err := positive("store", "usage_retention", s.UsageRetention); err != nil {
		return RuntimeConfig{}, err
	}
	if err := count("store", "usage_max_rows", s.UsageMaxRows); err != nil {
		return RuntimeConfig{}, err
	}
	if err := positive("store", "recovery_event_retention", s.RecoveryEventRetention); err != nil {
		return RuntimeConfig{}, err
	}
	return rt, nil
}
