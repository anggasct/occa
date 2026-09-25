package router

import "time"

type Config struct {
	ContextStaleAfter      time.Duration
	ProgressQuietThreshold time.Duration
	MaxQueuedMessages      int
	MaxPickerSessions      int
	MaxPickerPages         int
	ModelBrowserTTL        time.Duration
	ModelBrowserPage       int
	ModelBrowserNavRows    int
	ModelBrowserCap        int
	AgentBrowserTTL        time.Duration
	AgentBrowserCap        int
	QuestionTombstoneTTL   time.Duration
	PermissionTombstoneTTL time.Duration
	RecoveryBudget         time.Duration
	RecoveryBaseBackoff    time.Duration
	RecoveryMaxBackoff     time.Duration
	UsagePageSize          int
	UsageDefaultWindow     time.Duration
	StreamerNoEventTimeout time.Duration
}
