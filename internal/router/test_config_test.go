package router

import "time"

func testRouterConfig() Config {
	return Config{
		ContextStaleAfter:      15 * time.Minute,
		ProgressQuietThreshold: 90 * time.Second,
		MaxQueuedMessages:      5,
		MaxPickerSessions:      6,
		MaxPickerPages:         5,
		ModelBrowserTTL:        30 * time.Minute,
		ModelBrowserPage:       10,
		ModelBrowserNavRows:    100,
		ModelBrowserCap:        1000,
		AgentBrowserTTL:        30 * time.Minute,
		AgentBrowserCap:        256,
		QuestionTombstoneTTL:   10 * time.Minute,
		PermissionTombstoneTTL: 10 * time.Minute,
		RecoveryBudget:         60 * time.Second,
		RecoveryBaseBackoff:    10 * time.Second,
		RecoveryMaxBackoff:     40 * time.Second,
		UsagePageSize:          5,
		UsageDefaultWindow:     168 * time.Hour,
		StreamerNoEventTimeout: 15 * time.Minute,
	}
}
