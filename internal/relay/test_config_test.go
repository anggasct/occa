package relay

import "time"

func testConfig() Config {
	return Config{
		DiscoveryTimeout:    5 * time.Second,
		ClientTimeout:       3 * time.Minute,
		MaxAttachmentBytes:  10 * 1024 * 1024,
		MaxEventLineBytes:   1024*1024 + 64*1024,
		WebhookAbortTimeout: 5 * time.Second,
		VerifyTimeout:       15 * time.Second,
		StallFreshness:      2 * time.Minute,
		NoEventTimeout:      15 * time.Minute,
	}
}
