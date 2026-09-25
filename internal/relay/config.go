package relay

import "time"

type Config struct {
	DiscoveryTimeout    time.Duration
	ClientTimeout       time.Duration
	MaxAttachmentBytes  int64
	MaxEventLineBytes   int
	WebhookAbortTimeout time.Duration
	VerifyTimeout       time.Duration
	StallFreshness      time.Duration
	NoEventTimeout      time.Duration
}
