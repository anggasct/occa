package store

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("store: not found")

type Session struct {
	ID             int64
	ChannelID      string
	Platform       string
	AgentSessionID string
	ThreadID       string
	UserID         string
	Title          string
	Active         bool
	CreatedAt      int64
	UpdatedAt      int64
}

type Channel struct {
	ChannelID  string
	Platform   string
	Model      string
	ListenMode string
	Workdir    string
	AutoThread bool
	CreatedAt  int64
	UpdatedAt  int64
}

type UserOverride struct {
	ID        int64
	ChannelID string
	Platform  string
	UserID    string
	Role      string
	Model     string
	CreatedAt int64
	UpdatedAt int64
}

type ProgressNotice struct {
	ID        int64
	Platform  string
	ChannelID string
	ThreadID  string
	MessageID string
	CreatedAt int64
}

type ThreadConfig struct {
	ID         int64
	Platform   string
	ChannelID  string
	ThreadID   string
	Workdir    string
	Model      string
	ListenMode string
	CreatedAt  int64
	UpdatedAt  int64
}

type PermissionRule struct {
	ID        int64
	Platform  string
	ChannelID string
	ThreadID  string
	UserID    string
	Tool      string
	Patterns  string
	CreatedAt int64
}

type WebhookStatus string

const (
	WebhookStatusReceived   WebhookStatus = "received"
	WebhookStatusAccepted   WebhookStatus = "accepted"
	WebhookStatusProcessing WebhookStatus = "processing"
	WebhookStatusCompleted  WebhookStatus = "completed"
	WebhookStatusSkipped    WebhookStatus = "skipped"
	WebhookStatusFailed     WebhookStatus = "failed"
)

type WebhookDelivery struct {
	ID           int64
	Endpoint     string
	DeliveryID   string
	EventType    string
	PayloadHash  string
	Status       WebhookStatus
	Attempt      int
	ErrorSummary string
	CreatedAt    int64
	UpdatedAt    int64
	StartedAt    int64
	CompletedAt  int64
}

type SessionRepo interface {
	Active(ctx context.Context, platform, channelID, threadID, userID string) (sessionID string, agentPID int, err error)
	SetActive(ctx context.Context, platform, channelID, threadID, userID, sessionID string, agentPID int) error
	Deactivate(ctx context.Context, platform, channelID, threadID, userID string) error
	SetTitle(ctx context.Context, id int64, title string) error
	SetModel(ctx context.Context, platform, channelID, threadID, userID, model string) error
	ActiveModel(ctx context.Context, platform, channelID, threadID, userID string) (string, error)
	List(ctx context.Context, platform, channelID string) ([]Session, error)
	ListConversation(ctx context.Context, platform, channelID, threadID, userID string) ([]Session, error)
	ThreadChannel(ctx context.Context, platform, threadID string) (string, error)
	Delete(ctx context.Context, id int64) error
}

type ChannelRepo interface {
	Get(ctx context.Context, platform, channelID string) (*Channel, error)
	UpsertModel(ctx context.Context, platform, channelID, model string) error
	UpsertListenMode(ctx context.Context, platform, channelID, listenMode string) error
	UpsertWorkdir(ctx context.Context, platform, channelID, workdir string) error
}

type OverrideRepo interface {
	Get(ctx context.Context, platform, channelID, userID string) (*UserOverride, error)
	UpsertRole(ctx context.Context, platform, channelID, userID, role string) error
	UpsertModel(ctx context.Context, platform, channelID, userID, model string) error
	Delete(ctx context.Context, platform, channelID, userID string) error
	ListByChannel(ctx context.Context, platform, channelID string) ([]UserOverride, error)
}

type ProgressNoticeRepo interface {
	Put(ctx context.Context, platform, channelID, threadID, messageID string) error
	List(ctx context.Context) ([]ProgressNotice, error)
	Delete(ctx context.Context, platform, channelID, threadID, messageID string) error
}

type ThreadConfigRepo interface {
	Get(ctx context.Context, platform, channelID, threadID string) (*ThreadConfig, error)
	UpsertWorkdir(ctx context.Context, platform, channelID, threadID, workdir string) error
	UpsertModel(ctx context.Context, platform, channelID, threadID, model string) error
	UpsertListenMode(ctx context.Context, platform, channelID, threadID, mode string) error
	SnapshotFromChannel(ctx context.Context, platform, channelID, threadID, defaultWorkdir string) error
}

type PermissionRuleRepo interface {
	Add(ctx context.Context, owner PermissionOwner, tool string, patterns []string) (int64, error)
	ListByOwner(ctx context.Context, owner PermissionOwner) ([]PermissionRule, error)
	DeleteByID(ctx context.Context, owner PermissionOwner, id int64) error
	ClearByOwner(ctx context.Context, owner PermissionOwner) error
	Match(ctx context.Context, owner PermissionOwner, tool string, patterns []string) (*PermissionRule, error)
}

type WebhookDeliveryRepo interface {
	// Create inserts a receipt for a validated delivery. It returns whether a
	// new row was created; a duplicate (same endpoint + delivery ID) bumps the
	// attempt count and reports created=false.
	Create(ctx context.Context, d WebhookDelivery) (bool, error)
	// Get returns the current receipt, or nil when the key is unknown.
	Get(ctx context.Context, endpoint, deliveryID string) (*WebhookDelivery, error)
	// Transition moves a receipt between statuses only when it currently holds
	// one of the from statuses (compare-and-swap) and returns whether it moved.
	Transition(ctx context.Context, id int64, from []WebhookStatus, to WebhookStatus, summary string) (bool, error)
	// List returns the most recent deliveries, newest first, capped at limit.
	List(ctx context.Context, limit int) ([]WebhookDelivery, error)
	// Prune deletes rows older than cutoff and caps newer rows at keep entries.
	Prune(ctx context.Context, cutoff int64, keep int) (int, error)
	// FailStale marks in-flight receipts (received/accepted/processing) whose
	// last update predates cutoff as failed — startup recovery after a crash.
	FailStale(ctx context.Context, cutoff int64, summary string) (int, error)
}

type Store interface {
	SessionRepo() SessionRepo
	ChannelRepo() ChannelRepo
	OverrideRepo() OverrideRepo
	ScheduleRepo() ScheduleRepo
	ProgressNoticeRepo() ProgressNoticeRepo
	ThreadConfigRepo() ThreadConfigRepo
	PermissionRuleRepo() PermissionRuleRepo
	WebhookDeliveryRepo() WebhookDeliveryRepo
	Close() error
}
