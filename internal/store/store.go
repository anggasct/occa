package store

import "context"

type Session struct {
	ID             int64
	ChannelID      string
	Platform       string
	AgentSessionID string
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

type SessionRepo interface {
	Active(ctx context.Context, platform, channelID string) (string, error)
	SetActive(ctx context.Context, platform, channelID, sessionID string) error
	Deactivate(ctx context.Context, platform, channelID string) error
	List(ctx context.Context, platform, channelID string) ([]Session, error)
	Delete(ctx context.Context, id int64) error
}

type ChannelRepo interface {
	Get(ctx context.Context, platform, channelID string) (*Channel, error)
	UpsertModel(ctx context.Context, platform, channelID, model string) error
	UpsertListenMode(ctx context.Context, platform, channelID, listenMode string) error
	UpsertWorkdir(ctx context.Context, platform, channelID, workdir string) error
}

// OverrideRepo splits writes by field (UpsertRole, UpsertModel) rather than a single
// full-row Upsert: role is admin-managed, model is self-service, and a combined write
// would let one caller silently clobber the other's field.
type OverrideRepo interface {
	Get(ctx context.Context, platform, channelID, userID string) (*UserOverride, error)
	UpsertRole(ctx context.Context, platform, channelID, userID, role string) error
	UpsertModel(ctx context.Context, platform, channelID, userID, model string) error
	Delete(ctx context.Context, platform, channelID, userID string) error
	ListByChannel(ctx context.Context, platform, channelID string) ([]UserOverride, error)
}

type Store interface {
	SessionRepo() SessionRepo
	ChannelRepo() ChannelRepo
	OverrideRepo() OverrideRepo
	ScheduleRepo() ScheduleRepo
	Close() error
}
