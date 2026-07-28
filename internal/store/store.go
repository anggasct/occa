package store

import "context"

type Session struct {
	ID                int64
	ChannelID         string
	Platform          string
	OpenCodeSessionID string
	Active            bool
	CreatedAt         int64
	UpdatedAt         int64
}

type Channel struct {
	ChannelID  string
	Platform   string
	Model      string
	ListenMode string
	CreatedAt  int64
	UpdatedAt  int64
}

type UserOverride struct {
	ID        int64
	ChannelID string
	UserID    string
	Role      string
	Model     string
	CreatedAt int64
	UpdatedAt int64
}

type SessionRepo interface {
	Active(ctx context.Context, platform, channelID string) (string, error)
	SetActive(ctx context.Context, platform, channelID, sessionID string) error
	List(ctx context.Context, platform, channelID string) ([]Session, error)
	Delete(ctx context.Context, id int64) error
}

type ChannelRepo interface {
	Get(ctx context.Context, platform, channelID string) (*Channel, error)
	Upsert(ctx context.Context, ch *Channel) error
}

type OverrideRepo interface {
	Get(ctx context.Context, channelID, userID string) (*UserOverride, error)
	Upsert(ctx context.Context, o *UserOverride) error
	Delete(ctx context.Context, channelID, userID string) error
	ListByChannel(ctx context.Context, channelID string) ([]UserOverride, error)
}

type Store interface {
	SessionRepo() SessionRepo
	ChannelRepo() ChannelRepo
	OverrideRepo() OverrideRepo
	Close() error
}
