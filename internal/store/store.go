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

type SessionRepository interface {
	FindActive(ctx context.Context, platform, channelID string) (*Session, error)
	Create(ctx context.Context, s *Session) error
	Deactivate(ctx context.Context, platform, channelID string) error
}
