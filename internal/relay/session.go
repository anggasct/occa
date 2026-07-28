package relay

import (
	"context"
	"fmt"
	"time"

	"github.com/anggasct/occa/internal/store"
)

type SessionResolver struct {
	repo   store.SessionRepository
	client Client
}

func NewSessionResolver(repo store.SessionRepository, client Client) *SessionResolver {
	return &SessionResolver{repo: repo, client: client}
}

func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID string) (string, error) {
	s, err := r.repo.FindActive(ctx, platform, channelID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if s != nil {
		return s.OpenCodeSessionID, nil
	}

	sessionID, err := r.client.CreateSession(ctx)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: create: %w", err)
	}

	now := time.Now().Unix()
	err = r.repo.Create(ctx, &store.Session{
		ChannelID:         channelID,
		Platform:          platform,
		OpenCodeSessionID: sessionID,
		Active:            true,
		CreatedAt:         now,
		UpdatedAt:         now,
	})
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: persist: %w", err)
	}

	return sessionID, nil
}
