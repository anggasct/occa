package relay

import (
	"context"
	"fmt"

	"github.com/anggasct/occa/internal/store"
)

type SessionResolver struct {
	repo   store.SessionRepo
	client Client
}

func NewSessionResolver(repo store.SessionRepo, client Client) *SessionResolver {
	return &SessionResolver{repo: repo, client: client}
}

func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID, threadID, userID string, agentPID int) (string, error) {
	sessionID, ownerPID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if sessionID != "" {
		if ownerPID == agentPID {
			return sessionID, nil
		}

		exists, err := r.client.SessionExists(ctx, sessionID)
		if err != nil {
			return "", fmt.Errorf("relay: resolve session: check exists: %w", err)
		}
		if exists {
			if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
				return "", fmt.Errorf("relay: resolve session: persist: %w", err)
			}
			return sessionID, nil
		}
	}

	sessionID, err = r.client.CreateSession(ctx)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: create: %w", err)
	}

	if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
		return "", fmt.Errorf("relay: resolve session: persist: %w", err)
	}

	return sessionID, nil
}
