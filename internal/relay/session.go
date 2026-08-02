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

// Resolve returns the active agent session for a conversation identified by
// (platform, channelID, threadID, userID). Creates one when none is active.
// threadID and userID are normalized: empty string when not applicable
// (see the session-key policy in the store docs).
func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID, threadID, userID string) (string, error) {
	sessionID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if sessionID != "" {
		return sessionID, nil
	}

	sessionID, err = r.client.CreateSession(ctx)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: create: %w", err)
	}

	if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID); err != nil {
		return "", fmt.Errorf("relay: resolve session: persist: %w", err)
	}

	return sessionID, nil
}
