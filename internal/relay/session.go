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
// A stored session is reused only when its owning agent process is unknown
// (legacy rows) or still alive (agentPID matches); a session owned by a dead
// agent is replaced so dispatches never target a vanished process.
// threadID and userID are normalized: empty string when not applicable
// (see the session-key policy in the store docs).
func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID, threadID, userID string, agentPID int) (string, error) {
	sessionID, ownerPID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if sessionID != "" && (ownerPID == 0 || ownerPID == agentPID) {
		return sessionID, nil
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
