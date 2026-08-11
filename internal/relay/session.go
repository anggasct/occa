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
// A stored session is reused without validation only when it is owned by the
// current agent process (ownerPID == agentPID). Legacy rows with no recorded
// owner and rows owned by a replaced process are validated against the agent:
// the session is adopted (and stamped with the current PID) when it still
// exists, or a new session is created when it does not.
// threadID and userID are normalized: empty string when not applicable
// (see the session-key policy in the store docs).
func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID, threadID, userID string, agentPID int) (string, error) {
	sessionID, ownerPID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if sessionID != "" {
		if ownerPID == agentPID {
			return sessionID, nil
		}

		// ownerPID == 0 (legacy row) or ownerPID != agentPID (replaced
		// process): verify the session is still hosted before trusting it.
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
