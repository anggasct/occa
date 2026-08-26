package relay

import (
	"context"
	"fmt"

	"github.com/anggasct/occa/internal/store"
)

// SessionResolution reports how a conversation's agent session was resolved:
// whether an existing session was resumed and whether a stored session
// existed before resolution began. A caller that recreates a session after
// a stored one existed uses HadStored to report context loss.
type SessionResolution struct {
	SessionID string
	Resumed   bool
	HadStored bool
}

type SessionResolver struct {
	repo   store.SessionRepo
	client Client
}

func NewSessionResolver(repo store.SessionRepo, client Client) *SessionResolver {
	return &SessionResolver{repo: repo, client: client}
}

func (r *SessionResolver) Resolve(ctx context.Context, platform, channelID, threadID, userID string, agentPID int) (string, error) {
	res, err := r.ResolveDetailed(ctx, platform, channelID, threadID, userID, agentPID)
	return res.SessionID, err
}

func (r *SessionResolver) ResolveDetailed(ctx context.Context, platform, channelID, threadID, userID string, agentPID int) (SessionResolution, error) {
	sessionID, ownerPID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return SessionResolution{}, fmt.Errorf("relay: resolve session: %w", err)
	}
	hadStored := sessionID != ""
	if hadStored {
		if ownerPID == agentPID {
			return SessionResolution{SessionID: sessionID, Resumed: true, HadStored: true}, nil
		}

		exists, err := r.client.SessionExists(ctx, sessionID)
		if err != nil {
			return SessionResolution{}, fmt.Errorf("relay: resolve session: check exists: %w", err)
		}
		if exists {
			if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
				return SessionResolution{}, fmt.Errorf("relay: resolve session: persist: %w", err)
			}
			return SessionResolution{SessionID: sessionID, Resumed: true, HadStored: true}, nil
		}
	}

	sessionID, err = r.client.CreateSession(ctx)
	if err != nil {
		return SessionResolution{}, fmt.Errorf("relay: resolve session: create: %w", err)
	}

	if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
		return SessionResolution{}, fmt.Errorf("relay: resolve session: persist: %w", err)
	}

	return SessionResolution{SessionID: sessionID, Resumed: false, HadStored: hadStored}, nil
}
