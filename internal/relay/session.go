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
	return r.ResolveWithPermission(ctx, platform, channelID, threadID, userID, agentPID, nil)
}

func (r *SessionResolver) ResolveWithPermission(ctx context.Context, platform, channelID, threadID, userID string, agentPID int, ruleset PermissionRuleset) (string, error) {
	sessionID, ownerPID, err := r.repo.Active(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: %w", err)
	}
	if sessionID != "" {
		if ownerPID == agentPID {
			if err := r.applyPermission(ctx, sessionID, ruleset); err != nil {
				return "", err
			}
			return sessionID, nil
		}

		exists, err := r.client.SessionExists(ctx, sessionID)
		if err != nil {
			return "", fmt.Errorf("relay: resolve session: check exists: %w", err)
		}
		if exists {
			if err := r.applyPermission(ctx, sessionID, ruleset); err != nil {
				return "", err
			}
			if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
				return "", fmt.Errorf("relay: resolve session: persist: %w", err)
			}
			return sessionID, nil
		}
	}

	createdWithPermission := false
	if ruleset != nil {
		creator, ok := r.client.(interface {
			CreateSessionWithPermission(context.Context, PermissionRuleset) (string, error)
		})
		if ok {
			sessionID, err = creator.CreateSessionWithPermission(ctx, ruleset)
			createdWithPermission = true
		} else {
			sessionID, err = r.client.CreateSession(ctx)
		}
	} else {
		sessionID, err = r.client.CreateSession(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("relay: resolve session: create: %w", err)
	}
	if ruleset != nil && !createdWithPermission {
		if err := r.applyPermission(ctx, sessionID, ruleset); err != nil {
			return "", err
		}
	}

	if err := r.repo.SetActive(ctx, platform, channelID, threadID, userID, sessionID, agentPID); err != nil {
		return "", fmt.Errorf("relay: resolve session: persist: %w", err)
	}

	return sessionID, nil
}

func (r *SessionResolver) applyPermission(ctx context.Context, sessionID string, ruleset PermissionRuleset) error {
	if ruleset == nil {
		return nil
	}
	permissionClient, ok := r.client.(interface {
		SetSessionPermission(context.Context, string, PermissionRuleset) error
	})
	if !ok {
		return fmt.Errorf("relay: resolve session: permission policy unsupported")
	}
	if err := permissionClient.SetSessionPermission(ctx, sessionID, ruleset); err != nil {
		return fmt.Errorf("relay: resolve session: permission policy: %w", err)
	}
	return nil
}
