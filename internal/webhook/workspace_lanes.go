package webhook

import (
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

const reviewWorkspaceTTL = 2 * time.Hour

type laneWorkspacePolicy struct {
	Type          string
	Mode          string
	IsolatedTTL   time.Duration
	RevisionKeyed bool
}

var laneWorkspacePolicies = map[string]laneWorkspacePolicy{
	"review": {
		Type:          config.WorkspaceTypeGit,
		Mode:          config.WorkspaceModeIsolated,
		IsolatedTTL:   reviewWorkspaceTTL,
		RevisionKeyed: true,
	},
	"fix": {
		Type: config.WorkspaceTypeGit,
		Mode: config.WorkspaceModeMutable,
	},
	"merge": {
		Type: config.WorkspaceTypeNone,
	},
	"merged": {
		Type: config.WorkspaceTypeNone,
	},
}

func lanePolicyForWorkflow(workflow string) (laneWorkspacePolicy, bool) {
	policy, ok := laneWorkspacePolicies[strings.ToLower(strings.TrimSpace(workflow))]
	return policy, ok
}
