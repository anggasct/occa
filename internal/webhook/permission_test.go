package webhook

import (
	"testing"

	"github.com/anggasct/occa/internal/relay"
)

func TestBuildPermissionPolicyCoversEveryWebhookWorkflow(t *testing.T) {
	for _, workflow := range []string{"github_reviewer", "github_fix", "github_merge", "github_merged"} {
		t.Run(workflow, func(t *testing.T) {
			policy, err := BuildPermissionPolicy(PermissionPolicyInput{
				Workflow:        workflow,
				Repository:      "anggasct/occa",
				PRNumber:        "42",
				Branch:          "fix/webhook-permissions",
				Worktree:        "/srv/projects/occa/.worktree/pr-42",
				ProjectDocsRoot: "/srv/vault/1-projects/occa",
			})
			if err != nil {
				t.Fatalf("BuildPermissionPolicy: %v", err)
			}
			if len(policy) == 0 {
				t.Fatal("expected non-empty policy")
			}
			if policy[0] != (relay.PermissionRule{Permission: "*", Pattern: "*", Action: "deny"}) {
				t.Fatalf("policy must start with a fail-closed default, got %+v", policy[0])
			}
			for _, rule := range policy {
				if rule.Permission == "*" && rule.Pattern == "*" && rule.Action == "allow" {
					t.Fatal("policy contains a global allow rule")
				}
			}
		})
	}
}

func TestBuildPermissionPolicyExactPathIsolation(t *testing.T) {
	input := PermissionPolicyInput{
		Workflow:        "github_fix",
		Repository:      "anggasct/occa",
		PRNumber:        "42",
		Branch:          "fix/webhook-permissions",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	}
	policy, err := BuildPermissionPolicy(input)
	if err != nil {
		t.Fatalf("BuildPermissionPolicy: %v", err)
	}

	if !hasRule(policy, "edit", "/srv/projects/occa/.worktree/pr-42/**", "allow") {
		t.Fatal("fixer must allow edits below the exact PR worktree")
	}
	if hasRule(policy, "edit", "/srv/vault/1-projects/occa/**", "allow") {
		t.Fatal("fixer must not edit project docs")
	}
	if hasRule(policy, "edit", "/srv/projects/occa/.worktree/other/**", "allow") {
		t.Fatal("fixer must not edit another worktree")
	}
	if !hasRule(policy, "external_directory", "/srv/vault/1-projects/occa/**", "allow") {
		t.Fatal("fixer must be able to read the matching project docs")
	}
}

func TestBuildPermissionPolicyWorkflowBoundaries(t *testing.T) {
	base := PermissionPolicyInput{
		Repository:      "anggasct/occa",
		PRNumber:        "42",
		Branch:          "fix/webhook-permissions",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	}

	tests := []struct {
		workflow string
		allow    relay.PermissionRule
		deny     relay.PermissionRule
	}{
		{
			workflow: "github_reviewer",
			allow:    relay.PermissionRule{Permission: "gh", Pattern: "unused", Action: "allow"},
			deny:     relay.PermissionRule{Permission: "edit", Pattern: "*", Action: "deny"},
		},
		{
			workflow: "github_merge",
			allow:    relay.PermissionRule{Permission: "bash", Pattern: "gh pr merge 42 --repo anggasct/occa*", Action: "allow"},
			deny:     relay.PermissionRule{Permission: "edit", Pattern: "*", Action: "deny"},
		},
		{
			workflow: "github_merged",
			allow:    relay.PermissionRule{Permission: "edit", Pattern: "/srv/vault/1-projects/occa/**", Action: "allow"},
			deny:     relay.PermissionRule{Permission: "bash", Pattern: "gh pr merge*", Action: "deny"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.workflow, func(t *testing.T) {
			base.Workflow = tt.workflow
			policy, err := BuildPermissionPolicy(base)
			if err != nil {
				t.Fatalf("BuildPermissionPolicy: %v", err)
			}
			if tt.workflow == "github_reviewer" {
				if !hasRule(policy, "external_directory", "/srv/vault/1-projects/occa/**", "allow") {
					t.Fatal("reviewer must read the matching project docs")
				}
			} else if !hasRule(policy, tt.allow.Permission, tt.allow.Pattern, tt.allow.Action) {
				t.Fatalf("missing allow rule %+v", tt.allow)
			}
			if !hasRule(policy, tt.deny.Permission, tt.deny.Pattern, tt.deny.Action) {
				t.Fatalf("missing deny rule %+v", tt.deny)
			}
		})
	}
}

func TestBuildPermissionPolicyFailsClosedForInvalidScope(t *testing.T) {
	_, err := BuildPermissionPolicy(PermissionPolicyInput{Workflow: "github_unknown"})
	if err == nil {
		t.Fatal("unknown workflow must fail closed")
	}
	_, err = BuildPermissionPolicy(PermissionPolicyInput{
		Workflow:        "github_merge",
		Repository:      "anggasct/occa",
		PRNumber:        "not-a-number",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	})
	if err == nil {
		t.Fatal("invalid pull request scope must fail closed")
	}
}

func hasRule(policy relay.PermissionRuleset, permission, pattern, action string) bool {
	for _, rule := range policy {
		if rule.Permission == permission && rule.Pattern == pattern && rule.Action == action {
			return true
		}
	}
	return false
}
