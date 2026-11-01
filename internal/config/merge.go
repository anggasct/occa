package config

import (
	"fmt"
	"strings"
)

// RepoMerge is the optional repos[].merge block.
type RepoMerge struct {
	// Mode picks who runs the merge lane: auto (code with the built-in merge
	// prompt, the agent with a custom one), code, or agent.
	Mode string `yaml:"mode,omitempty"`
	// Method is merge, squash or rebase; empty follows the repository's
	// allowed methods.
	Method       string `yaml:"method,omitempty"`
	DeleteBranch bool   `yaml:"delete_branch,omitempty"`
}

// MergePolicy is the resolved merge behaviour of a merge endpoint.
type MergePolicy struct {
	// Code is true when occa merges in code instead of an agent turn.
	Code         bool
	Method       string
	DeleteBranch bool
}

const (
	MergeModeAuto  = "auto"
	MergeModeCode  = "code"
	MergeModeAgent = "agent"
)

var mergeMethods = map[string]bool{"merge": true, "squash": true, "rebase": true}

// ResolveMergePolicy validates a repo's merge block and resolves it for the
// merge prompt source the repo ends up with.
func ResolveMergePolicy(m *RepoMerge, promptSource, context string) (MergePolicy, error) {
	var in RepoMerge
	if m != nil {
		in = *m
	}
	mode := strings.ToLower(strings.TrimSpace(in.Mode))
	switch mode {
	case "", MergeModeAuto:
		mode = MergeModeAuto
	case MergeModeCode, MergeModeAgent:
	default:
		return MergePolicy{}, fmt.Errorf("config: %smerge.mode must be auto, code or agent, got %q", context, in.Mode)
	}
	method := strings.ToLower(strings.TrimSpace(in.Method))
	if method != "" && !mergeMethods[method] {
		return MergePolicy{}, fmt.Errorf("config: %smerge.method must be merge, squash or rebase, got %q", context, in.Method)
	}
	code := mode == MergeModeCode || (mode == MergeModeAuto && promptSource == "builtin")
	return MergePolicy{Code: code, Method: method, DeleteBranch: in.DeleteBranch}, nil
}
