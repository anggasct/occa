package webhook

import "strings"

type Phase string

const (
	PhaseQueued   Phase = "queued"
	PhaseDispatch Phase = "dispatch"
	PhaseReview   Phase = "review"
	PhaseFix      Phase = "fix"
	PhaseMerge    Phase = "merge"
	PhaseMerged   Phase = "merged"
	PhaseBlocked  Phase = "blocked"
	PhaseFailed   Phase = "failed"
)

var DefaultPhaseIcons = map[Phase]string{
	PhaseQueued:   "⏳",
	PhaseDispatch: "🛠️",
	PhaseReview:   "👀",
	PhaseFix:      "🔨",
	PhaseMerge:    "🔀",
	PhaseMerged:   "✅",
	PhaseBlocked:  "🔴",
	PhaseFailed:   "❌",
}

var KnownPhases = []Phase{
	PhaseQueued,
	PhaseDispatch,
	PhaseReview,
	PhaseFix,
	PhaseMerge,
	PhaseMerged,
	PhaseBlocked,
	PhaseFailed,
}

func ParsePhase(value string) (Phase, bool) {
	candidate := Phase(strings.ToLower(strings.TrimSpace(value)))
	for _, known := range KnownPhases {
		if candidate == known {
			return known, true
		}
	}
	return "", false
}

func PhaseIcon(phase Phase, overrides map[string]string) string {
	if icon := phaseIconOverride(overrides, string(phase)); icon != "" {
		return icon
	}
	return DefaultPhaseIcons[phase]
}

func phaseIconOverride(overrides map[string]string, key string) string {
	if len(overrides) == 0 {
		return ""
	}
	for name, icon := range overrides {
		if strings.EqualFold(strings.TrimSpace(name), key) {
			return strings.TrimSpace(icon)
		}
	}
	return ""
}

func PhaseForWorkflow(workflow string) Phase {
	switch strings.TrimSpace(strings.ToLower(workflow)) {
	case "review", "reviewer":
		return PhaseReview
	case "fix", "pr-fix":
		return PhaseFix
	case "merge":
		return PhaseMerge
	case "merged":
		return PhaseMerged
	case "blocked":
		return PhaseBlocked
	case "failed":
		return PhaseFailed
	case "dispatch", "dispatched":
		return PhaseDispatch
	default:
		return PhaseQueued
	}
}

func admissionPhase(workflow string) Phase {
	switch strings.TrimSpace(strings.ToLower(workflow)) {
	case "review", "reviewer", "fix", "pr-fix", "merge":
		return PhaseQueued
	case "merged":
		return ""
	default:
		return PhaseForWorkflow(workflow)
	}
}

func IsTerminalPhase(phase Phase) bool {
	switch phase {
	case PhaseMerged, PhaseFailed, PhaseBlocked:
		return true
	}
	return false
}

func FormatPhaseParent(repo, prNumber string, phase Phase, icons map[string]string) string {
	head := strings.TrimSpace(repo)
	if number := strings.TrimSpace(prNumber); number != "" {
		if head != "" {
			head += "#"
		}
		head += number
	}
	return PhaseIcon(phase, icons) + " " + head + " — " + string(phase)
}

func NextStepForPhase(phase Phase) string {
	switch phase {
	case PhaseReview:
		return "review"
	case PhaseFix:
		return "push + re-review"
	case PhaseMerge:
		return "merge"
	case PhaseDispatch:
		return "agent step"
	default:
		return ""
	}
}
