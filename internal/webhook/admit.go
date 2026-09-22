package webhook

import (
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/config"
)

type admissionPolicy struct {
	trustLogins map[string]bool
	verdicts    map[string][]string
}

func policyFromConfig(policy config.WebhookPolicy) admissionPolicy {
	out := admissionPolicy{
		trustLogins: make(map[string]bool, len(policy.TrustReviewLogins)),
		verdicts:    make(map[string][]string, len(policy.Verdicts)),
	}
	for _, login := range policy.TrustReviewLogins {
		out.trustLogins[login] = true
	}
	for name, phrases := range policy.Verdicts {
		lower := strings.ToLower(strings.TrimSpace(name))
		copied := make([]string, 0, len(phrases))
		for _, phrase := range phrases {
			copied = append(copied, strings.ToLower(strings.TrimSpace(phrase)))
		}
		out.verdicts[lower] = copied
	}
	return out
}

func resolveVerdict(body string, verdicts map[string][]string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
		line = strings.TrimSpace(strings.Trim(line, "*_` "))
		line = strings.ReplaceAll(line, "**", "")
		lower := strings.ToLower(strings.TrimSpace(line))
		for name, phrases := range verdicts {
			for _, phrase := range phrases {
				if phrase == "" {
					continue
				}
				if lower == phrase || strings.HasPrefix(lower, "verdict: "+phrase) || strings.HasPrefix(lower, "verdict:"+phrase) {
					return name
				}
			}
		}
	}
	return ""
}

func admitDelivery(ep config.EndpointConfig, policy admissionPolicy, envelope WebhookEnvelope) (bool, string) {
	eventType := strings.ToLower(strings.TrimSpace(stringValue(envelope["event_type"])))
	action := strings.ToLower(stringValue(envelope["action"]))
	reviewState := strings.ToLower(stringValue(envelope["review_state"]))
	prNumber := stringValue(envelope["pr_number"])
	reviewUser := stringValue(envelope["review_user"])
	prAuthor := stringValue(envelope["pr_author"])
	reviewVerdict := strings.ToLower(stringValue(envelope["review_verdict"]))
	selfReview := policy.trustLogins[reviewUser] && reviewUser == prAuthor && reviewState == "commented"

	for _, rule := range ep.Admit {
		if !strings.EqualFold(strings.TrimSpace(rule.Event), eventType) {
			continue
		}
		if len(rule.Actions) > 0 && !containsFold(rule.Actions, action) {
			continue
		}
		if containsFold(rule.Unless, action) {
			continue
		}
		if len(rule.ReviewState) > 0 && !containsFold(rule.ReviewState, reviewState) {
			continue
		}
		if len(rule.ReviewVerdict) > 0 {
			if !containsFold(rule.ReviewVerdict, reviewVerdict) {
				continue
			}
			if !selfReview {
				continue
			}
		}
		if rule.CheckStatus != "" {
			status := strings.ToLower(stringValue(envelope["status"]))
			want := strings.ToLower(strings.TrimSpace(rule.CheckStatus))
			if action != want || (status != "" && status != want) {
				continue
			}
		}
		if rule.CheckApp != "" {
			appName := stringValue(envelope["app_name"])
			if !strings.EqualFold(appName, rule.CheckApp) {
				continue
			}
		}
		if rule.Merged != nil && boolValue(envelope["merged"]) != *rule.Merged {
			continue
		}
		if !requireFlagsHold(rule.Require, envelope, selfReview, reviewVerdict) {
			continue
		}
		if failReason := requireClosedPR(rule.Require, envelope, prNumber); failReason != "" {
			return false, failReason
		}
		return true, ""
	}
	return false, fmt.Sprintf("endpoint %s admitted no rule for %s.%s", ep.Name, eventType, action)
}

func requireFlagsHold(require []string, envelope WebhookEnvelope, selfReview bool, reviewVerdict string) bool {
	for _, flag := range require {
		switch strings.TrimSpace(flag) {
		case "comment_trigger_configured":
			if !boolValue(envelope["comment_trigger_configured"]) {
				return false
			}
		case "comment_trigger":
			if stringValue(envelope["comment_trigger"]) == "" {
				return false
			}
		case "pr_open":
			if state := prState(envelope); state == "closed" || state == "merged" {
				return false
			}
		case "pr_resolvable":
			if !hasResolvablePR(envelope) {
				return false
			}
		case "approved_without_findings":
			if selfReview && (reviewVerdict != "approved" || boolValue(envelope["has_findings"])) {
				return false
			}
		}
	}
	return true
}

func requireClosedPR(require []string, envelope WebhookEnvelope, prNumber string) string {
	wantsOpen := false
	for _, flag := range require {
		if strings.TrimSpace(flag) == "pr_open" {
			wantsOpen = true
			break
		}
	}
	if !wantsOpen {
		return ""
	}
	if state := prState(envelope); state == "closed" || state == "merged" {
		eventType := strings.ToLower(strings.TrimSpace(stringValue(envelope["event_type"])))
		if eventType == "check_suite" {
			if prNumber != "" {
				return fmt.Sprintf("skipped: PR #%s is %s; no merge execution", prNumber, state)
			}
			return fmt.Sprintf("skipped: PR is %s; no merge execution", state)
		}
		return fmt.Sprintf("skipped: PR #%s is %s; no re-review execution", prNumber, state)
	}
	return ""
}

func containsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), want) {
			return true
		}
	}
	return false
}
