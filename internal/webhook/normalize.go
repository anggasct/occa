package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type WebhookEnvelope map[string]any

func normalizeWebhook(body []byte, eventType, deliveryID string, skipped bool, skipReason string, commentTriggers ...[]string) WebhookEnvelope {
	payload := make(map[string]any)
	var decoded any
	if json.Unmarshal(body, &decoded) == nil {
		if object, ok := decoded.(map[string]any); ok {
			payload = object
		}
	}

	var triggers []string
	if len(commentTriggers) > 0 {
		triggers = commentTriggers[0]
	}
	var validTriggers []string
	for _, t := range triggers {
		if s := strings.TrimSpace(t); s != "" {
			validTriggers = append(validTriggers, s)
		}
	}

	envelope := WebhookEnvelope{
		"source":                     "github",
		"event_type":                 strings.TrimSpace(eventType),
		"action":                     stringValue(payload["action"]),
		"delivery_id":                deliveryID,
		"repository":                 repoName(payload["repository"]),
		"project":                    "",
		"pr_number":                  "",
		"pr_url":                     "",
		"title":                      "",
		"head_branch":                "",
		"base_branch":                "",
		"default_branch":             repoDefaultBranch(payload["repository"]),
		"pr_author":                  "",
		"pr_state":                   "",
		"review_state":               "",
		"review_user":                "",
		"review_verdict":             "",
		"review_commit":              "",
		"review_url":                 "",
		"has_findings":               false,
		"comment_body":               "",
		"comment_trigger":            "",
		"comment_trigger_configured": len(validTriggers) > 0,
		"merged":                     false,
		"merge_commit":               "",
		"suite_id":                   "",
		"app_name":                   "",
		"status":                     "",
		"conclusion":                 "",
		"head_sha":                   "",
		"pr_numbers":                 []string{},
		"skip":                       skipped,
		"skip_reason":                skipReason,
	}

	if envelope["event_type"] == "" {
		envelope["event_type"] = stringValue(payload["event_type"])
	}

	pullRequest, _ := payload["pull_request"].(map[string]any)
	switch envelope["event_type"] {
	case "pull_request", "pull_request_review":
		fillPullRequest(envelope, pullRequest)
		if envelope["event_type"] == "pull_request_review" {
			if review, ok := payload["review"].(map[string]any); ok {
				envelope["review_state"] = stringValue(review["state"])
				envelope["review_user"] = userName(review["user"])
				envelope["comment_body"] = stringValue(review["body"])
				envelope["review_verdict"] = reviewVerdict(stringValue(review["body"]))
				envelope["review_commit"] = stringValue(review["commit_id"])
				envelope["review_url"] = stringValue(review["html_url"])
				envelope["has_findings"] = hasActionableFindings(stringValue(review["body"]))
			}
		}
	case "issue_comment":
		issue, _ := payload["issue"].(map[string]any)
		comment, _ := payload["comment"].(map[string]any)
		if issuePR, ok := issue["pull_request"].(map[string]any); ok {
			envelope["pr_number"] = numberValue(issue["number"])
			envelope["title"] = stringValue(issue["title"])
			envelope["pr_url"] = stringValue(issuePR["html_url"])
			envelope["comment_trigger"] = commentTrigger(stringValue(comment["body"]), validTriggers)
			// GitHub sets issue.state to "closed" on both merged and plain
			// closed PRs; "merged" disambiguates. Expose both so the gate can
			// refuse re-review execution on a terminal PR.
			envelope["pr_state"] = stringValue(issue["state"])
			if boolValue(issuePR["merged"]) {
				envelope["pr_state"] = "merged"
			}
		}
		envelope["comment_body"] = stringValue(comment["body"])
		envelope["review_user"] = userName(comment["user"])
	case "check_suite":
		fillCheckSuite(envelope, payload)
	}

	if envelope["repository"] == "" {
		envelope["repository"] = repoName(mapValue(pullRequest, "base")["repo"])
	}
	if envelope["default_branch"] == "" {
		envelope["default_branch"] = repoDefaultBranch(mapValue(pullRequest, "base")["repo"])
	}
	return envelope
}

func fillPullRequest(envelope WebhookEnvelope, pullRequest map[string]any) {
	if pullRequest == nil {
		return
	}
	envelope["pr_number"] = numberValue(pullRequest["number"])
	envelope["pr_url"] = stringValue(pullRequest["html_url"])
	envelope["title"] = stringValue(pullRequest["title"])
	envelope["head_branch"] = stringValue(mapValue(pullRequest, "head")["ref"])
	envelope["base_branch"] = stringValue(mapValue(pullRequest, "base")["ref"])
	envelope["pr_author"] = userName(pullRequest["user"])
	envelope["merged"] = boolValue(pullRequest["merged"])
	envelope["merge_commit"] = stringValue(pullRequest["merge_commit_sha"])
	envelope["pr_state"] = stringValue(pullRequest["state"])
	if boolValue(pullRequest["merged"]) {
		envelope["pr_state"] = "merged"
	}
	if envelope["repository"] == "" {
		envelope["repository"] = repoName(mapValue(pullRequest, "base")["repo"])
	}
	if envelope["default_branch"] == "" {
		envelope["default_branch"] = repoDefaultBranch(mapValue(pullRequest, "base")["repo"])
	}
}

func fillCheckSuite(envelope WebhookEnvelope, payload map[string]any) {
	cs, _ := payload["check_suite"].(map[string]any)
	if cs == nil {
		cs = payload
	}
	if id := cs["id"]; id != nil {
		envelope["suite_id"] = numberValue(id)
	} else if id := cs["suite_id"]; id != nil {
		envelope["suite_id"] = numberValue(id)
	}
	if status := cs["status"]; status != nil {
		envelope["status"] = stringValue(status)
	}
	if conclusion := cs["conclusion"]; conclusion != nil {
		envelope["conclusion"] = stringValue(conclusion)
	}
	if headBranch := cs["head_branch"]; headBranch != nil {
		envelope["head_branch"] = stringValue(headBranch)
	}
	if headSHA := cs["head_sha"]; headSHA != nil {
		envelope["head_sha"] = stringValue(headSHA)
	}
	if app, ok := cs["app"].(map[string]any); ok {
		envelope["app_name"] = stringValue(app["name"])
	} else if appName := cs["app_name"]; appName != nil {
		envelope["app_name"] = stringValue(appName)
	}

	var prNumbers []string
	rawPRs, ok := cs["pull_requests"].([]any)
	if !ok {
		rawPRs, _ = payload["pull_requests"].([]any)
	}
	if rawPRs != nil {
		for _, prItem := range rawPRs {
			if prMap, ok := prItem.(map[string]any); ok {
				num := numberValue(prMap["number"])
				if num != "" {
					prNumbers = append(prNumbers, num)
				}
			}
		}
		if len(rawPRs) == 1 {
			if prMap, ok := rawPRs[0].(map[string]any); ok {
				if envelope["head_branch"] == "" {
					envelope["head_branch"] = stringValue(mapValue(prMap, "head")["ref"])
				}
				if envelope["base_branch"] == "" {
					envelope["base_branch"] = stringValue(mapValue(prMap, "base")["ref"])
				}
				if envelope["pr_url"] == "" {
					if u := stringValue(prMap["html_url"]); u != "" {
						envelope["pr_url"] = u
					} else {
						envelope["pr_url"] = stringValue(prMap["url"])
					}
				}
				if envelope["title"] == "" {
					envelope["title"] = stringValue(prMap["title"])
				}
				if st := stringValue(prMap["state"]); st != "" {
					envelope["pr_state"] = st
				}
				if boolValue(prMap["merged"]) {
					envelope["pr_state"] = "merged"
				}
			}
		}
		if len(rawPRs) > 1 {
			allClosed := true
			for _, prItem := range rawPRs {
				if prMap, ok := prItem.(map[string]any); ok {
					st := strings.ToLower(stringValue(prMap["state"]))
					merged := boolValue(prMap["merged"])
					if !merged && st != "closed" && st != "merged" {
						allClosed = false
						break
					}
				}
			}
			if allClosed {
				envelope["pr_state"] = "closed"
			}
		}
	} else if prs, ok := cs["pr_numbers"].([]any); ok {
		for _, item := range prs {
			if num := numberValue(item); num != "" {
				prNumbers = append(prNumbers, num)
			}
		}
	} else if prs, ok := cs["pr_numbers"].([]string); ok {
		prNumbers = append(prNumbers, prs...)
	} else if prNum := cs["pr_number"]; prNum != nil {
		if num := numberValue(prNum); num != "" {
			prNumbers = append(prNumbers, num)
		}
	}

	envelope["pr_numbers"] = prNumbers
	if len(prNumbers) == 1 {
		envelope["pr_number"] = prNumbers[0]
	}

	if envelope["repository"] == "" {
		if repo := cs["repository"]; repo != nil {
			envelope["repository"] = repoName(repo)
		} else if repo := payload["repository"]; repo != nil {
			envelope["repository"] = repoName(repo)
		}
	}
	if envelope["default_branch"] == "" {
		if repo := cs["repository"]; repo != nil {
			envelope["default_branch"] = repoDefaultBranch(repo)
		} else if repo := payload["repository"]; repo != nil {
			envelope["default_branch"] = repoDefaultBranch(repo)
		}
	}
}

func mapValue(object map[string]any, key string) map[string]any {
	if object == nil {
		return nil
	}
	value, _ := object[key].(map[string]any)
	return value
}

func repoName(value any) string {
	if object, ok := value.(map[string]any); ok {
		return stringValue(object["full_name"])
	}
	return stringValue(value)
}

func repoDefaultBranch(value any) string {
	if object, ok := value.(map[string]any); ok {
		return strings.TrimSpace(stringValue(object["default_branch"]))
	}
	return ""
}

func userName(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if login := stringValue(object["login"]); login != "" {
		return login
	}
	return stringValue(object["name"])
}

func stringValue(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case string:
		return value
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func numberValue(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case json.Number:
		return value.String()
	case float64:
		return fmt.Sprintf("%.0f", value)
	case float32:
		return fmt.Sprintf("%.0f", value)
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	default:
		return stringValue(value)
	}
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func commentTrigger(body string, triggers []string) string {
	lower := strings.ToLower(body)
	for _, trigger := range triggers {
		trimmed := strings.TrimSpace(trigger)
		if trimmed != "" && strings.Contains(lower, strings.ToLower(trimmed)) {
			return trigger
		}
	}
	return ""
}

func commentTriggerKey(envelope WebhookEnvelope) string {
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	pr := strings.TrimSpace(stringValue(envelope["pr_number"]))
	if pr == "" {
		return ""
	}
	if repo == "" {
		return "comment_trigger:" + pr
	}
	return "comment_trigger:" + repo + "#" + pr
}

func normalizedReviewBody(body string) string {
	return strings.Join(strings.Fields(body), " ")
}

func reviewDedupeKey(envelope WebhookEnvelope) string {
	if stringValue(envelope["event_type"]) != "pull_request_review" {
		return ""
	}
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	pr := strings.TrimSpace(stringValue(envelope["pr_number"]))
	commit := strings.TrimSpace(stringValue(envelope["review_commit"]))
	state := strings.ToLower(strings.TrimSpace(stringValue(envelope["review_state"])))
	if repo == "" || pr == "" || state == "" {
		return ""
	}
	body := normalizedReviewBody(stringValue(envelope["comment_body"]))
	sum := sha256.Sum256([]byte(repo + "\n" + pr + "\n" + commit + "\n" + state + "\n" + body))
	return hex.EncodeToString(sum[:])
}

func reviewVerdict(body string) string {
	return reviewVerdictWith(map[string][]string{
		"approved":        {"approved"},
		"request_changes": {"request changes", "request_changes"},
	}, body)
}

func reviewVerdictWith(verdicts map[string][]string, body string) string {
	return resolveVerdict(body, verdicts)
}

func hasActionableFindings(body string) bool {
	lower := strings.ToLower(body)
	if declaresNoFindings(lower) {
		return false
	}

	lines := strings.Split(body, "\n")
	for i, line := range lines {
		heading := strings.TrimSpace(line)
		if !strings.HasPrefix(heading, "#") {
			continue
		}
		heading = strings.TrimSpace(strings.TrimLeft(heading, "#"))
		heading = strings.Trim(heading, " *_`")
		if !strings.EqualFold(heading, "findings") {
			continue
		}

		sectionEnd := len(lines)
		for j := i + 1; j < len(lines); j++ {
			next := strings.TrimSpace(lines[j])
			if strings.HasPrefix(next, "#") {
				sectionEnd = j
				break
			}
		}
		section := strings.TrimSpace(strings.Join(lines[i+1:sectionEnd], "\n"))
		return section != "" && !declaresNoFindings(section)
	}

	if strings.Contains(lower, "severity:") &&
		strings.Contains(lower, "type:") &&
		strings.Contains(lower, "problem:") {
		return true
	}
	if strings.Contains(lower, "actionable finding") || strings.Contains(lower, "finding #") {
		return true
	}

	return true
}

func declaresNoFindings(value string) bool {
	return strings.Contains(value, "no actionable finding") ||
		strings.Contains(value, "actionable findings: 0") ||
		strings.Contains(value, "actionable findings: none") ||
		strings.Contains(value, "no blocking findings") ||
		strings.Contains(value, "findings: none") ||
		strings.Contains(value, "findings: 0") ||
		strings.Contains(value, "no findings") ||
		strings.Contains(value, "findings\nnone")
}
