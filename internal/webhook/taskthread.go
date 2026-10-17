package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
)

type TaskStatus string

const (
	TaskDispatched TaskStatus = "dispatched"
	TaskClaimed    TaskStatus = "claimed"
	TaskPROpened   TaskStatus = "pr_opened"
	TaskMerged     TaskStatus = "merged"
	TaskEvent      TaskStatus = "event"
	TaskFailed     TaskStatus = "failed"
)

var DefaultTaskStatusIcons = map[string]string{
	"dispatched": "⏳",
	"claimed":    "👀",
	"pr_opened":  "🔨",
	"merged":     "✅",
	"event":      "📨",
	"failed":     "❌",
}

var DefaultTaskStatusColors = map[string]int{
	"dispatched": 0x3498DB,
	"claimed":    0x3498DB,
	"pr_opened":  0x2ECC71,
	"merged":     0x9B59B6,
	"event":      0x95A5A6,
	"failed":     0xE74C3C,
}

var RequiredTaskWorkflows = []string{"review", "fix", "merge", "merged"}

func StatusForWorkflow(workflow string) TaskStatus {
	switch strings.TrimSpace(strings.ToLower(workflow)) {
	case "review":
		return TaskClaimed
	case "fix", "merge":
		return TaskPROpened
	case "merged":
		return TaskMerged
	case "custom":
		return TaskEvent
	default:
		return TaskDispatched
	}
}

func TaskStatusForAudit(auditStatus, workflow string) TaskStatus {
	if strings.EqualFold(strings.TrimSpace(auditStatus), "FAILED") {
		return TaskFailed
	}
	return StatusForWorkflow(workflow)
}

func isTaskTerminal(status TaskStatus) bool {
	switch status {
	case TaskMerged, TaskFailed:
		return true
	}
	return false
}

func lookupStatusOverride(keys []string, want TaskStatus) string {
	for _, k := range keys {
		if strings.EqualFold(strings.TrimSpace(k), string(want)) {
			return k
		}
	}
	return ""
}

func TaskIcon(status TaskStatus, overrides map[string]string) string {
	if key := lookupStatusOverride(statusOverrideKeys(overrides), status); key != "" {
		if icon := strings.TrimSpace(overrides[key]); icon != "" {
			return icon
		}
	}
	if icon := DefaultTaskStatusIcons[string(status)]; icon != "" {
		return icon
	}
	return DefaultTaskStatusIcons["event"]
}

func TaskColor(status TaskStatus, overrides map[string]int) int {
	if key := lookupStatusOverride(statusColorKeys(overrides), status); key != "" {
		if color, ok := overrides[key]; ok && color >= 0 && color <= 0xFFFFFF {
			return color
		}
	}
	if color, ok := DefaultTaskStatusColors[string(status)]; ok {
		return color
	}
	return DefaultTaskStatusColors["event"]
}

func statusOverrideKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func statusColorKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func normalizeRepoKey(repo string) string {
	s := strings.TrimSpace(repo)
	if s == "" {
		return ""
	}
	if canonical, err := config.CanonicalRepository(s); err == nil {
		return config.RepositoryPath(canonical)
	}
	lower := strings.ToLower(s)
	lower = strings.TrimSuffix(lower, ".git")
	lower = strings.Trim(lower, "/")
	if rest, ok := strings.CutPrefix(lower, "github.com/"); ok {
		return rest
	}
	return lower
}

func normalizeBranchKey(branch string) string {
	return strings.ToLower(strings.TrimSpace(branch))
}

func TaskThreadKey(repo, branch string) string {
	return normalizeRepoKey(repo) + "#" + normalizeBranchKey(branch)
}

func TaskRepoBranch(key WebhookExecutionKey, envelope WebhookEnvelope) (string, string) {
	repo := strings.TrimSpace(key.Repository)
	branch := strings.TrimSpace(key.Branch)
	if repo == "" {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
	}
	if branch == "" {
		branch = strings.TrimSpace(stringValue(envelope["head_branch"]))
	}
	return normalizeRepoKey(repo), normalizeBranchKey(branch)
}

type TaskThreadRecord struct {
	Repo          string
	Branch        string
	RootMessageID string
	ThreadID      string
	Status        TaskStatus
	Color         int
	UpdatedAt     time.Time
}

type TaskThreadRegistry struct {
	mu      sync.RWMutex
	records map[string]TaskThreadRecord
	icons   map[string]string
	colors  map[string]int
}

func NewTaskThreadRegistry() *TaskThreadRegistry {
	return &TaskThreadRegistry{records: make(map[string]TaskThreadRecord)}
}

func (t *TaskThreadRegistry) SetStatusConfig(icons map[string]string, colors map[string]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	normalizedIcons := make(map[string]string, len(icons))
	for k, v := range icons {
		normalizedIcons[strings.ToLower(strings.TrimSpace(k))] = v
	}
	normalizedColors := make(map[string]int, len(colors))
	for k, v := range colors {
		normalizedColors[strings.ToLower(strings.TrimSpace(k))] = v
	}
	t.icons = normalizedIcons
	t.colors = normalizedColors
}

func (t *TaskThreadRegistry) Lookup(repo, branch string) (TaskThreadRecord, bool) {
	if strings.TrimSpace(repo) == "" || strings.TrimSpace(branch) == "" {
		return TaskThreadRecord{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.records[TaskThreadKey(repo, branch)]
	return rec, ok
}

func (t *TaskThreadRegistry) Upsert(repo, branch, rootID, threadID string, status TaskStatus) TaskThreadRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.records == nil {
		t.records = make(map[string]TaskThreadRecord)
	}
	key := TaskThreadKey(repo, branch)
	prev := t.records[key]
	if rootID == "" {
		rootID = prev.RootMessageID
	}
	if threadID == "" {
		threadID = prev.ThreadID
	}
	rec := TaskThreadRecord{
		Repo:          normalizeRepoKey(repo),
		Branch:        normalizeBranchKey(branch),
		RootMessageID: rootID,
		ThreadID:      threadID,
		Status:        status,
		Color:         TaskColor(status, t.colors),
		UpdatedAt:     time.Now(),
	}
	t.records[key] = rec
	return rec
}

func (t *TaskThreadRegistry) Snapshot() map[string]TaskThreadRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]TaskThreadRecord, len(t.records))
	for k, v := range t.records {
		out[k] = v
	}
	return out
}

func (t *TaskThreadRegistry) MissDetail(repo, branch string) (string, string, string, bool, bool) {
	tried := TaskThreadKey(repo, branch)
	wantBranch := normalizeBranchKey(branch)
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, triedExists := t.records[tried]
	if wantBranch == "" {
		return tried, "", "", false, triedExists
	}
	fallbackKey := ""
	for key, rec := range t.records {
		if key == tried {
			continue
		}
		_, keyBranch, ok := strings.Cut(key, "#")
		if !ok || keyBranch != wantBranch {
			continue
		}
		if strings.TrimSpace(rec.ThreadID) != "" {
			return tried, key, rec.ThreadID, true, triedExists
		}
		if fallbackKey == "" {
			fallbackKey = key
		}
	}
	if fallbackKey != "" {
		return tried, fallbackKey, "", true, triedExists
	}
	return tried, "", "", false, triedExists
}

func FormatTaskParent(repo, branch, prNumber string, status TaskStatus, icons map[string]string) string {
	icon := TaskIcon(status, icons)
	label := string(status)
	head := strings.TrimSpace(repo) + "#" + strings.TrimSpace(branch)
	if prNumber = strings.TrimSpace(prNumber); prNumber != "" {
		head += " · PR #" + prNumber
	}
	return icon + " " + head + " — " + label
}

func FormatTaskLinks(prURL string) string {
	prURL = normalizeTaskPRURL(prURL)
	if prURL == "" {
		return ""
	}
	return "[Open PR](" + prURL + ")"
}

func normalizeTaskPRURL(raw string) string {
	prURL := strings.TrimSpace(raw)
	if prURL == "" {
		return ""
	}
	if rewritten := rewriteGitHubAPIPrURL(prURL); rewritten != "" {
		return rewritten
	}
	return prURL
}

func rewriteGitHubAPIPrURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	lower := strings.ToLower(trimmed)
	const marker = "api.github.com/repos/"
	idx := strings.Index(lower, marker)
	if idx < 0 {
		return ""
	}
	rest := trimmed[idx+len(marker):]
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 4 {
		return ""
	}
	owner, repo, pulls, number := parts[0], parts[1], parts[2], parts[3]
	if owner == "" || repo == "" || number == "" {
		return ""
	}
	if !strings.EqualFold(pulls, "pulls") {
		return ""
	}
	digits := number
	for i, r := range number {
		if r < '0' || r > '9' {
			digits = number[:i]
			break
		}
	}
	if digits == "" {
		return ""
	}
	return "https://github.com/" + owner + "/" + repo + "/pull/" + digits
}

func TaskCardHeader(envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string) string {
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	if repo == "" || branch == "" {
		return ""
	}
	prNumber := strings.TrimSpace(stringValue(envelope["pr_number"]))
	return FormatTaskParent(repo, branch, prNumber, status, icons)
}

func HasOpenPRButton(buttons []channel.Button) bool {
	for _, b := range buttons {
		if b.Label == "Open PR" && strings.TrimSpace(b.URL) != "" {
			return true
		}
	}
	return false
}

func TaskInlineFallback(prURL, platform string, buttons []channel.Button) string {
	if normalizeTaskPRURL(prURL) == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(platform), "discord") && HasOpenPRButton(buttons) {
		return ""
	}
	return FormatTaskLinks(prURL)
}

func TaskCardHeaderWithButtons(envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string, platform string, buttons []channel.Button) string {
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	if repo == "" || branch == "" {
		return ""
	}
	prNumber := strings.TrimSpace(stringValue(envelope["pr_number"]))
	header := FormatTaskParent(repo, branch, prNumber, status, icons)
	if link := TaskInlineFallback(stringValue(envelope["pr_url"]), platform, buttons); link != "" {
		header += "\n" + link
	}
	return header
}

func WithTaskHeader(card string, envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string) string {
	if header := TaskCardHeader(envelope, workflow, status, icons); header != "" {
		return header + "\n" + card
	}
	return card
}

func WithTaskHeaderWithButtons(card string, envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string, platform string, buttons []channel.Button) string {
	if header := TaskCardHeaderWithButtons(envelope, workflow, status, icons, platform, buttons); header != "" {
		return header + "\n" + card
	}
	return card
}

type TaskOptions struct {
	Registry *TaskThreadRegistry
	Icons    map[string]string
	Colors   map[string]int
}

func (s *Server) recordTaskThread(ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext, status TaskStatus) {
	if s == nil || s.tasks == nil || workCtx == nil {
		return
	}
	repo, branch := TaskRepoBranch(workCtx.Key, envelope)
	if repo == "" || branch == "" {
		return
	}
	s.tasks.Upsert(repo, branch, workCtx.RootMessageID, workCtx.ThreadID, status)
}

func MissingTaskWorkflows(endpoints []config.EndpointConfig, repo string) []string {
	want := normalizeRepoKey(repo)
	present := make(map[string]bool, len(RequiredTaskWorkflows))
	for _, ep := range endpoints {
		if ep.Repository == "" {
			continue
		}
		if normalizeRepoKey(ep.Repository) != want || want == "" {
			continue
		}
		present[strings.TrimSpace(strings.ToLower(ep.Workflow))] = true
	}
	var missing []string
	for _, workflow := range RequiredTaskWorkflows {
		if !present[workflow] {
			missing = append(missing, workflow)
		}
	}
	return missing
}

type BranchPRResult struct {
	Found  bool
	Number string
	URL    string
	State  string
}

type BranchPRChecker func(ctx context.Context, repo, branch string) (BranchPRResult, error)

func ghBranchPRChecker(ctx context.Context, repo, branch string) (BranchPRResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", "pr", "list", "--repo", repo, "--head", branch, "--json", "number,url,state", "--limit", "1")
	out, err := cmd.Output()
	if err != nil {
		return BranchPRResult{}, fmt.Errorf("webhook: fallback poll gh pr list: %w", err)
	}
	return parseBranchPRList(out)
}

func parseBranchPRList(out []byte) (BranchPRResult, error) {
	var rows []struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
		State  string `json:"state"`
	}
	if err := json.Unmarshal(out, &rows); err != nil {
		return BranchPRResult{}, fmt.Errorf("webhook: fallback poll decode: %w", err)
	}
	if len(rows) == 0 {
		return BranchPRResult{}, nil
	}
	return BranchPRResult{
		Found:  true,
		Number: fmt.Sprintf("%d", rows[0].Number),
		URL:    strings.TrimSpace(rows[0].URL),
		State:  strings.TrimSpace(rows[0].State),
	}, nil
}

func FormatFallbackFinding(repo, branch string, result BranchPRResult) string {
	head := strings.TrimSpace(repo) + "#" + strings.TrimSpace(branch)
	if !result.Found {
		return "📨 Fallback poll: no open PR found for " + head + "."
	}
	line := "📨 Fallback poll: PR #" + strings.TrimSpace(result.Number) + " (" + strings.TrimSpace(result.State) + ") for " + head + "."
	if strings.TrimSpace(result.URL) != "" {
		line += " " + FormatTaskLinks(result.URL)
	}
	return line
}

func FallbackDue(lastEventAt time.Time, now time.Time, after time.Duration) bool {
	if after <= 0 {
		return false
	}
	if lastEventAt.IsZero() {
		return false
	}
	return !now.Before(lastEventAt.Add(after))
}

func clipTaskField(s string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max])
}

const taskCallbackPrefix = "task:"

const taskCallbackLimit = 100

func TaskThreadURL(platform, guildID, threadID string) string {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ""
	}
	if strings.Contains(threadID, "://") {
		return threadID
	}
	if !strings.EqualFold(strings.TrimSpace(platform), "discord") {
		return ""
	}
	if guildID = strings.TrimSpace(guildID); guildID == "" {
		return "https://discord.com/channels/_/" + threadID
	}
	return "https://discord.com/channels/" + guildID + "/" + threadID
}

func TaskLinkButtons(platform, guildID, prURL, threadID string) []channel.Button {
	var buttons []channel.Button
	if normalized := normalizeTaskPRURL(prURL); normalized != "" {
		buttons = append(buttons, channel.Button{Label: "Open PR", URL: normalized, Row: 1})
	}
	if url := TaskThreadURL(platform, guildID, threadID); url != "" {
		buttons = append(buttons, channel.Button{Label: "Open thread", URL: url, Row: 1})
	}
	return buttons
}

func TaskActionButtons(repo, branch string) []channel.Button {
	repo = strings.TrimSpace(repo)
	branch = strings.TrimSpace(branch)
	if repo == "" || branch == "" {
		return nil
	}
	key := TaskThreadKey(repo, branch)
	mergeID := "task:merge:" + key
	redispatchID := "task:redispatch:" + key
	if utf8.RuneCountInString(mergeID) > taskCallbackLimit || utf8.RuneCountInString(redispatchID) > taskCallbackLimit {
		return nil
	}
	return []channel.Button{
		{Label: "Merge", Value: mergeID, Row: 1},
		{Label: "Re-dispatch", Value: redispatchID, Row: 1},
	}
}

func TaskParentButtons(platform, guildID string, envelope WebhookEnvelope, repo, branch, threadID string) []channel.Button {
	if repo == "" || branch == "" {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
		branch = strings.TrimSpace(stringValue(envelope["head_branch"]))
	}
	if repo == "" || branch == "" {
		return nil
	}
	prURL := strings.TrimSpace(stringValue(envelope["pr_url"]))
	buttons := TaskLinkButtons(platform, guildID, prURL, threadID)
	buttons = append(buttons, TaskActionButtons(repo, branch)...)
	return buttons
}

func ParseTaskCallback(data string) (string, string, string, bool) {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(trimmed, taskCallbackPrefix) {
		return "", "", "", false
	}
	rest := strings.TrimPrefix(trimmed, taskCallbackPrefix)
	action := ""
	if strings.HasPrefix(rest, "merge:") {
		action = "merge"
		rest = strings.TrimPrefix(rest, "merge:")
	} else if strings.HasPrefix(rest, "redispatch:") {
		action = "redispatch"
		rest = strings.TrimPrefix(rest, "redispatch:")
	} else {
		return "", "", "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", "", "", false
	}
	repo, branch, ok := strings.Cut(rest, "#")
	repo = strings.TrimSpace(repo)
	branch = strings.TrimSpace(branch)
	if !ok || repo == "" || branch == "" {
		return "", "", "", false
	}
	return action, repo, branch, true
}
