package webhook

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anggasct/occa/internal/config"
)

const defaultDispatchPath = "/dispatch"

type DispatchRequest struct {
	Repo     string `json:"repo"`
	BaseSHA  string `json:"base_sha"`
	Branch   string `json:"branch"`
	SpecPath string `json:"spec_path"`
	Scope    string `json:"scope"`
	Verify   string `json:"verify"`
}

func (req DispatchRequest) Validate() error {
	repo := strings.TrimSpace(req.Repo)
	if repo == "" {
		return fmt.Errorf("repo is required")
	}
	if _, err := config.CanonicalRepository(repo); err != nil {
		return fmt.Errorf("repo: %w", err)
	}
	branch := strings.TrimSpace(req.Branch)
	if branch == "" {
		return fmt.Errorf("branch is required")
	}
	if len([]rune(branch)) > 255 || strings.HasPrefix(branch, "-") || strings.HasPrefix(branch, "/") {
		return fmt.Errorf("branch is invalid")
	}
	for _, r := range branch {
		if r <= 0x1F || r == 0x7F || r == ' ' || r == '~' || r == '^' || r == ':' || r == '?' || r == '*' || r == '[' {
			return fmt.Errorf("branch is invalid")
		}
	}
	if sha := strings.TrimSpace(req.BaseSHA); sha != "" && normalizeRevision(sha) == "" {
		return fmt.Errorf("base_sha must be a 7-40 char hex revision")
	}
	spec := strings.TrimSpace(req.SpecPath)
	if spec == "" {
		return fmt.Errorf("spec_path is required")
	}
	if len([]rune(spec)) > 512 {
		return fmt.Errorf("spec_path is too long")
	}
	for _, part := range strings.Split(spec, "/") {
		if part == ".." {
			return fmt.Errorf("spec_path must not escape the repository")
		}
	}
	if len([]rune(req.Scope)) > 2000 {
		return fmt.Errorf("scope is too long")
	}
	if len([]rune(req.Verify)) > 2000 {
		return fmt.Errorf("verify is too long")
	}
	return nil
}

type DispatchTask struct {
	ID        string
	Repo      string
	Branch    string
	BaseSHA   string
	SpecPath  string
	Scope     string
	Verify    string
	CreatedAt time.Time
}

type DispatchStore struct {
	mu      sync.Mutex
	tasks   map[string]DispatchTask
	counter int64
}

func NewDispatchStore() *DispatchStore {
	return &DispatchStore{tasks: make(map[string]DispatchTask)}
}

func (d *DispatchStore) Create(req DispatchRequest) DispatchTask {
	n := atomic.AddInt64(&d.counter, 1)
	task := DispatchTask{
		ID:        fmt.Sprintf("task-%d-%d", time.Now().UnixNano(), n),
		Repo:      normalizeRepoKey(req.Repo),
		Branch:    strings.TrimSpace(req.Branch),
		BaseSHA:   strings.ToLower(strings.TrimSpace(req.BaseSHA)),
		SpecPath:  strings.TrimSpace(req.SpecPath),
		Scope:     clipTaskField(strings.TrimSpace(req.Scope), 2000),
		Verify:    clipTaskField(strings.TrimSpace(req.Verify), 2000),
		CreatedAt: time.Now(),
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tasks == nil {
		d.tasks = make(map[string]DispatchTask)
	}
	d.tasks[task.ID] = task
	return task
}

func (d *DispatchStore) Get(id string) (DispatchTask, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	task, ok := d.tasks[id]
	return task, ok
}

func (d *DispatchStore) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.tasks)
}

func FormatDispatchSummary(task DispatchTask, icons map[string]string) string {
	head := normalizeRepoKey(task.Repo) + "#" + strings.TrimSpace(task.Branch)
	first := TaskIcon(TaskDispatched, icons) + " Dispatched " + head + " · " + task.ID
	second := "Spec: " + task.SpecPath
	if strings.TrimSpace(task.Verify) != "" {
		second += " · Verify: " + strings.TrimSpace(task.Verify)
	}
	return first + "\n" + second
}

func dispatchToken(r *http.Request) string {
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		if token, ok := strings.CutPrefix(auth, "Bearer "); ok {
			return strings.TrimSpace(token)
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Dispatch-Token"))
}

func writeDispatchJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) handleDispatch(w http.ResponseWriter, r *http.Request) {
	if !s.dispatchEnabled || strings.TrimSpace(s.dispatchToken) == "" {
		http.NotFound(w, r)
		return
	}
	requestPath := r.URL.Path
	if strings.HasPrefix(requestPath, "/occa/") {
		requestPath = strings.TrimPrefix(requestPath, "/occa")
	}
	if requestPath != s.dispatchPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength > s.maxBodySize {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}
	if subtle.ConstantTimeCompare([]byte(dispatchToken(r)), []byte(s.dispatchToken)) != 1 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodySize))
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	var req DispatchRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeDispatchJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid dispatch body: " + err.Error()})
		return
	}
	if err := req.Validate(); err != nil {
		writeDispatchJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var endpoints []config.EndpointConfig
	for _, ep := range s.endpoints {
		endpoints = append(endpoints, ep)
	}
	if missing := MissingTaskWorkflows(endpoints, req.Repo); len(missing) > 0 {
		writeDispatchJSON(w, http.StatusConflict, map[string]any{
			"error":   "repo webhooks missing for " + strings.TrimSpace(req.Repo),
			"missing": missing,
		})
		return
	}
	task := s.dispatchTasks.Create(req)
	suppressAfter := task.CreatedAt
	if s.tasks != nil {
		suppressAfter = s.tasks.Upsert(task.Repo, task.Branch, "", "", TaskDispatched).UpdatedAt
	}
	platform, channelID, rootID := s.postDispatchSummary(task)
	if rec, ok := s.openDispatchThread(task, platform, channelID, rootID); ok {
		suppressAfter = rec.UpdatedAt
	}
	s.scheduleDispatchFallback(task, suppressAfter)
	writeDispatchJSON(w, http.StatusCreated, map[string]string{"task_id": task.ID})
}

func (s *Server) dispatchNotifyTarget(repo string) (string, string) {
	want := normalizeRepoKey(repo)
	fallbackPlatform, fallbackChannel := "", ""
	for _, ep := range s.endpoints {
		if ep.Repository == "" {
			continue
		}
		if normalizeRepoKey(ep.Repository) != want || want == "" {
			continue
		}
		if ep.Platform == "" || ep.ChannelID == "" {
			continue
		}
		if strings.TrimSpace(strings.ToLower(ep.Workflow)) == "review" {
			return ep.Platform, ep.ChannelID
		}
		if fallbackPlatform == "" {
			fallbackPlatform, fallbackChannel = ep.Platform, ep.ChannelID
		}
	}
	return fallbackPlatform, fallbackChannel
}

func (s *Server) postDispatchSummary(task DispatchTask) (string, string, string) {
	platform, channelID := s.dispatchNotifyTarget(task.Repo)
	if platform == "" || channelID == "" {
		return "", "", ""
	}
	summary := FormatDispatchSummary(task, s.statusIcons)
	buttons := TaskActionButtons(task.Repo, task.Branch)
	rootID, err := s.sendParent(context.Background(), platform, channelID, summary, buttons)
	if err != nil {
		slog.Warn("webhook: dispatch notify failed", "repo", task.Repo, "branch", task.Branch, "error", err)
		return platform, channelID, ""
	}
	if rootID == "" && s.sender == nil && s.senderButtons == nil {
		return platform, channelID, ""
	}
	return platform, channelID, rootID
}

func dispatchThreadName(task DispatchTask) string {
	return FormatThreadName(WebhookEnvelope{"repository": task.Repo, "head_branch": task.Branch}, "dispatch")
}

func (s *Server) openDispatchThread(task DispatchTask, platform, channelID, rootID string) (TaskThreadRecord, bool) {
	if s.tasks == nil || strings.TrimSpace(rootID) == "" || strings.TrimSpace(platform) == "" || strings.TrimSpace(channelID) == "" {
		return TaskThreadRecord{}, false
	}
	if s.threadStarter == nil {
		return s.tasks.Upsert(task.Repo, task.Branch, rootID, "", TaskDispatched), true
	}
	threadID, err := s.threadStarter(context.Background(), platform, channelID, rootID, dispatchThreadName(task))
	if err != nil {
		slog.Warn("webhook: dispatch thread failed", "repo", task.Repo, "branch", task.Branch, "error", err)
		return s.tasks.Upsert(task.Repo, task.Branch, rootID, "", TaskDispatched), true
	}
	if strings.TrimSpace(threadID) == "" {
		return s.tasks.Upsert(task.Repo, task.Branch, rootID, "", TaskDispatched), true
	}
	rec := s.tasks.Upsert(task.Repo, task.Branch, rootID, threadID, TaskDispatched)
	summary := FormatDispatchSummary(task, s.statusIcons)
	buttons := TaskLinkButtons(platform, "", threadID)
	buttons = append(buttons, TaskActionButtons(task.Repo, task.Branch)...)
	if err := s.editParent(context.Background(), platform, channelID, rootID, summary, buttons); err != nil {
		slog.Warn("webhook: dispatch parent buttons failed", "repo", task.Repo, "branch", task.Branch, "error", err)
	}
	return rec, true
}

func (s *Server) scheduleDispatchFallback(task DispatchTask, suppressAfter time.Time) {
	if s.fallbackAfter <= 0 || s.branchChecker == nil {
		return
	}
	after := s.fallbackAfter
	stamp := suppressAfter
	if stamp.IsZero() {
		stamp = task.CreatedAt
	}
	go func() {
		select {
		case <-time.After(after):
		case <-s.shutdownCtx.Done():
			return
		}
		select {
		case <-s.shutdownCtx.Done():
			return
		default:
		}
		if rec, ok := s.tasks.Lookup(task.Repo, task.Branch); ok {
			if rec.UpdatedAt.After(stamp) {
				return
			}
		} else if s.tasks != nil {
			tried, sameKey, sameThread, hasSame, triedExists := s.tasks.MissDetail(task.Repo, task.Branch)
			slog.Warn("webhook: dispatch fallback no record", "repo", task.Repo, "branch", task.Branch, "tried_key", tried, "same_branch_key", sameKey, "same_branch_thread", sameThread, "has_same_branch", hasSame, "tried_exists", triedExists, "task_id", task.ID)
		}
		var threadID string
		if rec, ok := s.tasks.Lookup(task.Repo, task.Branch); ok {
			threadID = rec.ThreadID
		} else if s.tasks != nil {
			tried, sameKey, sameThread, hasSame, triedExists := s.tasks.MissDetail(task.Repo, task.Branch)
			slog.Warn("webhook: dispatch fallback thread lookup miss", "repo", task.Repo, "branch", task.Branch, "tried_key", tried, "same_branch_key", sameKey, "same_branch_thread", sameThread, "has_same_branch", hasSame, "tried_exists", triedExists, "task_id", task.ID)
		}
		s.pollBranchOnce(task.Repo, task.Branch, "", "", threadID)
	}()
}

func (s *Server) scheduleDeliveryFallback(ep config.EndpointConfig, workCtx *WebhookWorkContext) {
	if s.fallbackAfter <= 0 || s.branchChecker == nil || workCtx == nil {
		return
	}
	repo, branch := TaskRepoBranch(workCtx.Key, workCtx.Envelope)
	if repo == "" || branch == "" {
		return
	}
	if rec, ok := s.tasks.Lookup(repo, branch); ok && isTaskTerminal(rec.Status) {
		return
	}
	after := s.fallbackAfter
	since := time.Now()
	threadID := workCtx.ThreadID
	go func() {
		select {
		case <-time.After(after):
		case <-s.shutdownCtx.Done():
			return
		}
		select {
		case <-s.shutdownCtx.Done():
			return
		default:
		}
		if rec, ok := s.tasks.Lookup(repo, branch); ok {
			if rec.UpdatedAt.After(since) {
				return
			}
			if isTaskTerminal(rec.Status) {
				return
			}
			if strings.TrimSpace(rec.ThreadID) != "" {
				threadID = rec.ThreadID
			}
		} else if s.tasks != nil {
			tried, sameKey, sameThread, hasSame, triedExists := s.tasks.MissDetail(repo, branch)
			slog.Warn("webhook: delivery fallback thread lookup miss", "repo", repo, "branch", branch, "tried_key", tried, "same_branch_key", sameKey, "same_branch_thread", sameThread, "has_same_branch", hasSame, "tried_exists", triedExists, "endpoint", ep.Name, "delivery_id", workCtx.DeliveryID)
		}
		s.pollBranchOnce(repo, branch, ep.Platform, ep.ChannelID, threadID)
	}()
}

func (s *Server) pollBranchOnce(repo, branch, platform, channelID, threadID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := s.branchChecker(ctx, repo, branch)
	if err != nil {
		slog.Warn("webhook: fallback poll failed", "repo", repo, "branch", branch, "error", err)
		return
	}
	finding := FormatFallbackFinding(repo, branch, result)
	if threadID == "" && s.tasks != nil {
		if rec, ok := s.tasks.Lookup(repo, branch); ok && strings.TrimSpace(rec.ThreadID) != "" {
			threadID = rec.ThreadID
		} else {
			tried, sameKey, sameThread, hasSame, triedExists := s.tasks.MissDetail(repo, branch)
			slog.Warn("webhook: fallback poll thread lookup miss", "repo", repo, "branch", branch, "tried_key", tried, "same_branch_key", sameKey, "same_branch_thread", sameThread, "has_same_branch", hasSame, "tried_exists", triedExists, "platform", platform, "channel_id", channelID)
		}
	}
	if platform == "" || channelID == "" {
		for _, ep := range s.endpoints {
			if ep.Repository == "" {
				continue
			}
			if normalizeRepoKey(ep.Repository) != normalizeRepoKey(repo) || normalizeRepoKey(repo) == "" {
				continue
			}
			platform, channelID = ep.Platform, ep.ChannelID
			break
		}
	}
	if platform == "" || channelID == "" || s.notifier == nil {
		slog.Info("webhook: fallback poll finding", "repo", repo, "branch", branch, "finding", finding)
		return
	}
	if !result.Found && threadID == "" {
		slog.Info("webhook: fallback poll no PR, thread-only suppressed top-level", "repo", repo, "branch", branch)
		return
	}
	target := channelID
	threadTarget := ""
	if platform == "discord" && threadID != "" {
		target = threadID
		threadTarget = threadID
	} else if platform == "telegram" && threadID != "" && !strings.Contains(channelID, ":") {
		target = channelID + ":" + threadID
		threadTarget = target
	}
	if err := s.notifier(context.Background(), platform, target, finding); err != nil {
		slog.Warn("webhook: fallback poll notify failed", "repo", repo, "branch", branch, "error", err)
		if threadTarget != "" && target == threadTarget && result.Found {
			if fallbackErr := s.notifier(context.Background(), platform, channelID, finding); fallbackErr != nil {
				slog.Warn("webhook: fallback poll parent fallback failed", "repo", repo, "branch", branch, "error", fallbackErr)
			} else {
				slog.Info("webhook: fallback poll fell back to parent", "repo", repo, "branch", branch)
			}
		}
	}
}
