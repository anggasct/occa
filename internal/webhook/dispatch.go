package webhook

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anggasct/occa/internal/config"
)

const defaultDispatchPath = "/dispatch"

const dispatchThreadAttempts = 3

const dispatchThreadRetryBaseDelay = 500 * time.Millisecond

const dispatchThreadTimeout = dispatchThreadRetryBaseDelay * 30

var errDispatchThreadEmpty = errors.New("dispatch thread starter returned empty thread id")

type DispatchRequest struct {
	Repo     string `json:"repo"`
	BaseSHA  string `json:"base_sha"`
	Branch   string `json:"branch"`
	SpecPath string `json:"spec_path"`
	Scope    string `json:"scope"`
	Verify   string `json:"verify"`
}

func isSpecURL(s string) bool {
	t := strings.TrimSpace(s)
	if t == "" {
		return false
	}
	if !strings.HasPrefix(strings.ToLower(t), "https://") {
		return false
	}
	u, err := url.Parse(t)
	if err != nil {
		return false
	}
	if strings.ToLower(u.Scheme) != "https" {
		return false
	}
	return strings.TrimSpace(u.Host) != ""
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
	if strings.Contains(spec, "://") {
		if !isSpecURL(spec) {
			return fmt.Errorf("spec looks like a URL but is not a valid https:// URL with host")
		}
	} else {
		for _, part := range strings.Split(spec, "/") {
			if part == ".." {
				return fmt.Errorf("spec_path must not escape the repository")
			}
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
	byKey   map[string]string
	counter int64
}

func NewDispatchStore() *DispatchStore {
	return &DispatchStore{tasks: make(map[string]DispatchTask), byKey: make(map[string]string)}
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
	if d.byKey == nil {
		d.byKey = make(map[string]string)
	}
	d.tasks[task.ID] = task
	d.byKey[TaskThreadKey(task.Repo, task.Branch)] = task.ID
	return task
}

func (d *DispatchStore) Get(id string) (DispatchTask, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	task, ok := d.tasks[id]
	return task, ok
}

func (d *DispatchStore) LatestFor(repo, branch string) (DispatchTask, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.byKey[TaskThreadKey(repo, branch)]
	if !ok {
		return DispatchTask{}, false
	}
	task, ok := d.tasks[id]
	return task, ok
}

func (d *DispatchStore) Delete(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	task, ok := d.tasks[id]
	if !ok {
		return
	}
	delete(d.tasks, id)
	if key := TaskThreadKey(task.Repo, task.Branch); d.byKey[key] == id {
		delete(d.byKey, key)
	}
}

func (d *DispatchStore) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.tasks)
}

func injectTaskContext(envelope WebhookEnvelope, key WebhookExecutionKey, store *DispatchStore) {
	if envelope == nil {
		return
	}
	empty := func() {
		envelope["task_id"] = ""
		envelope["task_spec"] = ""
		envelope["task_spec_is_url"] = false
		envelope["task_scope"] = ""
		envelope["task_verify"] = ""
	}
	repo, branch := TaskRepoBranch(key, envelope)
	if repo == "" || branch == "" || store == nil {
		empty()
		return
	}
	task, ok := store.LatestFor(repo, branch)
	if !ok {
		empty()
		return
	}
	envelope["task_id"] = task.ID
	envelope["task_spec"] = task.SpecPath
	envelope["task_spec_is_url"] = isSpecURL(task.SpecPath)
	envelope["task_scope"] = task.Scope
	envelope["task_verify"] = task.Verify
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

func FormatDispatchFailed(task DispatchTask, icons map[string]string, attempts int) string {
	head := normalizeRepoKey(task.Repo) + "#" + normalizeBranchKey(task.Branch)
	first := TaskIcon(TaskFailed, icons) + " Dispatch failed " + head + " · " + task.ID
	second := fmt.Sprintf("Could not open a thread after %d attempts. Re-dispatch once the platform recovers.", attempts)
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
	if isRedispatchScope(req.Scope) && s.handleRedispatch(w, r, req) {
		return
	}
	s.dispatchCreateMu.Lock()
	if rec, ok := s.tasks.ResolveTaskThread(normalizeRepoKey(req.Repo), normalizeBranchKey(req.Branch), "dispatch"); ok && !isTaskTerminal(rec.Status) {
		if existing, ok := s.dispatchTasks.LatestFor(req.Repo, req.Branch); ok {
			s.dispatchCreateMu.Unlock()
			writeDispatchJSON(w, http.StatusOK, map[string]string{"task_id": existing.ID})
			return
		}
		slog.Warn("webhook: dispatch live record without task, allowing fresh", "repo", normalizeRepoKey(req.Repo), "branch", normalizeBranchKey(req.Branch))
	}
	task := s.dispatchTasks.Create(req)
	if s.tasks != nil {
		s.tasks.UpsertWithSpec(task.Repo, task.Branch, "", "", TaskDispatched, task.SpecPath)
	}
	platform, channelID, rootID, err := s.postDispatchSummary(task)
	if err != nil {
		s.dispatchTasks.Delete(task.ID)
		if s.tasks != nil {
			s.tasks.UpsertWithSpec(task.Repo, task.Branch, "", "", TaskFailed, task.SpecPath)
		}
		s.dispatchCreateMu.Unlock()
		slog.Error("webhook: dispatch parent post failed", "repo", task.Repo, "branch", task.Branch, "task_id", task.ID, "error", err)
		writeDispatchJSON(w, http.StatusBadGateway, map[string]string{"error": "dispatch parent post failed: " + err.Error()})
		return
	}
	suppressAfter := task.CreatedAt
	if s.tasks != nil {
		suppressAfter = s.tasks.UpsertWithSpec(task.Repo, task.Branch, rootID, "", TaskDispatched, task.SpecPath).UpdatedAt
	}
	s.dispatchCreateMu.Unlock()
	if rec, ok := s.openDispatchThread(task, platform, channelID, rootID); ok {
		suppressAfter = rec.UpdatedAt
	}
	s.scheduleDispatchFallback(task, suppressAfter)
	writeDispatchJSON(w, http.StatusCreated, map[string]string{"task_id": task.ID})
}

func isRedispatchScope(scope string) bool {
	return strings.HasPrefix(strings.TrimSpace(scope), "redispatch ")
}

func FormatRedispatchSeparator(task DispatchTask, icons map[string]string) string {
	return "---\n🔁 Re-dispatch requested — retrying in this thread.\n" + FormatDispatchSummary(task, icons)
}

func (s *Server) handleRedispatch(w http.ResponseWriter, r *http.Request, req DispatchRequest) bool {
	if s.tasks == nil {
		return false
	}
	repo := normalizeRepoKey(req.Repo)
	branch := strings.TrimSpace(req.Branch)
	rec, ok := s.tasks.ResolveTaskThread(repo, branch, "redispatch")
	if !ok {
		return false
	}
	if strings.TrimSpace(rec.RootMessageID) == "" {
		orig := strings.TrimSpace(rec.SpecPath)
		if orig != "" {
			req.SpecPath = orig
		}
		task := s.dispatchTasks.Create(req)
		s.tasks.UpsertWithSpec(task.Repo, task.Branch, "", "", TaskDispatched, task.SpecPath)
		platform, channelID, rootID, err := s.postDispatchSummary(task)
		if err != nil {
			s.dispatchTasks.Delete(task.ID)
			s.tasks.UpsertWithSpec(task.Repo, task.Branch, "", "", TaskFailed, task.SpecPath)
			slog.Error("webhook: dispatch parent post failed", "repo", task.Repo, "branch", task.Branch, "task_id", task.ID, "error", err)
			writeDispatchJSON(w, http.StatusBadGateway, map[string]string{"error": "dispatch parent post failed: " + err.Error()})
			return true
		}
		suppressAfter := s.tasks.UpsertWithSpec(task.Repo, task.Branch, rootID, "", TaskDispatched, task.SpecPath).UpdatedAt
		if r2, ok := s.openDispatchThread(task, platform, channelID, rootID); ok {
			suppressAfter = r2.UpdatedAt
		}
		s.scheduleDispatchFallback(task, suppressAfter)
		writeDispatchJSON(w, http.StatusCreated, map[string]string{"task_id": task.ID})
		return true
	}
	if rec.Status == TaskMerged {
		writeDispatchJSON(w, http.StatusConflict, map[string]string{"error": "task already merged for " + repo + "#" + branch + " — re-dispatch refused"})
		return true
	}
	ctx := r.Context()
	if s.branchChecker != nil {
		result, err := s.branchChecker(ctx, repo, branch)
		if err == nil {
			state := strings.ToUpper(strings.TrimSpace(result.State))
			if result.Found && (state == "MERGED" || state == "CLOSED") {
				writeDispatchJSON(w, http.StatusConflict, map[string]string{"error": "PR #" + strings.TrimSpace(result.Number) + " (" + strings.TrimSpace(result.State) + ") for " + repo + "#" + branch + " is closed — re-dispatch refused"})
				return true
			}
			if !result.Found && rec.Status == TaskPROpened {
				writeDispatchJSON(w, http.StatusConflict, map[string]string{"error": "no open PR found for " + repo + "#" + branch + " — re-dispatch refused"})
				return true
			}
		} else {
			slog.Warn("webhook: redispatch PR check failed, continuing with branch check", "repo", repo, "branch", branch, "error", err)
		}
	}
	if s.branchExists != nil {
		exists, err := s.branchExists(ctx, repo, branch)
		if err != nil {
			slog.Warn("webhook: redispatch branch check failed, allowing retry", "repo", repo, "branch", branch, "error", err)
		} else if !exists {
			writeDispatchJSON(w, http.StatusConflict, map[string]string{"error": "branch gone for " + repo + "#" + branch + " — re-dispatch refused"})
			return true
		}
	}
	origSpec := strings.TrimSpace(rec.SpecPath)
	if origSpec != "" {
		req.SpecPath = origSpec
	}
	task := s.dispatchTasks.Create(req)
	platform, channelID := s.dispatchNotifyTarget(task.Repo)
	if platform == "" || channelID == "" {
		for _, ep := range s.endpoints {
			if ep.Repository == "" {
				continue
			}
			if normalizeRepoKey(ep.Repository) != normalizeRepoKey(task.Repo) {
				continue
			}
			platform, channelID = ep.Platform, ep.ChannelID
			break
		}
	}
	summary := FormatDispatchSummary(task, s.statusIcons)
	if prKey, isPRSurface := s.tasks.PRKeyForTask(TaskThreadKey(task.Repo, task.Branch)); isPRSurface {
		// The channel parent is the index surface of a PR: a re-dispatch
		// re-renders its phase line instead of writing the dispatch card
		// over it. The card belongs in the thread.
		s.reopenPRSurface(prKey)
		repo, number, _ := splitPRKey(prKey)
		envelope := WebhookEnvelope{"repository": repo, "pr_number": number}
		if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
			envelope["pr_url"] = detail.PRURL
			envelope["head_branch"] = detail.Branch
		}
		ep := config.EndpointConfig{
			Name:       task.Repo,
			Platform:   platform,
			ChannelID:  channelID,
			Workflow:   "dispatch",
			Repository: task.Repo,
		}
		s.syncPhaseParent(ctx, ep, prKey, envelope, PhaseQueued)
	} else if strings.TrimSpace(rec.RootMessageID) != "" && platform != "" && channelID != "" {
		if err := s.editParent(ctx, platform, channelID, rec.RootMessageID, summary, nil); err != nil {
			slog.Warn("webhook: redispatch parent edit failed", "repo", task.Repo, "branch", task.Branch, "error", err)
		}
	}
	threadID := strings.TrimSpace(rec.ThreadID)
	if threadID == "" && s.threadStarter != nil && platform != "" && channelID != "" && strings.TrimSpace(rec.RootMessageID) != "" {
		newThread, err := s.threadStarter(ctx, platform, channelID, rec.RootMessageID, dispatchThreadName(task))
		if err != nil {
			slog.Warn("webhook: redispatch thread start failed", "repo", task.Repo, "branch", task.Branch, "error", err)
		} else if strings.TrimSpace(newThread) != "" {
			threadID = strings.TrimSpace(newThread)
		}
	}
	if threadID != "" && platform != "" && channelID != "" && s.notifier != nil {
		target := channelID
		if platform == "discord" {
			target = threadID
		} else if platform == "telegram" && !strings.Contains(channelID, ":") {
			target = channelID + ":" + threadID
		}
		if err := s.notifier(ctx, platform, target, FormatRedispatchSeparator(task, s.statusIcons)); err != nil {
			slog.Warn("webhook: redispatch thread post failed", "repo", task.Repo, "branch", task.Branch, "error", err)
		}
	}
	suppressAfter := s.tasks.UpsertWithSpec(task.Repo, task.Branch, rec.RootMessageID, threadID, TaskDispatched, task.SpecPath).UpdatedAt
	s.scheduleDispatchFallback(task, suppressAfter)
	writeDispatchJSON(w, http.StatusCreated, map[string]string{"task_id": task.ID})
	return true
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

func (s *Server) postDispatchSummary(task DispatchTask) (string, string, string, error) {
	platform, channelID := s.dispatchNotifyTarget(task.Repo)
	if platform == "" || channelID == "" {
		return "", "", "", nil
	}
	summary := FormatDispatchSummary(task, s.statusIcons)
	buttons := TaskActionButtons(task.Repo, task.Branch)
	rootID, err := s.sendParent(context.Background(), platform, channelID, summary, buttons)
	if err != nil {
		return platform, channelID, "", err
	}
	if rootID == "" && s.sender == nil && s.senderButtons == nil {
		return platform, channelID, "", nil
	}
	return platform, channelID, rootID, nil
}

func dispatchThreadName(task DispatchTask) string {
	return FormatThreadName(WebhookEnvelope{"repository": task.Repo, "head_branch": task.Branch}, "dispatch")
}

func (s *Server) openDispatchThread(task DispatchTask, platform, channelID, rootID string) (TaskThreadRecord, bool) {
	if s.tasks == nil || strings.TrimSpace(rootID) == "" || strings.TrimSpace(platform) == "" || strings.TrimSpace(channelID) == "" {
		return TaskThreadRecord{}, false
	}
	if s.threadStarter == nil {
		return s.tasks.UpsertWithSpec(task.Repo, task.Branch, rootID, "", TaskDispatched, task.SpecPath), true
	}
	threadID, err := s.startDispatchThread(task, platform, channelID, rootID)
	if err != nil {
		rec := s.tasks.UpsertWithSpec(task.Repo, task.Branch, rootID, "", TaskFailed, task.SpecPath)
		failed := FormatDispatchFailed(task, s.statusIcons, dispatchThreadAttempts)
		buttons := TaskActionButtons(task.Repo, task.Branch)
		if editErr := s.editParent(context.Background(), platform, channelID, rootID, failed, buttons); editErr != nil {
			slog.Warn("webhook: dispatch failed-mark edit failed", "repo", task.Repo, "branch", task.Branch, "task_id", task.ID, "error", editErr)
		}
		slog.Error("webhook: dispatch thread failed, record marked failed", "repo", task.Repo, "branch", task.Branch, "task_id", task.ID, "error", err)
		return rec, true
	}
	rec := s.tasks.UpsertWithSpec(task.Repo, task.Branch, rootID, threadID, TaskDispatched, task.SpecPath)
	summary := FormatDispatchSummary(task, s.statusIcons)
	buttons := TaskLinkButtons(platform, s.guildIDFor(platform, channelID, threadID), "", threadID)
	buttons = append(buttons, TaskActionButtons(task.Repo, task.Branch)...)
	if err := s.editParent(context.Background(), platform, channelID, rootID, summary, buttons); err != nil {
		slog.Warn("webhook: dispatch parent buttons failed", "repo", task.Repo, "branch", task.Branch, "error", err)
	}
	ctx := context.Background()
	opener := FormatThreadOpener(WebhookEnvelope{"repository": task.Repo, "head_branch": task.Branch}, "dispatch", TaskDispatched, s.statusIcons)
	if strings.TrimSpace(opener) != "" {
		_ = s.notifyThread(ctx, platform, threadTargetFor(platform, channelID, threadID), opener)
	}
	s.postPhaseBannerIfChanged(ctx, platform, channelID, threadID, task.Repo, task.Branch, "dispatch", "")
	s.ensureThreadStatus(ctx, platform, channelID, threadID, task.Repo, task.Branch, "", ThreadStatusUpdate{
		Phase: PhaseDispatch,
		Next:  NextStepForPhase(PhaseDispatch),
	})
	if fresh, ok := s.tasks.Lookup(task.Repo, task.Branch); ok {
		return fresh, true
	}
	return rec, true
}

func (s *Server) startDispatchThread(task DispatchTask, platform, channelID, rootID string) (string, error) {
	var err error
	threadID := ""
	for attempt := 1; attempt <= dispatchThreadAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchThreadTimeout)
		threadID, err = s.threadStarter(ctx, platform, channelID, rootID, dispatchThreadName(task))
		cancel()
		if err == nil && strings.TrimSpace(threadID) != "" {
			return threadID, nil
		}
		if err == nil {
			err = errDispatchThreadEmpty
		}
		if attempt < dispatchThreadAttempts {
			slog.Warn("webhook: dispatch thread retry", "repo", task.Repo, "branch", task.Branch, "task_id", task.ID, "attempt", attempt, "error", err)
			time.Sleep(dispatchThreadRetryBaseDelay * time.Duration(attempt))
		}
	}
	return "", err
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
		if rec, ok := s.tasks.ResolveTaskThread(task.Repo, task.Branch, "dispatch-fallback"); ok {
			if rec.UpdatedAt.After(stamp) {
				return
			}
		}
		var threadID string
		if rec, ok := s.tasks.ResolveTaskThread(task.Repo, task.Branch, "dispatch-fallback-thread"); ok {
			threadID = rec.ThreadID
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
	if rec, ok := s.tasks.ResolveTaskThread(repo, branch, "delivery-fallback"); ok && isTaskTerminal(rec.Status) {
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
		if rec, ok := s.tasks.ResolveTaskThread(repo, branch, "delivery-fallback"); ok {
			if rec.UpdatedAt.After(since) {
				return
			}
			if isTaskTerminal(rec.Status) {
				return
			}
			if strings.TrimSpace(rec.ThreadID) != "" {
				threadID = rec.ThreadID
			}
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
	if threadID == "" {
		if rec, ok := s.tasks.ResolveTaskThread(repo, branch, "fallback-poll"); ok && strings.TrimSpace(rec.ThreadID) != "" {
			threadID = rec.ThreadID
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
