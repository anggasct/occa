package webhook

import (
	"context"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const numPRLanes = 4

func laneOfWorkflow(workflow string) int {
	switch strings.TrimSpace(strings.ToLower(workflow)) {
	case "dispatch", "merged":
		return 0
	case "review":
		return 1
	case "fix":
		return 2
	default:
		return 3
	}
}

func prKeyForEndpoint(ep config.EndpointConfig, key WebhookExecutionKey, envelope WebhookEnvelope) (string, bool) {
	if ep.Automation == nil {
		return "", false
	}
	repo := strings.TrimSpace(key.Repository)
	if repo == "" {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
	}
	number := strings.TrimSpace(stringValue(envelope["pr_number"]))
	if repo == "" || number == "" {
		return "", false
	}
	return normalizeRepoKey(repo) + "#" + number, true
}

func automationMode(ep config.EndpointConfig, author string) string {
	if ep.Automation == nil {
		return config.AutomationFull
	}
	for login, mode := range ep.Automation.Authors {
		if strings.EqualFold(strings.TrimSpace(login), strings.TrimSpace(author)) {
			return mode
		}
	}
	if ep.Automation.Default != "" {
		return ep.Automation.Default
	}
	return config.AutomationReviewOnly
}

func (s *Server) resolvePRAuthor(ctx context.Context, repo, number string) string {
	if s.prAuthorResolver != nil {
		if author, err := s.prAuthorResolver(ctx, repo, number); err == nil {
			return strings.TrimSpace(author)
		}
		return ""
	}
	n, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil || n <= 0 || strings.TrimSpace(repo) == "" {
		return ""
	}
	timeout := 15 * time.Second
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", "pr", "view", strconv.Itoa(n), "--repo", repo, "--json", "author", "-q", ".author.login")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (s *Server) automationAllows(ep config.EndpointConfig, key WebhookExecutionKey, envelope WebhookEnvelope) bool {
	if ep.Automation == nil {
		return true
	}
	workflow := strings.TrimSpace(strings.ToLower(ep.Workflow))
	if workflow != "fix" && workflow != "merge" {
		return true
	}
	// A matched comment trigger is an explicit human instruction, so it may
	// start a fix run even in review-only mode. Merge stays policy-gated.
	trigger := strings.TrimSpace(stringValue(envelope["comment_trigger"]))
	if workflow == "fix" && trigger != "" && len(ep.CommentAuthors) > 0 &&
		commentAuthorAllowed(ep.CommentAuthors, trigger, stringValue(envelope["review_user"])) {
		return true
	}
	author := strings.TrimSpace(stringValue(envelope["pr_author"]))
	if author == "" {
		if prKey, ok := prKeyForEndpoint(ep, key, envelope); ok {
			if repo, number, found := splitPRKey(prKey); found {
				author = s.resolvePRAuthor(context.Background(), repo, number)
			}
		}
	}
	return automationMode(ep, author) == config.AutomationFull
}

func splitPRKey(prKey string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(prKey), "#", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

type prLane struct {
	mu    sync.Mutex
	items []dispatchItem
}

func (l *prLane) push(item dispatchItem) {
	l.mu.Lock()
	l.items = append(l.items, item)
	l.mu.Unlock()
}

func (l *prLane) pop() (dispatchItem, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.items) == 0 {
		return dispatchItem{}, false
	}
	item := l.items[0]
	l.items = l.items[1:]
	return item, true
}

func (l *prLane) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

func (l *prLane) removeMatching(match func(dispatchItem) bool) (dispatchItem, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, item := range l.items {
		if match(item) {
			l.items = append(l.items[:i], l.items[i+1:]...)
			return item, true
		}
	}
	return dispatchItem{}, false
}

func (l *prLane) removeOldest() (dispatchItem, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.items) == 0 {
		return dispatchItem{}, false
	}
	item := l.items[0]
	l.items = l.items[1:]
	return item, true
}

func makePRLanes(int) [numPRLanes]*prLane {
	var lanes [numPRLanes]*prLane
	for i := range lanes {
		lanes[i] = &prLane{}
	}
	return lanes
}

func newDispatcher(server *Server, key string) *dispatcher {
	return &dispatcher{
		server:       server,
		key:          key,
		lanes:        makePRLanes(server.maxQueuedPerKey),
		notify:       make(chan struct{}, 1),
		lastActivity: time.Now(),
	}
}

func (d *dispatcher) laneFor(item dispatchItem) int {
	lane := item.lane
	if lane < 0 || lane >= numPRLanes {
		lane = numPRLanes - 1
	}
	return lane
}

func (d *dispatcher) queueLen() int {
	n := 0
	for i := range d.lanes {
		n += d.lanes[i].len()
	}
	return n
}

func (d *dispatcher) laneLen(lane int) int {
	if lane < 0 || lane >= numPRLanes {
		return 0
	}
	return d.lanes[lane].len()
}

// queueCap reports the per-PR budget, not the sum of the lane slices: the
// budget is the admission ceiling and the lanes share it.
func (d *dispatcher) queueCap() int {
	return d.server.maxQueuedPerKey
}

func (d *dispatcher) signal() {
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *dispatcher) push(item dispatchItem) bool {
	d.lanes[d.laneFor(item)].push(item)
	d.touch()
	d.signal()
	return true
}

type admissionDecision struct {
	admitted bool
	replaced *dispatchItem
	evicted  *dispatchItem
}

// admit reserves a queue slot for an item without inserting it. The caller
// mirrors the reservation into the store and then pushes the item; both
// happen under dispatchMu, so no other admission can overbook the budget in
// between. State events are latest-wins: a matching queued item is removed and
// returned so the caller can mark it superseded.
func (d *dispatcher) admit(item dispatchItem, budget, stateShare, overflow int) admissionDecision {
	lane := d.laneFor(item)
	item.lane = lane
	for i := range d.lanes {
		if replaced, ok := d.lanes[i].removeMatching(func(queued dispatchItem) bool {
			return queued.ep.Name == item.ep.Name && queued.deliveryID == item.deliveryID
		}); ok {
			return admissionDecision{admitted: true, replaced: &replaced}
		}
	}
	if lane == numPRLanes-1 {
		if item.actionClass != "" {
			replaced, ok := d.lanes[lane].removeMatching(func(queued dispatchItem) bool {
				if queued.ep.Name != item.ep.Name ||
					queued.eventType != item.eventType ||
					queued.prKey != item.prKey ||
					queued.actionClass != item.actionClass {
					return false
				}
				// Without a PR identity, only the same delivery is provably
				// the same state; distinct deliveries may target different
				// resources.
				return item.prKey != "" || queued.deliveryID == item.deliveryID
			})
			if ok {
				return admissionDecision{admitted: true, replaced: &replaced}
			}
		}
		if budget <= 0 || d.laneLen(lane) >= stateShare || d.queueLen() >= budget {
			return admissionDecision{}
		}
		return admissionDecision{admitted: true}
	}
	total := d.queueLen()
	if total >= budget {
		if evicted, ok := d.lanes[numPRLanes-1].removeOldest(); ok {
			return admissionDecision{admitted: true, evicted: &evicted}
		}
		if total >= budget+overflow {
			return admissionDecision{}
		}
	}
	return admissionDecision{admitted: true}
}

func (d *dispatcher) pop() (dispatchItem, bool) {
	for i := range d.lanes {
		if item, ok := d.lanes[i].pop(); ok {
			return item, true
		}
	}
	return dispatchItem{}, false
}

func (d *dispatcher) hasQueuedHigher() bool {
	for i := 0; i < numPRLanes-1; i++ {
		if d.lanes[i].len() > 0 {
			return true
		}
	}
	return false
}

func (d *dispatcher) drain() []dispatchItem {
	var out []dispatchItem
	for {
		item, ok := d.pop()
		if !ok {
			return out
		}
		out = append(out, item)
	}
}

func (s *Server) mirrorEnqueue(prKey, endpoint, eventType string, deliveryID int64, priority int) int64 {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return 0
	}
	if _, err := s.prQueues.EnsurePR(context.Background(), prKey, "", ""); err != nil {
		return 0
	}
	id, err := s.prQueues.Enqueue(context.Background(), prKey, endpoint, eventType, deliveryID, priority)
	if err != nil {
		return 0
	}
	return id
}

func (s *Server) prMirrorBlocked(prKey, reason string) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	if err := s.prQueues.SetBlocked(context.Background(), prKey, reason); err != nil {
		slog.Warn("webhook: pr blocked mark failed", "pr_key", prKey, "error", err)
	}
}

func (s *Server) mirrorDelete(queueID int64) {
	if s.prQueues == nil || queueID == 0 {
		return
	}
	_ = s.prQueues.DeleteItem(context.Background(), queueID)
}

// drainPRQueue drops everything still waiting for the PR without executing
// it. Durable ACK means each queued item already owns a received receipt, so
// the drop must settle that receipt instead of leaving it in flight.
func (s *Server) drainPRQueue(d *dispatcher, prKey string) int {
	dropped := 0
	for _, item := range d.drain() {
		s.mirrorDelete(item.queueID)
		s.settleDrainedReceipt(item)
		dropped++
	}
	if dropped > 0 {
		slog.Info("webhook: pr queue drained", "pr_key", prKey, "dropped", dropped)
	}
	return dropped
}

func (s *Server) settleDrainedReceipt(item dispatchItem) {
	ctx := context.Background()
	receipt, err := s.deliveries.Get(ctx, item.ep.Name, item.deliveryID)
	if err != nil || receipt == nil {
		return
	}
	ok, err := s.deliveries.Transition(ctx, receipt.ID,
		[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
		store.WebhookStatusSkipped, "queue drained")
	if err != nil {
		slog.Warn("webhook: drained delivery settle failed",
			"endpoint", item.ep.Name,
			"delivery_id", item.deliveryID,
			"error", err,
		)
	} else if ok {
		slog.Info("webhook: drained delivery settled",
			"endpoint", item.ep.Name,
			"delivery_id", item.deliveryID,
		)
	}
}

func (s *Server) prMirrorSetActive(item dispatchItem, deliveryID int64) {
	if s.prQueues == nil || strings.TrimSpace(item.prKey) == "" {
		return
	}
	kind := strings.TrimSpace(strings.ToLower(item.ep.Workflow))
	if kind == "" {
		kind = "dispatch"
	}
	if _, err := s.prQueues.EnsurePR(context.Background(), item.prKey, "", ""); err != nil {
		return
	}
	if err := s.prQueues.SetActive(context.Background(), item.prKey, kind, "", deliveryID); err != nil {
		slog.Warn("webhook: pr active mark failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "pr_key", item.prKey, "error", err)
	}
	// A new run starts: the PR left the blocked state, so the next blocking
	// episode escalates to the channel again.
	s.clearEscalated(item.prKey)
}

func (s *Server) prMirrorSync(workCtx *WebhookWorkContext) {
	if s.prQueues == nil || workCtx == nil || strings.TrimSpace(workCtx.PRKey) == "" {
		return
	}
	ctx := context.Background()
	if strings.TrimSpace(workCtx.SessionID) != "" {
		if err := s.prQueues.UpdateSession(ctx, workCtx.PRKey, workCtx.SessionID); err != nil {
			slog.Warn("webhook: pr session stamp failed", "pr_key", workCtx.PRKey, "error", err)
		}
	}
	if strings.TrimSpace(workCtx.ThreadID) != "" {
		if err := s.prQueues.UpdateThread(ctx, workCtx.PRKey, workCtx.ThreadID, workCtx.RootMessageID); err != nil {
			slog.Warn("webhook: pr thread stamp failed", "pr_key", workCtx.PRKey, "error", err)
		}
	}
}

func (s *Server) prMirrorClearActive(prKey string) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	if err := s.prQueues.ClearActive(context.Background(), prKey); err != nil {
		slog.Warn("webhook: pr active clear failed", "pr_key", prKey, "error", err)
	}
}
