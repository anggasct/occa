package webhook

import (
	"context"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
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

func makePRLanes(capacity int) [numPRLanes]chan dispatchItem {
	var lanes [numPRLanes]chan dispatchItem
	for i := range lanes {
		lanes[i] = make(chan dispatchItem, capacity)
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

func (d *dispatcher) queueLen() int {
	n := 0
	for i := range d.lanes {
		n += len(d.lanes[i])
	}
	return n
}

func (d *dispatcher) queueCap() int {
	n := 0
	for i := range d.lanes {
		n += cap(d.lanes[i])
	}
	return n
}

func (d *dispatcher) push(item dispatchItem) bool {
	lane := item.lane
	if lane < 0 || lane >= numPRLanes {
		lane = numPRLanes - 1
	}
	select {
	case d.lanes[lane] <- item:
		d.touch()
		select {
		case d.notify <- struct{}{}:
		default:
		}
		return true
	default:
		return false
	}
}

func (d *dispatcher) pop() (dispatchItem, bool) {
	for i := range d.lanes {
		select {
		case item := <-d.lanes[i]:
			return item, true
		default:
		}
	}
	return dispatchItem{}, false
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

func (s *Server) mirrorDelete(queueID int64) {
	if s.prQueues == nil || queueID == 0 {
		return
	}
	_ = s.prQueues.DeleteItem(context.Background(), queueID)
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
