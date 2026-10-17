package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/relay"
)

type parkTestClient struct {
	*responseClient
	exists  bool
	freshID string
}

func (c *parkTestClient) SessionExists(_ context.Context, _ string) (bool, error) {
	return c.exists, nil
}

func (c *parkTestClient) CreateSession(_ context.Context) (string, error) {
	return c.freshID, nil
}

func seedParkedSession(t *testing.T, r *Router, repo *fakeSessionRepo, sessionID string, pid int) {
	t.Helper()
	ctx := context.Background()
	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", sessionID, pid); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	r.instances.(*fakeInstanceProvider).pidSeq = pid - 1
	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "user1", "idle"); err != nil {
		t.Fatalf("MarkParked: %v", err)
	}
}

func TestTimeoutParksSessionKeepsContext(t *testing.T) {
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{Type: "delta", Delta: "partial"}
		<-ctx.Done()
		return ctx.Err()
	})
	r, st := newResponseRouter(client)
	r.streamerNoEventTimeout = 100 * time.Millisecond
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	want := "⚠️ Task timed out (no response for 15 minutes). Send a message to resume or check /status."
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reply.contains(want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reply.contains(want) {
		t.Fatalf("expected generic timeout copy, got: %v", reply.texts())
	}
	waitForResponse(t, r)

	ctx := context.Background()
	repo := st.SessionRepo().(*fakeSessionRepo)
	active, _, err := repo.Active(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if active != "session" {
		t.Fatalf("timeout must keep the session row, active = %q", active)
	}
	parked, reason, err := repo.ParkState(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if !parked || reason != "idle" {
		t.Fatalf("ParkState = (%v, %q), want (true, idle)", parked, reason)
	}
	if len(repo.deletes) != 0 {
		t.Fatalf("timeout deleted sessions: %v", repo.deletes)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.createCalls != 1 {
		t.Fatalf("createCalls = %d, want exactly 1 (initial only, no auto-reset)", client.createCalls)
	}
	if len(client.sendSessions) != 1 || client.sendSessions[0] != "session" {
		t.Fatalf("sendSessions = %v, want exactly [session]", client.sendSessions)
	}
}

func TestTimeoutPermissionParksWithPermissionReason(t *testing.T) {
	client := newResponseClient(func(ctx context.Context, events chan<- relay.Event) error {
		events <- relay.Event{
			Type: "permission_asked",
			Permission: &relay.PermissionRequest{
				ID:         "req1",
				Permission: "shell",
				Tool:       "bash",
			},
		}
		<-ctx.Done()
		return ctx.Err()
	})
	r, st := newResponseRouter(client)
	r.permissions.window = 15 * time.Millisecond
	r.streamerNoEventTimeout = 150 * time.Millisecond
	reply := newResponseReply()

	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reply.contains("Permission requested") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reply.contains("Permission requested") {
		t.Fatalf("permission prompt was not registered: %v", reply.texts())
	}

	want := "⚠️ Task timed out waiting for your permission (no response for 15 minutes). Send a message to resume or check /status."
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if reply.contains(want) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !reply.contains(want) {
		t.Fatalf("expected permission-caused timeout copy, got: %v", reply.texts())
	}
	waitForResponse(t, r)

	parked, reason, err := st.SessionRepo().ParkState(context.Background(), "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if !parked || reason != "permission" {
		t.Fatalf("ParkState = (%v, %q), want (true, permission)", parked, reason)
	}
}

func TestParkedResumeReusesSameSession(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	repo := st.SessionRepo().(*fakeSessionRepo)
	seedParkedSession(t, r, repo, "ses-parked", 7)

	reply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "continue", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.sendSessions) != 1 || client.sendSessions[0] != "ses-parked" {
		t.Fatalf("resume went to sessions %v, want exactly [ses-parked]", client.sendSessions)
	}
	if client.createCalls != 0 {
		t.Fatalf("resume created %d sessions, want 0", client.createCalls)
	}
	parked, _, err := repo.ParkState(context.Background(), "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("ParkState: %v", err)
	}
	if parked {
		t.Fatal("resuming the parked session must clear the park marker")
	}
	if reply.contains("could not be revived") {
		t.Fatalf("resume of a live session must not warn: %v", reply.texts())
	}
	if !reply.contains("response") {
		t.Fatalf("resumed turn produced no response: %v", reply.texts())
	}
}

func TestParkedUnrevivableStartsFreshWithReport(t *testing.T) {
	base := newResponseClient(nil)
	client := &parkTestClient{responseClient: base, exists: false, freshID: "ses-fresh"}
	r, st := newResponseRouter(client)
	repo := st.SessionRepo().(*fakeSessionRepo)
	seedParkedSession(t, r, repo, "ses-parked", 7)

	reply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "continue", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	base.mu.Lock()
	defer base.mu.Unlock()
	if len(base.sendSessions) != 1 || base.sendSessions[0] != "ses-fresh" {
		t.Fatalf("unrevivable resume went to sessions %v, want exactly [ses-fresh]", base.sendSessions)
	}
	if !reply.contains("ses-parked") || !reply.contains("ses-fresh") || !reply.contains("could not be revived") {
		t.Fatalf("fresh-session report must name both sessions: %v", reply.texts())
	}
	active, _, err := repo.Active(context.Background(), "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if active != "ses-fresh" {
		t.Fatalf("active = %q, want ses-fresh", active)
	}
}

func TestGoneSessionDispatchStartsFreshWithReport(t *testing.T) {
	base := newResponseClient(func(_ context.Context, _ chan<- relay.Event) error {
		return relay.ErrNotFound
	})
	client := &parkTestClient{responseClient: base, exists: true, freshID: "ses-fresh"}
	r, st := newResponseRouter(client)
	repo := st.SessionRepo().(*fakeSessionRepo)

	ctx := context.Background()
	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-dead", 7); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	r.instances.(*fakeInstanceProvider).pidSeq = 6

	reply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForResponse(t, r)

	if !reply.contains("ses-dead") || !reply.contains("ses-fresh") || !reply.contains("is gone on the agent") {
		t.Fatalf("dead-session report must name both sessions: %v", reply.texts())
	}
	if reply.contains("Agent unreachable") {
		t.Fatalf("dead session must not report generic unreachable: %v", reply.texts())
	}
	active, _, err := repo.Active(ctx, "telegram", "chat1", "", "user1")
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if active != "ses-fresh" {
		t.Fatalf("active = %q, want ses-fresh", active)
	}
}

func TestStatusShowsParkedSession(t *testing.T) {
	client := newResponseClient(nil)
	r, st := newResponseRouter(client)
	repo := st.SessionRepo().(*fakeSessionRepo)

	ctx := context.Background()
	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-parked", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	r.instances.(*fakeInstanceProvider).pidSeq = 99
	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "user1", "idle"); err != nil {
		t.Fatalf("MarkParked: %v", err)
	}

	reply := newResponseReply()
	if err := r.Route(ctx, responseMessage("user1", "chat1", "/status", reply)); err != nil {
		t.Fatalf("Route status: %v", err)
	}
	if !reply.contains("ses-parked") || !reply.contains("timed out — send a message to resume") {
		t.Fatalf("status must surface the parked session: %v", reply.texts())
	}
}

func TestStatusDeadParkedIssuesFreshWithoutSuffix(t *testing.T) {
	base := newResponseClient(nil)
	client := &parkTestClient{responseClient: base, exists: false, freshID: "ses-fresh"}
	r, st := newResponseRouter(client)
	repo := st.SessionRepo().(*fakeSessionRepo)

	ctx := context.Background()
	if err := repo.SetActive(ctx, "telegram", "chat1", "", "user1", "ses-dead", 7); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	r.instances.(*fakeInstanceProvider).pidSeq = 10
	if err := repo.MarkParked(ctx, "telegram", "chat1", "", "user1", "idle"); err != nil {
		t.Fatalf("MarkParked: %v", err)
	}

	reply := newResponseReply()
	if err := r.Route(ctx, responseMessage("user1", "chat1", "/status", reply)); err != nil {
		t.Fatalf("Route status: %v", err)
	}
	texts := strings.Join(reply.texts(), "\n")
	if !strings.Contains(texts, "ses-fresh") {
		t.Fatalf("status must name the fresh session, got: %v", reply.texts())
	}
	if strings.Contains(texts, "timed out") {
		t.Fatalf("fresh session must not carry the timed-out suffix, got: %v", reply.texts())
	}
	if parked, _, err := repo.ParkState(ctx, "telegram", "chat1", "", "user1"); err != nil {
		t.Fatalf("ParkState: %v", err)
	} else if parked {
		t.Fatal("fresh session must not stay parked")
	}
}

func TestProgressTickerStallsWithSuspectedGate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending bool
		outputs int64
		gate    string
	}{
		{name: "permission", pending: true, outputs: 0, gate: "permission"},
		{name: "provider", pending: false, outputs: 0, gate: "provider"},
		{name: "idle", pending: false, outputs: 1, gate: "idle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := newResponseReply()
			stopCh := make(chan struct{})
			defer close(stopCh)

			tracker := newTurnTracker(time.Now(), func() bool { return tc.pending })
			for i := int64(0); i < tc.outputs; i++ {
				tracker.markProductive(time.Now())
			}
			go startProgressTicker(context.Background(), reply, make(chan struct{}, 1), stopCh, 2*time.Millisecond, 5*time.Millisecond, nil, "", "", "", nil, 10*time.Millisecond, tracker)

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				found := false
				for _, text := range reply.texts() {
					if strings.Contains(text, "Stalled") && strings.Contains(text, tc.gate) {
						found = true
						break
					}
				}
				if found {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatalf("stalled notice with gate %q never posted: %v", tc.gate, reply.texts())
		})
	}
}

func TestProgressTickerProductivityKeepsWorking(t *testing.T) {
	reply := newResponseReply()
	stopCh := make(chan struct{})
	defer close(stopCh)

	tracker := newTurnTracker(time.Now(), func() bool { return false })
	markDone := make(chan struct{})
	go func() {
		defer close(markDone)
		feed := time.NewTicker(4 * time.Millisecond)
		defer feed.Stop()
		timeout := time.NewTimer(120 * time.Millisecond)
		defer timeout.Stop()
		for {
			select {
			case <-feed.C:
				tracker.markProductive(time.Now())
			case <-timeout.C:
				return
			}
		}
	}()

	go startProgressTicker(context.Background(), reply, make(chan struct{}, 1), stopCh, 2*time.Millisecond, 5*time.Millisecond, nil, "", "", "", nil, 50*time.Millisecond, tracker)
	<-markDone

	sawWorking := false
	for _, text := range reply.texts() {
		if strings.Contains(text, "Stalled") {
			t.Fatalf("productive turn must never report stalled: %v", reply.texts())
		}
		if strings.Contains(text, "Still working") {
			sawWorking = true
		}
	}
	if !sawWorking {
		t.Fatalf("productive-but-quiet turn must keep the working notice: %v", reply.texts())
	}
}

func TestFrozenResponseFlipsToStalled(t *testing.T) {
	client := newResponseClient(func(_ context.Context, events chan<- relay.Event) error {
		time.Sleep(300 * time.Millisecond)
		events <- relay.Event{Type: "delta", Delta: "late"}
		events <- relay.Event{Type: "done"}
		close(events)
		return nil
	})
	r, _ := newResponseRouter(client)
	r.progressQuietThreshold = 10 * time.Millisecond
	r.progressStalledAfter = 50 * time.Millisecond
	r.streamerNoEventTimeout = 5 * time.Second

	oldInterval := progressTickerInterval
	progressTickerInterval = 5 * time.Millisecond
	defer func() { progressTickerInterval = oldInterval }()

	reply := newResponseReply()
	if err := r.Route(context.Background(), responseMessage("user1", "chat1", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reply.contains("Stalled") && reply.contains("suspected gate: provider") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !reply.contains("Stalled") || !reply.contains("suspected gate: provider") {
		t.Fatalf("frozen response must flip to STALLED: %v", reply.texts())
	}
	waitForResponse(t, r)
	if !reply.contains("late") {
		t.Fatalf("turn output missing after stall: %v", reply.texts())
	}
}
