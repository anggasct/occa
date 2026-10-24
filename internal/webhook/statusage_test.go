package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func seedRunDelivery(t *testing.T, st *store.SQLiteStore, prKey string, startedAt, activityAt time.Time) *store.WebhookDelivery {
	t.Helper()
	repo := st.WebhookDeliveryRepo()
	deliveryID := fmt.Sprintf("del-age-%d", time.Now().UnixNano())
	created, err := repo.Create(context.Background(), store.WebhookDelivery{
		Endpoint:   "github-widget-review",
		DeliveryID: deliveryID,
		EventType:  "pull_request_review",
		PRKey:      prKey,
		Status:     store.WebhookStatusProcessing,
		CreatedAt:  startedAt.Unix(),
		UpdatedAt:  activityAt.Unix(),
	})
	if err != nil || !created {
		t.Fatalf("seed delivery: created=%v err=%v", created, err)
	}
	d, err := repo.Get(context.Background(), "github-widget-review", deliveryID)
	if err != nil || d == nil {
		t.Fatalf("load delivery: %v", err)
	}
	return d
}

func seedActiveRun(t *testing.T, st *store.SQLiteStore, prKey, threadID, rootID string, deliveryID int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, threadID, rootID); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, prKey, "review", "sess-age", deliveryID); err != nil {
		t.Fatalf("set active: %v", err)
	}
}

func seedParentSurface(srv *Server, prKey, threadID, rootID string, phase Phase, activityAt time.Time) {
	srv.tasks.SetSurface(prKey, SurfaceUpdate{
		RootMessageID: rootID,
		Repo:          "acme/widget",
		Branch:        "feat/x",
		PRNumber:      "181",
		ThreadID:      threadID,
		Phase:         string(phase),
		LastWriteAt:   activityAt,
	})
}

func parentEdits(rec *surfaceRecorder) []surfaceMessage {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []surfaceMessage
	for _, msg := range rec.edited {
		if msg.messageID == "parent-1" {
			out = append(out, msg)
		}
	}
	return out
}

func TestParentCardShowsRunAge(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	startedAt := time.Now().Add(-12*time.Minute - 20*time.Second)
	seedRunDelivery(t, st, prKey, startedAt, time.Now())

	srv.syncPhaseParent(ctx, srv.endpoints["/review"], prKey, surfaceEnvelope(), PhaseReview)

	sent, _, _, _ := rec.snapshot()
	if len(sent) != 1 {
		t.Fatalf("parent sends = %d, want one", len(sent))
	}
	if !strings.HasSuffix(sent[0].text, "· ⏱️ 12m") {
		t.Fatalf("parent %q must end with the run age", sent[0].text)
	}
}

func TestParentCardOmitsAgeWithoutDelivery(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	prKey := "acme/widget#181"

	srv.syncPhaseParent(context.Background(), srv.endpoints["/review"], prKey, surfaceEnvelope(), PhaseReview)

	sent, _, _, _ := rec.snapshot()
	if len(sent) != 1 {
		t.Fatalf("parent sends = %d, want one", len(sent))
	}
	if strings.Contains(sent[0].text, "⏱️") || strings.Contains(sent[0].text, "total") {
		t.Fatalf("parent %q must carry no age without a delivery", sent[0].text)
	}
	if !strings.HasSuffix(sent[0].text, " — review") {
		t.Fatalf("parent %q must end with the phase", sent[0].text)
	}
}

func TestParentCardIdleMarker(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(-12 * time.Minute)
	idle := FormatRunAgeSegment(now, startedAt, now.Add(-3*time.Minute), PhaseReview, 90*time.Second)
	if !strings.HasSuffix(idle, "· ⏱️ 12m · idle 3m") {
		t.Fatalf("age segment %q must carry the idle marker", idle)
	}
	busy := FormatRunAgeSegment(now, startedAt, now.Add(-10*time.Second), PhaseReview, 90*time.Second)
	if strings.Contains(busy, "idle") {
		t.Fatalf("age segment %q must not carry an idle marker for an active run", busy)
	}
}

func TestParentCardFreezesOnTerminal(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)

	srv.syncPhaseParent(ctx, srv.endpoints["/review"], prKey, surfaceEnvelope(), PhaseMerged)
	sent, edited, _, _ := rec.snapshot()
	writes := append(sent, edited...)
	frozen := false
	for _, msg := range writes {
		if msg.messageID == "parent-1" && strings.HasSuffix(msg.text, "· 12m total") {
			frozen = true
		}
	}
	if !frozen {
		t.Fatalf("terminal parent writes = %+v, want a frozen total", writes)
	}

	before := len(parentEdits(rec))
	if got := srv.refreshRunAges(ctx, now.Add(time.Minute)); got != 0 {
		t.Fatalf("terminal tick edits = %d, want none", got)
	}
	if got := srv.refreshRunAges(ctx, now.Add(2*time.Minute)); got != 0 {
		t.Fatalf("terminal tick edits = %d, want none", got)
	}
	if after := len(parentEdits(rec)); after != before {
		t.Fatalf("terminal parent was edited %d more times", after-before)
	}
}

func TestParentCardEditsOncePerMinute(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	if got := srv.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("first tick edits = %d, want one", got)
	}
	if got := len(parentEdits(rec)); got != 1 {
		t.Fatalf("parent edits after first tick = %d, want one", got)
	}
	if got := srv.refreshRunAges(ctx, now.Add(10*time.Second)); got != 0 {
		t.Fatalf("same-minute tick edits = %d, want none", got)
	}
	if got := srv.refreshRunAges(ctx, now.Add(65*time.Second)); got != 1 {
		t.Fatalf("next-minute tick edits = %d, want one", got)
	}
	if got := len(parentEdits(rec)); got != 2 {
		t.Fatalf("parent edits = %d, want two", got)
	}
}

func TestParentCardAgeSurvivesRestart(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)

	srv.syncPhaseParent(ctx, srv.endpoints["/review"], prKey, surfaceEnvelope(), PhaseReview)

	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil || row == nil {
		t.Fatalf("load queue row: %v", err)
	}
	if row.Branch != "feat/x" || row.PRURL != "https://github.com/acme/widget/pull/181" {
		t.Fatalf("persisted surface meta = %+v, want branch and PR URL", row)
	}

	restarted, rec := newSurfaceServerOn(t, st)
	if _, ok := restarted.tasks.SurfaceDetail(prKey); ok {
		t.Fatal("registry must be empty to simulate a restart")
	}
	segment := restarted.runAgeSegment(ctx, prKey, PhaseReview)
	if !strings.HasSuffix(segment, "· ⏱️ 12m") {
		t.Fatalf("age segment after restart = %q, want the delivery row age", segment)
	}

	if got := restarted.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("first tick after restart edits = %d, want one", got)
	}
	edits := parentEdits(rec)
	if len(edits) != 1 || !strings.HasSuffix(edits[0].text, "· ⏱️ 12m") {
		t.Fatalf("first tick after restart = %+v, want the delivery row age", edits)
	}
	labels := make(map[string]bool, len(edits[0].buttons))
	for _, button := range edits[0].buttons {
		labels[button.Label] = true
	}
	for _, want := range []string{"Open PR", "Merge", "Re-dispatch"} {
		if !labels[want] {
			t.Fatalf("first tick after restart buttons = %+v, want %q", edits[0].buttons, want)
		}
	}
}

func TestParentCardAgeTickKeepsConcurrentPhase(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	transitioned := false
	srv.SetEditorWithButtons(func(_ context.Context, _, _, msgID, text string, buttons []channel.Button) error {
		if msgID == "parent-1" && !transitioned {
			transitioned = true
			srv.syncPhaseParent(ctx, srv.endpoints["/merge"], prKey, surfaceEnvelope(), PhaseMerge)
			// The stale age write lands after the transition, as it would when
			// the platform call completes out of order.
			rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text, buttons: buttons})
			return nil
		}
		rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text, buttons: buttons})
		return nil
	})

	if got := srv.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("tick edits = %d, want one", got)
	}
	edits := parentEdits(rec)
	if len(edits) == 0 {
		t.Fatal("tick produced no parent writes")
	}
	if last := edits[len(edits)-1].text; !strings.Contains(last, "— merge") {
		t.Fatalf("final parent text = %q, want the merge phase", last)
	}
}

func TestParentCardEditBackoff(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	attempts := 0
	srv.SetEditorWithButtons(func(_ context.Context, _, _, _, _ string, _ []channel.Button) error {
		attempts++
		return errors.New("edit rejected")
	})
	if got := srv.refreshRunAges(ctx, now); got != 0 {
		t.Fatalf("failing tick edits = %d, want none", got)
	}
	if attempts != 1 {
		t.Fatalf("failing tick attempts = %d, want one", attempts)
	}
	if got := strings.Count(logs.String(), "run age edit failed"); got != 1 {
		t.Fatalf("failing edit logged %d warnings, want one: %s", got, logs.String())
	}

	srv.SetEditorWithButtons(func(_ context.Context, _, _, msgID, text string, _ []channel.Button) error {
		attempts++
		if msgID != "parent-1" || !strings.HasSuffix(text, "· ⏱️ 12m") {
			t.Fatalf("retry edit = %q on %q", text, msgID)
		}
		return nil
	})
	if got := srv.refreshRunAges(ctx, now.Add(30*time.Second)); got != 1 {
		t.Fatalf("retry tick edits = %d, want one", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts after retry = %d, want two", attempts)
	}
}

func TestParentCardEditBackoffRateLimited(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	attempts := 0
	srv.SetEditorWithButtons(func(_ context.Context, _, _, _, _ string, _ []channel.Button) error {
		attempts++
		return fmt.Errorf("%w: slow down", channel.ErrRateLimited)
	})
	if got := srv.refreshRunAges(ctx, now); got != 0 {
		t.Fatalf("rate-limited tick edits = %d, want none", got)
	}
	if got := srv.refreshRunAges(ctx, now.Add(30*time.Second)); got != 0 {
		t.Fatalf("one interval after 429 edits = %d, want none", got)
	}
	if attempts != 1 {
		t.Fatalf("attempts one interval after 429 = %d, want one", attempts)
	}
	if got := srv.refreshRunAges(ctx, now.Add(61*time.Second)); got != 0 {
		t.Fatalf("second interval after 429 edits = %d, want none", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts two intervals after 429 = %d, want two", attempts)
	}
}

func TestParentCardRefreshConfig(t *testing.T) {
	cases := []struct {
		name    string
		ep      configEndpoint
		refresh time.Duration
		idle    time.Duration
	}{
		{name: "defaults", ep: configEndpoint{}, refresh: 30 * time.Second, idle: 90 * time.Second},
		{name: "zero", ep: configEndpoint{refresh: 0, idle: 0}, refresh: 30 * time.Second, idle: 90 * time.Second},
		{name: "negative", ep: configEndpoint{refresh: -time.Second, idle: -time.Second}, refresh: 30 * time.Second, idle: 90 * time.Second},
		{name: "configured", ep: configEndpoint{refresh: 10 * time.Second, idle: 45 * time.Second}, refresh: 10 * time.Second, idle: 45 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := tc.ep.endpoint()
			if got := ep.EffectiveRefreshInterval(); got != tc.refresh {
				t.Fatalf("refresh interval = %v, want %v", got, tc.refresh)
			}
			if got := ep.EffectiveIdleAfter(); got != tc.idle {
				t.Fatalf("idle after = %v, want %v", got, tc.idle)
			}
		})
	}
}

func TestThreadStatusLineAge(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	seedRunDelivery(t, st, prKey, now.Add(-12*time.Minute-20*time.Second), now)

	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase:  PhaseReview,
		Queued: "merge (1)",
		Next:   "fix",
	})

	sent := rec.threadSends()
	if len(sent) != 1 {
		t.Fatalf("thread sends = %d, want one", len(sent))
	}
	if !strings.HasSuffix(sent[0].text, "· ⏱️ 12m") {
		t.Fatalf("thread line %q must end with the run age", sent[0].text)
	}

	noDelivery, _, noRec := newSurfaceServer(t)
	noDelivery.ensureThreadStatus(ctx, "discord", "chan-1", "thread-8", "acme/widget", "feat/y", "acme/widget#182", ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  "fix",
	})
	plain := noRec.threadSends()
	if len(plain) != 1 || strings.Contains(plain[0].text, "⏱️") {
		t.Fatalf("thread line without an in-flight delivery = %+v, want no age", plain)
	}
}

func TestPhaseParentRateLimitKeepsSurfacePhase(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	delivery := seedRunDelivery(t, st, prKey, now.Add(-5*time.Minute), now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	attempts := 0
	srv.SetEditorWithButtons(func(_ context.Context, _, _, _, _ string, _ []channel.Button) error {
		attempts++
		return fmt.Errorf("%w: slow down", channel.ErrRateLimited)
	})
	srv.syncPhaseParent(ctx, srv.endpoints["/merge"], prKey, surfaceEnvelope(), PhaseMerge)
	if attempts != 1 {
		t.Fatalf("phase edit attempts = %d, want one", attempts)
	}
	if _, phase, _, ok := srv.tasks.SurfaceState(prKey); !ok || phase != PhaseMerge {
		t.Fatalf("surface phase = %q, want merge after a throttled edit", phase)
	}

	srv.SetEditorWithButtons(func(_ context.Context, _, _, msgID, text string, buttons []channel.Button) error {
		rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text, buttons: buttons})
		return nil
	})
	if got := srv.refreshRunAges(ctx, now.Add(time.Minute)); got == 0 {
		t.Fatal("tick after the throttled phase edit produced no write")
	}
	edits := parentEdits(rec)
	if len(edits) == 0 {
		t.Fatal("tick produced no parent writes")
	}
	if last := edits[len(edits)-1].text; !strings.Contains(last, "— merge") {
		t.Fatalf("repaired parent text = %q, want the merge phase", last)
	}
}

func TestThreadStatusRateLimitDoesNotDuplicate(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  "fix",
	})
	if got := len(rec.threadSends()); got != 1 {
		t.Fatalf("thread sends = %d, want one", got)
	}

	attempts := 0
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error {
		attempts++
		return fmt.Errorf("%w: slow down", channel.ErrRateLimited)
	})
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase: PhaseFix,
		Next:  "push + re-review",
	})
	if attempts != 1 {
		t.Fatalf("thread edit attempts = %d, want one", attempts)
	}
	if got := len(rec.threadSends()); got != 1 {
		t.Fatalf("thread sends after a throttled edit = %d, want no duplicate", got)
	}
}

func TestThreadAgeTickKeepsConcurrentPhase(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  "fix",
	})

	transitioned := false
	srv.SetEditor(func(_ context.Context, _, _, msgID, text string) error {
		if msgID == "status-1" && !transitioned {
			transitioned = true
			srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
				Phase: PhaseMerge,
				Next:  "merge",
			})
		}
		rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text})
		return nil
	})

	srv.refreshRunAges(ctx, now.Add(61*time.Second))

	rec.mu.Lock()
	var threadEdits []surfaceMessage
	for _, msg := range rec.edited {
		if msg.messageID == "status-1" {
			threadEdits = append(threadEdits, msg)
		}
	}
	rec.mu.Unlock()
	if len(threadEdits) == 0 {
		t.Fatal("tick produced no thread writes")
	}
	if last := threadEdits[len(threadEdits)-1].text; !strings.Contains(last, "merge") {
		t.Fatalf("final thread text = %q, want the merge phase", last)
	}
}

type configEndpoint struct {
	refresh time.Duration
	idle    time.Duration
}

func (c configEndpoint) endpoint() config.EndpointConfig {
	return config.EndpointConfig{RefreshInterval: c.refresh, IdleAfter: c.idle}
}

func TestParentCardAgeTickRepairsTerminalPhase(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12*time.Minute - 20*time.Second)
	delivery := seedRunDelivery(t, st, prKey, startedAt, now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)

	transitioned := false
	srv.SetEditorWithButtons(func(_ context.Context, _, _, msgID, text string, buttons []channel.Button) error {
		if msgID == "parent-1" && !transitioned {
			transitioned = true
			srv.syncPhaseParent(ctx, srv.endpoints["/merged"], prKey, surfaceEnvelope(), PhaseMerged)
		}
		rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text, buttons: buttons})
		return nil
	})

	if got := srv.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("tick edits = %d, want one", got)
	}
	edits := parentEdits(rec)
	if len(edits) == 0 {
		t.Fatal("tick produced no parent writes")
	}
	if last := edits[len(edits)-1].text; !strings.HasSuffix(last, "· 12m total") {
		t.Fatalf("final parent text = %q, want the frozen terminal total", last)
	}
}

func TestThreadAgeTickRepairsTerminalPhase(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	delivery := seedRunDelivery(t, st, prKey, now.Add(-12*time.Minute-20*time.Second), now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, prKey, "thread-9", "parent-1", PhaseReview, now)
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  "fix",
	})

	transitioned := false
	srv.SetEditor(func(_ context.Context, _, _, msgID, text string) error {
		if msgID == "status-1" && !transitioned {
			transitioned = true
			srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
				Phase: PhaseMerged,
				Next:  "merge",
			})
		}
		rec.add(&rec.edited, surfaceMessage{messageID: msgID, text: text})
		return nil
	})

	srv.refreshRunAges(ctx, now.Add(61*time.Second))

	rec.mu.Lock()
	var threadEdits []surfaceMessage
	for _, msg := range rec.edited {
		if msg.messageID == "status-1" {
			threadEdits = append(threadEdits, msg)
		}
	}
	rec.mu.Unlock()
	if len(threadEdits) == 0 {
		t.Fatal("tick produced no thread writes")
	}
	if last := threadEdits[len(threadEdits)-1].text; !strings.HasSuffix(last, "· 13m total") {
		t.Fatalf("final thread text = %q, want the frozen terminal total", last)
	}
}

func TestThreadStatusLineAgeSurvivesRestart(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	delivery := seedRunDelivery(t, st, prKey, now.Add(-12*time.Minute-20*time.Second), now)
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)

	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase:  PhaseReview,
		Queued: "merge (1)",
		Next:   "fix",
	})

	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil || row == nil {
		t.Fatalf("load queue row: %v", err)
	}
	if row.ThreadStatusBase == "" || row.ThreadStatusMsgID != "status-1" {
		t.Fatalf("persisted thread status = %+v, want the base and message id", row)
	}

	restarted, rec := newSurfaceServerOn(t, st)
	if _, ok := restarted.tasks.SurfaceDetail(prKey); ok {
		t.Fatal("registry must be empty to simulate a restart")
	}
	if got := restarted.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("first tick after restart edits = %d, want one", got)
	}

	rec.mu.Lock()
	var threadEdits []surfaceMessage
	for _, msg := range rec.edited {
		if msg.messageID == "status-1" {
			threadEdits = append(threadEdits, msg)
		}
	}
	rec.mu.Unlock()
	if len(threadEdits) != 1 {
		t.Fatalf("thread edits after restart = %d, want the pinned line edited in place", len(threadEdits))
	}
	if !strings.HasSuffix(threadEdits[0].text, "· ⏱️ 12m") {
		t.Fatalf("thread line after restart = %q, want the delivery row age", threadEdits[0].text)
	}
	if got := len(rec.threadSends()); got != 0 {
		t.Fatalf("thread sends after restart = %d, want no duplicate status line", got)
	}
}

func TestParentCardRestartActivityUnknownHasNoIdle(t *testing.T) {
	_, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	now := time.Now()
	startedAt := now.Add(-12 * time.Minute)
	delivery := seedRunDelivery(t, st, prKey, startedAt, startedAt.Add(time.Minute))
	seedActiveRun(t, st, prKey, "thread-9", "parent-1", delivery.ID)
	if err := st.PRQueueRepo().UpdatePRMeta(ctx, prKey, "feat/x", "https://github.com/acme/widget/pull/181"); err != nil {
		t.Fatalf("persist surface meta: %v", err)
	}

	restarted, rec := newSurfaceServerOn(t, st)
	if got := restarted.refreshRunAges(ctx, now); got != 1 {
		t.Fatalf("first tick after restart edits = %d, want one", got)
	}
	edits := parentEdits(rec)
	if len(edits) != 1 {
		t.Fatalf("parent edits after restart = %d, want one", len(edits))
	}
	if strings.Contains(edits[0].text, "idle") {
		t.Fatalf("parent text after restart = %q, must not claim idle without an in-memory activity source", edits[0].text)
	}
}
