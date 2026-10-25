package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type surfaceMessage struct {
	channelID string
	messageID string
	text      string
	buttons   []channel.Button
}

type surfaceRecorder struct {
	mu           sync.Mutex
	sent         []surfaceMessage
	threadSent   []surfaceMessage
	edited       []surfaceMessage
	notified     []surfaceMessage
	mentions     []surfaceMessage
	mentionUsers []string
}

func (r *surfaceRecorder) add(list *[]surfaceMessage, msg surfaceMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*list = append(*list, msg)
}

func (r *surfaceRecorder) snapshot() ([]surfaceMessage, []surfaceMessage, []surfaceMessage, []surfaceMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]surfaceMessage(nil), r.sent...),
		append([]surfaceMessage(nil), r.edited...),
		append([]surfaceMessage(nil), r.notified...),
		append([]surfaceMessage(nil), r.mentions...)
}

func (r *surfaceRecorder) threadSends() []surfaceMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]surfaceMessage(nil), r.threadSent...)
}

func surfaceEndpoint(workflow, name, path string) config.EndpointConfig {
	return config.EndpointConfig{
		Name:       name,
		Path:       path,
		Secret:     "s3cret",
		Platform:   "discord",
		ChannelID:  "chan-1",
		Prompt:     "do it",
		Workflow:   workflow,
		Thread:     true,
		Repository: "acme/widget",
		Automation: &config.RepoAutomation{Default: config.AutomationFull},
	}
}

func newSurfaceServer(t *testing.T) (*Server, *store.SQLiteStore, *surfaceRecorder) {
	t.Helper()
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "surface.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, rec := newSurfaceServerOn(t, st)
	return srv, st, rec
}

func newSurfaceServerOn(t *testing.T, st *store.SQLiteStore) (*Server, *surfaceRecorder) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: []config.EndpointConfig{
			surfaceEndpoint("review", "github-widget-review", "/review"),
			surfaceEndpoint("fix", "github-widget-fix", "/fix"),
			surfaceEndpoint("merge", "github-widget-merge", "/merge"),
			surfaceEndpoint("merged", "github-widget-merged", "/merged"),
		},
	}
	srv := New(cfg, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	rec := &surfaceRecorder{}
	srv.SetSenderWithButtons(func(_ context.Context, _, channelID, text string, buttons []channel.Button) (string, error) {
		rec.add(&rec.sent, surfaceMessage{channelID: channelID, messageID: "parent-1", text: text, buttons: buttons})
		return "parent-1", nil
	})
	srv.SetEditorWithButtons(func(_ context.Context, _, channelID, messageID, text string, buttons []channel.Button) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text, buttons: buttons})
		return nil
	})
	srv.SetSender(func(_ context.Context, _, channelID, text string) (string, error) {
		rec.add(&rec.threadSent, surfaceMessage{channelID: channelID, messageID: "status-1", text: text})
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, channelID, messageID, text string) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text})
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, channelID, text string) error {
		rec.add(&rec.notified, surfaceMessage{channelID: channelID, text: text})
		return nil
	})
	srv.SetMentionNotifier(func(_ context.Context, _, channelID, text, mentionUserID string) error {
		rec.mu.Lock()
		rec.mentionUsers = append(rec.mentionUsers, mentionUserID)
		rec.mu.Unlock()
		rec.add(&rec.mentions, surfaceMessage{channelID: channelID, text: text})
		return nil
	})
	srv.SetMentionResolver(func(_ context.Context, _, name string) (string, bool) {
		if name == "operator" {
			return "user-42", true
		}
		return "", false
	})
	return srv, rec
}

func surfaceEnvelope() WebhookEnvelope {
	return WebhookEnvelope{
		"repository":  "acme/widget",
		"pr_number":   "181",
		"pr_url":      "https://github.com/acme/widget/pull/181",
		"head_branch": "feat/x",
		"title":       "chore: dispatch probe",
		"delivery_id": "9bccf660-cbe5-11f1-9d3c-1197f0c92220",
		"event_type":  "pull_request_review",
	}
}

var phaseParentPattern = regexp.MustCompile(`^(\S+) ([^#\s]+/[^#\s]+)#(\d+) — (queued|dispatch|review|fix|merge|merged|blocked|failed)( · ⏱️ \d+m( · idle \d+m)?| · \d+m total)?$`)

func TestPhaseParentIsOneLineForEveryPhase(t *testing.T) {
	for _, phase := range KnownPhases {
		got := FormatPhaseParent("acme/widget", "181", phase, nil)
		if !phaseParentPattern.MatchString(got) {
			t.Errorf("parent %q must match the one-line phase contract", got)
		}
		if strings.Contains(got, "\n") {
			t.Errorf("parent %q must be a single line", got)
		}
		if !strings.HasPrefix(got, PhaseIcon(phase, nil)+" ") {
			t.Errorf("parent %q must carry the icon of the phase", got)
		}
		if !strings.HasSuffix(got, " — "+string(phase)) {
			t.Errorf("parent %q must carry the phase word of its icon", got)
		}
	}
}

func TestPhaseIconOverrideComesFromTheSameEnum(t *testing.T) {
	icons := map[string]string{"review": "🔍", "fix": "🔨", "merge": "🔀", "queued": "⏳", "blocked": "🔴"}
	for _, phase := range KnownPhases {
		icon := PhaseIcon(phase, icons)
		parent := FormatPhaseParent("acme/widget", "181", phase, icons)
		if !strings.HasPrefix(parent, icon+" ") {
			t.Errorf("parent %q must start with the configured icon %q", parent, icon)
		}
		if !strings.HasSuffix(parent, " — "+string(phase)) {
			t.Errorf("parent %q must end with the phase word %q", parent, phase)
		}
	}
}

func TestPhaseParentCarriesNoAuditFields(t *testing.T) {
	banned := []string{
		"9bccf660", "Delivery", "Reason", "Event", "➡️", "━━━", "Antre", "Buka PR", "Aktif",
		"chore: dispatch probe", "feat/x", "queued:",
	}
	for _, phase := range KnownPhases {
		got := FormatPhaseParent("acme/widget", "181", phase, nil)
		for _, word := range banned {
			if strings.Contains(got, word) {
				t.Errorf("parent %q must not carry %q", got, word)
			}
		}
	}
}

func TestParentEditsOnlyOnPhaseChange(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseQueued)
	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 1 || len(edited) != 0 {
		t.Fatalf("first parent write must send once: sent=%d edited=%d", len(sent), len(edited))
	}

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseQueued)
	_, edited, _, _ = rec.snapshot()
	if len(edited) != 0 {
		t.Fatalf("a delivery that keeps the phase must not edit the parent: %+v", edited)
	}

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	_, edited, _, _ = rec.snapshot()
	if len(edited) != 1 {
		t.Fatalf("a phase change must edit the parent exactly once: %+v", edited)
	}
	if edited[0].messageID != "parent-1" {
		t.Errorf("parent edit must reuse the same message id, got %q", edited[0].messageID)
	}
	if !phaseParentPattern.MatchString(edited[0].text) {
		t.Errorf("parent edit %q must match the one-line phase contract", edited[0].text)
	}
}

func TestParentFreezesAfterTerminalPhase(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := mergeSurfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseMerged)
	_, edited, _, _ := rec.snapshot()
	frozen := len(edited)

	for _, phase := range []Phase{PhaseFailed, PhaseBlocked, PhaseReview, PhaseQueued} {
		srv.syncPhaseParent(ctx, ep, prKey, envelope, phase)
	}
	_, edited, _, _ = rec.snapshot()
	if len(edited) != frozen {
		t.Fatalf("a terminal parent must never be edited again: %+v", edited)
	}
}

func TestParentCarriesExactlyFourButtons(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	rec2 := srv.tasks.Upsert("acme/widget", "feat/x", "parent-1", "thread-9", TaskClaimed)
	srv.tasks.LinkPRThread(prKey, rec2)

	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseReview)
	sent, edited, _, _ := rec.snapshot()
	parent := surfaceMessage{}
	switch {
	case len(sent) == 1:
		parent = sent[0]
	case len(edited) == 1:
		// The task parent is the PR parent: one parent per task.
		parent = edited[0]
	default:
		t.Fatalf("expected one parent write, got %d sent and %d edited", len(sent), len(edited))
	}
	labels := buttonLabels(parent.buttons)
	want := []string{"Open PR", "Open thread", "Merge", "Re-dispatch"}
	if len(labels) != len(want) {
		t.Fatalf("parent buttons = %v, want %v", labels, want)
	}
	for i, label := range want {
		if labels[i] != label {
			t.Errorf("parent button %d = %q, want %q", i, labels[i], label)
		}
	}
	if buttonDisabled(parent.buttons, "Merge") {
		t.Error("merge must be enabled while the gate would pass")
	}

	if _, err := st.PRQueueRepo().Enqueue(ctx, prKey, "github-widget-fix", "pull_request_review", 1, 3); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseFix)
	_, edited, _, _ = rec.snapshot()
	if len(edited) != 2 {
		t.Fatalf("expected two parent edits, got %d", len(edited))
	}
	if !buttonDisabled(edited[len(edited)-1].buttons, "Merge") {
		t.Error("merge must be disabled while another item waits for the PR")
	}
}

func buttonLabels(buttons []channel.Button) []string {
	labels := make([]string, 0, len(buttons))
	for _, b := range buttons {
		labels = append(labels, b.Label)
	}
	return labels
}

func buttonDisabled(buttons []channel.Button, label string) bool {
	for _, b := range buttons {
		if b.Label == label {
			return b.Disabled
		}
	}
	return false
}

func TestThreadStatusLineIsWrittenOnceAndEditedInPlace(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"

	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseReview})
	threadSends := rec.threadSends()
	if len(threadSends) != 1 {
		t.Fatalf("first state line must be sent once, got %d", len(threadSends))
	}
	if threadSends[0].channelID != "thread-9" {
		t.Errorf("state line must live in the thread, got channel %q", threadSends[0].channelID)
	}
	if !strings.HasPrefix(threadSends[0].text, "📌 Status — ") {
		t.Errorf("state line %q must carry the pinned marker", threadSends[0].text)
	}

	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseFix})
	if got := len(rec.threadSends()); got != 1 {
		t.Fatalf("a thread must carry exactly one state line, got %d sends", got)
	}
	_, edited, _, _ := rec.snapshot()
	found := false
	for _, msg := range edited {
		if msg.messageID == "status-1" && strings.Contains(msg.text, "📌 Status — 🔨 fix") {
			found = true
		}
	}
	if !found {
		t.Errorf("the state line must be edited in place: %+v", edited)
	}
}

func TestThreadStatusShowsQueueDepthOnlyInThread(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	if _, err := st.PRQueueRepo().Enqueue(ctx, prKey, "github-widget-merge", "pull_request_review", 1, 3); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := st.PRQueueRepo().Enqueue(ctx, prKey, "github-widget-fix", "pull_request_review", 2, 3); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseFix})
	threadSends := rec.threadSends()
	if len(threadSends) != 1 {
		t.Fatalf("expected one state line, got %d", len(threadSends))
	}
	if !strings.Contains(threadSends[0].text, "queued: fix (1), merge (1)") {
		t.Errorf("state line %q must render the queue depth", threadSends[0].text)
	}

	srv.syncPhaseParent(ctx, srv.endpoints["/review"], prKey, surfaceEnvelope(), PhaseFix)
	sent, edited, _, _ := rec.snapshot()
	for _, msg := range append(sent, edited...) {
		if strings.Contains(msg.text, "queued:") {
			t.Errorf("channel parent %q must not carry the queue depth", msg.text)
		}
	}
}

func TestThreadStatusHasNoRunIdentifiers(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase:   PhaseMerge,
		Waiting: "CI (check_suite)",
		Next:    "merge",
	})
	threadSends := rec.threadSends()
	if len(threadSends) != 1 {
		t.Fatalf("expected one state line, got %d", len(threadSends))
	}
	text := threadSends[0].text
	for _, banned := range []string{"9bccf660", "run ", "elapsed", "panic:", "Delivery"} {
		if strings.Contains(text, banned) {
			t.Errorf("state line %q must not carry %q", text, banned)
		}
	}
	if strings.Contains(text, "pr_opened") || strings.Contains(text, "claimed") {
		t.Errorf("state line %q must not carry a task status word", text)
	}
}

func TestEscalationPostsOneChannelMessageWithMention(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	ep := srv.endpoints["/review"]
	srv.escalationMention = "operator"

	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "parent-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, prKey, "fix", "sess-1", 7); err != nil {
		t.Fatalf("set active: %v", err)
	}
	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil {
		t.Fatalf("get pr: %v", err)
	}

	srv.escalateBlocked(ep, prKey, row, failureBuild, 3, 3)

	_, _, _, mentions := rec.snapshot()
	if len(mentions) != 1 {
		t.Fatalf("escalation must post exactly one channel message, got %d", len(mentions))
	}
	if mentions[0].channelID != "chan-1" {
		t.Errorf("escalation must be posted in the channel, got %q", mentions[0].channelID)
	}
	for _, want := range []string{"🔴 acme/widget#181 — blocked needs you", "Action: /retry · /skip · /cancel"} {
		if !strings.Contains(mentions[0].text, want) {
			t.Errorf("escalation copy %q must contain %q", mentions[0].text, want)
		}
	}
	if strings.Contains(mentions[0].text, "@operator") {
		t.Errorf("escalation copy %q must not carry a bare name, the mention is rendered from the resolved id", mentions[0].text)
	}
	rec.mu.Lock()
	users := append([]string(nil), rec.mentionUsers...)
	rec.mu.Unlock()
	if len(users) != 1 || users[0] != "user-42" {
		t.Fatalf("escalation must ping the resolved user id, got %v", users)
	}

	_, _, notified, _ := rec.snapshot()
	for _, msg := range notified {
		if strings.Contains(msg.text, "@operator") {
			t.Errorf("only the escalation may carry a mention, got %q", msg.text)
		}
	}
}

func TestOnlyEscalationCarriesMention(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"
	srv.escalationMention = "operator"

	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseReview)
	srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseReview})
	_ = srv.notifyThread(ctx, "discord", "thread-9", "🔍 REVIEW verdict: REQUEST_CHANGES")

	sent, edited, notified, mentions := rec.snapshot()
	if len(mentions) != 0 {
		t.Fatalf("no surface may carry a mention outside escalation, got %d", len(mentions))
	}
	for _, msg := range append(append(sent, edited...), notified...) {
		if strings.Contains(msg.text, "@operator") {
			t.Errorf("message %q must stay mention-free", msg.text)
		}
	}
}

func TestEscalationWithoutResolvableMentionStillPosts(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	ep := srv.endpoints["/review"]
	srv.escalationMention = "ghost"
	srv.SetMentionResolver(func(_ context.Context, _, _ string) (string, bool) { return "", false })

	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "parent-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil {
		t.Fatalf("get pr: %v", err)
	}
	srv.escalateBlocked(ep, prKey, row, failureBuild, 2, 2)

	_, _, notified, mentions := rec.snapshot()
	if len(mentions) != 0 {
		t.Fatalf("an unresolved mention must not produce a ping, got %d", len(mentions))
	}
	escalations := 0
	for _, msg := range notified {
		if msg.channelID == "chan-1" && strings.Contains(msg.text, "needs you") {
			escalations++
		}
	}
	if escalations != 1 {
		t.Fatalf("an unresolved mention must still post the escalation once, got %d", escalations)
	}
}

func TestBlockedParentLeavesBlockedAfterOperatorAction(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseBlocked)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "parent-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, prKey, "retry exhausted"); err != nil {
		t.Fatalf("block pr: %v", err)
	}

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	if _, edited, _, _ := rec.snapshot(); len(edited) != 0 {
		t.Fatalf("a blocked parent must stay frozen until the operator acts: %+v", edited)
	}

	if _, _, err := srv.operatorCancel(ctx, channel.IncomingMessage{}, prKey); err != nil {
		t.Fatalf("operator cancel: %v", err)
	}

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	_, edited, _, _ := rec.snapshot()
	if len(edited) != 1 {
		t.Fatalf("the parent must be edited after the operator acted: %+v", edited)
	}
	if !strings.HasSuffix(edited[0].text, " — review") {
		t.Errorf("parent %q must show the new phase", edited[0].text)
	}
	if !phaseParentPattern.MatchString(edited[0].text) {
		t.Errorf("parent %q must match the one-line phase contract", edited[0].text)
	}
}

func TestSecondBlockingEpisodeEscalatesAgain(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	ep := srv.endpoints["/review"]
	srv.escalationMention = "operator"

	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "parent-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil || row == nil {
		t.Fatalf("get pr: %v", err)
	}
	srv.escalateBlocked(ep, prKey, row, failureBuild, 3, 3)
	if _, _, _, mentions := rec.snapshot(); len(mentions) != 1 {
		t.Fatalf("first blocking episode must post one channel message, got %d", len(mentions))
	}

	srv.prMirrorSetActive(dispatchItem{ep: ep, prKey: prKey}, 7)
	row, err = st.PRQueueRepo().Get(ctx, prKey)
	if err != nil || row == nil {
		t.Fatalf("get pr: %v", err)
	}
	srv.escalateBlocked(ep, prKey, row, failureBuild, 3, 3)
	_, _, _, mentions := rec.snapshot()
	if len(mentions) != 2 {
		t.Fatalf("a second blocking episode must post a fresh channel message, got %d", len(mentions))
	}
	for i, mention := range mentions {
		if !strings.Contains(mention.text, "needs you") {
			t.Errorf("escalation %d must carry the required copy: %q", i, mention.text)
		}
	}
}

func TestMergeButtonFollowsApprovalAndCIState(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"

	srv.recordMergeGate(prKey, WebhookEnvelope{"review_verdict": "approved", "status": "completed", "conclusion": "failure"})
	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseMerge)
	sent, _, _, _ := rec.snapshot()
	if len(sent) != 1 {
		t.Fatalf("expected one parent send, got %d", len(sent))
	}
	if !buttonDisabled(sent[0].buttons, "Merge") {
		t.Error("merge must be disabled while the checks are red")
	}

	srv.recordMergeGate(prKey, WebhookEnvelope{"review_verdict": "approved", "status": "completed", "conclusion": "success"})
	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseReview)
	_, edited, _, _ := rec.snapshot()
	if len(edited) != 1 {
		t.Fatalf("expected one parent edit, got %d", len(edited))
	}
	if buttonDisabled(edited[0].buttons, "Merge") {
		t.Error("merge must be enabled while the gate would pass")
	}

	srv.recordMergeGate(prKey, WebhookEnvelope{"review_verdict": "request_changes"})
	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseFix)
	_, edited, _, _ = rec.snapshot()
	if len(edited) != 2 {
		t.Fatalf("expected two parent edits, got %d", len(edited))
	}
	if !buttonDisabled(edited[1].buttons, "Merge") {
		t.Error("merge must be disabled while the review asks for changes")
	}
}

func TestPRParentCarriesFourButtonsAfterFirstPhase(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		workCtx.ThreadID = "thread-7"
		return nil
	}
	srv, st, url := newSurfaceHarness(t, exec)
	rec := &surfaceRecorder{}
	srv.SetSenderWithButtons(func(_ context.Context, _, channelID, text string, buttons []channel.Button) (string, error) {
		rec.add(&rec.sent, surfaceMessage{channelID: channelID, messageID: "parent-1", text: text, buttons: buttons})
		return "parent-1", nil
	})
	srv.SetEditorWithButtons(func(_ context.Context, _, channelID, messageID, text string, buttons []channel.Button) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text, buttons: buttons})
		return nil
	})
	srv.SetEditor(func(_ context.Context, _, channelID, messageID, text string) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text})
		return nil
	})
	srv.SetSender(func(_ context.Context, _, channelID, text string) (string, error) {
		rec.add(&rec.threadSent, surfaceMessage{channelID: channelID, messageID: "status-1", text: text})
		return "status-1", nil
	})
	srv.SetNotifier(func(_ context.Context, _, channelID, text string) error {
		rec.add(&rec.notified, surfaceMessage{channelID: channelID, text: text})
		return nil
	})

	body := `{"action":"submitted","pull_request":{"number":7,"state":"open","html_url":"https://github.com/acme/widget/pull/7","user":{"login":"owner"},"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"review":{"state":"commented","commit_id":"c0ffee-buttons","user":{"login":"reviewer"}},"repository":{"full_name":"acme/widget"}}`
	if resp := post(t, url+"/review?secret=s3cret", "del-buttons-1", "pull_request_review", body); resp.StatusCode != 200 {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-buttons-1", store.WebhookStatusCompleted)
	waitForParentWrites(t, rec, 3)

	sent, edited, _, _ := rec.snapshot()
	parentWrites := append(append([]surfaceMessage(nil), sent...), edited...)
	if len(parentWrites) == 0 {
		t.Fatal("a PR delivery must write the channel parent")
	}
	last := parentWrites[len(parentWrites)-1]
	labels := buttonLabels(last.buttons)
	want := []string{"Open PR", "Open thread", "Merge", "Re-dispatch"}
	if len(labels) != len(want) {
		t.Fatalf("parent buttons = %v, want %v", labels, want)
	}
	for i, label := range want {
		if labels[i] != label {
			t.Errorf("parent button %d = %q, want %q", i, labels[i], label)
		}
	}
	if !phaseParentPattern.MatchString(last.text) {
		t.Errorf("parent %q must match the one-line phase contract", last.text)
	}
}

// newSurfaceHarness starts a webhook server whose endpoints live on Discord,
// so the parent carries the thread link button.
func newSurfaceHarness(t *testing.T, exec func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error) (*Server, *store.SQLiteStore, string) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{
			surfaceEndpoint("review", "github-widget-review", "/review"),
			surfaceEndpoint("fix", "github-widget-fix", "/fix"),
			surfaceEndpoint("merge", "github-widget-merge", "/merge"),
			surfaceEndpoint("merged", "github-widget-merged", "/merged"),
		}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "surface.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st, ts.URL
}

// waitForParentWrites waits until the recorded channel parent carries the
// expected number of writes.
func waitForParentWrites(t *testing.T, rec *surfaceRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sent, edited, _, _ := rec.snapshot()
		if len(sent)+len(edited) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent writes = %d, want %d", len(sent)+len(edited), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSurfaceStringsAreEnglish(t *testing.T) {
	samples := []string{
		FormatPhaseParent("acme/widget", "181", PhaseFix, nil),
		FormatThreadStatusLine(PhaseFix, "merge (1)", "CI (check_suite)", "push + re-review", "retry exhausted — run /retry, /skip or /cancel", nil),
		FormatEscalationCopy("acme/widget", "181", PhaseBlocked, "fix run failed 3/3 times", "build/test failure", nil),
		FormatRunOutcome(surfaceEnvelope(), "fix", "COMPLETED", "", "push + re-review"),
		FormatPRThreadOpener(surfaceEnvelope()),
	}
	for _, sample := range samples {
		for _, banned := range []string{"Buka PR", "Aktif", "Antre", "Sedang", "Selesai", "Gagal"} {
			if strings.Contains(sample, banned) {
				t.Errorf("surface string %q must not use %q", sample, banned)
			}
		}
	}
}

func TestPRDeliveryWritesOneLineParentAndOneOutcome(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	srv, st, url := newRetryHarness(t, exec)
	rec := &surfaceRecorder{}
	srv.SetSenderWithButtons(func(_ context.Context, _, channelID, text string, buttons []channel.Button) (string, error) {
		rec.add(&rec.sent, surfaceMessage{channelID: channelID, messageID: "parent-1", text: text, buttons: buttons})
		return "parent-1", nil
	})
	srv.SetSender(func(_ context.Context, _, channelID, text string) (string, error) {
		rec.add(&rec.threadSent, surfaceMessage{channelID: channelID, messageID: "status-1", text: text})
		return "status-1", nil
	})
	srv.SetEditorWithButtons(func(_ context.Context, _, channelID, messageID, text string, buttons []channel.Button) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text, buttons: buttons})
		return nil
	})
	srv.SetEditor(func(_ context.Context, _, channelID, messageID, text string) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text})
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, channelID, text string) error {
		rec.add(&rec.notified, surfaceMessage{channelID: channelID, text: text})
		return nil
	})

	prKey := "acme/widget#7"
	taskRec := srv.tasks.Upsert("acme/widget", "feat/x", "parent-1", "thread-7", TaskClaimed)
	srv.tasks.LinkPRThread(prKey, taskRec)
	body := prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-parent")
	if resp := post(t, url+"/review?secret=s3cret", "del-parent-1", "pull_request_review", body); resp.StatusCode != 200 {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-parent-1", store.WebhookStatusCompleted)
	waitForSurface(t, rec, 1)
	waitForRunOutcome(t, rec, "🔍 REVIEW")

	sent, edited, notified, _ := rec.snapshot()
	if len(sent) != 0 {
		t.Fatalf("the task parent is the PR parent, no second parent may be posted: %+v", sent)
	}
	parentWrites := edited
	if len(parentWrites) != 2 {
		t.Fatalf("parent writes = %d, want one parent edited per phase: %+v", len(parentWrites), parentWrites)
	}
	if parentWrites[0].messageID != parentWrites[1].messageID {
		t.Fatalf("the phase changes must edit one parent: %+v", parentWrites)
	}
	phases := []string{"queued", "review"}
	for i, msg := range parentWrites {
		if !phaseParentPattern.MatchString(msg.text) {
			t.Errorf("parent %q must match the one-line phase contract", msg.text)
		}
		if !strings.Contains(msg.text, " — "+phases[i]) {
			t.Errorf("parent %q must show the phase %q", msg.text, phases[i])
		}
		for _, banned := range []string{"Delivery", "Reason", "Event", "del-parent-1", "c0ffee-parent", "feat/x", "queued:", "━━━"} {
			if strings.Contains(msg.text, banned) {
				t.Errorf("parent %q must not carry %q", msg.text, banned)
			}
		}
	}

	stateLines := 0
	for _, msg := range rec.threadSends() {
		if strings.HasPrefix(msg.text, "📌 Status — ") {
			stateLines++
		}
	}
	if stateLines != 1 {
		t.Fatalf("state lines = %d, want exactly one: %+v", stateLines, rec.threadSends())
	}

	outcomes := 0
	for _, msg := range notified {
		if strings.HasPrefix(msg.text, "🔍 REVIEW") && msg.text != "🔍 REVIEW started" {
			outcomes++
		}
	}
	if outcomes != 1 {
		t.Fatalf("run outcome messages = %d, want exactly one: %+v", outcomes, notified)
	}
	for _, msg := range append(append(sent, edited...), notified...) {
		if strings.Contains(msg.text, "[Step ") {
			t.Errorf("per-step tool lines must stay off: %q", msg.text)
		}
	}
}

// waitForSurface waits until the recorded thread carries the expected number
// of state lines.
func waitForSurface(t *testing.T, rec *surfaceRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		count := 0
		for _, msg := range rec.threadSends() {
			if strings.HasPrefix(msg.text, "📌 Status — ") {
				count++
			}
		}
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state lines = %d, want %d", count, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForRunOutcome waits until the terminal card is recorded. The delivery
// transitions to completed before its audit surfaces are written, so polling
// the receipt alone would race the outcome notification.
func waitForRunOutcome(t *testing.T, rec *surfaceRecorder, prefix string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, notified, _ := rec.snapshot()
		matches := 0
		for _, msg := range notified {
			if strings.HasPrefix(msg.text, prefix) && msg.text != prefix+" started" {
				matches++
			}
		}
		if matches > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run outcome %q never recorded: %+v", prefix, notified)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPrSurfaceReusesBranchParent(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"
	srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)

	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)

	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 0 {
		t.Fatalf("a branch binding must reuse the dispatch parent, got %d new cards: %+v", len(sent), sent)
	}
	if len(edited) != 1 || edited[0].messageID != "dispatch-parent" {
		t.Fatalf("the dispatch parent must be edited in place, got %+v", edited)
	}
	if root, _, _, ok := srv.prSurfaceState(prKey); !ok || root != "dispatch-parent" {
		t.Fatalf("surface root = %q (known %v), want the dispatch parent", root, ok)
	}
}

func TestPrSurfaceBranchRootFallback(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"
	srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)
	stale := srv.tasks.Upsert("acme/widget", "feat/stale", "", "thread-stale", TaskClaimed)
	srv.tasks.LinkPRThread(prKey, stale)

	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)

	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 0 {
		t.Fatalf("an empty PR binding must fall back to the branch parent, got %d new cards: %+v", len(sent), sent)
	}
	if len(edited) != 1 || edited[0].messageID != "dispatch-parent" {
		t.Fatalf("the branch parent must be edited in place, got %+v", edited)
	}
}

func TestPrSurfacePostsCardWhenNoHome(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"

	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)

	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 1 {
		t.Fatalf("a PR without a home must get exactly one card, got %d: %+v", len(sent), sent)
	}
	if len(edited) != 0 {
		t.Fatalf("a new card must not be edited on its first write: %+v", edited)
	}
	if root, _, _, ok := srv.prSurfaceState(prKey); !ok || root != sent[0].messageID {
		t.Fatalf("surface root = %q, want the posted card %q", root, sent[0].messageID)
	}
}

func TestPrSurfaceSingleParentAcrossPhases(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"

	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)
	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseReview)
	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseFix)

	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 1 {
		t.Fatalf("one PR must have one parent, got %d sends: %+v", len(sent), sent)
	}
	if len(edited) != 2 {
		t.Fatalf("phase changes must edit the same parent, got %d edits: %+v", len(edited), edited)
	}
	for _, msg := range edited {
		if msg.messageID != sent[0].messageID {
			t.Errorf("edit target = %q, want %q", msg.messageID, sent[0].messageID)
		}
	}
}

func TestTaskThreadRootImmutable(t *testing.T) {
	srv, _, _ := newSurfaceServer(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)
	rec := srv.tasks.SetSurface("acme/widget#181", SurfaceUpdate{
		RootMessageID: "card-2",
		Repo:          "acme/widget",
		Branch:        "feat/x",
		Phase:         string(PhaseQueued),
	})

	if rec.RootMessageID != "dispatch-parent" {
		t.Fatalf("task thread root = %q, want the existing dispatch parent", rec.RootMessageID)
	}
	if stored, ok := srv.tasks.Lookup("acme/widget", "feat/x"); !ok || stored.RootMessageID != "dispatch-parent" {
		t.Fatalf("stored root = %q (found %v), want the existing dispatch parent", stored.RootMessageID, ok)
	}
	if !strings.Contains(logs.String(), "webhook: task thread root preserved") {
		t.Fatalf("a replaced root must be logged, got %q", logs.String())
	}
}

func TestTaskThreadRootFilledWhenEmpty(t *testing.T) {
	srv, _, _ := newSurfaceServer(t)
	srv.tasks.Upsert("acme/widget", "feat/x", "", "thread-9", TaskClaimed)

	rec := srv.tasks.SetSurface("acme/widget#181", SurfaceUpdate{
		RootMessageID: "card-1",
		Repo:          "acme/widget",
		Branch:        "feat/x",
		Phase:         string(PhaseQueued),
	})

	if rec.RootMessageID != "card-1" {
		t.Fatalf("task thread root = %q, want the first parent", rec.RootMessageID)
	}
}

func TestPrSurfaceBranchFromPRRecord(t *testing.T) {
	t.Run("envelope branch selects the binding", func(t *testing.T) {
		srv, _, rec := newSurfaceServer(t)
		ep := srv.endpoints["/review"]
		prKey := "acme/widget#181"
		srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)
		srv.tasks.Upsert("acme/widget", "feat/other", "other-parent", "thread-other", TaskDispatched)
		empty := srv.tasks.Upsert("acme/widget", "feat/empty", "", "thread-empty", TaskClaimed)
		srv.tasks.LinkPRThread(prKey, empty)

		srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)

		sent, edited, _, _ := rec.snapshot()
		if len(sent) != 0 {
			t.Fatalf("a known branch must reuse its parent, got %d new cards: %+v", len(sent), sent)
		}
		if len(edited) != 1 || edited[0].messageID != "dispatch-parent" {
			t.Fatalf("the envelope branch parent must be edited, got %+v", edited)
		}
	})

	t.Run("pr record fills missing branch", func(t *testing.T) {
		srv, _, rec := newSurfaceServer(t)
		ep := srv.endpoints["/review"]
		prKey := "acme/widget#181"
		srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)
		srv.tasks.SetSurface(prKey, SurfaceUpdate{Branch: "feat/x"})
		envelope := surfaceEnvelope()
		delete(envelope, "head_branch")

		srv.syncPhaseParent(context.Background(), ep, prKey, envelope, PhaseQueued)

		sent, edited, _, _ := rec.snapshot()
		if len(sent) != 0 {
			t.Fatalf("a branch from the PR record must reuse its parent, got %d new cards: %+v", len(sent), sent)
		}
		if len(edited) != 1 || edited[0].messageID != "dispatch-parent" {
			t.Fatalf("the PR record branch parent must be edited, got %+v", edited)
		}
	})
}

func TestPrSurfaceIgnoresForeignChannelRoot(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ep.ChannelID = "chan-2"
	prKey := "acme/widget#181"
	srv.tasks.Upsert("acme/widget", "feat/x", "dispatch-parent", "thread-9", TaskDispatched)

	srv.syncPhaseParent(context.Background(), ep, prKey, surfaceEnvelope(), PhaseQueued)

	sent, edited, _, _ := rec.snapshot()
	if len(edited) != 0 {
		t.Fatalf("a parent in another channel must not be edited: %+v", edited)
	}
	if len(sent) != 1 {
		t.Fatalf("a foreign-channel home must get exactly one card, got %d: %+v", len(sent), sent)
	}
	if sent[0].channelID != "chan-2" {
		t.Fatalf("the card must land in the endpoint channel, got %q", sent[0].channelID)
	}
}

func TestPrSurfaceReusesPersistedHomeAfterRestart(t *testing.T) {
	for _, phase := range []Phase{PhaseQueued, PhaseBlocked, PhaseMerged} {
		t.Run(string(phase), func(t *testing.T) {
			srv, st, rec := newSurfaceServer(t)
			ep := srv.endpoints["/review"]
			ctx := context.Background()
			prKey := "acme/widget#181"
			if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "dispatch-parent"); err != nil {
				t.Fatalf("ensure pr: %v", err)
			}

			envelope := surfaceEnvelope()
			if phase == PhaseMerged {
				envelope = mergeSurfaceEnvelope()
			}
			srv.syncPhaseParent(ctx, ep, prKey, envelope, phase)

			sent, edited, _, _ := rec.snapshot()
			if len(sent) != 0 {
				t.Fatalf("a persisted home must not post another card, got %d: %+v", len(sent), sent)
			}
			if len(edited) != 1 || edited[0].messageID != "dispatch-parent" {
				t.Fatalf("the persisted home must be edited in place, got %+v", edited)
			}
			row, err := st.PRQueueRepo().Get(ctx, prKey)
			if err != nil {
				t.Fatalf("get pr: %v", err)
			}
			if row == nil || strings.TrimSpace(row.RootMsgID) != "dispatch-parent" {
				t.Fatalf("persisted root = %+v, want the dispatch parent", row)
			}
			detail, ok := srv.tasks.SurfaceDetail(prKey)
			if !ok || detail.ThreadID != "thread-9" {
				t.Fatalf("surface detail = %+v (known %v), want the persisted thread", detail, ok)
			}
		})
	}
}

func TestPrSurfaceKeepsPersistedRootWhenHomeIsForeign(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ep.ChannelID = "chan-2"
	ctx := context.Background()
	prKey := "acme/widget#181"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "dispatch-parent"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}

	rootID, _ := srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseQueued)
	srv.prMirrorSync(&WebhookWorkContext{PRKey: prKey, ThreadID: "thread-9", RootMessageID: rootID})

	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 1 || len(edited) != 0 {
		t.Fatalf("a foreign home must post one card and edit none, got %d sent and %d edited", len(sent), len(edited))
	}
	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil {
		t.Fatalf("get pr: %v", err)
	}
	if row == nil || strings.TrimSpace(row.RootMsgID) != "dispatch-parent" {
		t.Fatalf("persisted root = %+v, want the original home preserved", row)
	}
}
