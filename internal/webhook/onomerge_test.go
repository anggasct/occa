package webhook

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type mergedCall struct {
	prompt  string
	workCtx *WebhookWorkContext
}

func newMergedHarness(t *testing.T, ep config.EndpointConfig, promptDir string) (*Server, *store.SQLiteStore, *[]mergedCall, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var calls []mergedCall
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		defer mu.Unlock()
		cp := *workCtx
		calls = append(calls, mergedCall{prompt: prompt, workCtx: &cp})
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-m"
		}
		return nil
	}
	muxCfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{ep}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(muxCfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	if promptDir != "" {
		srv.SetPromptDir(promptDir)
	}
	return srv, st, &calls, &mu
}

func mergedBody(number int) string {
	return fmt.Sprintf(`{"action":"closed","pull_request":{"number":%d,"state":"merged","merged":true,`+
		`"user":{"login":"owner"},"title":"T",`+
		`"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},`+
		`"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`,
		number)
}

func seedProcessingDelivery(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID, eventType string) int64 {
	t.Helper()
	ctx := context.Background()
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint: endpoint, DeliveryID: deliveryID, EventType: eventType, PayloadHash: deliveryID,
	})
	if err != nil || !created {
		t.Fatalf("create receipt: %v", err)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, endpoint, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	ok, err := st.WebhookDeliveryRepo().Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
	if err != nil || !ok {
		t.Fatalf("claim receipt: %v %v", err, ok)
	}
	return receipt.ID
}

func runMergedDelivery(t *testing.T, srv *Server, st *store.SQLiteStore, ep config.EndpointConfig, deliveryID string, number int) {
	t.Helper()
	seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request")
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	item := dispatchItem{ep: ep, body: []byte(mergedBody(number)), deliveryID: deliveryID, eventType: "pull_request", payloadHash: deliveryID}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, srv.verdicts, false, "", ep.CommentTrigger)
	srv.handleMergedDelivery(item, receipt, envelope, nil)
}

func waitForMergedCalls(t *testing.T, mu *sync.Mutex, calls *[]mergedCall, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(*calls)
		mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("merged calls = %d, want %d", len(*calls), want)
}

func TestOnMergedDefaultNoExtraCalls(t *testing.T) {
	ep := prTestEndpoint("merged", "/merged", config.AutomationFull)
	srv, st, calls, mu := newMergedHarness(t, ep, "")
	runMergedDelivery(t, srv, st, ep, "del-m-1", 7)
	waitForMergedCalls(t, mu, calls, 1)
	mu.Lock()
	defer mu.Unlock()
	if len(*calls) != 1 {
		t.Fatalf("default on_merged must not add turns, calls = %d", len(*calls))
	}
}

func TestOnMergedPromptFileRendersContext(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "merged-note.md"), []byte("Update docs for {{.webhook.repository}}#{{.webhook.pr_number}}"), 0o644); err != nil {
		t.Fatal(err)
	}
	ep := prTestEndpoint("merged", "/merged", config.AutomationFull)
	ep.OnMerged = []config.MergedAction{{PromptFile: "merged-note.md"}}
	srv, st, calls, mu := newMergedHarness(t, ep, dir)
	runMergedDelivery(t, srv, st, ep, "del-m-2", 42)
	waitForMergedCalls(t, mu, calls, 2)
	mu.Lock()
	defer mu.Unlock()
	if len(*calls) != 2 {
		t.Fatalf("expected main turn + 1 merged prompt turn, got %d", len(*calls))
	}
	if !strings.Contains((*calls)[1].prompt, "acme/widget#42") {
		t.Fatalf("merged prompt missing rendered context: %q", (*calls)[1].prompt)
	}
	if !strings.Contains((*calls)[1].prompt, "<untrusted_payload>") {
		t.Fatalf("merged prompt missing untrusted wrapper: %q", (*calls)[1].prompt)
	}
}

func TestOnMergedMissingPromptSkips(t *testing.T) {
	ep := prTestEndpoint("merged", "/merged", config.AutomationFull)
	ep.OnMerged = []config.MergedAction{{PromptFile: "nope.md"}}
	srv, st, calls, mu := newMergedHarness(t, ep, t.TempDir())
	runMergedDelivery(t, srv, st, ep, "del-m-3", 7)
	waitForMergedCalls(t, mu, calls, 1)
	mu.Lock()
	defer mu.Unlock()
	if len(*calls) != 1 {
		t.Fatalf("missing prompt must skip the extra turn, calls = %d", len(*calls))
	}
	rec, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-m-3")
	if err != nil || rec == nil || rec.Status != store.WebhookStatusCompleted {
		t.Fatalf("merge must still complete: %+v %v", rec, err)
	}
}

func TestOnMergedRunCommand(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "merged-ok")
	fixEp := prTestEndpoint("fix", "/fix", config.AutomationFull)
	fixEp.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: dir, Mode: config.WorkspaceModeIsolated}
	fixEp.Repository = "github.com/acme/widget"
	mergeEp := prTestEndpoint("merged", "/merged", config.AutomationFull)
	mergeEp.OnMerged = []config.MergedAction{{Run: "touch " + sentinel}}
	var mu sync.Mutex
	var calls []mergedCall
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		defer mu.Unlock()
		cp := *workCtx
		calls = append(calls, mergedCall{prompt: prompt, workCtx: &cp})
		return nil
	}
	muxCfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{fixEp, mergeEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(muxCfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	runMergedDelivery(t, srv, st, mergeEp, "del-m-4", 7)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(sentinel); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merged run: command did not execute in checkout dir: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("run: action must not spawn agent turns, calls = %d", len(calls))
	}
	rec, err := st.WebhookDeliveryRepo().Get(context.Background(), mergeEp.Name, "del-m-4")
	if err != nil || rec == nil || rec.Status != store.WebhookStatusCompleted {
		t.Fatalf("merge must complete despite extra: %+v %v", rec, err)
	}
}
