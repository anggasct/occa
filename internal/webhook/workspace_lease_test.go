package webhook

import (
	"context"
	"os/exec"
	"sync/atomic"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestRefusedUnboundRunReleasesWorkspaceLease(t *testing.T) {
	root := t.TempDir()
	initTestGitRepoWithRemote(t, root, "https://github.com/acme/widget.git")
	branch := exec.Command("git", "branch", "feat/unbound")
	branch.Dir = root
	if out, err := branch.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}

	manager := NewWorkspaceManager()
	req := WorkspaceRequest{
		Repository: "github.com/acme/widget",
		Path:       root,
		Mode:       config.WorkspaceModeMutable,
		Key:        WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/unbound", HeadRevision: headRevision(t, root)},
		DeliveryID: "del-unbound-lease",
	}
	lease, err := manager.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}

	var calls int64
	execFn := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	srv, st := newThreadBindingServer(t, execFn)
	srv.SetWorkspaceResolver(manager)
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)

	ctx := context.Background()
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint:    ep.Name,
		DeliveryID:  "del-unbound-lease",
		EventType:   "pull_request_review",
		PayloadHash: "h",
	})
	if err != nil || !created {
		t.Fatalf("create receipt: %v %v", created, err)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, ep.Name, "del-unbound-lease")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	ok, err := st.WebhookDeliveryRepo().Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
	if err != nil || !ok {
		t.Fatalf("transition to processing: %v %v", ok, err)
	}

	body := []byte(prReviewBody(7, "feat/unbound", "owner", "changes_requested", "abc1234def5678"))
	if err := srv.executeDelivery(ep, body, int64(receipt.ID), "del-unbound-lease", "pull_request_review", 1, lease, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("refused run executed %d times", got)
	}
	if run := srv.activeRunFor("acme/widget#7", ep.Name, "del-unbound-lease"); run != nil {
		t.Fatal("refused run left an active-run entry")
	}

	released, err := manager.ResolveWorkspace(ctx, req)
	if err != nil {
		t.Fatalf("workspace lease not released: %v", err)
	}
	if err := released.Release(ctx); err != nil {
		t.Fatalf("release re-resolved lease: %v", err)
	}
}
