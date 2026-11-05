package webhook

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type fakeOpenPRs struct {
	mu    sync.Mutex
	prs   map[string][]openPR
	fail  map[string]error
	calls map[string]int
}

func (f *fakeOpenPRs) list(_ context.Context, repo string) ([]openPR, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[repo]++
	if err := f.fail[repo]; err != nil {
		return nil, err
	}
	return f.prs[repo], nil
}

func unknownPR(number int, age time.Duration) openPR {
	pr := openPR{
		Number:      number,
		HeadRefName: "feat/x",
		HeadRefOid:  "abcdef0123456789abcdef0123456789abcdef01",
		URL:         "https://github.com/acme/widget/pull/7",
		Title:       "Add widget",
		CreatedAt:   time.Now().Add(-age),
	}
	pr.Author.Login = "owner"
	return pr
}

func discoveryReviewEndpoint(repo string) config.EndpointConfig {
	ep := prTestEndpoint("review", "/review-"+strings.ReplaceAll(repo, "/", "-"), config.AutomationFull)
	ep.Name = "github-" + strings.ReplaceAll(repo, "/", "-") + "-review"
	ep.Repository = "github.com/" + repo
	ep.CommentTrigger = []string{"please re-review"}
	ep.CommentAuthors = []string{"maintainer"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 6, Window: time.Hour}
	ep.Discovery = true
	return ep
}

type discoveryHarness struct {
	srv    *Server
	st     *store.SQLiteStore
	lister *fakeOpenPRs
	mu     sync.Mutex
	runs   []string
}

func newDiscoveryHarness(t *testing.T, endpoints ...config.EndpointConfig) *discoveryHarness {
	t.Helper()
	h := &discoveryHarness{lister: &fakeOpenPRs{prs: map[string][]openPR{}, fail: map[string]error{}}}
	srv, st, _ := newStageServer(t, func(_ *Server, _ int, workCtx *WebhookWorkContext) error {
		h.mu.Lock()
		h.runs = append(h.runs, workCtx.PRKey)
		h.mu.Unlock()
		return nil
	}, endpoints...)
	srv.openPRs = h.lister.list
	srv.retentionAge = 7 * 24 * time.Hour
	h.srv, h.st = srv, st
	return h
}

func (h *discoveryHarness) executed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.runs...)
}

func (h *discoveryHarness) discoveredReceipts(t *testing.T) []string {
	t.Helper()
	rows, err := h.st.DB().Query(`SELECT delivery_id FROM webhook_delivery WHERE delivery_id LIKE 'discover-%' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func TestDiscoveryQueuesOneReviewForAnUnknownPR(t *testing.T) {
	logs := captureServerLogs(t)
	ep := discoveryReviewEndpoint("acme/widget")
	h := newDiscoveryHarness(t, ep)
	h.lister.prs["acme/widget"] = []openPR{unknownPR(7, time.Hour)}
	ctx := context.Background()

	h.srv.reconcileOnce(ctx)
	waitForCondition(t, "discovered review completed", func() bool {
		ids := h.discoveredReceipts(t)
		if len(ids) != 1 {
			return false
		}
		d, err := h.st.WebhookDeliveryRepo().Get(ctx, ep.Name, ids[0])
		return err == nil && d != nil && d.Status == store.WebhookStatusCompleted
	})
	waitForPRQueueStatus(t, h.st, stagePR, store.PRStatusDrained)
	if got := h.executed(); len(got) != 1 || got[0] != stagePR {
		t.Fatalf("executor ran %v, want one review of %s", got, stagePR)
	}
	if item, err := h.st.StageRepo().GetWorkItem(ctx, stagePR); err != nil || item.State != store.WorkItemOpen {
		t.Fatalf("work item = %+v, %v", item, err)
	}
	runs := assertStageRuns(t, h.st, stagePR, []stageRunWant{{store.StageReview, 1, store.StageStateDone, ""}})
	if !strings.HasPrefix(runs[0].TriggeredBy, stageTriggerPrefix+ep.Name+"/"+discoverDeliveryPrefix) {
		t.Fatalf("run trigger = %q, want the discovery delivery", runs[0].TriggeredBy)
	}
	found := logs.all("webhook: discovered unknown PR")
	if len(found) != 1 || found[0].attrs["pr_key"] != stagePR || found[0].attrs["head"] != shortRevision(unknownPR(7, 0).HeadRefOid) {
		t.Fatalf("discovery logs = %+v", found)
	}

	h.srv.reconcileOnce(ctx)
	if got := h.discoveredReceipts(t); len(got) != 1 {
		t.Fatalf("second tick queued again: %v", got)
	}
	if got := len(logs.all("webhook: discovered unknown PR")); got != 1 {
		t.Fatalf("discovery logs after the second tick = %d, want 1", got)
	}
	if got := h.executed(); len(got) != 1 {
		t.Fatalf("executor ran %v after the second tick", got)
	}
}

func TestDiscoveryLeavesPRsItMustNotReview(t *testing.T) {
	cases := []struct {
		name  string
		pr    openPR
		setup func(t *testing.T, h *discoveryHarness)
	}{
		{
			name: "known work item",
			pr:   unknownPR(7, time.Hour),
			setup: func(t *testing.T, h *discoveryHarness) {
				if err := h.st.StageRepo().UpsertWorkItem(context.Background(), store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "known pr queue row",
			pr:   unknownPR(7, time.Hour),
			setup: func(t *testing.T, h *discoveryHarness) {
				if _, err := h.st.PRQueueRepo().EnsurePR(context.Background(), stagePR, "", ""); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "draft",
			pr: func() openPR {
				pr := unknownPR(7, time.Hour)
				pr.IsDraft = true
				return pr
			}(),
		},
		{name: "older than retention", pr: unknownPR(7, 8*24*time.Hour)},
		{
			name: "author the endpoint ignores",
			pr: func() openPR {
				pr := unknownPR(7, time.Hour)
				pr.Author.Login = "dependabot[bot]"
				return pr
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureServerLogs(t)
			ep := discoveryReviewEndpoint("acme/widget")
			ep.IgnoreAuthors = []string{"dependabot[bot]"}
			h := newDiscoveryHarness(t, ep)
			h.lister.prs["acme/widget"] = []openPR{tc.pr}
			if tc.setup != nil {
				tc.setup(t, h)
			}
			h.srv.reconcileOnce(context.Background())
			if got := h.discoveredReceipts(t); len(got) != 0 {
				t.Fatalf("discovery queued %v", got)
			}
			if got := logs.all("webhook: discovered unknown PR"); len(got) != 0 {
				t.Fatalf("discovery logs = %+v", got)
			}
			h.srv.dispatchMu.Lock()
			_, queued := h.srv.dispatchers["pr:"+stagePR]
			h.srv.dispatchMu.Unlock()
			if queued || len(h.executed()) != 0 {
				t.Fatalf("a PR discovery must leave was queued (dispatcher %v, runs %v)", queued, h.executed())
			}
		})
	}
}

func TestDiscoveryQueuesAtMostThreePerTickOldestFirst(t *testing.T) {
	logs := captureServerLogs(t)
	h := newDiscoveryHarness(t, discoveryReviewEndpoint("acme/widget"))
	var prs []openPR
	for n := 1; n <= 5; n++ {
		prs = append(prs, unknownPR(n, time.Duration(n)*time.Hour))
	}
	h.lister.prs["acme/widget"] = prs
	ctx := context.Background()

	discovered := func() []any {
		var keys []any
		for _, rec := range logs.all("webhook: discovered unknown PR") {
			keys = append(keys, rec.attrs["pr_key"])
		}
		return keys
	}
	h.srv.reconcileOnce(ctx)
	if got := discovered(); len(got) != 3 || got[0] != "acme/widget#5" || got[1] != "acme/widget#4" || got[2] != "acme/widget#3" {
		t.Fatalf("first tick discovered %v, want the three oldest", got)
	}
	h.srv.reconcileOnce(ctx)
	if got := discovered(); len(got) != 5 || got[3] != "acme/widget#2" || got[4] != "acme/widget#1" {
		t.Fatalf("second tick discovered %v, want the remaining two", got)
	}
	h.srv.reconcileOnce(ctx)
	if got := discovered(); len(got) != 5 {
		t.Fatalf("third tick discovered %v, want nothing new", got)
	}
}

func TestDiscoveryListFailureDoesNotStopOtherRepos(t *testing.T) {
	logs := captureServerLogs(t)
	widget := discoveryReviewEndpoint("acme/widget")
	gadget := discoveryReviewEndpoint("acme/gadget")
	h := newDiscoveryHarness(t, widget, gadget)
	h.lister.fail["acme/widget"] = errors.New("gh pr list: HTTP 502")
	h.lister.prs["acme/gadget"] = []openPR{unknownPR(3, time.Hour)}

	h.srv.reconcileOnce(context.Background())

	failed := logs.all("webhook: discovery list failed")
	if len(failed) != 1 || failed[0].attrs["repo"] != "acme/widget" {
		t.Fatalf("list failure logs = %+v, want one for acme/widget", failed)
	}
	found := logs.all("webhook: discovered unknown PR")
	if len(found) != 1 || found[0].attrs["pr_key"] != "acme/gadget#3" {
		t.Fatalf("discovery logs = %+v, want acme/gadget#3", found)
	}
	h.lister.mu.Lock()
	calls := h.lister.calls
	h.lister.mu.Unlock()
	if calls["acme/widget"] != 1 || calls["acme/gadget"] != 1 {
		t.Fatalf("list calls = %v, want one per repo", calls)
	}
}

func TestDiscoverySkipsRepoWithDiscoveryOff(t *testing.T) {
	widget := discoveryReviewEndpoint("acme/widget")
	widget.Discovery = false
	h := newDiscoveryHarness(t, widget)
	h.lister.prs["acme/widget"] = []openPR{unknownPR(7, time.Hour)}

	h.srv.reconcileOnce(context.Background())

	h.lister.mu.Lock()
	calls := h.lister.calls["acme/widget"]
	h.lister.mu.Unlock()
	if calls != 0 {
		t.Fatalf("list calls = %d, want none for a repo with discovery off", calls)
	}
	if ids := h.discoveredReceipts(t); len(ids) != 0 {
		t.Fatalf("discovered receipts = %v, want none", ids)
	}
}

func TestDiscoverDeliveryIsInternal(t *testing.T) {
	if !isInternalDelivery(discoverDeliveryPrefix + "1") {
		t.Fatal("a discovery delivery is not internal")
	}
}
