package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func TestFixKindMatrix(t *testing.T) {
	cases := []struct {
		name   string
		reason []string
		want   string
	}{
		{"review only", []string{"review"}, fixKindReview},
		{"ci only", []string{"ci"}, fixKindCI},
		{"conflict only", []string{"conflict"}, fixKindConflict},
		{"mixed", []string{"ci", "review"}, fixKindMixed},
		{"unknown becomes mixed", []string{"bogus"}, fixKindMixed},
		{"empty becomes mixed", nil, fixKindMixed},
		{"duplicates collapse", []string{"ci", "ci"}, fixKindCI},
		{"order independent", []string{"conflict", "review", "ci"}, fixKindMixed},
	}
	for _, tc := range cases {
		if got := fixKindOf(tc.reason); got != tc.want {
			t.Errorf("%s: fixKindOf(%v) = %q, want %q", tc.name, tc.reason, got, tc.want)
		}
	}
}

func TestClassifyCommentTrigger(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		want  []string
		empty bool
	}{
		{"ci trigger", "please fix ci", []string{"ci"}, false},
		{"review trigger", "please fix review", []string{"review"}, false},
		{"unknown trigger", "please do the thing", []string{"mixed"}, false},
		{"empty", "  ", nil, true},
	}
	for _, tc := range cases {
		got := classifyCommentTrigger(tc.text)
		if tc.empty {
			if len(got) != 0 {
				t.Errorf("%s: got %v, want empty", tc.name, got)
			}
			continue
		}
		if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsRedSuiteConclusion(t *testing.T) {
	for _, c := range []string{"failure", "timed_out", "action_required", "FAILURE", " Timed_Out "} {
		if !isRedSuiteConclusion(c) {
			t.Errorf("conclusion %q should be red", c)
		}
	}
	for _, c := range []string{"success", "neutral", "cancelled", "stale", "", "queued"} {
		if isRedSuiteConclusion(c) {
			t.Errorf("conclusion %q should not be red", c)
		}
	}
}

func TestMergeFixReasons(t *testing.T) {
	got := mergeFixReasons([]string{"ci"}, []string{"review", "ci", "bogus"})
	if len(got) != 2 || got[0] != "ci" || got[1] != "review" {
		t.Fatalf("merge = %v, want [ci review]", got)
	}
	if got := mergeFixReasons(nil, nil); len(got) != 0 {
		t.Fatalf("merge empty = %v, want empty", got)
	}
}

func TestPickAutoFixTrigger(t *testing.T) {
	endpoints := map[string]config.EndpointConfig{
		"fix": {Repository: "github.com/acme/widget", Workflow: "fix", CommentTrigger: []string{"please fix review", "please fix ci"}},
	}
	if got := pickAutoFixTrigger(endpoints, "acme/widget"); got != "please fix ci" {
		t.Fatalf("trigger = %q, want ci trigger", got)
	}
	reordered := map[string]config.EndpointConfig{
		"fix": {Repository: "github.com/acme/widget", Workflow: "fix", CommentTrigger: []string{"please fix ci", "please fix review"}},
	}
	if got := pickAutoFixTrigger(reordered, "acme/widget"); got != "please fix ci" {
		t.Fatalf("trigger = %q, want ci trigger", got)
	}
	plain := map[string]config.EndpointConfig{
		"fix": {Repository: "github.com/acme/widget", Workflow: "fix", CommentTrigger: []string{"do it"}},
	}
	if got := pickAutoFixTrigger(plain, "acme/widget"); got != "do it" {
		t.Fatalf("trigger = %q, want fallback", got)
	}
	if got := pickAutoFixTrigger(map[string]config.EndpointConfig{}, "acme/widget"); got != "" {
		t.Fatalf("trigger = %q, want empty", got)
	}
}

func TestAutoFixEnqueueAllowed(t *testing.T) {
	now := time.Unix(1000, 0)
	if _, _, ok := autoFixEnqueueAllowed(0, 0, now); !ok {
		t.Fatal("fresh state should allow")
	}
	if _, _, ok := autoFixEnqueueAllowed(2, 990, now); !ok {
		t.Fatal("under cap should allow")
	}
	if c, _, ok := autoFixEnqueueAllowed(3, 990, now); ok || c != 3 {
		t.Fatal("at cap should block without reset")
	}
	dayAgo := now.Add(-autoFixStormWindow - time.Minute).Unix()
	if c, w, ok := autoFixEnqueueAllowed(3, dayAgo, now); !ok || c != 0 || w != now.Unix() {
		t.Fatalf("expired window should reset and allow, got count=%d start=%d allowed=%v", c, w, ok)
	}
}

func TestFixReasonsFromEnvelope(t *testing.T) {
	cases := []struct {
		name     string
		envelope WebhookEnvelope
		want     []string
	}{
		{
			"verdict review",
			WebhookEnvelope{"review_verdict": "REQUEST_CHANGES"},
			[]string{"review"},
		},
		{
			"ci trigger",
			WebhookEnvelope{"comment_trigger": "please fix ci"},
			[]string{"ci"},
		},
		{
			"marker conflict",
			WebhookEnvelope{"comment_body": "note <!-- occa:conflict:abc --> done"},
			[]string{"conflict"},
		},
		{
			"red suite",
			WebhookEnvelope{"event_type": "check_suite", "conclusion": "failure"},
			[]string{"ci"},
		},
		{
			"green suite",
			WebhookEnvelope{"event_type": "check_suite", "conclusion": "success"},
			nil,
		},
		{
			"mixed verdict and ci",
			WebhookEnvelope{"review_verdict": "request_changes", "event_type": "check_suite", "conclusion": "timed_out"},
			[]string{"ci", "review"},
		},
	}
	for _, tc := range cases {
		got := fixReasonsFromEnvelope(tc.envelope)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}

func autoFixTestEndpoints(mode string, autoFix config.RepoAutoFix, triggers []string) []config.EndpointConfig {
	mk := func(workflow string) config.EndpointConfig {
		return config.EndpointConfig{
			Name:           "github-widget-" + workflow,
			Path:           "/webhooks/github-widget-" + workflow,
			Secret:         "s3cret",
			Platform:       "telegram",
			ChannelID:      "chat1",
			Prompt:         "do it",
			Workflow:       workflow,
			Thread:         true,
			Repository:     "github.com/acme/widget",
			Admit:          defaultCatchAllAdmit(),
			Automation:     &config.RepoAutomation{Default: mode},
			CommentTrigger: triggers,
			AutoFix:        autoFix,
			Limits:         &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
		}
	}
	return []config.EndpointConfig{mk("review"), mk("fix"), mk("merge"), mk("merged")}
}

func redSuiteEnvelope() WebhookEnvelope {
	return WebhookEnvelope{
		"event_type":  "check_suite",
		"action":      "completed",
		"conclusion":  "failure",
		"repository":  "acme/widget",
		"pr_number":   "7",
		"pr_state":    "open",
		"head_sha":    "abc1234",
		"pr_author":   "owner",
		"head_branch": "feat/x",
	}
}

func TestMaybeAutoFixCIEnqueues(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix review", "please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], redSuiteEnvelope(), WebhookExecutionKey{}, "acme/widget#7")
	row, err := srv.prQueues.Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil {
		t.Fatalf("Get: %v %+v", err, row)
	}
	if row.AutoFixCount != 1 || row.AutoFixSHA != "abc1234" {
		t.Fatalf("storm state wrong: %+v", row)
	}
}

func TestMaybeAutoFixCISkips(t *testing.T) {
	build := func(t *testing.T, mode string, autoFix config.RepoAutoFix, mutate func(WebhookEnvelope) WebhookEnvelope) (*Server, WebhookEnvelope) {
		t.Helper()
		rec := newPRRecorder("thread-pr", nil)
		srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(mode, autoFix, []string{"please fix review", "please fix ci"}))
		srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
			return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
		})
		if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
			t.Fatalf("EnsurePR: %v", err)
		}
		return srv, mutate(redSuiteEnvelope())
	}
	cases := []struct {
		name    string
		mode    string
		autoFix config.RepoAutoFix
		mutate  func(WebhookEnvelope) WebhookEnvelope
	}{
		{"flag off", config.AutomationFull, config.RepoAutoFix{}, func(e WebhookEnvelope) WebhookEnvelope { return e }},
		{"green suite", config.AutomationFull, config.RepoAutoFix{CI: true}, func(e WebhookEnvelope) WebhookEnvelope {
			e["conclusion"] = "success"
			return e
		}},
		{"review-only author", config.AutomationReviewOnly, config.RepoAutoFix{CI: true}, func(e WebhookEnvelope) WebhookEnvelope { return e }},
		{"closed pr", config.AutomationFull, config.RepoAutoFix{CI: true}, func(e WebhookEnvelope) WebhookEnvelope {
			e["pr_state"] = "closed"
			return e
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, env := build(t, tc.mode, tc.autoFix, tc.mutate)
			srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
			items, err := srv.prQueues.ListItems(context.Background(), "acme/widget#7")
			if err != nil || len(items) != 0 {
				t.Fatalf("ListItems: %v (n=%d, want 0)", err, len(items))
			}
		})
	}
}

func TestMaybeAutoFixCIDedupesSameSHA(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	env := redSuiteEnvelope()
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
	row, err := srv.prQueues.Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil {
		t.Fatalf("Get: %v %+v", err, row)
	}
	if row.AutoFixCount != 1 {
		t.Fatalf("storm count = %d, want 1", row.AutoFixCount)
	}
}

func TestMaybeAutoFixCIStormBlocks(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	var mentioned []string
	srv.notifier = func(ctx context.Context, platform, target, text string) error {
		mentioned = append(mentioned, text)
		return nil
	}
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := srv.prQueues.SetAutoFixState(context.Background(), "acme/widget#7", 3, time.Now().Unix(), "oldsha"); err != nil {
		t.Fatalf("SetAutoFixState: %v", err)
	}
	env := redSuiteEnvelope()
	env["head_sha"] = "newsha"
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
	row, _ := srv.prQueues.Get(context.Background(), "acme/widget#7")
	if row.Status != "blocked" {
		t.Fatalf("status = %q, want blocked", row.Status)
	}
	if len(mentioned) == 0 {
		t.Fatal("expected operator mention on storm block")
	}
	items, _ := srv.prQueues.ListItems(context.Background(), "acme/widget#7")
	if len(items) != 0 {
		t.Fatalf("no item should be enqueued on storm block, got %d", len(items))
	}
}

func TestRevalidateFixItem(t *testing.T) {
	fixEp := func(autoFix config.RepoAutoFix) config.EndpointConfig {
		return config.EndpointConfig{
			Name:       "github-widget-fix",
			Workflow:   "fix",
			Repository: "github.com/acme/widget",
			AutoFix:    autoFix,
			Automation: &config.RepoAutomation{Default: config.AutomationFull},
		}
	}
	mergeState := prLiveState{mergeable: true}
	newSrv := func() *Server {
		s := &Server{}
		s.SetMergeableChecker(func(ctx context.Context, repo, number string) (prLiveState, error) {
			return mergeState, nil
		})
		return s
	}
	baseEnv := func() WebhookEnvelope {
		return WebhookEnvelope{
			"event_type":      "issue_comment",
			"comment_trigger": "please fix ci",
			"pr_author":       "owner",
			"repository":      "acme/widget",
		}
	}
	t.Run("draft skips auto", func(t *testing.T) {
		mergeState = prLiveState{mergeable: true, draft: true}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{CI: true}), fixKind: "ci", fixReasons: []string{"ci"}}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); !skip {
			t.Fatal("expected skip on draft for auto item")
		}
	})
	t.Run("draft allows manual", func(t *testing.T) {
		mergeState = prLiveState{mergeable: true, draft: true}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{CI: true})}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); skip {
			t.Fatal("manual item should proceed on draft")
		}
	})
	t.Run("conflict added when flagged", func(t *testing.T) {
		mergeState = prLiveState{mergeable: false, dirty: true}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{Conflict: true}), fixKind: "ci", fixReasons: []string{"ci"}}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); skip {
			t.Fatal("should proceed with conflict added")
		}
		if item.fixKind != "mixed" {
			t.Fatalf("kind = %q, want mixed", item.fixKind)
		}
	})
	t.Run("conflict ignored when flag off", func(t *testing.T) {
		mergeState = prLiveState{mergeable: false, dirty: true}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{}), fixKind: "ci", fixReasons: []string{"ci"}}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); skip {
			t.Fatal("should proceed without conflict reason")
		}
		if item.fixKind != "ci" {
			t.Fatalf("kind = %q, want ci", item.fixKind)
		}
	})
	t.Run("stale green CI skips auto", func(t *testing.T) {
		mergeState = prLiveState{mergeable: true, checksDone: true, checksRed: false}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{CI: true}), fixKind: "ci", fixReasons: []string{"ci"}}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); !skip {
			t.Fatal("expected skip when CI no longer red")
		}
	})
	t.Run("manual fix without conflict flag skips the live lookup", func(t *testing.T) {
		calls := 0
		srv := &Server{}
		srv.SetMergeableChecker(func(ctx context.Context, repo, number string) (prLiveState, error) {
			calls++
			return prLiveState{}, nil
		})
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{CI: true})}
		if skip, _ := srv.revalidateFixItem(item, baseEnv()); skip {
			t.Fatal("manual item must proceed")
		}
		if calls != 0 {
			t.Fatalf("live lookups = %d, want 0", calls)
		}
	})
	t.Run("red CI proceeds", func(t *testing.T) {
		mergeState = prLiveState{mergeable: true, checksDone: true, checksRed: true}
		item := &dispatchItem{prKey: "acme/widget#7", ep: fixEp(config.RepoAutoFix{CI: true}), fixKind: "ci", fixReasons: []string{"ci"}}
		if skip, _ := newSrv().revalidateFixItem(item, baseEnv()); skip {
			t.Fatal("should proceed on red CI")
		}
	})
}

func TestEnqueueAutoFixCoalesces(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	id, err := srv.prQueues.EnqueueFix(context.Background(), "acme/widget#7", "github-widget-fix", "issue_comment", 11, 3, "ci", `["ci"]`)
	if err != nil || id == 0 {
		t.Fatalf("EnqueueFix: %v %d", err, id)
	}
	fixEp, _ := srv.fixEndpointForRepo("acme/widget")
	merged, err := srv.enqueueAutoFix(context.Background(), fixEp, []byte(`{}`), "issue_comment", "acme/widget#7", []string{"review"})
	if err != nil || merged {
		t.Fatalf("coalesce = %v, %v; want merged without insert", merged, err)
	}
	items, err := srv.prQueues.ListItems(context.Background(), "acme/widget#7")
	if err != nil || len(items) != 1 {
		t.Fatalf("ListItems: %v (n=%d, want 1)", err, len(items))
	}
	if items[0].FixKind != "mixed" || items[0].FixReasons != `["ci","review"]` {
		t.Fatalf("merged item wrong: %+v", items[0])
	}
	if d := srv.dispatchers["pr:acme/widget#7"]; d != nil && d.queueLen() != 0 {
		t.Fatalf("coalesce must not push a lane item, queueLen=%d", d.queueLen())
	}
}

func syntheticFixBody(trigger string) []byte {
	return []byte(`{"action":"created",` +
		`"repository":{"full_name":"acme/widget"},` +
		`"issue":{"number":7,"state":"open","title":"Fix it","pull_request":{"html_url":"https://github.com/acme/widget/pull/7"}},` +
		`"comment":{"body":"` + trigger + `","user":{"login":"someone"}},` +
		`"branch":"feat/x","head_revision":"abc1234","head_repository":"acme/widget"}`)
}

func TestAutoFixExecutesWithKind(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	endpoints := autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"})
	for i, ep := range endpoints {
		if ep.Workflow == "fix" {
			ep.Prompt = "kind={{.webhook.fix_kind}} reasons={{.webhook.fix_reasons}}"
			endpoints[i] = ep
		}
	}
	srv, _, _ := newPRTestServer(t, rec, endpoints)
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return syntheticFixBody("please fix ci"), "issue_comment", "owner", nil
	})
	srv.SetMergeableChecker(func(ctx context.Context, repo, number string) (prLiveState, error) {
		return prLiveState{mergeable: true, checksDone: true, checksRed: true, headSHA: "abc1234"}, nil
	})
	var mu sync.Mutex
	var prompts []string
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		prompts = append(prompts, prompt)
		mu.Unlock()
		return rec.exec(ctx, platform, channelID, prompt, workCtx)
	}
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	env := redSuiteEnvelope()
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
	waitForCalls(t, rec, 1)
	mu.Lock()
	defer mu.Unlock()
	if len(prompts) != 1 {
		t.Fatalf("prompts = %d, want 1", len(prompts))
	}
	if !strings.Contains(prompts[0], "kind=ci") {
		t.Fatalf("prompt missing fix kind: %q", prompts[0])
	}
}

func TestExecuteDeliveryWithFixDerivesKind(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	eps := autoFixTestEndpoints(config.AutomationFull, config.RepoAutoFix{}, []string{"please fix review"})
	for i, ep := range eps {
		if ep.Workflow == "fix" {
			ep.Prompt = "kind={{.webhook.fix_kind}}"
			eps[i] = ep
		}
	}
	srv, st, _ := newPRTestServer(t, rec, eps)
	var prompts []string
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		prompts = append(prompts, prompt)
		return nil
	}
	id := seedProcessingDelivery(t, st, "github-widget-fix", "del-fix-1", "issue_comment")
	body := []byte(`{"action":"created",` +
		`"repository":{"full_name":"acme/widget"},` +
		`"issue":{"number":7,"state":"open","title":"Fix it","pull_request":{"html_url":"https://github.com/acme/widget/pull/7"}},` +
		`"comment":{"body":"please fix review","user":{"login":"someone"}},` +
		`"branch":"feat/x","head_revision":"abc1234","head_repository":"acme/widget"}`)
	ep := srv.endpoints["/webhooks/github-widget-fix"]
	if err := srv.executeDeliveryWithFix(ep, body, id, "del-fix-1", "issue_comment", 1, nil, nil, nil, "", ""); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(prompts) != 1 || !strings.Contains(prompts[0], "kind=review") {
		t.Fatalf("prompts = %v, want kind=review", prompts)
	}
	if err := srv.executeDeliveryWithFix(ep, body, id, "del-fix-1", "issue_comment", 1, nil, nil, nil, "mixed", `["ci","review"]`); err != nil {
		t.Fatalf("execute mixed: %v", err)
	}
}

func fixKindEnvelope(kind string) WebhookEnvelope {
	return WebhookEnvelope{
		"event_type":       "issue_comment",
		"delivery_id":      "del-1",
		"repository":       "acme/widget",
		"pr_number":        "7",
		"pr_url":           "https://github.com/acme/widget/pull/7",
		"title":            "t",
		"head_branch":      "feat/x",
		"base_branch":      "main",
		"review_verdict":   "request_changes",
		"comment_trigger":  "please fix ci",
		"comment_body":     "please fix ci",
		"task_id":          "",
		"task_brief":       "",
		"task_spec":        "",
		"task_spec_is_url": false,
		"task_scope":       "",
		"task_verify":      "",
		"fix_kind":         kind,
		"fix_reasons":      `["` + kind + `"]`,
	}
}

func TestBuiltinFixRendersPerKind(t *testing.T) {
	cases := []struct {
		kind    string
		keyword string
	}{
		{"review", "triggering comment"},
		{"ci", "failing checks"},
		{"conflict", "merge origin/main into this branch (never rebase)"},
		{"mixed", "failing checks"},
		{"", "requested fix"},
	}
	for _, tc := range cases {
		rendered, err := renderTemplate(config.BuiltinPrompt("fix"), map[string]any{"webhook": fixKindEnvelope(tc.kind)})
		if err != nil {
			t.Fatalf("kind %q: %v", tc.kind, err)
		}
		if !strings.Contains(strings.ToLower(rendered), tc.keyword) {
			t.Fatalf("kind %q missing %q in %q", tc.kind, tc.keyword, rendered)
		}
		if !strings.Contains(rendered, "Never post trigger phrases") {
			t.Fatalf("kind %q missing guardrail", tc.kind)
		}
		if !strings.Contains(rendered, "Never push: occa commits and pushes your changes.") {
			t.Fatalf("kind %q missing the no-push guardrail", tc.kind)
		}
		if strings.Contains(rendered, "force-with-lease") {
			t.Fatalf("kind %q must not ask for a force push", tc.kind)
		}
		if !strings.Contains(rendered, "Never merge") {
			t.Fatalf("kind %q missing no-merge guardrail", tc.kind)
		}
	}
}

func TestMaybeAutoFixCIRespectsBlocked(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	var mentioned []string
	srv.notifier = func(ctx context.Context, platform, target, text string) error {
		mentioned = append(mentioned, text)
		return nil
	}
	if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := srv.prQueues.SetBlocked(context.Background(), "acme/widget#7", "manual hold"); err != nil {
		t.Fatalf("SetBlocked: %v", err)
	}
	env := redSuiteEnvelope()
	env["head_sha"] = "newsha"
	srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
	row, _ := srv.prQueues.Get(context.Background(), "acme/widget#7")
	if row.Status != "blocked" {
		t.Fatalf("status = %q, want blocked (persisted)", row.Status)
	}
	items, _ := srv.prQueues.ListItems(context.Background(), "acme/widget#7")
	if len(items) != 0 {
		t.Fatalf("blocked queue must not gain items, got %d", len(items))
	}
	if len(mentioned) != 0 {
		t.Fatalf("blocked row must not re-mention, got %v", mentioned)
	}
}

// A red suite on a PR with no approval still starts the auto-fix even though
// the merge pre-gate (IMP-103) skips the merge turn: the fix needs no
// approval, the merge does.
func TestAutoFixCIFiresWhenMergePregateSkips(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, url := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return ReconcileFacts{State: "OPEN", HeadSHA: "abc1234", ApprovalCandidate: false}, nil
	})
	srv.SetMergeableChecker(func(context.Context, string, string) (prLiveState, error) {
		return prLiveState{}, nil
	})
	if _, err := st.PRQueueRepo().EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	body := `{"action":"completed","check_suite":{"status":"completed","conclusion":"failure","head_sha":"abc1234","head_branch":"feat/x","app":{"name":"GitHub Actions"},"pull_requests":[{"number":7,"state":"open"}]},"repository":{"full_name":"acme/widget"}}`
	if resp := post(t, url+"/webhooks/github-widget-merge?secret=s3cret", "del-suite-red", "check_suite", body); resp.StatusCode != 200 {
		t.Fatalf("post status = %d", resp.StatusCode)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if row, _ := st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); row != nil && row.AutoFixCount == 1 {
			if row.AutoFixSHA != "abc1234" {
				t.Fatalf("auto-fix sha = %q", row.AutoFixSHA)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	row, _ := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	t.Fatalf("auto-fix never enqueued: %+v", row)
}

// AC-04: the auto path treats max-1 as full so one slot stays free for a
// manual /retry; a dropped auto-fix writes no queue row.
func TestAutoFixKeepsOneSlotForManualRetry(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{"action":"created"}`), "issue_comment", "owner", nil
	})
	srv.maxQueuedPerKey = 2
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	// A dispatcher that is not consuming holds one queued review item.
	d := newDispatcher(srv, "pr:acme/widget#7")
	srv.dispatchMu.Lock()
	srv.dispatchers["pr:acme/widget#7"] = d
	srv.dispatchMu.Unlock()
	d.push(dispatchItem{ep: srv.endpoints["/webhooks/github-widget-review"], prKey: "acme/widget#7", lane: laneOfWorkflow("review")})

	err := srv.enqueueAutoFixCI(ctx, "acme/widget", "7", "abc1234", []string{fixKindCI})
	if err == nil || !strings.Contains(err.Error(), "pr queue full") {
		t.Fatalf("enqueueAutoFixCI err = %v, want pr queue full", err)
	}
	items, _ := st.PRQueueRepo().ListItems(ctx, "acme/widget#7")
	if len(items) != 0 {
		t.Fatalf("queue rows = %d, want 0 for a dropped auto-fix", len(items))
	}
	if d.queueLen() >= srv.maxQueuedPerKey {
		t.Fatalf("queue len %d leaves no slot for a manual retry (max %d)", d.queueLen(), srv.maxQueuedPerKey)
	}
}

func rollupPayload(rollup string) []byte {
	return []byte(`{"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","isDraft":false,"headRefOid":"abc1234","statusCheckRollup":[` + rollup + `]}`)
}

// F-002: gh emits "status" (not "state") for CheckRun rollup entries; live
// in-flight checks must decode as not-done and completed failures as red.
func TestDecodePrLiveStateRollup(t *testing.T) {
	t.Run("in progress check run is not done", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"CheckRun","name":"check","status":"IN_PROGRESS","conclusion":""}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if st.checksDone {
			t.Fatal("checksDone = true, want false for IN_PROGRESS entry")
		}
		if st.checksRed {
			t.Fatal("checksRed = true, want false for IN_PROGRESS entry")
		}
	})
	t.Run("queued check run is not done", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"CheckRun","name":"check","status":"QUEUED","conclusion":""}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if st.checksDone {
			t.Fatal("checksDone = true, want false for QUEUED entry")
		}
	})
	t.Run("requested check run is not done", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"CheckRun","name":"check","status":"REQUESTED","conclusion":""}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if st.checksDone {
			t.Fatal("checksDone = true, want false for REQUESTED entry")
		}
		if st.checksRed {
			t.Fatal("checksRed = true, want false for REQUESTED entry")
		}
	})
	t.Run("completed failure check run is red", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"CheckRun","name":"check","status":"COMPLETED","conclusion":"FAILURE"}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !st.checksDone || !st.checksRed {
			t.Fatalf("done=%v red=%v, want done and red for COMPLETED/FAILURE", st.checksDone, st.checksRed)
		}
	})
	t.Run("pending status context is not done", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"StatusContext","context":"ci","state":"PENDING"}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if st.checksDone {
			t.Fatal("checksDone = true, want false for PENDING status context")
		}
	})
	t.Run("success status context is done and green", func(t *testing.T) {
		st, err := decodePrLiveState(rollupPayload(`{"__typename":"StatusContext","context":"ci","state":"SUCCESS"}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !st.checksDone || st.checksRed {
			t.Fatalf("done=%v red=%v, want done and green", st.checksDone, st.checksRed)
		}
	})
}

// F-004: store-local gates (dedupe, storm window) run before any live lookup,
// so duplicates and capped PRs cost no gh call.
func TestAutoFixCIGatesRunBeforeLiveLookup(t *testing.T) {
	build := func(t *testing.T) (*Server, *int) {
		t.Helper()
		rec := newPRRecorder("thread-pr", nil)
		srv, _, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
			config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
		calls := 0
		srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
			calls++
			return []byte(`{}`), "issue_comment", "owner", nil
		})
		if _, err := srv.prQueues.EnsurePR(context.Background(), "acme/widget#7", "thread-1", "root-1"); err != nil {
			t.Fatalf("EnsurePR: %v", err)
		}
		return srv, &calls
	}
	t.Run("duplicate head sha skips live lookup", func(t *testing.T) {
		srv, calls := build(t)
		env := redSuiteEnvelope()
		srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
		srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
		if *calls != 1 {
			t.Fatalf("live lookups = %d, want 1 (duplicate must not re-lookup)", *calls)
		}
	})
	t.Run("storm capped skips live lookup", func(t *testing.T) {
		srv, calls := build(t)
		if err := srv.prQueues.SetAutoFixState(context.Background(), "acme/widget#7", 3, time.Now().Unix(), "oldsha"); err != nil {
			t.Fatalf("SetAutoFixState: %v", err)
		}
		env := redSuiteEnvelope()
		env["head_sha"] = "newsha"
		srv.maybeAutoFixCI(context.Background(), srv.endpoints["/webhooks/github-widget-merge"], env, WebhookExecutionKey{}, "acme/widget#7")
		if *calls != 0 {
			t.Fatalf("live lookups = %d, want 0 for capped PR", *calls)
		}
	})
}

// F-006: the overflow drop emits one WARN with queue and storm counters and
// never notifies chat.
func TestAutoFixOverflowDropsWithWarn(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, _ := newPRTestServer(t, rec, autoFixTestEndpoints(config.AutomationFull,
		config.RepoAutoFix{CI: true}, []string{"please fix ci"}))
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return []byte(`{}`), "issue_comment", "owner", nil
	})
	var mentioned []string
	srv.notifier = func(ctx context.Context, platform, target, text string) error {
		mentioned = append(mentioned, text)
		return nil
	}
	srv.maxQueuedPerKey = 2
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	d := newDispatcher(srv, "pr:acme/widget#7")
	srv.dispatchMu.Lock()
	srv.dispatchers["pr:acme/widget#7"] = d
	srv.dispatchMu.Unlock()
	d.push(dispatchItem{ep: srv.endpoints["/webhooks/github-widget-review"], prKey: "acme/widget#7", lane: laneOfWorkflow("review")})

	if err := srv.enqueueAutoFixCI(ctx, "acme/widget", "7", "abc1234", []string{fixKindCI}); err == nil || !strings.Contains(err.Error(), "pr queue full") {
		t.Fatalf("enqueueAutoFixCI err = %v, want pr queue full", err)
	}
	logged := buf.String()
	if !strings.Contains(logged, "level=WARN") || !strings.Contains(logged, "auto-fix dropped") {
		t.Fatalf("log = %q, want overflow WARN", logged)
	}
	for _, want := range []string{"pr_key=acme/widget#7", "queue_len=1", "auto_fix_count=0"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log = %q, want %s", logged, want)
		}
	}
	if len(mentioned) != 0 {
		t.Fatalf("overflow must not notify chat, got %v", mentioned)
	}
}

// F-003: a red suite coalescing into an already-queued review fix must reach
// the executed prompt (dequeue re-reads the store row).
func TestCoalescedAutoFixReachesQueuedRun(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	eps := autoFixTestEndpoints(config.AutomationFull, config.RepoAutoFix{CI: true}, []string{"please fix review", "please fix ci"})
	for i, ep := range eps {
		if ep.Workflow == "fix" {
			ep.Prompt = "kind={{.webhook.fix_kind}} reasons={{.webhook.fix_reasons}}"
			eps[i] = ep
		}
	}
	srv, st, _ := newPRTestServer(t, rec, eps)
	srv.SetAutoFixBodyBuilder(func(ctx context.Context, repo, number string) ([]byte, string, string, error) {
		return syntheticFixBody("please fix ci"), "issue_comment", "owner", nil
	})
	srv.SetMergeableChecker(func(ctx context.Context, repo, number string) (prLiveState, error) {
		return prLiveState{mergeable: true, checksDone: true, checksRed: true}, nil
	})
	var mu sync.Mutex
	var prompts []string
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		prompts = append(prompts, prompt)
		mu.Unlock()
		return rec.exec(ctx, platform, channelID, prompt, workCtx)
	}
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	fixEp, _ := srv.fixEndpointForRepo("acme/widget")
	// A pending manual review fix holds the fix lane; its dispatcher is not
	// running yet, so the queue keeps the item.
	d := newDispatcher(srv, "pr:acme/widget#7")
	srv.dispatchMu.Lock()
	srv.dispatchers["pr:acme/widget#7"] = d
	srv.dispatchMu.Unlock()
	lane := laneOfWorkflow("fix")
	id, err := st.PRQueueRepo().Enqueue(ctx, "acme/widget#7", fixEp.Name, "issue_comment", 0, lanePriority(lane))
	if err != nil || id == 0 {
		t.Fatalf("Enqueue: %v %d", err, id)
	}
	d.push(dispatchItem{
		ep:         fixEp,
		body:       syntheticFixBody("please fix review"),
		deliveryID: "op-manual-fix-1",
		eventType:  "issue_comment",
		prKey:      "acme/widget#7",
		lane:       lane,
		queueID:    id,
	})
	// The red suite coalesces into the pending review fix.
	if err := srv.enqueueAutoFixCI(ctx, "acme/widget", "7", "abc1234", []string{fixKindCI}); err != nil {
		t.Fatalf("enqueueAutoFixCI: %v", err)
	}
	items, err := st.PRQueueRepo().ListItems(ctx, "acme/widget#7")
	if err != nil || len(items) != 1 || items[0].FixReasons != `["ci"]` {
		t.Fatalf("coalesced row wrong: %v (n=%d)", err, len(items))
	}
	go d.run(srv.shutdownCtx)
	waitForCalls(t, rec, 1)
	mu.Lock()
	defer mu.Unlock()
	if len(prompts) != 1 {
		t.Fatalf("prompts = %d, want 1", len(prompts))
	}
	if !strings.Contains(prompts[0], "kind=mixed") || !strings.Contains(prompts[0], `["ci","review"]`) {
		t.Fatalf("prompt = %q, want mixed kind with ci+review reasons", prompts[0])
	}
}
