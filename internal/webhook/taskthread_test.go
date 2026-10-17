package webhook

import (
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func TestStatusForWorkflow(t *testing.T) {
	cases := map[string]TaskStatus{
		"review": TaskClaimed,
		"fix":    TaskPROpened,
		"merge":  TaskPROpened,
		"merged": TaskMerged,
		"custom": TaskEvent,
		"":       TaskDispatched,
		"Review": TaskClaimed,
		"nope":   TaskDispatched,
		"MERGED": TaskMerged,
	}
	for workflow, want := range cases {
		if got := StatusForWorkflow(workflow); got != want {
			t.Errorf("StatusForWorkflow(%q) = %q, want %q", workflow, got, want)
		}
	}
}

func TestTaskStatusForAudit(t *testing.T) {
	if got := TaskStatusForAudit("FAILED", "merged"); got != TaskFailed {
		t.Errorf("FAILED audit = %q, want failed", got)
	}
	if got := TaskStatusForAudit("failed", "review"); got != TaskFailed {
		t.Errorf("failed audit = %q, want failed", got)
	}
	if got := TaskStatusForAudit("COMPLETED", "merged"); got != TaskMerged {
		t.Errorf("COMPLETED merged audit = %q, want merged", got)
	}
	if got := TaskStatusForAudit("SKIP", "review"); got != TaskClaimed {
		t.Errorf("SKIP review audit = %q, want claimed", got)
	}
}

func TestTaskIconCaseInsensitiveOverride(t *testing.T) {
	overrides := map[string]string{"Merged": "🧪", "DISPATCHED": "🚀"}
	if got := TaskIcon(TaskMerged, overrides); got != "🧪" {
		t.Errorf("capitalized icon override = %q, want 🧪", got)
	}
	if got := TaskIcon(TaskDispatched, overrides); got != "🚀" {
		t.Errorf("uppercase icon override = %q, want 🚀", got)
	}
	if got := TaskColor(TaskMerged, map[string]int{"Merged": 0x123456}); got != 0x123456 {
		t.Errorf("capitalized color override = %#x, want 0x123456", got)
	}
	reg := NewTaskThreadRegistry()
	reg.SetStatusConfig(map[string]string{"Merged": "🧪"}, map[string]int{"Merged": 0x123456})
	rec := reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskMerged)
	if rec.Color != 0x123456 {
		t.Errorf("registry color = %#x, want 0x123456", rec.Color)
	}
	if got := TaskIcon(TaskMerged, map[string]string{"Merged": "🧪"}); got != "🧪" {
		t.Errorf("registry-style override = %q, want 🧪", got)
	}
}

func TestTaskIcon(t *testing.T) {
	if got := TaskIcon(TaskDispatched, nil); got != "⏳" {
		t.Errorf("dispatched icon = %q, want ⏳", got)
	}
	overrides := map[string]string{"dispatched": "🧪"}
	if got := TaskIcon(TaskDispatched, overrides); got != "🧪" {
		t.Errorf("override icon = %q, want 🧪", got)
	}
	if got := TaskIcon(TaskMerged, overrides); got != "✅" {
		t.Errorf("non-overridden icon = %q, want ✅", got)
	}
	if got := TaskIcon(TaskStatus("bogus"), nil); got != "📨" {
		t.Errorf("unknown status icon = %q, want 📨", got)
	}
}

func TestTaskColor(t *testing.T) {
	if got := TaskColor(TaskMerged, nil); got != 0x9B59B6 {
		t.Errorf("merged color = %#x, want purple", got)
	}
	if got := TaskColor(TaskMerged, map[string]int{"merged": 0x123456}); got != 0x123456 {
		t.Errorf("override color = %#x, want 0x123456", got)
	}
	if got := TaskColor(TaskMerged, map[string]int{"merged": 0x1FFFFFF}); got != 0x9B59B6 {
		t.Errorf("out-of-range override must fall back, got %#x", got)
	}
}

func TestTaskThreadRegistry(t *testing.T) {
	reg := NewTaskThreadRegistry()
	if _, ok := reg.Lookup("acme/widget", "feat/x"); ok {
		t.Fatal("empty registry must miss")
	}
	if _, ok := reg.Lookup("", "feat/x"); ok {
		t.Fatal("empty repo must miss")
	}
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	reg.Upsert("acme/widget", "feat/y", "root-2", "thread-2", TaskClaimed)
	rec, ok := reg.Lookup("acme/widget", "feat/x")
	if !ok || rec.ThreadID != "thread-1" || rec.RootMessageID != "root-1" || rec.Status != TaskDispatched {
		t.Fatalf("lookup = %+v, %v; want thread-1/root-1/dispatched", rec, ok)
	}
	updated := reg.Upsert("ACME/widget", "feat/x", "", "", TaskMerged)
	if updated.ThreadID != "thread-1" || updated.RootMessageID != "root-1" {
		t.Errorf("empty IDs must preserve existing: %+v", updated)
	}
	if updated.Status != TaskMerged {
		t.Errorf("status = %q, want merged", updated.Status)
	}
	other, _ := reg.Lookup("acme/widget", "feat/y")
	if other.Status != TaskClaimed {
		t.Errorf("branches must not mix: %+v", other)
	}
	if n := len(reg.Snapshot()); n != 2 {
		t.Errorf("snapshot size = %d, want 2", n)
	}
}

func TestNormalizeRepoKeyHostAgnostic(t *testing.T) {
	cases := map[string]string{
		"acme/widget":                    "acme/widget",
		"github.com/acme/widget":         "acme/widget",
		" GitHub.com/Acme/Widget ":       "acme/widget",
		"https://github.com/acme/widget": "acme/widget",
		"acme/widget.git":                "acme/widget",
		"ACME/WIDGET":                    "acme/widget",
	}
	for in, want := range cases {
		if got := normalizeRepoKey(in); got != want {
			t.Errorf("normalizeRepoKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := normalizeRepoKey(""); got != "" {
		t.Errorf("empty repo = %q, want empty", got)
	}
}

func TestTaskThreadKeyHostAgnosticBothDirections(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("github.com/acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	if rec, ok := reg.Lookup("acme/widget", "feat/x"); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("host record must hit short lookup: %+v %v", rec, ok)
	}
	reg2 := NewTaskThreadRegistry()
	reg2.Upsert("acme/widget", "feat/y", "root-2", "thread-2", TaskDispatched)
	if rec, ok := reg2.Lookup("github.com/acme/widget", "feat/y"); !ok || rec.ThreadID != "thread-2" {
		t.Fatalf("short record must hit host lookup: %+v %v", rec, ok)
	}
	if got := TaskThreadKey("github.com/acme/widget", "feat/x"); got != TaskThreadKey("acme/widget", "feat/x") {
		t.Errorf("keys must match: %q", got)
	}
	if got := TaskThreadKey(" GitHub.com/Acme/Widget ", "feat/x"); got != "acme/widget#feat/x" {
		t.Errorf("normalized key = %q, want acme/widget#feat/x", got)
	}
}

func TestTaskThreadKeyBranchCaseWhitespace(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("github.com/anggasct/occa", "fix/card-openpr-dedup", "root-1", "thread-1", TaskDispatched)
	if rec, ok := reg.Lookup("anggasct/occa", "fix/card-openpr-dedup"); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("full record must hit short lookup: %+v %v", rec, ok)
	}
	reg2 := NewTaskThreadRegistry()
	reg2.Upsert("anggasct/occa", "fix/card-openpr-dedup", "root-2", "thread-2", TaskDispatched)
	if rec, ok := reg2.Lookup("github.com/anggasct/occa", "fix/card-openpr-dedup"); !ok || rec.ThreadID != "thread-2" {
		t.Fatalf("short record must hit full lookup: %+v %v", rec, ok)
	}
	cases := []string{"Fix/Card-OpenPR-Dedup", "  fix/card-openpr-dedup  ", "FIX/CARD-OPENPR-DEDUP", "  Fix/Card-OpenPR-Dedup "}
	for _, variant := range cases {
		if rec, ok := reg.Lookup("anggasct/occa", variant); !ok || rec.ThreadID != "thread-1" {
			t.Errorf("branch variant %q miss: %+v %v", variant, rec, ok)
		}
	}
	if got := TaskThreadKey("anggasct/occa", "Fix/Card-OpenPR-Dedup"); got != "anggasct/occa#fix/card-openpr-dedup" {
		t.Errorf("branch normalized key = %q, want lowercased", got)
	}
}

func TestMissDetailSameBranchHint(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	tried, sameKey, _, hasSame, triedExists := reg.MissDetail("acme/widget", "feat/none")
	if hasSame {
		t.Errorf("different branch must not hint: %q %q", tried, sameKey)
	}
	if tried != "acme/widget#feat/none" {
		t.Errorf("tried = %q, want acme/widget#feat/none", tried)
	}
	if triedExists {
		t.Errorf("missing key must report triedExists=false")
	}
	tried, sameKey, sameThread, hasSame, _ := reg.MissDetail("other/widget", "feat/x")
	if !hasSame || sameKey != "acme/widget#feat/x" || sameThread != "thread-1" {
		t.Errorf("same-branch hint = %q %q %q %v, want acme/widget#feat/x thread-1 true", tried, sameKey, sameThread, hasSame)
	}
	reg2 := NewTaskThreadRegistry()
	if _, _, _, hasSame, _ := reg2.MissDetail("acme/widget", "feat/x"); hasSame {
		t.Errorf("empty registry must not hint")
	}
	tried, _, _, _, triedExists = reg.MissDetail("acme/widget", "feat/x")
	if tried != "acme/widget#feat/x" || !triedExists {
		t.Errorf("existing key must report triedExists=true: %q %v", tried, triedExists)
	}
}

func TestTaskRepoBranch(t *testing.T) {
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}
	repo, branch := TaskRepoBranch(key, WebhookEnvelope{})
	if repo != "acme/widget" || branch != "feat/x" {
		t.Errorf("got %q %q", repo, branch)
	}
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/y"}
	repo, branch = TaskRepoBranch(WebhookExecutionKey{}, envelope)
	if repo != "acme/widget" || branch != "feat/y" {
		t.Errorf("envelope fallback got %q %q", repo, branch)
	}
}

func TestTaskRepoBranchNormalizesRepoKey(t *testing.T) {
	key := WebhookExecutionKey{Repository: "github.com/Acme/Widget", Branch: "feat/x"}
	if repo, branch := TaskRepoBranch(key, WebhookEnvelope{}); repo != "acme/widget" || branch != "feat/x" {
		t.Errorf("host key got %q %q, want acme/widget feat/x", repo, branch)
	}
	envelope := WebhookEnvelope{"repository": "https://github.com/acme/widget", "head_branch": "feat/y"}
	if repo, branch := TaskRepoBranch(WebhookExecutionKey{}, envelope); repo != "acme/widget" || branch != "feat/y" {
		t.Errorf("host envelope got %q %q, want acme/widget feat/y", repo, branch)
	}
	reg := NewTaskThreadRegistry()
	repo, branch := TaskRepoBranch(WebhookExecutionKey{Repository: "github.com/acme/widget", Branch: "feat/z"}, WebhookEnvelope{})
	reg.Upsert(repo, branch, "root-1", "thread-1", TaskDispatched)
	if rec, ok := reg.Lookup("acme/widget", "feat/z"); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("normalized record short lookup miss: %+v %v", rec, ok)
	}
	if rec, ok := reg.Lookup("github.com/acme/widget", "feat/z"); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("normalized record host lookup miss: %+v %v", rec, ok)
	}
}

func TestFormatTaskParent(t *testing.T) {
	got := FormatTaskParent("acme/widget", "feat/x", "42", TaskMerged, nil)
	for _, want := range []string{"✅", "acme/widget#feat/x", "PR #42", "merged"} {
		if !strings.Contains(got, want) {
			t.Errorf("parent %q must contain %q", got, want)
		}
	}
	override := FormatTaskParent("acme/widget", "feat/x", "", TaskDispatched, map[string]string{"dispatched": "🧪"})
	if !strings.HasPrefix(override, "🧪") {
		t.Errorf("override parent = %q, want 🧪 prefix", override)
	}
}

func TestTaskCardHeader(t *testing.T) {
	if got := TaskCardHeader(WebhookEnvelope{}, "review", TaskClaimed, nil); got != "" {
		t.Errorf("header without branch = %q, want empty", got)
	}
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "42",
		"pr_url":      "https://example.com/acme/widget/pull/42",
	}
	header := TaskCardHeader(envelope, "review", TaskClaimed, nil)
	for _, want := range []string{"👀", "acme/widget#feat/x"} {
		if !strings.Contains(header, want) {
			t.Errorf("header %q must contain %q", header, want)
		}
	}
	if strings.Contains(header, "[Open PR]") {
		t.Errorf("header %q must not contain inline Open PR link, button carries it", header)
	}
	if got := WithTaskHeader("body", envelope, "review", TaskClaimed, nil); !strings.HasPrefix(got, "👀") || !strings.HasSuffix(got, "body") {
		t.Errorf("wrapped card = %q", got)
	}
	if strings.Contains(WithTaskHeader("body", envelope, "review", TaskClaimed, nil), "[Open PR]") {
		t.Errorf("wrapped card must not contain inline Open PR link")
	}
	if got := WithTaskHeader("body", WebhookEnvelope{}, "review", TaskClaimed, nil); got != "body" {
		t.Errorf("empty header must leave card alone, got %q", got)
	}
}

func TestTaskCardHeaderFallback(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "42",
		"pr_url":      "https://example.com/acme/widget/pull/42",
	}
	prButtons := TaskParentButtons("discord", "guild-1", envelope, "acme/widget", "feat/x", "thread-1")
	if !HasOpenPRButton(prButtons) {
		t.Fatalf("discord parent must carry Open PR button: %+v", prButtons)
	}
	if got := TaskCardHeaderWithButtons(envelope, "review", TaskClaimed, nil, "discord", prButtons); strings.Contains(got, "[Open PR]") {
		t.Errorf("discord header with button must not contain inline link, got %q", got)
	}
	if got := TaskCardHeaderWithButtons(envelope, "review", TaskClaimed, nil, "telegram", prButtons); !strings.Contains(got, "[Open PR](https://example.com/acme/widget/pull/42)") {
		t.Errorf("telegram header must keep inline fallback, got %q", got)
	}
	if got := TaskCardHeaderWithButtons(envelope, "review", TaskClaimed, nil, "discord", nil); !strings.Contains(got, "[Open PR]") {
		t.Errorf("discord header without buttons must keep inline fallback, got %q", got)
	}
	if got := WithTaskHeaderWithButtons("body", envelope, "review", TaskClaimed, nil, "discord", prButtons); strings.Contains(got, "[Open PR]") {
		t.Errorf("discord wrapped card with button must not contain inline link, got %q", got)
	}
	if got := WithTaskHeaderWithButtons("body", envelope, "review", TaskClaimed, nil, "telegram", nil); !strings.Contains(got, "[Open PR]") {
		t.Errorf("telegram wrapped card must keep inline fallback, got %q", got)
	}
	if got := TaskInlineFallback("", "discord", nil); got != "" {
		t.Errorf("empty PR URL must have no fallback, got %q", got)
	}
}

func TestMissingTaskWorkflows(t *testing.T) {
	mk := func(workflow string) config.EndpointConfig {
		return config.EndpointConfig{
			Name:       "ep-" + workflow,
			Workflow:   workflow,
			Repository: "acme/widget",
		}
	}
	full := []config.EndpointConfig{mk("review"), mk("fix"), mk("merge"), mk("merged")}
	if missing := MissingTaskWorkflows(full, "acme/widget"); len(missing) != 0 {
		t.Errorf("full set missing = %v", missing)
	}
	partial := []config.EndpointConfig{mk("review"), mk("fix")}
	missing := MissingTaskWorkflows(partial, "ACME/widget")
	if len(missing) != 2 || missing[0] != "merge" || missing[1] != "merged" {
		t.Errorf("partial missing = %v, want [merge merged]", missing)
	}
	other := []config.EndpointConfig{mk("review"), mk("fix"), mk("merge"), mk("merged")}
	other[0].Repository = "acme/other"
	if missing := MissingTaskWorkflows(other, "acme/widget"); len(missing) != 1 || missing[0] != "review" {
		t.Errorf("cross-repo missing = %v, want [review]", missing)
	}
}

func TestFallbackDue(t *testing.T) {
	now := time.Now()
	if FallbackDue(time.Time{}, now, 10*time.Minute) {
		t.Error("zero event time must never be due")
	}
	if FallbackDue(now, now, 0) {
		t.Error("zero interval must never be due")
	}
	if FallbackDue(now.Add(-11*time.Minute), now, 10*time.Minute) != true {
		t.Error("stale event must be due")
	}
	if FallbackDue(now.Add(-9*time.Minute), now, 10*time.Minute) {
		t.Error("fresh event must not be due")
	}
}

func TestFormatFallbackFinding(t *testing.T) {
	empty := FormatFallbackFinding("acme/widget", "feat/x", BranchPRResult{})
	if !strings.Contains(empty, "no open PR") || !strings.Contains(empty, "acme/widget#feat/x") {
		t.Errorf("empty finding = %q", empty)
	}
	found := FormatFallbackFinding("acme/widget", "feat/x", BranchPRResult{Found: true, Number: "42", URL: "https://example.com/p/42", State: "OPEN"})
	for _, want := range []string{"PR #42", "OPEN", "[Open PR](https://example.com/p/42)"} {
		if !strings.Contains(found, want) {
			t.Errorf("found finding %q must contain %q", found, want)
		}
	}
}

func TestParseBranchPRList(t *testing.T) {
	res, err := parseBranchPRList([]byte(`[]`))
	if err != nil || res.Found {
		t.Errorf("empty list = %+v, %v; want not found", res, err)
	}
	res, err = parseBranchPRList([]byte(`[{"number":42,"url":"https://example.com/p/42","state":"OPEN"}]`))
	if err != nil || !res.Found || res.Number != "42" || res.URL != "https://example.com/p/42" || res.State != "OPEN" {
		t.Errorf("row = %+v, %v", res, err)
	}
	if _, err := parseBranchPRList([]byte(`{broken`)); err == nil {
		t.Error("invalid JSON must error")
	}
}

func TestFormatTaskLinksRewritesAPIURL(t *testing.T) {
	api := "https://api.github.com/repos/acme/widget/pulls/42"
	got := FormatTaskLinks(api)
	if !strings.Contains(got, "https://github.com/acme/widget/pull/42") {
		t.Errorf("api url rewrite = %q, want github.com/acme/widget/pull/42", got)
	}
	if strings.Contains(got, "api.github.com") {
		t.Errorf("api url must not leak into link: %q", got)
	}
	web := "https://github.com/acme/widget/pull/42"
	if got := FormatTaskLinks(web); !strings.Contains(got, web) {
		t.Errorf("web url must pass through: %q", got)
	}
	if got := FormatTaskLinks(""); got != "" {
		t.Errorf("empty url = %q, want empty", got)
	}
}

func TestTaskLinkButtons(t *testing.T) {
	buttons := TaskLinkButtons("discord", "guild-1", "https://github.com/acme/widget/pull/42", "thread-123")
	if len(buttons) != 2 {
		t.Fatalf("link buttons = %d, want 2 (Open PR + Open thread)", len(buttons))
	}
	if buttons[0].Label != "Open PR" || buttons[0].URL == "" {
		t.Errorf("open pr button = %+v", buttons[0])
	}
	if buttons[1].Label != "Open thread" || !strings.Contains(buttons[1].URL, "thread-123") {
		t.Errorf("open thread button = %+v", buttons[1])
	}
	if !strings.Contains(buttons[1].URL, "guild-1") {
		t.Errorf("open thread button must carry guild id: %+v", buttons[1])
	}
	apiButtons := TaskLinkButtons("discord", "guild-1", "https://api.github.com/repos/acme/widget/pulls/7", "")
	if len(apiButtons) != 1 || !strings.Contains(apiButtons[0].URL, "github.com/acme/widget/pull/7") {
		t.Errorf("api pr button must rewrite to web url: %+v", apiButtons)
	}
}

func TestTaskThreadURLPerPlatform(t *testing.T) {
	if got := TaskThreadURL("discord", "guild-1", "thread-123"); got != "https://discord.com/channels/guild-1/thread-123" {
		t.Errorf("discord thread url = %q, want guild link", got)
	}
	if got := TaskThreadURL("discord", "", "thread-123"); !strings.Contains(got, "thread-123") || !strings.Contains(got, "discord.com") {
		t.Errorf("discord thread url without guild = %q, want fallback link with thread id", got)
	}
	if got := TaskThreadURL("discord", "", "thread-123"); strings.Contains(got, "guild-1") {
		t.Errorf("fallback url must not invent guild: %q", got)
	}
	for _, platform := range []string{"telegram", "", "bogus"} {
		if got := TaskThreadURL(platform, "guild-1", "thread-123"); got != "" {
			t.Errorf("TaskThreadURL(%q, thread) = %q, want empty (no dead link)", platform, got)
		}
	}
	if got := TaskThreadURL("telegram", "guild-1", ""); got != "" {
		t.Errorf("empty thread id = %q, want empty", got)
	}
	full := "https://example.com/t/thread-123"
	for _, platform := range []string{"discord", "telegram"} {
		if got := TaskThreadURL(platform, "guild-1", full); got != full {
			t.Errorf("TaskThreadURL(%q, url) = %q, want passthrough", platform, got)
		}
	}
}

func TestTaskParentButtonsPerPlatform(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_url":      "https://github.com/acme/widget/pull/42",
	}
	telegram := TaskParentButtons("telegram", "", envelope, "acme/widget", "feat/x", "topic-123")
	for _, b := range telegram {
		if strings.Contains(b.URL, "discord.com") {
			t.Errorf("telegram parent must not carry a discord thread url: %+v", b)
		}
	}
	foundPR := false
	for _, b := range telegram {
		if b.Label == "Open PR" {
			foundPR = true
		}
		if b.Label == "Open thread" {
			t.Errorf("telegram parent must omit the thread button: %+v", b)
		}
	}
	if !foundPR {
		t.Errorf("telegram parent must keep the Open PR button: %+v", telegram)
	}
	discord := TaskParentButtons("discord", "guild-1", envelope, "acme/widget", "feat/x", "thread-123")
	if len(discord) != 4 {
		t.Fatalf("discord parent must carry 4 buttons in one row, got %+v", discord)
	}
	for _, b := range discord {
		if b.Row != 1 {
			t.Errorf("all card buttons must sit on row 1, got %+v", b)
		}
	}
	foundThread := false
	for _, b := range discord {
		if b.Label == "Open thread" && strings.Contains(b.URL, "thread-123") && strings.Contains(b.URL, "guild-1") {
			foundThread = true
		}
	}
	if !foundThread {
		t.Errorf("discord parent must carry a guild thread link: %+v", discord)
	}
}

func TestTaskActionButtons(t *testing.T) {
	buttons := TaskActionButtons("acme/widget", "feat/x")
	if len(buttons) != 2 {
		t.Fatalf("action buttons = %d, want 2 (Merge + Re-dispatch)", len(buttons))
	}
	if buttons[0].Label != "Merge" || !strings.HasPrefix(buttons[0].Value, "task:merge:") {
		t.Errorf("merge button = %+v", buttons[0])
	}
	if buttons[1].Label != "Re-dispatch" || !strings.HasPrefix(buttons[1].Value, "task:redispatch:") {
		t.Errorf("redispatch button = %+v", buttons[1])
	}
	if buttons[0].URL != "" || buttons[1].URL != "" {
		t.Errorf("action buttons must not carry URL: %+v", buttons)
	}
	if buttons[0].Row != 1 || buttons[1].Row != 1 {
		t.Errorf("action buttons must sit on row 1 with the link buttons, got %+v", buttons)
	}
	if len(buttons[0].Value) > 100 || len(buttons[1].Value) > 100 {
		t.Errorf("callback ids must fit Discord 100-char limit: %+v", buttons)
	}
	if action, repo, branch, ok := ParseTaskCallback(buttons[0].Value); !ok || action != "merge" || repo != "acme/widget" || branch != "feat/x" {
		t.Errorf("parse merge = %q %q %q %v", action, repo, branch, ok)
	}
	if action, _, _, ok := ParseTaskCallback(buttons[1].Value); !ok || action != "redispatch" {
		t.Errorf("parse redispatch = %q %v", action, ok)
	}
}

func TestTaskActionButtonsLongBranch(t *testing.T) {
	longBranch := "feat/" + strings.Repeat("x", 120)
	if buttons := TaskActionButtons("acme/widget", longBranch); buttons != nil {
		t.Errorf("oversize callback must omit buttons, got %+v", buttons)
	}
	multiByteBranch := "feat/" + strings.Repeat("ü", 30)
	buttons := TaskActionButtons("acme/widget", multiByteBranch)
	if len(buttons) != 2 {
		t.Fatalf("fitting multi-byte branch must keep buttons, got %+v", buttons)
	}
	if action, repo, branch, ok := ParseTaskCallback(buttons[0].Value); !ok || action != "merge" || repo != "acme/widget" || branch != multiByteBranch {
		t.Errorf("multi-byte round-trip = %q %q %q %v", action, repo, branch, ok)
	}
}
