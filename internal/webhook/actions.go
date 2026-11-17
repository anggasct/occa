package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/store"
)

const (
	ActionRetry          = "retry"
	ActionRetryWith      = "retry_with"
	ActionCleanWorkspace = "clean_workspace"
	ActionUnblock        = "unblock"
	ActionApprove        = "approve"
	ActionReject         = "reject"
	ActionSkipStage      = "skip_stage"
	ActionStop           = "stop"
	ActionRecheck        = "recheck"
	ActionReReview       = "re_review"
	ActionFixCI          = "fix_ci"
	ActionFixReview      = "fix_review"
	ActionRebind         = "rebind"
	ActionMergeNow       = "merge_now"
)

const (
	actionResultAccepted = "accepted"
	actionResultRefused  = "refused"
	actionResultDone     = "done"
)

const (
	refusalActionNotLegal  = "action_not_legal"
	refusalReasonRequired  = "reason_required"
	refusalConfirmMismatch = "confirm_mismatch"
	refusalActionUnknown   = "action_unknown"
	refusalItemUnknown     = "item_unknown"
	refusalParamsInvalid   = "params_invalid"
)

const manualRetryReasonCap = 3
const maxActionReasonRunes = 500

const actionActorOwner = "owner"

const actionDeliveryPrefix = "action-"

// stageStallThreshold derives the read-time "stalled" state of a running
// stage: the executor heartbeats once a second, so a minute of silence means
// the run's owner is gone and the row is stale.
const stageStallThreshold = time.Minute

type FireActionRequest struct {
	WorkItemID     string
	Action         string
	Reason         string
	Confirm        string
	Params         map[string]string
	IdempotencyKey string
	// Surface names the caller's front — "chat" keeps the legacy op-retry
	// delivery prefix; anything else uses the action prefix.
	Surface string
}

type ActionOutcome struct {
	ActionID   int64
	StageRunID int64
	Action     string
	Actor      string
	Accepted   bool
	Code       string
	Sentence   string
	Detail     string
	ThreadLine string
	Result     string
	Replayed   bool
}

func (o ActionOutcome) Error() string {
	if o.Detail != "" {
		return o.Sentence + " " + o.Detail
	}
	return o.Sentence
}

// actionRefusal lets an effect decline the action with a clean refusal
// sentence — e.g. a live-state check the legality pass cannot run — instead
// of the generic "could not run" wrapper used for real execution errors.
type actionRefusal struct {
	code     string
	sentence string
}

func (e *actionRefusal) Error() string { return e.sentence }

func refuseAction(sentence string) error {
	return &actionRefusal{code: refusalActionNotLegal, sentence: sentence}
}

type InboxRow struct {
	WorkItemID      string
	Repo            string
	Number          int
	Title           string
	Stage           string
	Attempt         int
	State           string
	Derived         string
	BlockerCode     string
	BlockerSentence string
	PrimaryAction   string
	AgeSeconds      int64
	WaitingInputs   int
}

type WorkItemSummary struct {
	ID      string
	Kind    string
	SpecRef string
	SpecSHA string
	State   string
}

type PhaseSummary struct {
	Phase       string
	Attempt     int
	State       string
	BlockerCode string
}

type StageRunView struct {
	ID                 int64
	Stage              string
	Attempt            int
	State              string
	StartedAt          int64
	EndedAt            int64
	LastHeartbeatAt    int64
	ArtifactRef        string
	GateResult         string
	WorkspacePath      string
	WorkspaceKeptUntil int64
	BlockerCode        string
	BlockerDetail      string
	AutoRetryUsed      int
	ManualRetryCount   int
	TriggeredBy        string
}

type WaitingInputView struct {
	DeliveryID string
	Endpoint   string
	ReceivedAt int64
}

type WorkItemDetailView struct {
	Item          WorkItemSummary
	Phases        PhaseSummary
	Stages        []StageRunView
	WaitingInputs []WaitingInputView
	Actions       []string
}

type actionSpec struct {
	name           string
	paramsAllowed  []string
	reasonRequired func(v *actionView) bool
	confirm        bool
	legal          func(v *actionView) (bool, string)
	effect         func(s *Server, ctx context.Context, req FireActionRequest, v *actionView) (string, error)
}

type actionView struct {
	item     *store.WorkItem
	latest   *store.StageRun
	runs     []store.StageRun
	waiting  []store.StageInput
	prRow    *store.PRRun
	busy     prDispatchBusy
	reviewEp bool
}

func (v *actionView) prKey() string { return v.item.ID }

func (v *actionView) state() store.StageState {
	if v.latest == nil {
		return ""
	}
	return v.latest.State
}

func (v *actionView) blocker() store.BlockerCode {
	if v.latest == nil {
		return ""
	}
	return v.latest.BlockerCode
}

func (v *actionView) stalled() bool {
	if v.latest == nil {
		return false
	}
	return v.latest.Stalled(time.Now(), stageStallThreshold)
}

func (v *actionView) blocked() bool {
	return v.state() == store.StageStateBlocked ||
		(v.prRow != nil && v.prRow.Status == store.PRStatusBlocked)
}

func (v *actionView) hasCode(codes ...store.BlockerCode) bool {
	return slices.Contains(codes, v.blocker())
}

func (v *actionView) derived() string {
	switch {
	case v.stalled():
		return "stalled"
	default:
		return ""
	}
}

var actionCatalog = []actionSpec{
	{
		name:           ActionRetry,
		reasonRequired: func(v *actionView) bool { return v.latest != nil && v.latest.ManualRetryCount >= manualRetryReasonCap },
		legal: func(v *actionView) (bool, string) {
			switch v.state() {
			case store.StageStateFailed, store.StageStateBlocked, store.StageStateCancelled:
				return true, ""
			}
			if v.stalled() || v.blocked() || v.prRow != nil {
				return true, ""
			}
			return false, "There is nothing to retry for " + v.prKey() + " — the stage is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectRetry,
	},
	{
		name:          ActionRetryWith,
		paramsAllowed: []string{"profile"},
		legal: func(v *actionView) (bool, string) {
			switch v.state() {
			case store.StageStateFailed, store.StageStateBlocked:
				return true, ""
			}
			return false, "retry_with needs a failed or blocked stage — " + v.prKey() + " is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectRetryWith,
	},
	{
		name:           ActionCleanWorkspace,
		paramsAllowed:  []string{"mode"},
		reasonRequired: func(*actionView) bool { return true },
		legal: func(v *actionView) (bool, string) {
			if v.blocked() && v.hasCode(store.BlockerWorkspaceDirty) {
				return true, ""
			}
			return false, "clean_workspace only applies when " + v.prKey() + " is blocked on a dirty workspace."
		},
		effect: (*Server).effectCleanWorkspace,
	},
	{
		name: ActionUnblock,
		legal: func(v *actionView) (bool, string) {
			if v.blocked() {
				return true, ""
			}
			return false, v.prKey() + " is not blocked — nothing to unblock."
		},
		effect: (*Server).effectUnblock,
	},
	{
		name: ActionApprove,
		legal: func(v *actionView) (bool, string) {
			if v.state() == store.StageStateWaitingHuman {
				return true, ""
			}
			return false, v.prKey() + " is not waiting for a human decision."
		},
		effect: (*Server).effectApprove,
	},
	{
		name:           ActionReject,
		reasonRequired: func(*actionView) bool { return true },
		legal: func(v *actionView) (bool, string) {
			if v.state() == store.StageStateWaitingHuman {
				return true, ""
			}
			return false, v.prKey() + " is not waiting for a human decision."
		},
		effect: (*Server).effectReject,
	},
	{
		name:           ActionSkipStage,
		reasonRequired: func(*actionView) bool { return true },
		legal: func(v *actionView) (bool, string) {
			switch v.state() {
			case store.StageStateFailed, store.StageStateBlocked, store.StageStateWaitingHuman, store.StageStateRunning:
				return true, ""
			}
			return false, "skip_stage needs a live or decided stage — " + v.prKey() + " is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectSkipStage,
	},
	{
		name: ActionStop,
		legal: func(v *actionView) (bool, string) {
			if v.state() == store.StageStateRunning || v.state() == store.StageStatePending || v.busy.activeKind != "" || v.busy.queued > 0 || v.prRow != nil {
				return true, ""
			}
			return false, "Nothing is running for " + v.prKey() + "."
		},
		effect: (*Server).effectStop,
	},
	{
		name: ActionRecheck,
		legal: func(v *actionView) (bool, string) {
			if v.state() == store.StageStateWaitingExternal || v.hasCode(store.BlockerGitHubUnreachable) {
				return true, ""
			}
			return false, v.prKey() + " is not waiting on an external check — nothing to recheck."
		},
		effect: (*Server).effectRecheck,
	},
	{
		name: ActionReReview,
		legal: func(v *actionView) (bool, string) {
			if v.busy.reviewActive {
				return false, "A review of " + v.prKey() + " is already running."
			}
			if v.busy.reviewQueued {
				return false, "A review of " + v.prKey() + " is already queued."
			}
			if !v.reviewEp {
				return false, "No review endpoint is configured for this repository."
			}
			return true, ""
		},
		effect: (*Server).effectReReview,
	},
	{
		name: ActionFixCI,
		legal: func(v *actionView) (bool, string) {
			if v.blocked() {
				return true, ""
			}
			return false, "fix_ci needs a blocked stage — " + v.prKey() + " is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectFixCI,
	},
	{
		name: ActionFixReview,
		legal: func(v *actionView) (bool, string) {
			if v.blocked() {
				return true, ""
			}
			return false, "fix_review needs a blocked stage — " + v.prKey() + " is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectFixReview,
	},
	{
		name: ActionRebind,
		legal: func(v *actionView) (bool, string) {
			if v.hasCode(store.BlockerBindingMissing) {
				return true, ""
			}
			return false, "rebind only applies when " + v.prKey() + " is blocked on a missing binding."
		},
		effect: (*Server).effectRebind,
	},
	{
		name:           ActionMergeNow,
		confirm:        true,
		reasonRequired: func(*actionView) bool { return true },
		legal: func(v *actionView) (bool, string) {
			if v.state() == store.StageStateWaitingExternal || v.hasCode(store.BlockerCIRed, store.BlockerMergeStarved) {
				return true, ""
			}
			return false, "merge_now needs a stage waiting on external checks or blocked on red CI — " + v.prKey() + " is " + describeStateForAction(v) + "."
		},
		effect: (*Server).effectMergeNow,
	},
}

func actionSpecFor(name string) (actionSpec, bool) {
	for _, spec := range actionCatalog {
		if spec.name == name {
			return spec, true
		}
	}
	return actionSpec{}, false
}

func describeStateForAction(v *actionView) string {
	if v.latest == nil {
		if v.prRow != nil {
			return "queue " + string(v.prRow.Status)
		}
		return "idle"
	}
	if d := v.derived(); d != "" {
		return d
	}
	return string(v.state())
}

func (s *Server) actionRepo() store.StageRepo {
	if s.stages == nil {
		return nil
	}
	return s.stages.repo
}

func isActionDelivery(deliveryID string) bool {
	return strings.HasPrefix(deliveryID, actionDeliveryPrefix)
}

func (s *Server) buildActionView(ctx context.Context, workItemID string) (*actionView, error) {
	repo := s.actionRepo()
	if repo == nil {
		return nil, errors.New("stage store not configured")
	}
	item, err := repo.GetWorkItem(ctx, workItemID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		item, err = s.adoptWorkItem(ctx, workItemID)
		if err != nil || item == nil {
			return nil, err
		}
	}
	v := &actionView{item: item}
	v.runs, err = repo.ListRuns(ctx, workItemID)
	if err != nil {
		return nil, err
	}
	if len(v.runs) > 0 {
		latest := v.runs[len(v.runs)-1]
		v.latest = &latest
	}
	v.waiting, err = repo.ListWaitingInputs(ctx, workItemID)
	if err != nil {
		return nil, err
	}
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, workItemID); err == nil {
			v.prRow = row
		}
	}
	v.busy = s.prDispatchState(workItemID)
	_, v.reviewEp = s.reviewEndpointForPR(workItemID)
	return v, nil
}

// adoptWorkItem creates the work item for a PR the queue or thread registry
// knows but no stage run has started for yet — the same upsert a first run
// would write.
func (s *Server) adoptWorkItem(ctx context.Context, prKey string) (*store.WorkItem, error) {
	known := false
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err != nil {
			return nil, err
		} else if row != nil {
			known = true
		}
	}
	if !known && s.tasks != nil {
		_, known = s.tasks.ResolvePRThread(prKey)
	}
	if !known {
		return nil, nil
	}
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return nil, nil
	}
	n, err := strconv.Atoi(number)
	if err != nil {
		return nil, nil
	}
	item := &store.WorkItem{ID: prKey, Repo: normalizeRepoKey(repo), Number: n, Kind: store.WorkItemPullRequest}
	if err := s.actionRepo().UpsertWorkItem(ctx, *item); err != nil {
		return nil, err
	}
	return s.actionRepo().GetWorkItem(ctx, prKey)
}

func (s *Server) legalActions(v *actionView) []string {
	var out []string
	for _, spec := range actionCatalog {
		if ok, _ := spec.legal(v); ok {
			out = append(out, spec.name)
		}
	}
	return out
}

func normalizeActionParams(spec actionSpec, params map[string]string) (map[string]string, bool) {
	out := map[string]string{}
	for k, val := range params {
		if !slices.Contains(spec.paramsAllowed, k) {
			return nil, false
		}
		if val = strings.TrimSpace(val); val != "" {
			out[k] = val
		}
	}
	return out, true
}

func (s *Server) FireAction(ctx context.Context, actor string, req FireActionRequest) (ActionOutcome, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = actionActorOwner
	}
	req.Action = strings.TrimSpace(req.Action)
	req.WorkItemID = strings.TrimSpace(req.WorkItemID)
	req.Reason = strings.TrimSpace(req.Reason)

	spec, ok := actionSpecFor(req.Action)
	if !ok {
		return ActionOutcome{
			Action:   req.Action,
			Actor:    actor,
			Code:     refusalActionUnknown,
			Sentence: "Unknown action " + strconv.Quote(req.Action) + ".",
			Result:   actionResultRefused,
		}, nil
	}

	v, err := s.buildActionView(ctx, req.WorkItemID)
	if err != nil {
		return ActionOutcome{}, err
	}
	if v == nil {
		return ActionOutcome{
			Action:   req.Action,
			Actor:    actor,
			Code:     refusalItemUnknown,
			Sentence: "No work item " + req.WorkItemID + " — nothing to act on.",
			Result:   actionResultRefused,
		}, nil
	}

	params, paramsOK := normalizeActionParams(spec, req.Params)
	req.Params = params

	refuse := func(code, sentence, detail string) (ActionOutcome, error) {
		return ActionOutcome{
			Action:     req.Action,
			Actor:      actor,
			StageRunID: latestRunID(v),
			Accepted:   false,
			Code:       code,
			Sentence:   sentence,
			Detail:     detail,
			Result:     actionResultRefused,
		}, nil
	}

	if !paramsOK {
		return refuse(refusalParamsInvalid, "Parameters for "+req.Action+" are not valid.", "")
	}
	if spec.reasonRequired != nil && spec.reasonRequired(v) {
		if req.Reason == "" {
			return refuse(refusalReasonRequired, req.Action+" needs a reason — say why in a sentence.", "")
		}
	}
	if len([]rune(req.Reason)) > maxActionReasonRunes {
		return refuse(refusalReasonRequired, "The reason is too long — keep it under 500 characters.", "")
	}
	if spec.confirm && req.Confirm != v.item.ID {
		return refuse(refusalConfirmMismatch, "Type "+v.item.ID+" to confirm "+req.Action+".", "")
	}
	if legal, sentence := spec.legal(v); !legal {
		return refuse(refusalActionNotLegal, sentence, "")
	}

	if req.IdempotencyKey == "" {
		req.IdempotencyKey = fmt.Sprintf("auto-%d", time.Now().UnixNano())
	}
	rec, replayed, err := s.actionRepo().RecordAction(ctx, store.ActionRequest{
		WorkItemID:     v.item.ID,
		StageRunID:     latestRunID(v),
		Action:         req.Action,
		Actor:          actor,
		Reason:         req.Reason,
		IdempotencyKey: req.IdempotencyKey,
		Result:         actionResultAccepted,
	})
	if err != nil {
		return ActionOutcome{}, err
	}
	if replayed {
		return outcomeFromRecord(rec), nil
	}

	threadTail, effErr := spec.effect(s, ctx, req, v)
	if effErr != nil {
		code, sentence := refusalActionNotLegal, req.Action+" could not run — "+redactSummary(effErr.Error(), 120, "")
		var refusal *actionRefusal
		if errors.As(effErr, &refusal) {
			code, sentence = refusal.code, refusal.sentence
		}
		out := ActionOutcome{
			ActionID: rec.ID, StageRunID: rec.StageRunID, Action: req.Action, Actor: actor,
			Accepted: false, Code: code,
			Sentence: sentence,
			Result:   actionResultRefused,
		}
		s.finishActionRecord(ctx, rec.ID, out)
		return out, nil
	}

	sentence := strings.TrimSpace(threadTail)
	if sentence == "" {
		sentence = actionAcceptedSentence(req.Action, v)
	}
	out := ActionOutcome{
		ActionID: rec.ID, StageRunID: rec.StageRunID, Action: req.Action, Actor: actor,
		Accepted:   true,
		Sentence:   sentence,
		ThreadLine: actionThreadLine(req.Action, actor, req.Reason, threadTail),
		Result:     actionResultAccepted,
	}
	s.finishActionRecord(ctx, rec.ID, out)
	s.notifyActionThread(ctx, v.prKey(), out.ThreadLine)
	return out, nil
}

func latestRunID(v *actionView) int64 {
	if v.latest == nil {
		return 0
	}
	return v.latest.ID
}

func (s *Server) finishActionRecord(ctx context.Context, id int64, out ActionOutcome) {
	repo := s.actionRepo()
	if repo == nil || id == 0 {
		return
	}
	if err := repo.SetActionResult(ctx, id, out.Result, out.Code, out.Sentence, out.Detail); err != nil {
		slog.Warn("action: store result failed", "action_id", id, "error", err)
	}
}

func outcomeFromRecord(rec store.ActionRequest) ActionOutcome {
	return ActionOutcome{
		ActionID:   rec.ID,
		StageRunID: rec.StageRunID,
		Action:     rec.Action,
		Actor:      rec.Actor,
		Accepted:   rec.Result == actionResultAccepted || rec.Result == actionResultDone,
		Code:       rec.Code,
		Sentence:   rec.Sentence,
		Detail:     rec.Detail,
		Result:     rec.Result,
		Replayed:   true,
	}
}

func actionAcceptedSentence(action string, v *actionView) string {
	switch action {
	case ActionRetry, ActionRetryWith:
		return "Re-running the stage for " + v.prKey() + "."
	case ActionUnblock:
		return v.prKey() + " released — queued work continues."
	case ActionStop:
		return "Stopping the active work for " + v.prKey() + "."
	case ActionSkipStage:
		return "Skipping the stage for " + v.prKey() + "."
	case ActionApprove:
		return "Approved " + v.prKey() + "."
	case ActionReject:
		return "Rejected " + v.prKey() + " — routing to fix."
	case ActionRecheck:
		return "Rechecked the external state of " + v.prKey() + "."
	case ActionReReview:
		return "Review queued for " + v.prKey() + "."
	case ActionFixCI:
		return "A CI fix run is queued for " + v.prKey() + "."
	case ActionFixReview:
		return "A review fix run is queued for " + v.prKey() + "."
	case ActionRebind:
		return "Re-binding " + v.prKey() + "."
	case ActionMergeNow:
		return "Merge override queued for " + v.prKey() + "."
	case ActionCleanWorkspace:
		return "Workspace cleanup queued for " + v.prKey() + "."
	}
	return "Done."
}

func actionThreadLine(action, actor, reason, tail string) string {
	line := "⚙️ " + action + " · @" + actor
	if reason = strings.TrimSpace(reason); reason != "" {
		line += " · reason: " + strconv.Quote(reason)
	}
	if tail = strings.TrimSpace(tail); tail != "" {
		line += " · " + tail
	}
	return line
}

func (s *Server) ListInbox(ctx context.Context, repo string, limit int, cursor string) ([]InboxRow, string, error) {
	repoStore := s.actionRepo()
	if repoStore == nil {
		return nil, "", errors.New("stage store not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	offset := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("inbox: bad cursor %q", cursor)
		}
		offset = n
	}
	items, err := repoStore.ListOpenWorkItems(ctx, repo, limit+offset+1)
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	latest, err := repoStore.ListLatestRuns(ctx, ids)
	if err != nil {
		return nil, "", err
	}
	waiting, err := repoStore.CountWaitingInputs(ctx, ids)
	if err != nil {
		return nil, "", err
	}
	now := time.Now()
	rows := make([]InboxRow, 0, len(items))
	for _, it := range items {
		row := InboxRow{
			WorkItemID:    it.ID,
			Repo:          it.Repo,
			Number:        it.Number,
			WaitingInputs: waiting[it.ID],
		}
		if runs := latest[it.ID]; len(runs) > 0 {
			var newest store.StageRun
			for _, r := range runs {
				if r.ID > newest.ID {
					newest = r
				}
			}
			row.Stage = string(newest.Stage)
			row.Attempt = newest.Attempt
			row.State = string(newest.State)
			if newest.Stalled(now, stageStallThreshold) {
				row.Derived = "stalled"
			}
			row.BlockerCode = string(newest.BlockerCode)
			row.BlockerSentence = newest.BlockerDetail
			row.AgeSeconds = now.Unix() - newest.StartedAt
			v := &actionView{item: &it, latest: &newest, runs: runs}
			if s.prQueues != nil {
				if pr, perr := s.prQueues.Get(ctx, it.ID); perr == nil {
					v.prRow = pr
				}
			}
			v.busy = s.prDispatchState(it.ID)
			_, v.reviewEp = s.reviewEndpointForPR(it.ID)
			if acts := s.legalActions(v); len(acts) > 0 {
				row.PrimaryAction = acts[0]
			}
		}
		rows = append(rows, row)
	}
	rank := func(state, derived string) int {
		switch {
		case state == string(store.StageStateBlocked):
			return 0
		case state == string(store.StageStateWaitingHuman):
			return 1
		case derived == "stalled":
			return 2
		case state == string(store.StageStateFailed):
			return 3
		case state == string(store.StageStateRunning), state == string(store.StageStatePending):
			return 4
		case state == string(store.StageStateWaitingExternal):
			return 5
		default:
			return 6
		}
	}
	slices.SortStableFunc(rows, func(a, b InboxRow) int {
		ra, rb := rank(a.State, a.Derived), rank(b.State, b.Derived)
		if ra != rb {
			return ra - rb
		}
		return int(b.AgeSeconds - a.AgeSeconds)
	})
	next := ""
	if len(rows) > offset+limit {
		next = strconv.Itoa(offset + limit)
		rows = rows[offset : offset+limit]
	} else if offset < len(rows) {
		rows = rows[offset:]
	} else {
		rows = rows[:0]
	}
	return rows, next, nil
}

func (s *Server) GetWorkItemDetail(ctx context.Context, id string) (*WorkItemDetailView, error) {
	v, err := s.buildActionView(ctx, id)
	if err != nil || v == nil {
		return nil, err
	}
	detail := &WorkItemDetailView{
		Item: WorkItemSummary{
			ID:      v.item.ID,
			Kind:    string(v.item.Kind),
			SpecRef: v.item.SpecRef,
			SpecSHA: v.item.SpecSHA,
			State:   string(v.item.State),
		},
	}
	for _, r := range v.runs {
		detail.Stages = append(detail.Stages, StageRunView{
			ID:               r.ID,
			Stage:            string(r.Stage),
			Attempt:          r.Attempt,
			State:            string(r.State),
			StartedAt:        r.StartedAt,
			EndedAt:          r.EndedAt,
			LastHeartbeatAt:  r.LastHeartbeatAt,
			ArtifactRef:      r.ArtifactRef,
			GateResult:       r.GateResult,
			WorkspacePath:    r.WorkspacePath,
			BlockerCode:      string(r.BlockerCode),
			BlockerDetail:    r.BlockerDetail,
			AutoRetryUsed:    r.AutoRetryUsed,
			ManualRetryCount: r.ManualRetryCount,
			TriggeredBy:      r.TriggeredBy,
		})
	}
	if v.latest != nil {
		detail.Phases = PhaseSummary{
			Phase:       string(v.latest.Stage),
			Attempt:     v.latest.Attempt,
			State:       describeStateForAction(v),
			BlockerCode: string(v.latest.BlockerCode),
		}
	}
	for _, in := range v.waiting {
		detail.WaitingInputs = append(detail.WaitingInputs, WaitingInputView{
			DeliveryID: in.DeliveryID,
			Endpoint:   in.Endpoint,
			ReceivedAt: in.ReceivedAt,
		})
	}
	detail.Actions = s.legalActions(v)
	return detail, nil
}

func (s *Server) GetActionOutcome(ctx context.Context, id int64) (*ActionOutcome, error) {
	repo := s.actionRepo()
	if repo == nil {
		return nil, errors.New("stage store not configured")
	}
	rec, err := repo.GetAction(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := outcomeFromRecord(*rec)
	out.Replayed = false
	return &out, nil
}

var actionChatCommands = map[string]string{
	"retry":      ActionRetry,
	"retry-with": ActionRetryWith,
	"unblock":    ActionUnblock,
	"cancel":     ActionStop,
	"stop":       ActionStop,
	"skip":       ActionSkipStage,
	"skip-stage": ActionSkipStage,
	"recheck":    ActionRecheck,
	"re-review":  ActionReReview,
	"rereview":   ActionReReview,
	"fix-ci":     ActionFixCI,
	"fix-review": ActionFixReview,
	"rebind":     ActionRebind,
	"merge-now":  ActionMergeNow,
	"approve":    ActionApprove,
	"reject":     ActionReject,
	"clean":      ActionCleanWorkspace,
}
