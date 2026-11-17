package web

import "github.com/anggasct/occa/internal/webhook"

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code     string `json:"code"`
	Sentence string `json:"sentence"`
	Detail   string `json:"detail"`
}

type inboxRowDTO struct {
	WorkItemID      string `json:"work_item_id"`
	Repo            string `json:"repo"`
	Number          int    `json:"number"`
	Title           string `json:"title"`
	Stage           string `json:"stage"`
	Attempt         int    `json:"attempt"`
	State           string `json:"state"`
	Derived         string `json:"derived"`
	BlockerCode     string `json:"blocker_code"`
	BlockerSentence string `json:"blocker_sentence"`
	PrimaryAction   string `json:"primary_action"`
	AgeSeconds      int64  `json:"age_seconds"`
	WaitingInputs   int    `json:"waiting_inputs"`
}

type inboxResponse struct {
	Rows       []inboxRowDTO `json:"rows"`
	NextCursor string        `json:"next_cursor"`
}

type workItemDTO struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	SpecRef string `json:"spec_ref"`
	SpecSHA string `json:"spec_sha"`
	State   string `json:"state"`
}

type phaseDTO struct {
	Phase       string `json:"phase"`
	Attempt     int    `json:"attempt"`
	State       string `json:"state"`
	BlockerCode string `json:"blocker_code"`
}

type stageRunDTO struct {
	ID                 int64  `json:"id"`
	Stage              string `json:"stage"`
	Attempt            int    `json:"attempt"`
	State              string `json:"state"`
	StartedAt          int64  `json:"started_at"`
	EndedAt            int64  `json:"ended_at"`
	LastHeartbeatAt    int64  `json:"last_heartbeat_at"`
	ArtifactRef        string `json:"artifact_ref"`
	GateResult         string `json:"gate_result"`
	WorkspacePath      string `json:"workspace_path"`
	WorkspaceKeptUntil int64  `json:"workspace_kept_until"`
	BlockerCode        string `json:"blocker_code"`
	BlockerDetail      string `json:"blocker_detail"`
	AutoRetryUsed      int    `json:"auto_retry_used"`
	ManualRetryCount   int    `json:"manual_retry_count"`
	TriggeredBy        string `json:"triggered_by"`
}

type waitingInputDTO struct {
	DeliveryID string `json:"delivery_id"`
	Endpoint   string `json:"endpoint"`
	ReceivedAt int64  `json:"received_at"`
}

type workItemDetailResponse struct {
	Item          workItemDTO       `json:"item"`
	Phases        phaseDTO          `json:"phases"`
	Stages        []stageRunDTO     `json:"stages"`
	WaitingInputs []waitingInputDTO `json:"waiting_inputs"`
	Actions       []string          `json:"actions"`
}

type actionRequestBody struct {
	Reason  string            `json:"reason"`
	Confirm string            `json:"confirm"`
	Params  map[string]string `json:"params"`
}

type actionAcceptedResponse struct {
	ActionID   int64  `json:"action_id"`
	StageRunID int64  `json:"stage_run_id"`
	State      string `json:"state"`
	ThreadLine string `json:"thread_line"`
}

type actionResultResponse struct {
	ActionID int64  `json:"action_id"`
	Action   string `json:"action"`
	Actor    string `json:"actor"`
	Result   string `json:"result"`
	Code     string `json:"code"`
	Sentence string `json:"sentence"`
}

type pairRequest struct {
	Code string `json:"code"`
}

type pairResponse struct {
	CSRFToken string `json:"csrf_token"`
}

func toInboxRow(r webhook.InboxRow) inboxRowDTO {
	return inboxRowDTO{
		WorkItemID:      r.WorkItemID,
		Repo:            r.Repo,
		Number:          r.Number,
		Title:           r.Title,
		Stage:           r.Stage,
		Attempt:         r.Attempt,
		State:           r.State,
		Derived:         r.Derived,
		BlockerCode:     r.BlockerCode,
		BlockerSentence: r.BlockerSentence,
		PrimaryAction:   r.PrimaryAction,
		AgeSeconds:      r.AgeSeconds,
		WaitingInputs:   r.WaitingInputs,
	}
}

func toDetailResponse(d *webhook.WorkItemDetailView) workItemDetailResponse {
	resp := workItemDetailResponse{
		Item: workItemDTO{
			ID:      d.Item.ID,
			Kind:    d.Item.Kind,
			SpecRef: d.Item.SpecRef,
			SpecSHA: d.Item.SpecSHA,
			State:   d.Item.State,
		},
		Phases: phaseDTO{
			Phase:       d.Phases.Phase,
			Attempt:     d.Phases.Attempt,
			State:       d.Phases.State,
			BlockerCode: d.Phases.BlockerCode,
		},
		Stages:        make([]stageRunDTO, 0, len(d.Stages)),
		WaitingInputs: make([]waitingInputDTO, 0, len(d.WaitingInputs)),
		Actions:       d.Actions,
	}
	if resp.Actions == nil {
		resp.Actions = []string{}
	}
	for _, st := range d.Stages {
		resp.Stages = append(resp.Stages, stageRunDTO{
			ID:                 st.ID,
			Stage:              st.Stage,
			Attempt:            st.Attempt,
			State:              st.State,
			StartedAt:          st.StartedAt,
			EndedAt:            st.EndedAt,
			LastHeartbeatAt:    st.LastHeartbeatAt,
			ArtifactRef:        st.ArtifactRef,
			GateResult:         st.GateResult,
			WorkspacePath:      st.WorkspacePath,
			WorkspaceKeptUntil: st.WorkspaceKeptUntil,
			BlockerCode:        st.BlockerCode,
			BlockerDetail:      st.BlockerDetail,
			AutoRetryUsed:      st.AutoRetryUsed,
			ManualRetryCount:   st.ManualRetryCount,
			TriggeredBy:        st.TriggeredBy,
		})
	}
	for _, wi := range d.WaitingInputs {
		resp.WaitingInputs = append(resp.WaitingInputs, waitingInputDTO{
			DeliveryID: wi.DeliveryID,
			Endpoint:   wi.Endpoint,
			ReceivedAt: wi.ReceivedAt,
		})
	}
	return resp
}
