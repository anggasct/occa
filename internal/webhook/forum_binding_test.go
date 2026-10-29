package webhook

import (
	"context"
	"testing"
)

func TestForumParentBoundAsPRThread(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	ep := srv.endpoints["/fix"]
	ep.ForumChannelID = ep.ChannelID

	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseQueued)

	if len(rec.sent) != 1 {
		t.Fatalf("parent sends = %d, want one forum post", len(rec.sent))
	}
	row, err := st.PRQueueRepo().Get(ctx, prKey)
	if err != nil || row == nil {
		t.Fatalf("pr queue row = %+v, err %v", row, err)
	}
	if row.ThreadID != "parent-1" || row.RootMsgID != "parent-1" {
		t.Fatalf("persisted binding = thread %q root %q, want the forum post for both", row.ThreadID, row.RootMsgID)
	}
	if detail, ok := srv.tasks.SurfaceDetail(prKey); !ok || detail.ThreadID != "parent-1" {
		t.Fatalf("surface thread = %+v, want parent-1", detail)
	}
	if res := srv.resolvePRBinding(ctx, prKey, "pull_request_review", surfaceEnvelope()); res.ThreadID != "parent-1" {
		t.Fatalf("binding thread = %q, want parent-1 so a fix run is not refused", res.ThreadID)
	}
}

func TestForumParentBindingSkipsTextChannels(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"

	srv.syncPhaseParent(ctx, srv.endpoints["/fix"], prKey, surfaceEnvelope(), PhaseQueued)

	if row, _ := st.PRQueueRepo().Get(ctx, prKey); row != nil && row.ThreadID != "" {
		t.Fatalf("text-channel parent bound as thread %q, want no binding", row.ThreadID)
	}
	if detail, ok := srv.tasks.SurfaceDetail(prKey); ok && detail.ThreadID != "" {
		t.Fatalf("surface thread = %q, want empty for a text channel", detail.ThreadID)
	}
}

func TestForumParentBindingKeepsExistingThread(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#181"
	srv.tasks.Upsert("acme/widget", "feat/x", "", "thread-9", "")
	srv.tasks.LinkPRThread(prKey, TaskThreadRecord{Repo: "acme/widget", Branch: "feat/x"})
	ep := srv.endpoints["/fix"]
	ep.ForumChannelID = ep.ChannelID

	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseQueued)

	if len(rec.sent) != 1 {
		t.Fatalf("parent sends = %d, want one", len(rec.sent))
	}
	if rec, ok := srv.tasks.ResolvePRThread(prKey); !ok || rec.ThreadID != "thread-9" {
		t.Fatalf("PR thread = %+v, want the existing thread-9 kept", rec)
	}
	if detail, ok := srv.tasks.SurfaceDetail(prKey); !ok || detail.ThreadID != "thread-9" {
		t.Fatalf("surface thread = %+v, want thread-9", detail)
	}
}
