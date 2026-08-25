package router

import (
	"context"
	"testing"
)

func TestModelPrecedenceAgainstRealStore(t *testing.T) {
	r, _, _, st := newSQLiteBackedRouter(t, "")
	ctx := context.Background()

	if err := st.ChannelRepo().UpsertModel(ctx, "discord", "parent", "anthropic/claude-3"); err != nil {
		t.Fatalf("Upsert channel model: %v", err)
	}
	if err := st.OverrideRepo().UpsertRole(ctx, "discord", "parent", "user1", "admin"); err != nil {
		t.Fatalf("Upsert role: %v", err)
	}
	if err := st.OverrideRepo().UpsertModel(ctx, "discord", "parent", "user1", "openai/gpt-4o"); err != nil {
		t.Fatalf("Upsert personal model: %v", err)
	}
	if err := st.SessionRepo().SetActive(ctx, "discord", "parent", "thread-1", "", "legacy-session", 1); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := st.SessionRepo().SetModel(ctx, "discord", "parent", "thread-1", "", "zai-coding-plan/glm-5.2"); err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if err := st.ThreadConfigRepo().UpsertModel(ctx, "discord", "parent", "thread-1", "openai/gpt-4o"); err != nil {
		t.Fatalf("Upsert thread model: %v", err)
	}

	thread := ownedThreadMsg("thread-1", "hello", &fakeReplyCtx{})
	resolution, err := r.resolveModel(ctx, thread)
	if err != nil {
		t.Fatalf("resolve thread model: %v", err)
	}
	if resolution.source != modelSourceThread || formatModelRef(*resolution.model) != "openai/gpt-4o" {
		t.Fatalf("thread resolution = %+v", resolution)
	}

	if err := st.ThreadConfigRepo().UpsertModel(ctx, "discord", "parent", "thread-1", ""); err != nil {
		t.Fatalf("clear thread model: %v", err)
	}
	resolution, err = r.resolveModel(ctx, thread)
	if err != nil {
		t.Fatalf("resolve channel model: %v", err)
	}
	if resolution.source != modelSourceChannel || formatModelRef(*resolution.model) != "anthropic/claude-3" {
		t.Fatalf("channel resolution = %+v", resolution)
	}

	if err := st.ChannelRepo().UpsertModel(ctx, "discord", "parent", ""); err != nil {
		t.Fatalf("clear channel model: %v", err)
	}
	resolution, err = r.resolveModel(ctx, thread)
	if err != nil {
		t.Fatalf("resolve personal model: %v", err)
	}
	if resolution.source != modelSourcePersonal || formatModelRef(*resolution.model) != "openai/gpt-4o" {
		t.Fatalf("personal resolution = %+v", resolution)
	}
}
