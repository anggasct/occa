package router

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

var errUnreachableForTest = errors.New("agent unreachable")

func lastSend(t *testing.T, reply *fakeReplyCtx) string {
	t.Helper()
	if len(reply.sends) == 0 {
		t.Fatal("nothing was sent")
	}
	return reply.sends[len(reply.sends)-1]
}

func TestCommandReplyEscapesMarkup(t *testing.T) {
	r, _, reply := newTestRouter()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", Workdir: "/tmp/a<b&c",
	}

	if err := r.Route(context.Background(), msg("/occa:dir", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}

	got := lastSend(t, reply)
	if strings.Contains(got, "a<b") || !strings.Contains(got, "a&lt;b&amp;c") {
		t.Fatalf("workdir markup not escaped: %q", got)
	}
}

func TestCommandErrorReplyEscapesMarkup(t *testing.T) {
	r, _, reply := newTestRouter()

	if err := r.Route(context.Background(), msg("/occa:model pro<vider", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}

	got := lastSend(t, reply)
	if strings.Contains(got, "pro<vider") || !strings.Contains(got, "pro&lt;vider") {
		t.Fatalf("error text not escaped: %q", got)
	}
}

func TestAgentUnreachableReplyStillDelivered(t *testing.T) {
	r, _, reply := newTestRouter()
	r.instances.(*fakeInstanceProvider).err = errUnreachableForTest

	if err := r.Route(context.Background(), msg("hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if got := lastSend(t, reply); !strings.Contains(got, "Agent unreachable") {
		t.Fatalf("unexpected reply: %q", got)
	}
}

func TestPermissionPromptEscapesToolName(t *testing.T) {
	r, _, _ := newTestRouter()
	h := &permissionPromptHandler{
		encode: func(text string) string { return r.inline("telegram", text) },
	}

	got := h.promptText(relay.PermissionRequest{Permission: "write", Tool: "bash<script>"})
	if strings.Contains(got, "<script>") || !strings.Contains(got, "&lt;script&gt;") {
		t.Fatalf("tool name not escaped: %q", got)
	}
}

func TestOutboundRenderingIsNotAppliedTwice(t *testing.T) {
	r, _, _ := newTestRouter()

	got := r.inline("telegram", "value <x> & more")
	if strings.Contains(got, "&amp;lt;") {
		t.Fatalf("double-escaped: %q", got)
	}
	if !strings.Contains(got, "&lt;x&gt;") || !strings.Contains(got, "&amp; more") {
		t.Fatalf("unexpected escaping: %q", got)
	}
}
