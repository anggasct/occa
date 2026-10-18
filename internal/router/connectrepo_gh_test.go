package router

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func connectTestEndpoints() []ConnectEndpoint {
	return []ConnectEndpoint{
		{Name: "github-acme-widget-review", Path: "/webhooks/github-acme-widget-review", Secret: "s1", Workflow: "review", Platform: "discord", ChannelID: "chan-1", Repo: "acme/widget", Events: []string{"pull_request"}},
		{Name: "github-acme-widget-pr-fix", Path: "/webhooks/github-acme-widget-pr-fix", Secret: "s2", Workflow: "fix", Platform: "discord", ChannelID: "chan-1", Repo: "acme/widget", Events: []string{"pull_request_review"}},
	}
}

func writeConnectTestConfig(t *testing.T) string {
	t.Helper()
	content := "webhooks:\n" +
		"  endpoints:\n" +
		"    - name: existing\n" +
		"      path: /webhooks/existing\n" +
		"runtime:\n" +
		"  loop:\n" +
		"    min_interval: 30s\n"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestAppendConnectEndpointsLandsInsideList(t *testing.T) {
	path := writeConnectTestConfig(t)
	if err := appendConnectEndpoints(path, connectTestEndpoints()); err != nil {
		t.Fatalf("append: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	content := string(data)
	for _, ep := range connectTestEndpoints() {
		if !strings.Contains(content, ep.Path) {
			t.Errorf("appended config must contain %q:\n%s", ep.Path, content)
		}
	}
	endpointsIdx := strings.Index(content, "endpoints:")
	runtimeIdx := strings.Index(content, "\nruntime:")
	for _, ep := range connectTestEndpoints() {
		at := strings.Index(content, ep.Path)
		if at < endpointsIdx || (runtimeIdx >= 0 && at > runtimeIdx) {
			t.Errorf("entry %q must sit inside the endpoints list (endpoints at %d, runtime at %d, entry at %d)", ep.Path, endpointsIdx, runtimeIdx, at)
		}
	}
	if !strings.Contains(content, "/webhooks/existing") || !strings.Contains(content, "min_interval: 30s") {
		t.Errorf("append must preserve existing content:\n%s", content)
	}
}

func TestAppendConnectEndpointsIdempotent(t *testing.T) {
	path := writeConnectTestConfig(t)
	endpoints := connectTestEndpoints()
	if err := appendConnectEndpoints(path, endpoints); err != nil {
		t.Fatalf("first append: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if err := appendConnectEndpoints(path, endpoints); err != nil {
		t.Fatalf("second append: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("re-run must not duplicate entries:\n%s", string(after))
	}
	for _, ep := range endpoints {
		if n := strings.Count(string(after), "path: "+ep.Path+"\n"); n != 1 {
			t.Errorf("path %q appears %d times, want 1", ep.Path, n)
		}
	}
}

func TestAppendConnectEndpointsSkipsPresentPaths(t *testing.T) {
	path := writeConnectTestConfig(t)
	endpoints := connectTestEndpoints()
	partial := endpoints[:1]
	if err := appendConnectEndpoints(path, partial); err != nil {
		t.Fatalf("partial append: %v", err)
	}
	if err := appendConnectEndpoints(path, endpoints); err != nil {
		t.Fatalf("full append: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if n := strings.Count(string(data), "path: "+partial[0].Path+"\n"); n != 1 {
		t.Errorf("already-present path appears %d times, want 1", n)
	}
	if !strings.Contains(string(data), endpoints[1].Path) {
		t.Errorf("missing path must still be appended:\n%s", string(data))
	}
}

func TestAppendConnectEndpointsPrefixPathStillAppended(t *testing.T) {
	path := writeConnectTestConfig(t)
	endpoints := connectTestEndpoints()
	if err := appendConnectEndpoints(path, endpoints[1:]); err != nil {
		t.Fatalf("fix-only append: %v", err)
	}
	if err := appendConnectEndpoints(path, endpoints); err != nil {
		t.Fatalf("full append: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if n := strings.Count(string(data), "path: "+endpoints[0].Path+"\n"); n != 1 {
		t.Errorf("review path must be appended even though pr-fix exists (count=%d):\n%s", n, string(data))
	}
}

func TestAppendConnectEndpointsWithoutListFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "webhooks:\n  bind: 127.0.0.1:8787\nruntime:\n  loop:\n    min_interval: 30s\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := appendConnectEndpoints(path, connectTestEndpoints()); err == nil {
		t.Fatal("config without an endpoints list must fail loudly")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(data) != content {
		t.Errorf("failed append must leave the config untouched:\n%s", string(data))
	}
}
