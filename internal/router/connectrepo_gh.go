package router

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func NewConnectRepoGHDeps(publicBaseURL, configPath string) *ConnectRepoDeps {
	base := strings.TrimSpace(publicBaseURL)
	cfgPath := strings.TrimSpace(configPath)
	if base == "" || cfgPath == "" {
		return nil
	}
	return &ConnectRepoDeps{
		PublicBaseURL:   base,
		CheckAdmin:      ghCheckAdmin,
		CreateHook:      ghCreateHook,
		AppendEndpoints: func(endpoints []ConnectEndpoint) error { return appendConnectEndpoints(cfgPath, endpoints) },
		Restart:         ghRestartOcca,
		PingHook:        ghPingHook,
	}
}

func ghCheckAdmin(ctx context.Context, repo string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", "api", "repos/"+repo, "--jq", ".permissions.admin")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("gh admin check: %w", err)
	}
	return strings.TrimSpace(string(out)) == "true", nil
}

func ghCreateHook(ctx context.Context, repo, hookURL, secret string, events []string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	args := []string{"api", "repos/" + repo + "/hooks", "-f", "name=web", "-f", "active=true", "-f", "config[url]=" + hookURL, "-f", "config[content_type]=json", "-f", "config[secret]=" + secret}
	for _, event := range events {
		if s := strings.TrimSpace(event); s != "" {
			args = append(args, "-f", "events[]="+s)
		}
	}
	args = append(args, "--jq", ".id")
	cmd := exec.CommandContext(ctx, "gh", args...)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("gh create hook: %w", err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("gh create hook: empty id")
	}
	return id, nil
}

func ghPingHook(ctx context.Context, repo, hookID string) error {
	hookID = strings.TrimSpace(hookID)
	if hookID == "" {
		return fmt.Errorf("missing hook id")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", "api", "repos/"+repo+"/hooks/"+hookID+"/pings", "-X", "POST", "--silent")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gh ping: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func ghRestartOcca(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", "systemctl restart occa")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("restart: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func appendConnectEndpoints(configPath string, endpoints []ConnectEndpoint) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	content := string(data)
	if !strings.Contains(content, "endpoints:") {
		return fmt.Errorf("config has no endpoints list: refusing to append")
	}
	var missing []ConnectEndpoint
	for _, ep := range endpoints {
		if configHasEndpointPath(content, ep.Path) {
			continue
		}
		missing = append(missing, ep)
	}
	if len(missing) == 0 {
		return nil
	}
	var sb strings.Builder
	for _, ep := range missing {
		sb.WriteString("    - name: " + ep.Name + "\n")
		sb.WriteString("      path: " + ep.Path + "\n")
		sb.WriteString("      auth: github_hmac_sha256\n")
		sb.WriteString("      secret: " + ep.Secret + "\n")
		sb.WriteString("      workflow: " + ep.Workflow + "\n")
		sb.WriteString("      platform: " + ep.Platform + "\n")
		sb.WriteString("      channel_id: '" + strings.ReplaceAll(ep.ChannelID, "'", "''") + "'\n")
		sb.WriteString("      repository: " + ep.Repo + "\n")
		sb.WriteString("      workspace:\n")
		sb.WriteString("        type: none\n")
		sb.WriteString("      prompt: Process " + ep.Workflow + " webhook for " + ep.Repo + "\n")
		sb.WriteString("      admit:\n")
		for _, admit := range connectAdmitRules(ep.Workflow) {
			sb.WriteString(admit)
		}
	}
	snippet := sb.String()
	if idx := strings.Index(content, "\nruntime:"); idx >= 0 {
		content = content[:idx] + "\n" + snippet + content[idx:]
	} else {
		if !strings.HasSuffix(content, "\n") {
			content += "\n"
		}
		content += snippet
	}
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func configHasEndpointPath(content, path string) bool {
	for line := range strings.Lines(content) {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "path:")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		if strings.Trim(fields[0], `"'`) == path {
			return true
		}
	}
	return false
}

func connectAdmitRules(workflow string) []string {
	switch strings.TrimSpace(workflow) {
	case "review":
		return []string{"        - event: pull_request\n          actions: [opened, reopened]\n"}
	case "fix":
		return []string{"        - event: pull_request_review\n          actions: [submitted]\n"}
	case "merge":
		return []string{
			"        - event: pull_request_review\n          actions: [submitted]\n",
			"        - event: check_suite\n          actions: [completed]\n",
		}
	case "merged":
		return []string{"        - event: pull_request\n          actions: [closed]\n"}
	default:
		return []string{"        - event: pull_request\n          actions: [opened]\n"}
	}
}
