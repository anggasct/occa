package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"
)

type OpenAPIDoc struct {
	Info  OpenAPIInfo    `json:"info"`
	Paths map[string]any `json:"paths"`
}

type OpenAPIInfo struct {
	Version string `json:"version"`
}

var requiredEndpoints = []string{
	"/session",
	"/session/{id}/prompt_async",
	"/session/{id}/command",
	"/event",
}

var pathParamRegexp = regexp.MustCompile(`\{[^}]+\}`)

func normalizePath(p string) string {
	return pathParamRegexp.ReplaceAllString(p, "{}")
}

func Discover(ctx context.Context, baseURL string) (*OpenAPIDoc, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/doc", nil)
	if err != nil {
		return nil, fmt.Errorf("relay: discovery: build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("relay: discovery: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay: discovery: unexpected status %d", resp.StatusCode)
	}

	var doc OpenAPIDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("relay: discovery: decode: %w", err)
	}
	return &doc, nil
}

func (d *OpenAPIDoc) MissingEndpoints() []string {
	docPaths := make(map[string]struct{}, len(d.Paths))
	for p := range d.Paths {
		docPaths[normalizePath(p)] = struct{}{}
	}

	var missing []string
	for _, ep := range requiredEndpoints {
		if _, ok := docPaths[normalizePath(ep)]; !ok {
			missing = append(missing, ep)
		}
	}
	return missing
}
