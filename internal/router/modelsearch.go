package router

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

func SearchModelIDs(models map[string]json.RawMessage, query string) []string {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}

	type rankedModel struct {
		id   string
		rank int
	}
	matches := make([]rankedModel, 0, len(models))
	for id := range models {
		lowerID := strings.ToLower(id)
		rank := -1
		switch {
		case lowerID == query:
			rank = 0
		case strings.HasPrefix(lowerID, query):
			rank = 1
		case modelSegmentHasPrefix(lowerID, query):
			rank = 2
		case strings.Contains(lowerID, query):
			rank = 3
		}
		if rank >= 0 {
			matches = append(matches, rankedModel{id: id, rank: rank})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].rank != matches[j].rank {
			return matches[i].rank < matches[j].rank
		}
		return matches[i].id < matches[j].id
	})
	ids := make([]string, len(matches))
	for i, match := range matches {
		ids[i] = match.id
	}
	return ids
}

func modelSegmentHasPrefix(modelID, query string) bool {
	for _, segment := range strings.Split(modelID, "/") {
		if strings.HasPrefix(segment, query) {
			return true
		}
	}
	return false
}

func searchProviderByID(providers relay.Providers, providerID string) (relay.Provider, bool) {
	provider, ok := providerByID(providers, providerID)
	if !ok {
		return relay.Provider{}, false
	}
	if len(providers.Connected) == 0 {
		return provider, true
	}
	for _, connectedID := range providers.Connected {
		if connectedID == providerID {
			return provider, true
		}
	}
	return relay.Provider{}, false
}

func searchModelAvailable(providers relay.Providers, providerID, modelID, query string) bool {
	provider, ok := searchProviderByID(providers, providerID)
	if !ok {
		return false
	}
	for _, id := range SearchModelIDs(provider.Models, query) {
		if id == modelID {
			return true
		}
	}
	return false
}

func (r *Router) modelSearchUnavailableView(_ string, providers relay.Providers, providerID, query, prefix string) (string, []channel.Button, error) {
	text := prefix + fmt.Sprintf("Provider: %s — search `%s`\n⚠️ Provider is unknown or no longer connected.\nUse /model to browse connected providers.", providerID, query)
	buttons, err := r.modelSearchNavigation(providerID, query, 0, 1, false)
	if err != nil {
		return "", nil, err
	}
	return text, buttons, nil
}
