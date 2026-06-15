// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// SearchResult is one global-search hit. URL is a Beacon SPA URL that can be
// applied to window.location/search params by the frontend command palette.
type SearchResult struct {
	Type     string         `json:"type"`
	ID       string         `json:"id"`
	Label    string         `json:"label"`
	Subtitle string         `json:"subtitle,omitempty"`
	URL      string         `json:"url"`
	Score    int            `json:"score"`
	Matched  string         `json:"matched,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SearchResponse is returned by GET /api/v1/search.
type SearchResponse struct {
	Query string         `json:"query"`
	Items []SearchResult `json:"items"`
}
