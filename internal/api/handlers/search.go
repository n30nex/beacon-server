// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

const defaultSearchLimit int32 = 24
const maxSearchLimit int32 = 60
const searchProviderBudget = time.Second
const searchTotalBudget = 1500 * time.Millisecond

var searchTypeOrder = map[string]int{
	"page":     0,
	"packet":   1,
	"node":     2,
	"observer": 3,
	"channel":  4,
	"route":    5,
	"trace":    6,
}

type searchPage struct {
	label    string
	subtitle string
	url      string
	aliases  string
}

var searchablePages = []searchPage{
	{"Home", "Beacon data overview", "/?tab=Home", "dashboard summary"},
	{"Live", "Live packet operations map", "/?tab=Live", "console realtime"},
	{"Map", "Node map and route replay", "/?tab=Map", "geography"},
	{"Packets", "Packet feed and analyzer", "/?tab=Packets", "messages traffic"},
	{"Channels", "Decoded channel messages", "/?tab=Channels", "chat catalogue"},
	{"Nodes", "Node directory", "/?tab=Nodes", "radios devices"},
	{"Observers", "Observer fleet", "/?tab=Observers", "gateways"},
	{"Analytics", "Mesh analytics and RF data", "/?tab=Analytics", "atlas stats statistics"},
	{"Netgraph", "Experimental 3D route topology", "/?tab=Netgraph", "network graph"},
	{"Routes", "Known route catalogue", "/?tab=Routes", "paths"},
	{"Traces", "Trace and ping series", "/?tab=Traces", "pings"},
	{"System", "API, readiness, broker, and live bus status", "/?tab=System", "diagnostics health ready readiness slo"},
	{"Investigations", "Saved browser-local workspaces", "/?tab=Investigations", "saved workspace watchlist case"},
}

// SearchRouter mounts global search endpoints.
func SearchRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", globalSearch(reader))
	return r
}

// globalSearch godoc
//
//	@Summary	Global Beacon search
//	@Tags		Search
//	@Produce	json
//	@Param		q		query	string	false	"Search query; empty returns page shortcuts"
//	@Param		types	query	string	false	"Comma-separated result types: page,packet,node,observer,channel,route,trace"
//	@Param		iatas	query	string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region	query	string	false	"Filter by region slug"
//	@Param		regionId	query	int		false	"Filter by region ID"
//	@Param		limit	query	int		false	"Max results, clamped to 1-60"
//	@Success	200		{object}	api.SearchResponse
//	@Failure	400		{object}	handlers.APIError
//	@Router		/search [get]
func globalSearch(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		limit, err := parseSearchLimit(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		allowed := parseSearchTypes(r.URL.Query().Get("types"))
		iatas := parseIATAs(r)
		if r.URL.Query().Get("regionId") != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), r.URL.Query().Get("regionId"), r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		iatas = uniqueStrings(iatas)

		items, providers, partial := collectSearchResults(r.Context(), reader, query, allowed, iatas, limit)
		respond(w, http.StatusOK, api.SearchResponse{Query: query, Items: items, Partial: partial, Providers: providers})
	}
}

func parseSearchLimit(r *http.Request) (int32, error) {
	limit := defaultSearchLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, err
		}
		limit = int32(value)
	}
	if limit < 1 {
		limit = 1
	}
	if limit > maxSearchLimit {
		limit = maxSearchLimit
	}
	return limit, nil
}

func parseSearchTypes(raw string) map[string]bool {
	allowed := map[string]bool{}
	if raw == "" {
		for k := range searchTypeOrder {
			allowed[k] = true
		}
		return allowed
	}
	for _, part := range strings.Split(raw, ",") {
		t := strings.ToLower(strings.TrimSpace(part))
		if _, ok := searchTypeOrder[t]; ok {
			allowed[t] = true
		}
	}
	return allowed
}

type searchCollector struct {
	query   string
	needle  string
	limit   int
	allowed map[string]bool
	seen    map[string]struct{}
	items   []api.SearchResult
	err     error
}

type searchProviderResult struct {
	name     string
	items    []api.SearchResult
	err      error
	duration time.Duration
}

func collectSearchResults(ctx context.Context, reader api.Reader, query string, allowed map[string]bool, iatas []string, limit int32) ([]api.SearchResult, map[string]api.SearchProviderStatus, bool) {
	c := &searchCollector{
		query:   query,
		needle:  strings.ToLower(query),
		limit:   int(limit),
		allowed: allowed,
		seen:    map[string]struct{}{},
	}
	c.addPages()
	providers := map[string]api.SearchProviderStatus{"page": {Status: "ok", DurationMs: 0}}
	if strings.TrimSpace(query) == "" {
		return c.sorted(), providers, false
	}

	type providerSpec struct {
		name string
		run  func(context.Context, *searchCollector)
	}
	specs := []providerSpec{
		{name: "packet", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addPacket(providerCtx, reader)
		}},
		{name: "node", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addNodes(providerCtx, reader, iatas)
		}},
		{name: "observer", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addObservers(providerCtx, reader, iatas)
		}},
		{name: "channel", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addChannels(providerCtx, reader, iatas)
		}},
		{name: "route", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addRoutes(providerCtx, reader, iatas)
		}},
		{name: "trace", run: func(providerCtx context.Context, collector *searchCollector) {
			collector.addTraces(providerCtx, reader, iatas)
		}},
	}
	totalCtx, cancel := context.WithTimeout(ctx, searchTotalBudget)
	defer cancel()
	resultCh := make(chan searchProviderResult, len(specs))
	pending := map[string]struct{}{}
	for _, spec := range specs {
		if !allowed[spec.name] {
			continue
		}
		pending[spec.name] = struct{}{}
		go func(spec providerSpec) {
			started := time.Now()
			providerCtx, providerCancel := context.WithTimeout(totalCtx, searchProviderBudget)
			defer providerCancel()
			collector := &searchCollector{query: query, needle: strings.ToLower(query), limit: int(limit), allowed: allowed, seen: map[string]struct{}{}}
			spec.run(providerCtx, collector)
			err := collector.err
			if providerCtx.Err() != nil {
				err = errors.Join(err, providerCtx.Err())
			}
			resultCh <- searchProviderResult{name: spec.name, items: collector.items, err: err, duration: time.Since(started)}
		}(spec)
	}

	partial := false
	for len(pending) > 0 {
		select {
		case result := <-resultCh:
			delete(pending, result.name)
			status := "ok"
			if result.err != nil {
				partial = true
				status = "error"
				if errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, context.Canceled) {
					status = "timeout"
				}
			}
			providers[result.name] = api.SearchProviderStatus{Status: status, DurationMs: result.duration.Milliseconds()}
			for _, item := range result.items {
				c.add(item)
			}
		case <-totalCtx.Done():
			partial = true
			for name := range pending {
				providers[name] = api.SearchProviderStatus{Status: "timeout", DurationMs: searchTotalBudget.Milliseconds()}
				delete(pending, name)
			}
		}
	}
	return c.sorted(), providers, partial
}

func (c *searchCollector) add(item api.SearchResult) {
	if !c.allowed[item.Type] {
		return
	}
	key := item.Type + ":" + item.ID
	if _, ok := c.seen[key]; ok {
		return
	}
	c.seen[key] = struct{}{}
	c.items = append(c.items, item)
}

func (c *searchCollector) sorted() []api.SearchResult {
	sort.SliceStable(c.items, func(i, j int) bool {
		if c.items[i].Score != c.items[j].Score {
			return c.items[i].Score > c.items[j].Score
		}
		if searchTypeOrder[c.items[i].Type] != searchTypeOrder[c.items[j].Type] {
			return searchTypeOrder[c.items[i].Type] < searchTypeOrder[c.items[j].Type]
		}
		return c.items[i].Label < c.items[j].Label
	})
	if len(c.items) > c.limit {
		return c.items[:c.limit]
	}
	return c.items
}

func (c *searchCollector) score(text string, base int) (int, bool) {
	if c.needle == "" {
		return base, true
	}
	idx := strings.Index(strings.ToLower(text), c.needle)
	if idx < 0 {
		return 0, false
	}
	score := base + 60 - min(idx, 60)
	if strings.EqualFold(text, c.query) {
		score += 80
	} else if strings.HasPrefix(strings.ToLower(text), c.needle) {
		score += 35
	}
	return score, true
}

func (c *searchCollector) addPages() {
	for _, p := range searchablePages {
		text := p.label + " " + p.subtitle + " " + p.aliases
		score, ok := c.score(text, 300)
		if !ok {
			continue
		}
		c.add(api.SearchResult{
			Type:     "page",
			ID:       strings.ToLower(p.label),
			Label:    p.label,
			Subtitle: p.subtitle,
			URL:      p.url,
			Score:    score,
			Matched: func() string {
				if strings.Contains(strings.ToLower(p.aliases), c.needle) && !strings.Contains(strings.ToLower(p.label+" "+p.subtitle), c.needle) {
					return "page alias"
				}
				return "page"
			}(),
		})
	}
}

func (c *searchCollector) addPacket(ctx context.Context, reader api.Reader) {
	if !c.allowed["packet"] || !isHex(c.query) || len(c.query)%2 != 0 || len(c.query) < 16 {
		return
	}
	hash, err := hex.DecodeString(strings.ToLower(c.query))
	if err != nil {
		return
	}
	packet, err := reader.GetPacket(ctx, hash)
	if err != nil || packet == nil {
		if ctx.Err() != nil {
			c.err = ctx.Err()
		}
		return
	}
	c.add(api.SearchResult{
		Type:     "packet",
		ID:       packet.PacketHash,
		Label:    strings.ToUpper(packet.PacketHash[:min(len(packet.PacketHash), 16)]),
		Subtitle: fmt.Sprintf("%s / %s / %d observations", packet.Header.PayloadTypeName, packet.Header.RouteTypeName, packet.ObservationCount),
		URL:      "/?tab=Packets&hash=" + packet.PacketHash,
		Score:    420,
		Matched:  "packet hash",
		Metadata: map[string]any{"packetHash": packet.PacketHash},
	})
}

func (c *searchCollector) addNodes(ctx context.Context, reader api.Reader, iatas []string) {
	if !c.allowed["node"] || len(c.needle) < 2 {
		return
	}
	var pubkey []byte
	if isHex(c.query) && len(c.query) == 64 {
		if decoded, err := hex.DecodeString(strings.ToLower(c.query)); err == nil {
			pubkey = decoded
		}
	}
	nameQuery := c.query
	if pubkey != nil {
		nameQuery = ""
	}
	page, err := reader.ListNodes(ctx, 0, iatas, nil, nil, pubkey, nameQuery, "", 0, int32(c.limit))
	if err != nil {
		c.err = err
		return
	}
	for _, node := range page.Items {
		label := stringValue(node.Name, strings.ToUpper(node.PublicKey[:min(len(node.PublicKey), 8)]))
		text := label + " " + node.PublicKey + " " + node.NodeTypeName
		score, ok := c.score(text, 240)
		if !ok {
			if pubkey == nil {
				continue
			}
			score = 390
		}
		iataText := nodeIATAs(node.IATAs)
		c.add(api.SearchResult{
			Type:     "node",
			ID:       node.ID.String(),
			Label:    label,
			Subtitle: strings.TrimSpace(strings.Join([]string{node.NodeTypeName, iataText}, " / ")),
			URL:      "/?tab=Nodes&nodeId=" + node.ID.String(),
			Score:    score,
			Matched:  "node",
			Metadata: map[string]any{"nodeId": node.ID.String(), "publicKey": node.PublicKey},
		})
	}
}

func (c *searchCollector) addObservers(ctx context.Context, reader api.Reader, iatas []string) {
	if !c.allowed["observer"] || len(c.needle) < 2 {
		return
	}
	page, err := reader.ListObservers(ctx, iatas, "", "", "", c.query, "", 0, int32(c.limit))
	if err != nil {
		c.err = err
		return
	}
	for _, obs := range page.Items {
		label := stringValue(obs.DisplayName, obs.ID.String()[:8])
		text := label + " " + obs.ID.String() + " " + obs.IATA + " " + stringValue(obs.ObserverType, "")
		score, ok := c.score(text, 230)
		if !ok {
			continue
		}
		c.add(api.SearchResult{
			Type:     "observer",
			ID:       obs.ID.String(),
			Label:    label,
			Subtitle: strings.TrimSpace(strings.Join([]string{obs.IATA, obs.Status, stringValue(obs.ObserverType, "")}, " / ")),
			URL:      "/?tab=Observers&observerId=" + obs.ID.String(),
			Score:    score,
			Matched:  "observer",
			Metadata: map[string]any{"observerId": obs.ID.String(), "iata": obs.IATA},
		})
	}
}

func (c *searchCollector) addChannels(ctx context.Context, reader api.Reader, iatas []string) {
	if !c.allowed["channel"] || len(c.needle) < 1 {
		return
	}
	var hash []byte
	if isHex(c.query) && len(c.query) == 2 {
		if decoded, err := hex.DecodeString(strings.ToLower(c.query)); err == nil {
			hash = decoded
		}
	}
	page, err := reader.ListChannels(ctx, min32(int32(c.limit*4), 200), hash, strings.Join(iatas, ","), 0)
	if err != nil {
		c.err = err
		return
	}
	for _, ch := range page.Items {
		label := channelLabel(ch)
		text := label + " " + ch.ChannelHash
		score, ok := c.score(text, 220)
		if !ok && len(hash) == 0 {
			continue
		}
		c.add(api.SearchResult{
			Type:     "channel",
			ID:       strconv.Itoa(ch.ID),
			Label:    label,
			Subtitle: fmt.Sprintf("hash %s / %s", strings.ToUpper(ch.ChannelHash), keyState(ch)),
			URL:      "/?tab=Channels&channelId=" + strconv.Itoa(ch.ID),
			Score:    score,
			Matched:  "channel",
			Metadata: map[string]any{"channelId": ch.ID, "channelHash": ch.ChannelHash},
		})
	}
}

func (c *searchCollector) addRoutes(ctx context.Context, reader api.Reader, iatas []string) {
	if !c.allowed["route"] || len(c.needle) < 1 {
		return
	}
	if routeID, err := strconv.ParseInt(c.query, 10, 64); err == nil && routeID > 0 {
		route, getErr := reader.GetKnownRoute(ctx, routeID)
		if getErr != nil || route == nil {
			if ctx.Err() != nil {
				c.err = ctx.Err()
			}
			return
		}
		c.add(api.SearchResult{Type: "route", ID: strconv.FormatInt(route.ID, 10), Label: fmt.Sprintf("%s route #%d", route.IATA, route.ID), Subtitle: fmt.Sprintf("%d hops / %d observations", route.HopCount, route.ObservationCount), URL: fmt.Sprintf("/?tab=Map&routeId=%d&routeReplay=1", route.ID), Score: 400, Matched: "route id", Metadata: map[string]any{"routeId": route.ID, "iata": route.IATA}})
		return
	}
	// Textual route scans are only meaningful for an exact IATA. Avoid reusing
	// the expensive unfiltered catalogue query as a global search provider.
	if len(c.query) != 3 || strings.IndexFunc(strings.ToUpper(c.query), func(r rune) bool { return r < 'A' || r > 'Z' }) >= 0 {
		return
	}
	if len(iatas) == 0 {
		iatas = []string{strings.ToUpper(c.query)}
	}
	targetIATAs := iatas
	if len(targetIATAs) == 0 {
		targetIATAs = []string{""}
	}
	for _, iata := range targetIATAs[:min(len(targetIATAs), 8)] {
		var iatas []string
		if iata != "" {
			iatas = []string{iata}
		}
		routes, err := reader.ListKnownRoutes(ctx, iatas, 0, time.Time{}, min32(int32(c.limit*3), 120))
		if err != nil {
			c.err = errors.Join(c.err, err)
			continue
		}
		for _, route := range routes {
			label := fmt.Sprintf("%s route #%d", route.IATA, route.ID)
			text := label + " " + routeText(route)
			score, ok := c.score(text, 180)
			if !ok {
				continue
			}
			c.add(api.SearchResult{
				Type:     "route",
				ID:       strconv.FormatInt(route.ID, 10),
				Label:    label,
				Subtitle: fmt.Sprintf("%d hops / %d observations", route.HopCount, route.ObservationCount),
				URL:      fmt.Sprintf("/?tab=Map&routeId=%d&routeReplay=1", route.ID),
				Score:    score,
				Matched:  "route",
				Metadata: map[string]any{"routeId": route.ID, "iata": route.IATA},
			})
		}
	}
}

func (c *searchCollector) addTraces(ctx context.Context, reader api.Reader, iatas []string) {
	if !c.allowed["trace"] || len(c.needle) < 4 || !isHex(c.query) {
		return
	}
	tags, err := reader.ListTraceTags(ctx, iatas, "", "", time.Time{}, time.Time{}, time.Time{}, min32(int32(c.limit*3), 120))
	if err != nil {
		c.err = err
		return
	}
	for _, tag := range tags {
		text := tag.TraceTag + " " + tag.TraceType + " " + strings.Join(tag.PathHashes, " ")
		score, ok := c.score(text, 190)
		if !ok {
			continue
		}
		c.add(api.SearchResult{
			Type:     "trace",
			ID:       tag.TraceTag,
			Label:    strings.ToUpper(tag.TraceTag),
			Subtitle: fmt.Sprintf("%s / %d packets / %d IATAs", tag.TraceType, tag.PacketCount, tag.IATACount),
			URL:      "/?tab=Traces&traceTag=" + tag.TraceTag,
			Score:    score,
			Matched:  "trace",
			Metadata: map[string]any{"traceTag": tag.TraceTag},
		})
	}
}

func channelLabel(ch api.ChannelSummary) string {
	if ch.Name == nil || strings.TrimSpace(*ch.Name) == "" {
		return strings.ToUpper(ch.ChannelHash)
	}
	if ch.IsHashtag || *ch.Name == "Public" {
		return *ch.Name
	}
	return "#" + *ch.Name
}

func keyState(ch api.ChannelSummary) string {
	if ch.KeyKnown {
		return "known key"
	}
	return "unknown key"
}

func routeText(route api.KnownRoute) string {
	parts := []string{route.IATA, strconv.FormatInt(route.ID, 10)}
	for _, hop := range route.Hops {
		parts = append(parts, hop.HashBytes)
		if hop.Node != nil {
			parts = append(parts, hop.Node.PublicKey)
			if hop.Node.Name != nil {
				parts = append(parts, *hop.Node.Name)
			}
		}
	}
	return strings.Join(parts, " ")
}

func nodeIATAs(iatas []api.NodeIATA) string {
	if len(iatas) == 0 {
		return ""
	}
	parts := make([]string, 0, min(len(iatas), 3))
	for _, i := range iatas[:min(len(iatas), 3)] {
		parts = append(parts, i.IATA)
	}
	if len(iatas) > 3 {
		parts = append(parts, fmt.Sprintf("+%d", len(iatas)-3))
	}
	return strings.Join(parts, ", ")
}

func stringValue(value *string, fallback string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return fallback
	}
	return *value
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	_, err := hex.DecodeString(strings.ToLower(s))
	return err == nil
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
