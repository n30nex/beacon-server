// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	defaultNetgraphRouteLimit int32 = 2500
	defaultNetgraphNodeLimit  int32 = 2600
	defaultNetgraphEdgeLimit  int32 = 4200
)

// NetgraphRouter mounts the experimental render-ready verified-route graph.
func NetgraphRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getNetgraph(reader))
	return r
}

// getNetgraph godoc
//
//	@Summary	3D netgraph topology snapshot
//	@Tags		Netgraph
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		routeLimit	query	int		false	"Known routes to scan, clamped to 1-2500"
//	@Success	200			{object}	api.NetgraphSnapshot
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/netgraph [get]
func getNetgraph(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}

		limits, err := parseNetgraphLimits(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}

		routes, err := reader.ListKnownRoutes(r.Context(), iatas, 0, time.Time{}, limits.RouteLimit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, buildNetgraphSnapshot(r.Context(), routes, limits, time.Now()))
	}
}

func parseNetgraphLimits(r *http.Request) (api.NetgraphLimits, error) {
	routeLimit, err := parseNetgraphLimit(r, "routeLimit", defaultNetgraphRouteLimit)
	if err != nil {
		return api.NetgraphLimits{}, err
	}
	nodeLimit, err := parseNetgraphLimit(r, "nodeLimit", defaultNetgraphNodeLimit)
	if err != nil {
		return api.NetgraphLimits{}, err
	}
	edgeLimit, err := parseNetgraphLimit(r, "edgeLimit", defaultNetgraphEdgeLimit)
	if err != nil {
		return api.NetgraphLimits{}, err
	}
	return api.NetgraphLimits{
		RouteLimit: min32(routeLimit, defaultNetgraphRouteLimit),
		NodeLimit:  min32(nodeLimit, defaultNetgraphNodeLimit),
		EdgeLimit:  min32(edgeLimit, defaultNetgraphEdgeLimit),
	}, nil
}

func parseNetgraphLimit(r *http.Request, name string, fallback int32) (int32, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return int32(parsed), nil
}

type netgraphNodeDraft struct {
	item     api.NetgraphNode
	iatas    map[string]struct{}
	routeIDs map[int64]struct{}
}

type netgraphEdgeDraft struct {
	item     api.NetgraphEdge
	iatas    map[string]struct{}
	routeIDs map[int64]struct{}
}

func buildNetgraphSnapshot(_ context.Context, routes []api.KnownRoute, limits api.NetgraphLimits, now time.Time) api.NetgraphSnapshot {
	nodes := make(map[uuid.UUID]*netgraphNodeDraft)
	edges := make(map[string]*netgraphEdgeDraft)
	activeIATAs := make(map[string]struct{})
	var totalObservations int64
	var mappedRoutes int64

	for _, route := range routes {
		if route.ID <= 0 || len(route.Hops) < 2 {
			continue
		}
		activeIATAs[route.IATA] = struct{}{}
		totalObservations += route.ObservationCount
		routeNodeIDs := make([]uuid.UUID, 0, len(route.Hops))
		seenNodesInRoute := make(map[uuid.UUID]struct{})
		for _, hop := range route.Hops {
			nodeID := hop.NodeID
			if nodeID == uuid.Nil && hop.Node != nil {
				nodeID = hop.Node.ID
			}
			if nodeID == uuid.Nil {
				continue
			}
			draft := nodes[nodeID]
			if draft == nil {
				draft = &netgraphNodeDraft{
					item: api.NetgraphNode{
						ID:           nodeID,
						PublicKey:    "",
						NodeTypeName: "unknown",
						FirstSeen:    route.FirstSeen,
						LastSeen:     route.LastSeen,
					},
					iatas:    map[string]struct{}{},
					routeIDs: map[int64]struct{}{},
				}
				nodes[nodeID] = draft
			}
			if hop.Node != nil {
				draft.item.Name = hop.Node.Name
				draft.item.PublicKey = hop.Node.PublicKey
				draft.item.NodeType = hop.Node.NodeType
				if hop.Node.NodeTypeName != "" {
					draft.item.NodeTypeName = hop.Node.NodeTypeName
				} else {
					draft.item.NodeTypeName = api.NodeTypeName(hop.Node.NodeType)
				}
				draft.item.Latitude = hop.Node.Latitude
				draft.item.Longitude = hop.Node.Longitude
				draft.item.IsObserver = hop.Node.IsObserver
			}
			if draft.item.FirstSeen == 0 || (route.FirstSeen != 0 && route.FirstSeen < draft.item.FirstSeen) {
				draft.item.FirstSeen = route.FirstSeen
			}
			if route.LastSeen > draft.item.LastSeen {
				draft.item.LastSeen = route.LastSeen
			}
			if route.IATA != "" {
				draft.iatas[route.IATA] = struct{}{}
			}
			draft.routeIDs[route.ID] = struct{}{}
			if _, ok := seenNodesInRoute[nodeID]; !ok {
				draft.item.ObservationCount += route.ObservationCount
				seenNodesInRoute[nodeID] = struct{}{}
			}
			routeNodeIDs = append(routeNodeIDs, nodeID)
		}
		if len(routeNodeIDs) < 2 {
			continue
		}
		mappedRoutes++
		for i := 1; i < len(routeNodeIDs); i++ {
			fromID := routeNodeIDs[i-1]
			toID := routeNodeIDs[i]
			if fromID == uuid.Nil || toID == uuid.Nil || fromID == toID {
				continue
			}
			edgeID := fromID.String() + ">" + toID.String()
			draft := edges[edgeID]
			if draft == nil {
				draft = &netgraphEdgeDraft{
					item: api.NetgraphEdge{
						ID:         edgeID,
						FromNodeID: fromID,
						ToNodeID:   toID,
						FirstSeen:  route.FirstSeen,
						LastSeen:   route.LastSeen,
					},
					iatas:    map[string]struct{}{},
					routeIDs: map[int64]struct{}{},
				}
				edges[edgeID] = draft
			}
			if draft.item.FirstSeen == 0 || (route.FirstSeen != 0 && route.FirstSeen < draft.item.FirstSeen) {
				draft.item.FirstSeen = route.FirstSeen
			}
			if route.LastSeen > draft.item.LastSeen {
				draft.item.LastSeen = route.LastSeen
			}
			if route.IATA != "" {
				draft.iatas[route.IATA] = struct{}{}
			}
			draft.routeIDs[route.ID] = struct{}{}
			draft.item.ObservationCount += route.ObservationCount
		}
	}

	outNodes := finalizeNetgraphNodes(nodes)
	truncatedNodes := len(outNodes) > int(limits.NodeLimit)
	if truncatedNodes {
		outNodes = outNodes[:limits.NodeLimit]
	}
	visibleNodeIDs := make(map[uuid.UUID]struct{}, len(outNodes))
	for _, node := range outNodes {
		visibleNodeIDs[node.ID] = struct{}{}
	}

	outEdges := finalizeNetgraphEdges(edges, visibleNodeIDs)
	truncatedEdges := len(outEdges) > int(limits.EdgeLimit)
	if truncatedEdges {
		outEdges = outEdges[:limits.EdgeLimit]
	}

	return api.NetgraphSnapshot{
		ServerTime: now.UnixMilli(),
		Limits:     limits,
		Stats: api.NetgraphStats{
			SourceRouteCount: int64(len(routes)),
			MappedRouteCount: mappedRoutes,
			NodeCount:        int64(len(outNodes)),
			EdgeCount:        int64(len(outEdges)),
			ObservationCount: totalObservations,
			ActiveIATAs:      int64(len(activeIATAs)),
			TruncatedRoutes:  len(routes) >= int(limits.RouteLimit),
			TruncatedNodes:   truncatedNodes,
			TruncatedEdges:   truncatedEdges,
		},
		Nodes: outNodes,
		Edges: outEdges,
	}
}

func finalizeNetgraphNodes(drafts map[uuid.UUID]*netgraphNodeDraft) []api.NetgraphNode {
	nodes := make([]api.NetgraphNode, 0, len(drafts))
	for _, draft := range drafts {
		item := draft.item
		item.IATAs = sortedStrings(draft.iatas)
		item.RouteIDs = sortedInt64s(draft.routeIDs)
		item.RouteCount = int64(len(item.RouteIDs))
		if item.NodeTypeName == "" {
			item.NodeTypeName = api.NodeTypeName(item.NodeType)
		}
		nodes = append(nodes, item)
	}
	sort.Slice(nodes, func(i, j int) bool {
		left, right := nodes[i], nodes[j]
		if left.RouteCount != right.RouteCount {
			return left.RouteCount > right.RouteCount
		}
		if left.ObservationCount != right.ObservationCount {
			return left.ObservationCount > right.ObservationCount
		}
		if left.LastSeen != right.LastSeen {
			return left.LastSeen > right.LastSeen
		}
		return netgraphNodeLabel(left) < netgraphNodeLabel(right)
	})
	return nodes
}

func finalizeNetgraphEdges(drafts map[string]*netgraphEdgeDraft, visibleNodeIDs map[uuid.UUID]struct{}) []api.NetgraphEdge {
	edges := make([]api.NetgraphEdge, 0, len(drafts))
	for _, draft := range drafts {
		if _, ok := visibleNodeIDs[draft.item.FromNodeID]; !ok {
			continue
		}
		if _, ok := visibleNodeIDs[draft.item.ToNodeID]; !ok {
			continue
		}
		item := draft.item
		item.IATAs = sortedStrings(draft.iatas)
		item.RouteIDs = sortedInt64s(draft.routeIDs)
		item.RouteCount = int64(len(item.RouteIDs))
		edges = append(edges, item)
	}
	sort.Slice(edges, func(i, j int) bool {
		left, right := edges[i], edges[j]
		if left.ObservationCount != right.ObservationCount {
			return left.ObservationCount > right.ObservationCount
		}
		if left.RouteCount != right.RouteCount {
			return left.RouteCount > right.RouteCount
		}
		if left.LastSeen != right.LastSeen {
			return left.LastSeen > right.LastSeen
		}
		return left.ID < right.ID
	})
	return edges
}

func sortedStrings(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedInt64s(values map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func netgraphNodeLabel(node api.NetgraphNode) string {
	if node.Name != nil && *node.Name != "" {
		return *node.Name
	}
	if node.PublicKey != "" {
		return node.PublicKey
	}
	return node.ID.String()
}
