// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// NodesRouter mounts all /nodes routes onto a subrouter.
//
// GET  /nodes                       → listNodes
// GET  /nodes/{nodeId}              → getNode
// GET  /nodes/{nodeId}/observations → listNodeObservations
func NodesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listNodes(reader))
	r.Route("/{nodeId}", func(r chi.Router) {
		r.Get("/", getNode(reader))
		r.Get("/analytics", getNodeAnalytics(reader))
		r.Get("/observations", listNodeObservations(reader))
		r.Get("/neighbors", listNodeNeighbors(reader))
		r.Get("/reach", getNodeReach(reader))
		r.Get("/route-neighborhood", getNodeRouteNeighborhood(reader))
	})
	return r
}

// listNodes godoc
//
//	@Summary	List nodes
//	@Tags		Nodes
//	@Produce	json
//	@Param		type					query		int		false	"Node type integer (1=companion, 2=repeater, 3=room_server, 4=sensor)"
//	@Param		typeName				query		string	false	"Node type name (companion, repeater, room_server, sensor)"
//	@Param		iata			query		string	false	"Filter by single IATA code (case-insensitive)"
//	@Param		iatas			query		string	false	"Filter by multiple IATA codes, comma-separated e.g. YVR,YYJ"
//	@Param		regionId		query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region			query		string	false	"Filter by region slug, expands to member IATAs"
//	@Param		name					query		string	false	"Partial case-insensitive name match"
//	@Param		scope	query		string	false	"Filter by transport scope name e.g. %23bc (URL-encoded #bc)"
//	@Param		pubkey					query		string	false	"Exact public key match (hex)"
//	@Param		supportsMultibytePaths	query		bool	false	"Filter by multibyte path support (true/false); omit for no filter"
//	@Param		supportsMultibyteTraces	query		bool	false	"Filter by multibyte trace support (true/false); omit for no filter"
//	@Param		cursor					query		int		false	"last_seen epoch ms of last item for pagination"
//	@Param		limit					query		int		false	"Max results (default 50)"
//	@Success	200						{object}	api.NodeSummaryPage
//	@Failure	400						{object}	handlers.APIError
//	@Failure	500						{object}	handlers.APIError
//	@Router		/nodes [get]
func listNodes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var nodeType int16
		if typeParam := r.URL.Query().Get("type"); typeParam != "" {
			t, err := strconv.ParseInt(typeParam, 10, 16)
			if err != nil {
				respondError(w, http.StatusBadRequest, "type must be an integer")
				return
			}
			nodeType = int16(t)
		} else if typeName := r.URL.Query().Get("typeName"); typeName != "" {
			nodeType = api.NodeTypeFromString(typeName)
		}
		var limit int32 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		var pubkey []byte
		if pubkeyParam := strings.ToLower(r.URL.Query().Get("pubkey")); pubkeyParam != "" {
			b, err := hex.DecodeString(pubkeyParam)
			if err != nil {
				respondError(w, http.StatusBadRequest, "pubkey must be a valid hex string")
				return
			}
			pubkey = b
		}
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		name := r.URL.Query().Get("name")
		scope := r.URL.Query().Get("scope")
		var supportsMultibytePaths *bool
		if v := r.URL.Query().Get("supportsMultibytePaths"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid supportsMultibytePaths value")
				return
			}
			supportsMultibytePaths = &b
		}
		var supportsMultibyteTraces *bool
		if v := r.URL.Query().Get("supportsMultibyteTraces"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid supportsMultibyteTraces value")
				return
			}
			supportsMultibyteTraces = &b
		}
		nodes, err := reader.ListNodes(r.Context(), nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, name, scope, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, nodes)
	}
}

// getNode godoc
//
//	@Summary	Get node detail
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Success	200		{object}	api.Node
//	@Failure	400		{object}	handlers.APIError
//	@Failure	404		{object}	handlers.APIError
//	@Router		/nodes/{nodeId} [get]
func getNode(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		node, err := reader.GetNode(r.Context(), nodeID)
		if err != nil {
			respondError(w, http.StatusNotFound, "node not found")
			return
		}
		respond(w, http.StatusOK, node)
	}
}

// getNodeAnalytics godoc
//
//	@Summary	Get node analytics
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path	string	true	"Node UUID"
//	@Param		iata	query	string	false	"Filter by single IATA code"
//	@Param		iatas	query	string	false	"Filter by multiple IATA codes, comma-separated"
//	@Param		region	query	string	false	"Filter by region slug"
//	@Param		regionId	query	int	false	"Filter by region ID"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Success	200	{object}	api.NodeAnalytics
//	@Failure	400	{object}	handlers.APIError
//	@Failure	404	{object}	handlers.APIError
//	@Router		/nodes/{nodeId}/analytics [get]
func getNodeAnalytics(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		if node, err := reader.GetNode(r.Context(), nodeID); err != nil || node == nil {
			respondError(w, http.StatusNotFound, "node not found")
			return
		}
		since, err := parseEpochMillisParam(r, "since")
		if err != nil {
			respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
			return
		}
		until, err := parseEpochMillisParam(r, "until")
		if err != nil {
			respondError(w, http.StatusBadRequest, "until must be epoch milliseconds")
			return
		}
		if !since.IsZero() && !until.IsZero() && since.After(until) {
			respondError(w, http.StatusBadRequest, "since must be before until")
			return
		}
		iatas := parseIATAs(r)
		if r.URL.Query().Get("regionId") != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), r.URL.Query().Get("regionId"), r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		analytics, err := reader.GetNodeAnalytics(r.Context(), nodeID, api.NodeAnalyticsFilter{Since: since, Until: until, IATAs: uniqueIATAs(iatas)})
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, analytics)
	}
}

// listNodeObservations godoc
//
//	@Summary	List packet observations originating from a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Param		cursor	query		int		false	"Observation ID of last item for pagination"
//	@Param		limit	query		int		false	"Max results (default 50)"
//	@Success	200		{object}	api.PacketObservationSummaryPage
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/nodes/{nodeId}/observations [get]
func listNodeObservations(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		var limit int32 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		observations, err := reader.ListNodeObservations(r.Context(), nodeID, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observations)
	}
}

// listNodeNeighbors godoc
//
//	@Summary	List neighbors for a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Success	200		{object}	[]api.NodeNeighbor
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/nodes/{nodeId}/neighbors [get]
func listNodeNeighbors(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		neighbors, err := reader.GetNodeNeighbors(r.Context(), nodeID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, neighbors)
	}
}

// getNodeReach godoc
//
//	@Summary	Get verified route-reach analytics for a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Param		iata	query		string	false	"Filter by single IATA code"
//	@Param		iatas	query		string	false	"Filter by multiple IATA codes, comma-separated"
//	@Param		region	query		string	false	"Filter by region slug"
//	@Param		regionId	query		int		false	"Filter by region ID"
//	@Param		maxHops	query		int		false	"Maximum graph hops, capped at 5"
//	@Success	200		{object}	api.NodeReach
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/nodes/{nodeId}/reach [get]
func getNodeReach(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}

		maxHops := int32(5)
		if v := r.URL.Query().Get("maxHops"); v != "" {
			parsed, err := strconv.ParseInt(v, 10, 32)
			if err != nil || parsed <= 0 {
				respondError(w, http.StatusBadRequest, "maxHops must be a positive integer")
				return
			}
			if parsed < int64(maxHops) {
				maxHops = int32(parsed)
			}
		}

		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		reach, err := buildNodeReach(r.Context(), reader, nodeID, uniqueIATAs(iatas), maxHops)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, reach)
	}
}

// getNodeRouteNeighborhood godoc
//
//	@Summary	List verified route-neighborhood edges for a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Param		iata	query		string	false	"Filter by single IATA code"
//	@Param		iatas	query		string	false	"Filter by multiple IATA codes, comma-separated"
//	@Param		region	query		string	false	"Filter by region slug"
//	@Param		regionId	query		int		false	"Filter by region ID"
//	@Param		maxHops	query		int		false	"Maximum graph hops, capped at 5"
//	@Success	200		{object}	api.NodeRouteNeighborhood
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/nodes/{nodeId}/route-neighborhood [get]
func getNodeRouteNeighborhood(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}

		maxHops := int32(5)
		if v := r.URL.Query().Get("maxHops"); v != "" {
			parsed, err := strconv.ParseInt(v, 10, 32)
			if err != nil || parsed <= 0 {
				respondError(w, http.StatusBadRequest, "maxHops must be a positive integer")
				return
			}
			if parsed < int64(maxHops) {
				maxHops = int32(parsed)
			}
		}

		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		neighborhood, err := buildRouteNeighborhood(r.Context(), reader, nodeID, uniqueIATAs(iatas), maxHops)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, neighborhood)
	}
}

type nodeReachNodeAccumulator struct {
	node             api.RouteNeighborhoodNode
	iatas            map[string]struct{}
	routeIDs         map[int64]struct{}
	lastSeen         int64
	observationCount int64
}

type nodeReachHopAccumulator struct {
	nodeCount        int64
	edgeCount        int64
	routeIDs         map[int64]struct{}
	observationCount int64
}

type nodeReachIATAAccumulator struct {
	iata             string
	nodeIDs          map[uuid.UUID]struct{}
	routeIDs         map[int64]struct{}
	edgeCount        int64
	lastSeen         int64
	observationCount int64
}

func buildNodeReach(ctx context.Context, reader api.Reader, nodeID uuid.UUID, iatas []string, maxHops int32) (*api.NodeReach, error) {
	neighborhood, err := buildRouteNeighborhood(ctx, reader, nodeID, iatas, maxHops)
	if err != nil {
		return nil, err
	}

	nodesByID := make(map[uuid.UUID]api.RouteNeighborhoodNode, len(neighborhood.Nodes))
	hopBuckets := make(map[int32]*nodeReachHopAccumulator)
	nodeAccs := make(map[uuid.UUID]*nodeReachNodeAccumulator)
	iataAccs := make(map[string]*nodeReachIATAAccumulator)
	routeIDs := make(map[int64]struct{})
	var observationCount int64

	for _, node := range neighborhood.Nodes {
		nodesByID[node.ID] = node
		if node.ID == nodeID {
			continue
		}
		acc := hopBuckets[node.HopDistance]
		if acc == nil {
			acc = &nodeReachHopAccumulator{routeIDs: make(map[int64]struct{})}
			hopBuckets[node.HopDistance] = acc
		}
		acc.nodeCount += 1
		nodeAccs[node.ID] = &nodeReachNodeAccumulator{
			node:     node,
			iatas:    make(map[string]struct{}),
			routeIDs: make(map[int64]struct{}),
		}
	}

	for _, edge := range neighborhood.Edges {
		observationCount += edge.ObservationCount
		hopAcc := hopBuckets[edge.HopDistance]
		if hopAcc == nil {
			hopAcc = &nodeReachHopAccumulator{routeIDs: make(map[int64]struct{})}
			hopBuckets[edge.HopDistance] = hopAcc
		}
		hopAcc.edgeCount += 1
		hopAcc.observationCount += edge.ObservationCount

		iataAcc := iataAccs[edge.IATA]
		if iataAcc == nil {
			iataAcc = &nodeReachIATAAccumulator{
				iata:     edge.IATA,
				nodeIDs:  make(map[uuid.UUID]struct{}),
				routeIDs: make(map[int64]struct{}),
			}
			iataAccs[edge.IATA] = iataAcc
		}
		iataAcc.edgeCount += 1
		iataAcc.observationCount += edge.ObservationCount
		if edge.LastSeen > iataAcc.lastSeen {
			iataAcc.lastSeen = edge.LastSeen
		}

		for _, routeID := range edge.RouteIDs {
			routeIDs[routeID] = struct{}{}
			hopAcc.routeIDs[routeID] = struct{}{}
			iataAcc.routeIDs[routeID] = struct{}{}
		}

		for _, endpointID := range []uuid.UUID{edge.FromNodeID, edge.ToNodeID} {
			if endpointID == nodeID {
				continue
			}
			if _, ok := nodesByID[endpointID]; !ok {
				continue
			}
			iataAcc.nodeIDs[endpointID] = struct{}{}
			nodeAcc := nodeAccs[endpointID]
			if nodeAcc == nil {
				continue
			}
			nodeAcc.iatas[edge.IATA] = struct{}{}
			nodeAcc.observationCount += edge.ObservationCount
			if edge.LastSeen > nodeAcc.lastSeen {
				nodeAcc.lastSeen = edge.LastSeen
			}
			for _, routeID := range edge.RouteIDs {
				nodeAcc.routeIDs[routeID] = struct{}{}
			}
		}
	}

	buckets := make([]api.NodeReachHopBucket, 0, len(hopBuckets))
	for hopDistance, acc := range hopBuckets {
		if hopDistance <= 0 || hopDistance > maxHops {
			continue
		}
		buckets = append(buckets, api.NodeReachHopBucket{
			HopDistance:      hopDistance,
			NodeCount:        acc.nodeCount,
			EdgeCount:        acc.edgeCount,
			RouteCount:       int64(len(acc.routeIDs)),
			ObservationCount: acc.observationCount,
		})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].HopDistance < buckets[j].HopDistance })

	topNodes := make([]api.NodeReachNode, 0, len(nodeAccs))
	for id, acc := range nodeAccs {
		if id == nodeID {
			continue
		}
		nodeIATAs := make([]string, 0, len(acc.iatas))
		for iata := range acc.iatas {
			nodeIATAs = append(nodeIATAs, iata)
		}
		sort.Strings(nodeIATAs)
		topNodes = append(topNodes, api.NodeReachNode{
			ID:               id,
			Name:             acc.node.Name,
			PublicKey:        acc.node.PublicKey,
			HopDistance:      acc.node.HopDistance,
			IATAs:            nodeIATAs,
			RouteCount:       int64(len(acc.routeIDs)),
			ObservationCount: acc.observationCount,
			LastSeen:         acc.lastSeen,
		})
	}
	sort.Slice(topNodes, func(i, j int) bool {
		if topNodes[i].ObservationCount != topNodes[j].ObservationCount {
			return topNodes[i].ObservationCount > topNodes[j].ObservationCount
		}
		if topNodes[i].RouteCount != topNodes[j].RouteCount {
			return topNodes[i].RouteCount > topNodes[j].RouteCount
		}
		if topNodes[i].HopDistance != topNodes[j].HopDistance {
			return topNodes[i].HopDistance < topNodes[j].HopDistance
		}
		return topNodes[i].ID.String() < topNodes[j].ID.String()
	})
	if len(topNodes) > 12 {
		topNodes = topNodes[:12]
	}

	topIATAs := make([]api.NodeReachIATA, 0, len(iataAccs))
	for _, acc := range iataAccs {
		topIATAs = append(topIATAs, api.NodeReachIATA{
			IATA:             acc.iata,
			NodeCount:        int64(len(acc.nodeIDs)),
			EdgeCount:        acc.edgeCount,
			RouteCount:       int64(len(acc.routeIDs)),
			ObservationCount: acc.observationCount,
			LastSeen:         acc.lastSeen,
		})
	}
	sort.Slice(topIATAs, func(i, j int) bool {
		if topIATAs[i].ObservationCount != topIATAs[j].ObservationCount {
			return topIATAs[i].ObservationCount > topIATAs[j].ObservationCount
		}
		if topIATAs[i].NodeCount != topIATAs[j].NodeCount {
			return topIATAs[i].NodeCount > topIATAs[j].NodeCount
		}
		return topIATAs[i].IATA < topIATAs[j].IATA
	})
	if len(topIATAs) > 8 {
		topIATAs = topIATAs[:8]
	}

	return &api.NodeReach{
		NodeID:           nodeID,
		MaxHops:          maxHops,
		GeneratedAt:      time.Now().UnixMilli(),
		ReachableNodes:   int64(len(nodeAccs)),
		VerifiedEdges:    int64(len(neighborhood.Edges)),
		RouteCount:       int64(len(routeIDs)),
		ObservationCount: observationCount,
		HopBuckets:       buckets,
		TopNodes:         topNodes,
		TopIATAs:         topIATAs,
	}, nil
}

type routeEdgeAccumulator struct {
	from             uuid.UUID
	to               uuid.UUID
	iata             string
	routeIDs         map[int64]struct{}
	hopDistance      int32
	lastSeen         int64
	observationCount int64
}

func buildRouteNeighborhood(ctx context.Context, reader api.Reader, nodeID uuid.UUID, iatas []string, maxHops int32) (*api.NodeRouteNeighborhood, error) {
	iataScopes := iatas
	if len(iataScopes) == 0 {
		iataScopes = []string{""}
	}

	distances := map[uuid.UUID]int32{nodeID: 0}
	frontier := []uuid.UUID{nodeID}
	queried := make(map[string]struct{})
	nodes := make(map[uuid.UUID]*api.ResolvedNode)
	edges := make(map[string]*routeEdgeAccumulator)

	for depth := int32(0); depth < maxHops && len(frontier) > 0; depth++ {
		next := make(map[uuid.UUID]struct{})
		for _, currentID := range frontier {
			for _, iata := range iataScopes {
				queryKey := currentID.String() + ":" + iata
				if _, ok := queried[queryKey]; ok {
					continue
				}
				queried[queryKey] = struct{}{}
				routes, err := reader.GetKnownRoutesByNode(ctx, iata, currentID)
				if err != nil {
					return nil, err
				}
				for _, route := range routes {
					for _, hop := range route.Hops {
						if hop.Node != nil {
							nodes[hop.NodeID] = hop.Node
						}
					}
					for idx, hop := range route.Hops {
						if hop.NodeID != currentID {
							continue
						}
						if idx > 0 {
							addNeighborhoodEdge(edges, distances, next, route, route.Hops[idx-1], hop, depth)
						}
						if idx+1 < len(route.Hops) {
							addNeighborhoodEdge(edges, distances, next, route, hop, route.Hops[idx+1], depth)
						}
					}
				}
			}
		}
		frontier = frontier[:0]
		for id := range next {
			frontier = append(frontier, id)
		}
		sort.Slice(frontier, func(i, j int) bool { return frontier[i].String() < frontier[j].String() })
	}

	outNodes := make([]api.RouteNeighborhoodNode, 0, len(nodes))
	for id, node := range nodes {
		distance, ok := distances[id]
		if !ok || distance > maxHops || node == nil || node.Latitude == nil || node.Longitude == nil {
			continue
		}
		outNodes = append(outNodes, api.RouteNeighborhoodNode{
			ID:          id,
			Name:        node.Name,
			PublicKey:   node.PublicKey,
			Latitude:    *node.Latitude,
			Longitude:   *node.Longitude,
			HopDistance: distance,
		})
	}
	sort.Slice(outNodes, func(i, j int) bool {
		if outNodes[i].HopDistance != outNodes[j].HopDistance {
			return outNodes[i].HopDistance < outNodes[j].HopDistance
		}
		return outNodes[i].ID.String() < outNodes[j].ID.String()
	})

	outEdges := make([]api.RouteNeighborhoodEdge, 0, len(edges))
	for _, edge := range edges {
		fromNode := nodes[edge.from]
		toNode := nodes[edge.to]
		if fromNode == nil || toNode == nil || fromNode.Latitude == nil || fromNode.Longitude == nil || toNode.Latitude == nil || toNode.Longitude == nil {
			continue
		}
		routeIDs := make([]int64, 0, len(edge.routeIDs))
		for id := range edge.routeIDs {
			routeIDs = append(routeIDs, id)
		}
		sort.Slice(routeIDs, func(i, j int) bool { return routeIDs[i] < routeIDs[j] })
		outEdges = append(outEdges, api.RouteNeighborhoodEdge{
			FromNodeID:       edge.from,
			ToNodeID:         edge.to,
			IATA:             edge.iata,
			RouteIDs:         routeIDs,
			HopDistance:      edge.hopDistance,
			LastSeen:         edge.lastSeen,
			ObservationCount: edge.observationCount,
		})
	}
	sort.Slice(outEdges, func(i, j int) bool {
		if outEdges[i].HopDistance != outEdges[j].HopDistance {
			return outEdges[i].HopDistance < outEdges[j].HopDistance
		}
		if outEdges[i].IATA != outEdges[j].IATA {
			return outEdges[i].IATA < outEdges[j].IATA
		}
		if outEdges[i].FromNodeID != outEdges[j].FromNodeID {
			return outEdges[i].FromNodeID.String() < outEdges[j].FromNodeID.String()
		}
		return outEdges[i].ToNodeID.String() < outEdges[j].ToNodeID.String()
	})

	return &api.NodeRouteNeighborhood{
		NodeID:  nodeID,
		MaxHops: maxHops,
		Nodes:   outNodes,
		Edges:   outEdges,
	}, nil
}

func addNeighborhoodEdge(edges map[string]*routeEdgeAccumulator, distances map[uuid.UUID]int32, next map[uuid.UUID]struct{}, route api.KnownRoute, a, b api.RouteHop, depth int32) {
	from, to := orderedNodePair(a.NodeID, b.NodeID)
	key := route.IATA + ":" + from.String() + ":" + to.String()
	edge := edges[key]
	if edge == nil {
		edge = &routeEdgeAccumulator{
			from:        from,
			to:          to,
			iata:        route.IATA,
			routeIDs:    make(map[int64]struct{}),
			hopDistance: depth + 1,
		}
		edges[key] = edge
	}
	if _, seen := edge.routeIDs[route.ID]; !seen {
		edge.routeIDs[route.ID] = struct{}{}
		if route.LastSeen > edge.lastSeen {
			edge.lastSeen = route.LastSeen
		}
		edge.observationCount += route.ObservationCount
	}
	if depth+1 < edge.hopDistance {
		edge.hopDistance = depth + 1
	}

	for _, id := range []uuid.UUID{a.NodeID, b.NodeID} {
		if _, ok := distances[id]; ok {
			continue
		}
		distances[id] = depth + 1
		next[id] = struct{}{}
	}
}

func orderedNodePair(a, b uuid.UUID) (uuid.UUID, uuid.UUID) {
	if a.String() < b.String() {
		return a, b
	}
	return b, a
}

func uniqueIATAs(iatas []string) []string {
	if len(iatas) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(iatas))
	out := make([]string, 0, len(iatas))
	for _, iata := range iatas {
		iata = strings.ToUpper(strings.TrimSpace(iata))
		if iata == "" {
			continue
		}
		if _, ok := seen[iata]; ok {
			continue
		}
		seen[iata] = struct{}{}
		out = append(out, iata)
	}
	sort.Strings(out)
	return out
}
