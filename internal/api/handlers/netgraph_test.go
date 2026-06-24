// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type netgraphReader struct {
	stubReader
	routes []api.KnownRoute
	iatas  []string
	limit  int32
}

func (r *netgraphReader) ListKnownRoutes(ctx context.Context, iatas []string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	r.iatas = append([]string(nil), iatas...)
	r.limit = limit
	if int(limit) < len(r.routes) {
		return r.routes[:limit], nil
	}
	return r.routes, nil
}

func TestGetNetgraph_AggregatesNodesAndDirectedEdges(t *testing.T) {
	alpha := routeNode("00000000-0000-0000-0000-000000000001", "Alpha", 2, ptrFloat(49.2), ptrFloat(-123.1), false)
	bravo := routeNode("00000000-0000-0000-0000-000000000002", "Bravo", 2, ptrFloat(49.3), ptrFloat(-123.0), true)
	charlie := routeNode("00000000-0000-0000-0000-000000000003", "Charlie", 1, nil, nil, false)
	reader := &netgraphReader{routes: []api.KnownRoute{
		knownRoute(1, "YVR", 5, 100, alpha, bravo, charlie),
		knownRoute(2, "YVR", 7, 120, alpha, bravo),
	}}
	router := chi.NewRouter()
	router.Mount("/netgraph", NetgraphRouter(reader))

	req := httptest.NewRequest(http.MethodGet, "/netgraph?iatas=yvr,yow&routeLimit=2", nil)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
	if !reflect.DeepEqual(reader.iatas, []string{"YVR", "YOW"}) {
		t.Fatalf("iatas = %#v", reader.iatas)
	}
	if reader.limit != 2 {
		t.Fatalf("route limit = %d, want 2", reader.limit)
	}
	var body api.NetgraphSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Stats.SourceRouteCount != 2 || body.Stats.MappedRouteCount != 2 || body.Stats.ObservationCount != 12 {
		t.Fatalf("stats = %#v", body.Stats)
	}
	if len(body.Nodes) != 3 || len(body.Edges) != 2 {
		t.Fatalf("counts nodes=%d edges=%d", len(body.Nodes), len(body.Edges))
	}
	edge := body.Edges[0]
	if edge.FromNodeID != alpha.ID || edge.ToNodeID != bravo.ID || edge.ObservationCount != 12 {
		t.Fatalf("first edge = %#v", edge)
	}
	if !reflect.DeepEqual(edge.RouteIDs, []int64{1, 2}) {
		t.Fatalf("edge route ids = %#v", edge.RouteIDs)
	}
	var observerNode *api.NetgraphNode
	var missingLocation *api.NetgraphNode
	for i := range body.Nodes {
		if body.Nodes[i].ID == bravo.ID {
			observerNode = &body.Nodes[i]
		}
		if body.Nodes[i].ID == charlie.ID {
			missingLocation = &body.Nodes[i]
		}
	}
	if observerNode == nil || !observerNode.IsObserver || observerNode.NodeTypeName != "repeater" {
		t.Fatalf("observer node = %#v", observerNode)
	}
	if missingLocation == nil || missingLocation.Latitude != nil || missingLocation.Longitude != nil {
		t.Fatalf("missing location node = %#v", missingLocation)
	}
}

func TestGetNetgraph_AppliesCapsDeterministically(t *testing.T) {
	alpha := routeNode("00000000-0000-0000-0000-000000000011", "Alpha", 2, nil, nil, false)
	bravo := routeNode("00000000-0000-0000-0000-000000000012", "Bravo", 2, nil, nil, false)
	charlie := routeNode("00000000-0000-0000-0000-000000000013", "Charlie", 2, nil, nil, false)
	delta := routeNode("00000000-0000-0000-0000-000000000014", "Delta", 2, nil, nil, false)
	reader := &netgraphReader{routes: []api.KnownRoute{
		knownRoute(1, "YVR", 20, 130, alpha, bravo),
		knownRoute(2, "YVR", 10, 120, bravo, charlie),
		knownRoute(3, "YVR", 5, 110, charlie, delta),
	}}
	router := chi.NewRouter()
	router.Mount("/netgraph", NetgraphRouter(reader))

	req := httptest.NewRequest(http.MethodGet, "/netgraph?nodeLimit=3&edgeLimit=1", nil)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", res.Code, res.Body.String())
	}
	var body api.NetgraphSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Nodes) != 3 || len(body.Edges) != 1 {
		t.Fatalf("counts nodes=%d edges=%d", len(body.Nodes), len(body.Edges))
	}
	if !body.Stats.TruncatedNodes || !body.Stats.TruncatedEdges {
		t.Fatalf("truncation flags = %#v", body.Stats)
	}
	if body.Nodes[0].ID != bravo.ID {
		t.Fatalf("first node = %#v, want Bravo as highest route/obs node", body.Nodes[0])
	}
}

func TestGetNetgraph_EmptyAndInvalidInput(t *testing.T) {
	router := chi.NewRouter()
	router.Mount("/netgraph", NetgraphRouter(&netgraphReader{}))

	empty := httptest.NewRecorder()
	router.ServeHTTP(empty, httptest.NewRequest(http.MethodGet, "/netgraph", nil))
	if empty.Code != http.StatusOK {
		t.Fatalf("empty status = %d body=%s", empty.Code, empty.Body.String())
	}
	var body api.NetgraphSnapshot
	if err := json.Unmarshal(empty.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(body.Nodes) != 0 || len(body.Edges) != 0 || body.Stats.SourceRouteCount != 0 {
		t.Fatalf("empty body = %#v", body)
	}

	for _, url := range []string{"/netgraph?routeLimit=nope", "/netgraph?routeLimit=0"} {
		res := httptest.NewRecorder()
		router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, url, nil))
		if res.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d body=%s", url, res.Code, res.Body.String())
		}
	}
}

func knownRoute(id int64, iata string, observations int64, lastSeen int64, nodes ...api.ResolvedNode) api.KnownRoute {
	hops := make([]api.RouteHop, 0, len(nodes))
	for index, node := range nodes {
		item := node
		hops = append(hops, api.RouteHop{
			NodeID:    node.ID,
			HashBytes: string(rune('a' + index)),
			Node:      &item,
		})
	}
	return api.KnownRoute{
		ID:               id,
		IATA:             iata,
		HopCount:         int32(len(hops)),
		Hops:             hops,
		FirstSeen:        10,
		LastSeen:         lastSeen,
		ObservationCount: observations,
	}
}

func routeNode(id string, name string, nodeType int16, lat *float64, lng *float64, observer bool) api.ResolvedNode {
	return api.ResolvedNode{
		ID:           uuid.MustParse(id),
		Name:         strPtr(name),
		PublicKey:    id[len(id)-12:],
		NodeType:     nodeType,
		NodeTypeName: api.NodeTypeName(nodeType),
		Latitude:     lat,
		Longitude:    lng,
		IsObserver:   observer,
	}
}

func ptrFloat(value float64) *float64 {
	return &value
}
