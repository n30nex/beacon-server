// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type nodeAnalyticsReader struct {
	stubReader
	filter api.NodeAnalyticsFilter
	nodeID uuid.UUID
}

func (r *nodeAnalyticsReader) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	return &api.Node{NodeSummary: api.NodeSummary{ID: nodeID}}, nil
}

func (r *nodeAnalyticsReader) GetNodeAnalytics(ctx context.Context, nodeID uuid.UUID, filter api.NodeAnalyticsFilter) (*api.NodeAnalytics, error) {
	r.nodeID = nodeID
	r.filter = filter
	return &api.NodeAnalytics{
		NodeID: nodeID,
		Since:  filter.Since.UnixMilli(),
		Until:  filter.Until.UnixMilli(),
		KPIs:   api.NodeAnalyticsKPI{PacketCount: 2, ObservationCount: 5, ActiveObservers: 3, ActiveIATAs: 2},
	}, nil
}

type nodeReachReader struct {
	stubReader
	calls  []string
	routes []api.KnownRoute
}

type nodeAdvertsReader struct {
	stubReader
	cursor int64
	limit  int32
	nodeID uuid.UUID
}

func (r *nodeAdvertsReader) ListNodeAdverts(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.NodeAdvertObservation], error) {
	r.nodeID = nodeID
	r.cursor = cursor
	r.limit = limit
	name := "Field Relay"
	nodeType := int16(2)
	nodeTypeName := "repeater"
	lat := 45.4215
	lng := -75.6972
	return api.Page[api.NodeAdvertObservation]{
		Items: []api.NodeAdvertObservation{{
			PacketObservationSummary: api.PacketObservationSummary{
				ID:              42,
				PacketHash:      "abcd",
				PayloadType:     4,
				PayloadTypeName: "advert",
				IATA:            "YOW",
				HeardAt:         1234,
			},
			AdvertisedName:         &name,
			AdvertisedNodeType:     &nodeType,
			AdvertisedNodeTypeName: &nodeTypeName,
			AdvertisedLat:          &lat,
			AdvertisedLng:          &lng,
		}},
	}, nil
}

func testResolvedNode(id uuid.UUID, name string, lat, lng float64) *api.ResolvedNode {
	return &api.ResolvedNode{
		ID:        id,
		Name:      &name,
		PublicKey: strings.ReplaceAll(id.String(), "-", ""),
		Latitude:  &lat,
		Longitude: &lng,
	}
}

func testRoute(id int64, iata string, observations int64, nodes ...*api.ResolvedNode) api.KnownRoute {
	hops := make([]api.RouteHop, 0, len(nodes))
	for index, node := range nodes {
		hops = append(hops, api.RouteHop{
			NodeID:    node.ID,
			HashBytes: strings.Repeat("0", index+1),
			Node:      node,
		})
	}
	return api.KnownRoute{
		ID:               id,
		IATA:             iata,
		HopCount:         int32(len(hops)),
		Hops:             hops,
		FirstSeen:        1000,
		LastSeen:         2000 + id,
		ObservationCount: observations,
	}
}

func (r *nodeReachReader) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error) {
	r.calls = append(r.calls, iata)
	out := make([]api.KnownRoute, 0, len(r.routes))
	for _, route := range r.routes {
		if iata != "" && route.IATA != iata {
			continue
		}
		for _, hop := range route.Hops {
			if hop.NodeID == nodeID {
				out = append(out, route)
				break
			}
		}
	}
	return out, nil
}

func TestGetNode_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}", getNode(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/observations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeAdverts_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/adverts", listNodeAdverts(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/adverts", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeAdverts_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/adverts", listNodeAdverts(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/adverts?cursor=bad", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeAdverts_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/adverts", listNodeAdverts(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/adverts?limit=bad", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeAdverts_FilterContract(t *testing.T) {
	reader := &nodeAdvertsReader{}
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/adverts", listNodeAdverts(reader))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/adverts?cursor=7&limit=9", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.nodeID != uuid.MustParse("00000000-0000-0000-0000-000000000001") || reader.cursor != 7 || reader.limit != 9 {
		t.Fatalf("unexpected reader args node=%s cursor=%d limit=%d", reader.nodeID, reader.cursor, reader.limit)
	}
	var body api.Page[api.NodeAdvertObservation]
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].AdvertisedName == nil || *body.Items[0].AdvertisedName != "Field Relay" {
		t.Fatalf("unexpected body %#v", body)
	}
}

func TestGetNodeReach_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/reach", getNodeReach(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/reach", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNodeReach_InvalidMaxHops(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/reach", getNodeReach(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/reach?maxHops=0", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNodeReach_FilterAndCap(t *testing.T) {
	nodeA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	nodeB := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	nodeC := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	nodeD := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	reader := &nodeReachReader{
		routes: []api.KnownRoute{
			testRoute(101, "YVR", 10,
				testResolvedNode(nodeA, "Alpha", 49, -123),
				testResolvedNode(nodeB, "Bravo", 50, -124),
				testResolvedNode(nodeC, "Charlie", 51, -125),
			),
			testRoute(202, "YOW", 7,
				testResolvedNode(nodeA, "Alpha", 49, -123),
				testResolvedNode(nodeD, "Delta", 45, -75),
			),
		},
	}
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/reach", getNodeReach(reader))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/reach?maxHops=99&iatas=yvr,YOW,yvr", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body api.NodeReach
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.MaxHops != 5 {
		t.Fatalf("expected maxHops cap 5, got %d", body.MaxHops)
	}
	if body.ReachableNodes != 3 || body.VerifiedEdges != 3 || body.RouteCount != 2 {
		t.Fatalf("unexpected reach summary: %#v", body)
	}
	if len(body.HopBuckets) != 2 || body.HopBuckets[0].HopDistance != 1 || body.HopBuckets[0].NodeCount != 2 || body.HopBuckets[1].NodeCount != 1 {
		t.Fatalf("unexpected hop buckets: %#v", body.HopBuckets)
	}
	gotCalls := strings.Join(reader.calls[:2], ",")
	if gotCalls != "YOW,YVR" {
		t.Fatalf("expected sorted unique initial IATA calls YOW,YVR, got %q", gotCalls)
	}
}

func TestGetNodeAnalytics_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/analytics", getNodeAnalytics(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/analytics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNodeAnalytics_InvalidWindow(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/analytics", getNodeAnalytics(&nodeAnalyticsReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/analytics?since=5000&until=1000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNodeAnalytics_FilterContract(t *testing.T) {
	reader := &nodeAnalyticsReader{}
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/analytics", getNodeAnalytics(reader))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/analytics?since=1000&until=5000&iatas=yvr,YOW,yvr", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.nodeID != uuid.MustParse("00000000-0000-0000-0000-000000000001") {
		t.Fatalf("unexpected node id %s", reader.nodeID)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YOW,YVR" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.NodeAnalytics
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.KPIs.ObservationCount != 5 {
		t.Fatalf("expected observation count 5, got %d", body.KPIs.ObservationCount)
	}
}

func TestListNodes_InvalidType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?type=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidPubkey(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkey=nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibytePaths(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibytePaths=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibyteTraces(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibyteTraces=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
