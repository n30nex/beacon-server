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
