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

type statsObserverCompareReader struct {
	stubReader
	filter api.StatsObserverCompareFilter
}

func (r *statsObserverCompareReader) GetStatsObserverCompare(ctx context.Context, filter api.StatsObserverCompareFilter) (*api.StatsObserverCompare, error) {
	r.filter = filter
	return &api.StatsObserverCompare{
		ServerTime: 123,
		Window:     api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		Items: []api.StatsObserverCompareItem{{
			StatsObserverHealth: api.StatsObserverHealth{ObserverID: filter.ObserverIDs[0], IATA: "YVR", ObservationCount: 8},
			PacketCount:         4,
		}},
	}, nil
}

func TestGetStatsObservations_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observations", getStatsObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observations?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloadBreakdown_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payload-breakdown", getStatsPayloadBreakdown(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payload-breakdown?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-nodes", getStatsTopNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsSummary_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/summary", getStatsSummary(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/summary?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsRegions_InvalidWindow(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/regions", getStatsRegions(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/regions?since=2000&until=1000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloads_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payloads", getStatsPayloads(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payloads?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsObserverHealth_InvalidStaleAfter(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observer-health", getStatsObserverHealth(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-health?staleAfterMinutes=nope", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsObserverCompare_InvalidObserverIDs(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observer-compare", getStatsObserverCompare(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-compare?observerIds=not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsObserverCompare_FilterContract(t *testing.T) {
	idA := uuid.MustParse("00000000-0000-0000-0000-000000000101")
	idB := uuid.MustParse("00000000-0000-0000-0000-000000000202")
	reader := &statsObserverCompareReader{}
	r := chi.NewRouter()
	r.Get("/stats/observer-compare", getStatsObserverCompare(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-compare?observerIds="+idA.String()+","+idB.String()+"&since=1000&until=5000&bucket=1h&iatas=yvr,YOW", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(reader.filter.ObserverIDs) != 2 || reader.filter.ObserverIDs[0] != idA || reader.filter.ObserverIDs[1] != idB {
		t.Fatalf("unexpected observer ids %#v", reader.filter.ObserverIDs)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsObserverCompare
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].PacketCount != 4 {
		t.Fatalf("unexpected response %#v", body.Items)
	}
}
