// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
)

type failingHealthReader struct {
	stubReader
}

func (failingHealthReader) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	return nil, errors.New("database unavailable")
}

func TestHealthHandlerReportsDegradedCache(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	HealthHandler(stubReader{}, nil, HealthConfig{Version: "test", CacheStatus: "degraded", CacheBackend: "127.0.0.1:6379"}).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with database fallback, got %d", w.Code)
	}
	var body HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.Status != "degraded" {
		t.Fatalf("expected degraded status, got %q", body.Status)
	}
	if !body.Ready {
		t.Fatal("expected degraded cache to keep runtime ready")
	}
	if body.Mode != "health" {
		t.Fatalf("expected health mode, got %q", body.Mode)
	}
	if body.Dependencies["cache"].Status != "degraded" {
		t.Fatalf("expected degraded cache dependency, got %#v", body.Dependencies["cache"])
	}
	if body.Dependencies["ingestWorkers"].Status != "disabled" {
		t.Fatalf("expected disabled ingest dependency without workers, got %#v", body.Dependencies["ingestWorkers"])
	}
	if body.Dependencies["websocket"].Status != "ok" {
		t.Fatalf("expected websocket dependency, got %#v", body.Dependencies["websocket"])
	}
}

func TestReadinessHandlerFailsWhenDatabaseIsDown(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	ReadinessHandler(failingHealthReader{}, nil, HealthConfig{Version: "test", CacheStatus: "ok"}).ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when database is down, got %d", w.Code)
	}
	var body HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if body.Ready {
		t.Fatal("expected readiness false when database is down")
	}
	if body.Mode != "readiness" {
		t.Fatalf("expected readiness mode, got %q", body.Mode)
	}
	if body.Dependencies["database"].Status != "down" {
		t.Fatalf("expected down database dependency, got %#v", body.Dependencies["database"])
	}
}

func TestReadinessHandlerFailsWhenConfiguredBrokerDisconnected(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	worker := ingest.New(ingest.Config{BrokerName: "mqtt-test"}, nil, nil, nil, nil)

	ReadinessHandler(stubReader{}, []*ingest.Worker{worker}, HealthConfig{Version: "test", CacheStatus: "ok"}).ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when configured broker is disconnected, got %d", w.Code)
	}
	var body HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if body.Ready {
		t.Fatal("expected readiness false when a configured broker is disconnected")
	}
	if len(body.Brokers) != 1 || body.Brokers[0].Name != "mqtt-test" || body.Brokers[0].Status != "down" {
		t.Fatalf("unexpected broker health: %#v", body.Brokers)
	}
	if body.Dependencies["ingestWorkers"].Status != "degraded" {
		t.Fatalf("expected degraded ingest dependency, got %#v", body.Dependencies["ingestWorkers"])
	}
}

func TestHealthHandlerReportsOperationalSnapshots(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	HealthHandler(stubReader{}, nil, HealthConfig{
		Version:      "test",
		CacheStatus:  "ok",
		CacheBackend: "127.0.0.1:6379",
		CacheSnapshot: func() map[string]cache.CategorySnapshot {
			return map[string]cache.CategorySnapshot{
				cache.CategoryStats: {
					Hits:          2,
					Misses:        1,
					Invalidations: 1,
					TTLSeconds:    3600,
				},
			}
		},
		BackgroundSnapshot: func() map[string]background.TaskSnapshot {
			return map[string]background.TaskSnapshot{
				"cleanup": {
					Runs:           1,
					Successes:      1,
					LastStatus:     "success",
					LastDurationMs: 7,
				},
			}
		},
		Build: BuildProvenance{Version: "test", SHA: "abc1234", BuildTime: "2026-07-10T12:00:00Z", Dirty: true},
		RequestSnapshot: func() mw.RequestMetricsSnapshot {
			return mw.RequestMetricsSnapshot{
				ServiceLevel: mw.ServiceLevelSnapshot{Status: "degraded", WorstRoute: "GET /api/v1/search", WorstP95Ms: 900, TargetMs: 500},
				Routes:       map[string]mw.RouteRequestSnapshot{"GET /api/v1/search": {Count: 3, P95DurationMs: 900, TargetMs: 500}},
			}
		},
		DatabasePoolSnapshot: func() DatabasePoolSnapshot {
			return DatabasePoolSnapshot{AcquiredConnections: 2, IdleConnections: 3, TotalConnections: 5, MaxConnections: 20, EmptyAcquireCount: 1}
		},
	}).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.CacheMetrics[cache.CategoryStats].Hits != 2 {
		t.Fatalf("expected cache metrics in health response, got %#v", body.CacheMetrics)
	}
	if body.BackgroundTasks["cleanup"].LastStatus != "success" {
		t.Fatalf("expected background task metrics in health response, got %#v", body.BackgroundTasks)
	}
	if !body.Ready || body.Status != "ok" || body.ServiceLevel.Status != "degraded" {
		t.Fatalf("performance degradation must remain separate from readiness: %#v", body)
	}
	if body.Build.SHA != "abc1234" || !body.Build.Dirty {
		t.Fatalf("expected build provenance, got %#v", body.Build)
	}
	if body.DatabasePool == nil || body.DatabasePool.TotalConnections != 5 {
		t.Fatalf("expected database pool snapshot, got %#v", body.DatabasePool)
	}
}
