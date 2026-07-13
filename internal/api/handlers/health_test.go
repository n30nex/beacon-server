// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestHealthHandlerIsMinimalLiveness(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	HealthHandler(stubReader{}, nil, HealthConfig{Version: "test", CacheStatus: "degraded", CacheBackend: "127.0.0.1:6379"}).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with database fallback, got %d", w.Code)
	}
	var body LivenessResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.Status != "ok" || body.Version != "test" || body.ServerTime == 0 {
		t.Fatalf("unexpected liveness body: %#v", body)
	}
	if strings.Contains(w.Body.String(), "dependencies") || strings.Contains(w.Body.String(), "127.0.0.1") {
		t.Fatalf("liveness leaked diagnostics: %s", w.Body.String())
	}
}

func TestReadinessHandlerFailsWhenDatabaseIsDown(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	ReadinessHandler(failingHealthReader{}, nil, HealthConfig{Version: "test", CacheStatus: "ok"}).ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when database is down, got %d", w.Code)
	}
	var body ReadinessResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if body.Ready {
		t.Fatal("expected readiness false when database is down")
	}
	if strings.Contains(w.Body.String(), "database unavailable") || strings.Contains(w.Body.String(), "dependencies") {
		t.Fatalf("readiness leaked dependency diagnostics: %s", w.Body.String())
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
	var body ReadinessResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if body.Ready {
		t.Fatal("expected readiness false when a configured broker is disconnected")
	}
	if strings.Contains(w.Body.String(), "mqtt-test") {
		t.Fatalf("readiness leaked broker name: %s", w.Body.String())
	}
}

func TestDiagnosticsHandlerReportsOperationalSnapshots(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ops/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	w := httptest.NewRecorder()

	DiagnosticsHandler(stubReader{}, nil, HealthConfig{
		Version:      "test",
		CacheStatus:  "ok",
		CacheBackend: "127.0.0.1:6379",
		CacheSnapshot: func() map[string]cache.CategorySnapshot {
			return map[string]cache.CategorySnapshot{cache.CategoryStats: {Hits: 2, Misses: 1, Invalidations: 1, TTLSeconds: 3600}}
		},
		BackgroundSnapshot: func() map[string]background.TaskSnapshot {
			return map[string]background.TaskSnapshot{"cleanup": {Runs: 1, Successes: 1, LastStatus: "success", LastDurationMs: 7}}
		},
		Build: BuildProvenance{Version: "test", SHA: "abc1234", BuildTime: "2026-07-10T12:00:00Z", Dirty: true},
		RequestSnapshot: func() mw.RequestMetricsSnapshot {
			return mw.RequestMetricsSnapshot{ServiceLevel: mw.ServiceLevelSnapshot{Status: "degraded", WorstRoute: "GET /api/v1/search", WorstP95Ms: 900, TargetMs: 500}, Routes: map[string]mw.RouteRequestSnapshot{"GET /api/v1/search": {Count: 3, P95DurationMs: 900, TargetMs: 500}}}
		},
		DatabasePoolSnapshot: func() DatabasePoolSnapshot {
			return DatabasePoolSnapshot{AcquiredConnections: 2, IdleConnections: 3, TotalConnections: 5, MaxConnections: 20, EmptyAcquireCount: 1}
		},
		DiagnosticsToken: "test-secret",
	}).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
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
	if body.Mode != "diagnostics" {
		t.Fatalf("mode = %q, want diagnostics", body.Mode)
	}
}

func TestDiagnosticsHandlerFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		token  string
		header string
		status int
	}{
		{name: "not configured", status: http.StatusServiceUnavailable},
		{name: "missing", token: "secret", status: http.StatusUnauthorized},
		{name: "invalid", token: "secret", header: "Bearer wrong", status: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/ops/diagnostics", nil)
			req.Header.Set("Authorization", tc.header)
			w := httptest.NewRecorder()
			DiagnosticsHandler(stubReader{}, nil, HealthConfig{DiagnosticsToken: tc.token}).ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d", w.Code, tc.status)
			}
		})
	}
}

func TestSystemStatusReportsAnalyticsSLOBreachWithoutDetails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/status", nil)
	w := httptest.NewRecorder()
	SystemStatusHandler(stubReader{}, nil, HealthConfig{RequestSnapshot: func() mw.RequestMetricsSnapshot {
		return mw.RequestMetricsSnapshot{Routes: map[string]mw.RouteRequestSnapshot{
			"GET /api/v1/stats/summary": {Errors: 1, P95DurationMs: 5100, TargetMs: 750},
		}}
	}}).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body SystemStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Analytics.Status != "degraded" {
		t.Fatalf("analytics status = %q, want degraded", body.Analytics.Status)
	}
	if strings.Contains(w.Body.String(), "summary") || strings.Contains(w.Body.String(), "5100") {
		t.Fatalf("public system status leaked route diagnostics: %s", w.Body.String())
	}
}
