// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestRequestMetricsUsesRoutePatternsAndNeverQueryStrings(t *testing.T) {
	metrics := NewRequestMetrics()
	r := chi.NewRouter()
	r.Use(metrics.Handler)
	r.Get("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	for _, uri := range []string{"/items/one?secret=alpha", "/items/two?secret=beta"} {
		res := httptest.NewRecorder()
		r.ServeHTTP(res, httptest.NewRequest(http.MethodGet, uri, nil))
	}

	snapshot := metrics.Snapshot()
	route, ok := snapshot.Routes["GET /items/{id}"]
	if !ok {
		t.Fatalf("expected bounded route-pattern metric, got %#v", snapshot.Routes)
	}
	if route.Count != 2 || len(snapshot.Routes) != 1 {
		t.Fatalf("unexpected route metric: %#v", snapshot.Routes)
	}
}

func TestRequestMetricsKeepsServiceLevelSeparateFromReadiness(t *testing.T) {
	metrics := NewRequestMetrics()
	now := time.Now()
	metrics.record("GET /api/v1/search", requestSample{at: now, duration: 3 * time.Second, status: http.StatusOK})

	snapshot := metrics.Snapshot()
	if snapshot.ServiceLevel.Status != "degraded" {
		t.Fatalf("service level = %q, want degraded", snapshot.ServiceLevel.Status)
	}
	if snapshot.Routes["GET /api/v1/search"].HardDeadlineBreaches != 1 {
		t.Fatalf("expected hard deadline breach, got %#v", snapshot.Routes)
	}
}

func TestRequestMetricsBoundsRouteCardinality(t *testing.T) {
	metrics := NewRequestMetrics()
	now := time.Now()
	for i := 0; i < maxRequestRoutes+10; i++ {
		metrics.record(http.MethodGet+" /route/"+string(rune('A'+i)), requestSample{at: now, duration: time.Millisecond, status: http.StatusOK})
	}

	if got := len(metrics.Snapshot().Routes); got > maxRequestRoutes+1 {
		t.Fatalf("route cardinality = %d, want at most %d", got, maxRequestRoutes+1)
	}
}
