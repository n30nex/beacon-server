// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
	if body.Dependencies["cache"].Status != "degraded" {
		t.Fatalf("expected degraded cache dependency, got %#v", body.Dependencies["cache"])
	}
}
