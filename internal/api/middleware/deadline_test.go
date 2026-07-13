// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequestDeadlineDoesNotBoundWebSocketSessions(t *testing.T) {
	hadDeadline := false
	handler := RequestDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadDeadline = r.Context().Deadline()
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ws", nil))

	if hadDeadline {
		t.Fatal("WebSocket request inherited an HTTP request deadline")
	}
}

func TestRequestDeadlineAllowsColdStatsCacheWarmup(t *testing.T) {
	var remaining time.Duration
	handler := RequestDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		if !ok {
			t.Fatal("stats request did not receive a deadline")
		}
		remaining = time.Until(deadline)
		w.WriteHeader(http.StatusOK)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/stats/summary", nil))

	if remaining < 19*time.Second || remaining > 20*time.Second {
		t.Fatalf("stats deadline remaining = %s, want approximately 20s", remaining)
	}
}

func TestRequestDeadlineStillBoundsOrdinaryHTTP(t *testing.T) {
	hadDeadline := false
	handler := RequestDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadDeadline = r.Context().Deadline()
		w.WriteHeader(http.StatusOK)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil))

	if !hadDeadline {
		t.Fatal("ordinary HTTP request did not receive a deadline")
	}
}
