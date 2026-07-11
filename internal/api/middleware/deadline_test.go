// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
