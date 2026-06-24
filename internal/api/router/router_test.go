// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

func TestDefaultCORSAllowsPublicReadOnlyOrigins(t *testing.T) {
	handler := New(hub.New(), nil, nil, config.WebSocketConfig{}, config.CORSConfig{}, config.RateLimitConfig{Disabled: true}, handlers.HealthConfig{})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/packets", nil)
	req.Header.Set("Origin", "https://reader.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if got := res.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := res.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodGet) {
		t.Fatalf("Access-Control-Allow-Methods = %q, want GET included", got)
	}
}

func TestConfiguredCORSRestrictsOrigins(t *testing.T) {
	handler := New(hub.New(), nil, nil, config.WebSocketConfig{}, config.CORSConfig{
		AllowedOrigins: []string{"https://beacon.canadaverse.org"},
	}, config.RateLimitConfig{Disabled: true}, handlers.HealthConfig{})

	allowed := httptest.NewRequest(http.MethodOptions, "/api/v1/packets", nil)
	allowed.Header.Set("Origin", "https://beacon.canadaverse.org")
	allowed.Header.Set("Access-Control-Request-Method", http.MethodGet)
	allowedRes := httptest.NewRecorder()
	handler.ServeHTTP(allowedRes, allowed)

	if got := allowedRes.Header().Get("Access-Control-Allow-Origin"); got != "https://beacon.canadaverse.org" {
		t.Fatalf("allowed origin header = %q, want configured origin", got)
	}

	rejected := httptest.NewRequest(http.MethodOptions, "/api/v1/packets", nil)
	rejected.Header.Set("Origin", "https://evil.example")
	rejected.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rejectedRes := httptest.NewRecorder()
	handler.ServeHTTP(rejectedRes, rejected)

	if got := rejectedRes.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("rejected origin header = %q, want empty", got)
	}
}

func TestPublicAPIRateLimitRejectsExcessRequests(t *testing.T) {
	handler := New(hub.New(), nil, nil, config.WebSocketConfig{}, config.CORSConfig{}, config.RateLimitConfig{
		RESTPerIPPerMinute: 1,
		RESTBurst:          1,
	}, handlers.HealthConfig{})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/v1/brokers", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", first.Code)
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/v1/brokers", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", second.Code)
	}
}
