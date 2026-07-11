// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// RequestDeadline applies the product hard deadlines at the application edge.
// Dependency readiness remains independent from timeout-driven SLO degradation.
func RequestDeadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A WebSocket is a long-lived session. Applying the ordinary five-second
		// HTTP deadline cancels the upgraded connection and forces Live clients
		// into a reconnect loop.
		if r.URL.Path == "/ws" {
			next.ServeHTTP(w, r)
			return
		}

		deadline := 5 * time.Second
		switch {
		case r.URL.Path == "/healthz" || r.URL.Path == "/readyz":
			deadline = time.Second
		case strings.HasPrefix(r.URL.Path, "/api/v1/search"):
			deadline = 2 * time.Second
		case strings.HasPrefix(r.URL.Path, "/api/v1/live"):
			deadline = 2 * time.Second
		case strings.HasPrefix(r.URL.Path, "/api/v1/atlas"), strings.HasPrefix(r.URL.Path, "/api/v1/stats"):
			deadline = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), deadline)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
