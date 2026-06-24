// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package router wires all HTTP routes onto the Chi router and injects
// dependencies (hub, reader, ingest workers) into the handler closures.
// All routes are mounted under /api/v1 with public and private groups
// stubbed for future auth middleware.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/ratelimit"
	"github.com/MeshCore-Beacon/beacon-server/internal/ws"
	httpSwagger "github.com/swaggo/http-swagger"
)

// New builds and returns the top-level Chi router.
//
// Route shape:
//
//	/ws                → WebSocket (public in v1)
//	/healthz           → liveness/dependency health
//	/readyz            → strict readiness for deploy checks
//	/api/v1/           → public group
//	  /packets         → packets subrouter
//	  /nodes           → nodes subrouter
//	  /brokers         → brokers subrouter
//	  /observers       → observers subrouter
//	  /channels        → channels subrouter
//	  /iatas           → iatas subrouter
//	  /regions         → regions subrouter
//	  /stats           → stats subrouter
//	  /netgraph        → render-ready verified-route graph
//
// The private group is stubbed and ready for the auth middleware drop-in
// described in Future Features → Admin authentication.
func New(h *hub.Hub, reader api.Reader, workers []*ingest.Worker, wsCfg config.WebSocketConfig, corsCfg config.CORSConfig, rateCfg config.RateLimitConfig, healthCfg handlers.HealthConfig) http.Handler {
	r := chi.NewRouter()
	rateCfg = config.ResolveRateLimits(rateCfg)
	restLimiter := ratelimit.New(ratelimit.Config{
		RequestsPerMinute: rateCfg.RESTPerIPPerMinute,
		Burst:             rateCfg.RESTBurst,
	})
	wsMessageLimiter := ratelimit.New(ratelimit.Config{
		RequestsPerMinute: rateCfg.WebSocketMessagesPerIPPerMinute,
		Burst:             rateCfg.WebSocketMessageBurst,
	})
	if rateCfg.Disabled {
		restLimiter = nil
		wsMessageLimiter = nil
	}
	healthCfg.RateLimitSnapshot = func() map[string]ratelimit.Snapshot {
		snapshot := map[string]ratelimit.Snapshot{}
		if restLimiter != nil {
			snapshot["publicRest"] = restLimiter.Snapshot()
		}
		if wsMessageLimiter != nil {
			snapshot["websocketMessages"] = wsMessageLimiter.Snapshot()
		}
		return snapshot
	}

	// ── CORS ─────────────────────────────────────────────────────────────────
	allowedOrigins := corsCfg.AllowedOrigins
	if len(allowedOrigins) == 0 {
		allowedOrigins = []string{"*"}
	}
	allowedMethods := corsCfg.AllowedMethods
	if len(allowedMethods) == 0 {
		allowedMethods = []string{"GET", "HEAD", "OPTIONS"}
	}
	allowedHeaders := corsCfg.AllowedHeaders
	if len(allowedHeaders) == 0 {
		allowedHeaders = []string{"Accept", "Authorization", "Content-Type"}
	}
	maxAge := corsCfg.MaxAge
	if maxAge == 0 {
		maxAge = 300
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   allowedMethods,
		AllowedHeaders:   allowedHeaders,
		AllowCredentials: corsCfg.AllowCredentials,
		MaxAge:           maxAge,
	}))

	// ── Global middleware ────────────────────────────────────────────────────
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.CleanPath)
	r.Use(middleware.StripSlashes)

	// ── Swagger UI ──────────────────────────────────────────────────────────
	r.Get("/swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusMovedPermanently)
	})

	r.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	// ── WebSocket ────────────────────────────────────────────────────────────
	r.Get("/ws", ws.Handler(h, reader, ws.Options{
		MaxConnectionsPerIP: wsCfg.MaxConnectionsPerIP,
		AllowedOrigins:      wsCfg.AllowedOrigins,
		MessageLimiter:      wsMessageLimiter,
	}))
	r.Get("/healthz", handlers.HealthHandler(reader, workers, healthCfg))
	r.Get("/readyz", handlers.ReadinessHandler(reader, workers, healthCfg))

	// ── Public REST API (v1) ─────────────────────────────────────────────────
	r.Route("/api/v1", func(r chi.Router) {
		// Public group — no authentication required (all of v1 is public).
		r.Group(func(r chi.Router) {
			r.Use(mw.RateLimit(restLimiter))
			r.Mount("/packets", handlers.PacketsRouter(reader))
			r.Mount("/nodes", handlers.NodesRouter(reader))
			r.Mount("/brokers", handlers.BrokersRouter(workers))
			r.Mount("/observers", handlers.ObserversRouter(reader))
			r.Mount("/channels", handlers.ChannelsRouter(reader))
			r.Mount("/messages", handlers.MessagesRouter(reader))
			r.Mount("/iatas", handlers.IATAsRouter(reader))
			r.Mount("/regions", handlers.RegionsRouter(reader))
			r.Mount("/routes", handlers.RoutesRouter(reader))
			r.Mount("/scopes", handlers.ScopesRouter(reader))
			r.Mount("/stats", handlers.StatsRouter(reader))
			r.Mount("/netgraph", handlers.NetgraphRouter(reader))
			r.Mount("/traces", handlers.TracesRouter(reader))
			r.Mount("/atlas", handlers.AtlasRouter(reader))
			r.Mount("/live", handlers.LiveRouter(reader))
			r.Mount("/search", handlers.SearchRouter(reader))
		})

		// Private group — auth middleware applied.
		// Stubbed for the admin endpoints described in Future Features.
		// Swap mw.NoopAuth for a real JWT/session middleware when ready.
		r.Group(func(r chi.Router) {
			r.Use(mw.NoopAuth)
			// r.Mount("/admin", handlers.AdminRouter())
		})
	})

	return r
}
