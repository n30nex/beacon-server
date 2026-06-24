// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/ratelimit"
)

type HealthConfig struct {
	Version            string
	CacheStatus        string
	CacheBackend       string
	RateLimitSnapshot  func() map[string]ratelimit.Snapshot
	CacheSnapshot      func() map[string]cache.CategorySnapshot
	BackgroundSnapshot func() map[string]background.TaskSnapshot
}

type HealthDependency struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type HealthBroker struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
	Status    string `json:"status"`
}

type HealthResponse struct {
	Status          string                             `json:"status"`
	Ready           bool                               `json:"ready"`
	Version         string                             `json:"version"`
	ServerTime      int64                              `json:"serverTime"`
	Mode            string                             `json:"mode"`
	Dependencies    map[string]HealthDependency        `json:"dependencies"`
	Brokers         []HealthBroker                     `json:"brokers"`
	RateLimits      map[string]ratelimit.Snapshot      `json:"rateLimits,omitempty"`
	CacheMetrics    map[string]cache.CategorySnapshot  `json:"cacheMetrics,omitempty"`
	BackgroundTasks map[string]background.TaskSnapshot `json:"backgroundTasks,omitempty"`
}

type healthSnapshot struct {
	status       string
	ready        bool
	serverTime   int64
	dependencies map[string]HealthDependency
	brokers      []HealthBroker
}

func buildHealthSnapshot(ctx context.Context, reader api.Reader, workers []*ingest.Worker, cfg HealthConfig) healthSnapshot {
	status := "ok"
	ready := true
	deps := map[string]HealthDependency{}

	dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := reader.ListRegions(dbCtx); err != nil {
		status = "degraded"
		ready = false
		deps["database"] = HealthDependency{Status: "down", Detail: err.Error()}
	} else {
		deps["database"] = HealthDependency{Status: "ok"}
	}

	cacheStatus := cfg.CacheStatus
	if cacheStatus == "" {
		cacheStatus = "disabled"
	}
	if cacheStatus == "degraded" {
		status = "degraded"
	}
	deps["cache"] = HealthDependency{Status: cacheStatus, Detail: cfg.CacheBackend}

	brokers := make([]HealthBroker, 0, len(workers))
	connectedBrokers := 0
	for _, worker := range workers {
		connected := worker.IsConnected()
		brokerStatus := "down"
		if connected {
			brokerStatus = "ok"
			connectedBrokers++
		} else {
			status = "degraded"
			ready = false
		}
		brokers = append(brokers, HealthBroker{Name: worker.BrokerName(), Connected: connected, Status: brokerStatus})
	}
	ingestStatus := "disabled"
	ingestDetail := "no brokers configured"
	if len(workers) > 0 {
		ingestStatus = "ok"
		if connectedBrokers != len(workers) {
			ingestStatus = "degraded"
		}
		ingestDetail = "brokers connected"
		if connectedBrokers != len(workers) {
			ingestDetail = "one or more brokers disconnected"
		}
	}
	deps["ingestWorkers"] = HealthDependency{Status: ingestStatus, Detail: ingestDetail}
	deps["websocket"] = HealthDependency{Status: "ok", Detail: "endpoint available at /ws"}

	return healthSnapshot{
		status:       status,
		ready:        ready,
		serverTime:   time.Now().UnixMilli(),
		dependencies: deps,
		brokers:      brokers,
	}
}

func respondHealth(w http.ResponseWriter, snap healthSnapshot, cfg HealthConfig, mode string, strictReady bool) {
	httpStatus := http.StatusOK
	if strictReady && !snap.ready {
		httpStatus = http.StatusServiceUnavailable
	} else if snap.dependencies["database"].Status == "down" {
		httpStatus = http.StatusServiceUnavailable
	}
	respond(w, httpStatus, HealthResponse{
		Status:          snap.status,
		Ready:           snap.ready,
		Version:         cfg.Version,
		ServerTime:      snap.serverTime,
		Mode:            mode,
		Dependencies:    snap.dependencies,
		Brokers:         snap.brokers,
		RateLimits:      rateLimitSnapshot(cfg),
		CacheMetrics:    cacheSnapshot(cfg),
		BackgroundTasks: backgroundSnapshot(cfg),
	})
}

func rateLimitSnapshot(cfg HealthConfig) map[string]ratelimit.Snapshot {
	if cfg.RateLimitSnapshot == nil {
		return nil
	}
	snapshot := cfg.RateLimitSnapshot()
	if len(snapshot) == 0 {
		return nil
	}
	return snapshot
}

func cacheSnapshot(cfg HealthConfig) map[string]cache.CategorySnapshot {
	if cfg.CacheSnapshot == nil {
		return nil
	}
	snapshot := cfg.CacheSnapshot()
	if len(snapshot) == 0 {
		return nil
	}
	return snapshot
}

func backgroundSnapshot(cfg HealthConfig) map[string]background.TaskSnapshot {
	if cfg.BackgroundSnapshot == nil {
		return nil
	}
	snapshot := cfg.BackgroundSnapshot()
	if len(snapshot) == 0 {
		return nil
	}
	return snapshot
}

// HealthHandler godoc
//
//	@Summary	Get runtime health
//	@Tags		Health
//	@Produce	json
//	@Success	200	{object}	handlers.HealthResponse
//	@Failure	503	{object}	handlers.HealthResponse
//	@Router		/healthz [get]
func HealthHandler(reader api.Reader, workers []*ingest.Worker, cfg HealthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respondHealth(w, buildHealthSnapshot(r.Context(), reader, workers, cfg), cfg, "health", false)
	}
}

// ReadinessHandler godoc
//
//	@Summary	Get runtime readiness
//	@Tags		Health
//	@Produce	json
//	@Success	200	{object}	handlers.HealthResponse
//	@Failure	503	{object}	handlers.HealthResponse
//	@Router		/readyz [get]
func ReadinessHandler(reader api.Reader, workers []*ingest.Worker, cfg HealthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respondHealth(w, buildHealthSnapshot(r.Context(), reader, workers, cfg), cfg, "readiness", true)
	}
}
