// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
)

type HealthConfig struct {
	Version      string
	CacheStatus  string
	CacheBackend string
}

type HealthDependency struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type HealthBroker struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
}

type HealthResponse struct {
	Status       string                      `json:"status"`
	Version      string                      `json:"version"`
	ServerTime   int64                       `json:"serverTime"`
	Dependencies map[string]HealthDependency `json:"dependencies"`
	Brokers      []HealthBroker              `json:"brokers"`
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
		status := "ok"
		deps := map[string]HealthDependency{}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if _, err := reader.ListRegions(ctx); err != nil {
			status = "degraded"
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
		for _, worker := range workers {
			connected := worker.IsConnected()
			if !connected {
				status = "degraded"
			}
			brokers = append(brokers, HealthBroker{Name: worker.BrokerName(), Connected: connected})
		}

		httpStatus := http.StatusOK
		if deps["database"].Status == "down" {
			httpStatus = http.StatusServiceUnavailable
		}
		respond(w, httpStatus, HealthResponse{
			Status:       status,
			Version:      cfg.Version,
			ServerTime:   time.Now().UnixMilli(),
			Dependencies: deps,
			Brokers:      brokers,
		})
	}
}
