// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// AtlasAggregateTask maintains hourly aggregate tables in bounded, atomic
// hour batches. It is scheduled independently but runs on the same serialized
// worker as all other maintenance.
func AtlasAggregateTask(store *db.Store, interval time.Duration) Task {
	return Task{
		Name:     "atlas_aggregate_refresh",
		Interval: interval,
		Run: func(ctx context.Context) (TaskResult, error) {
			rows, err := store.RefreshAtlasHourlyAggregates(ctx, time.Now())
			return TaskResult{AffectedRows: rows}, err
		},
	}
}

// ViewRefreshTask returns a Task that refreshes all materialized views.
func ViewRefreshTask(store *db.Store, interval time.Duration) Task {
	return Task{
		Name:     "view_refresh",
		Interval: interval,
		Run: func(ctx context.Context) (TaskResult, error) {
			var errs []error
			if err := store.RefreshHourlyStats(ctx); err != nil {
				log.Printf("background[view_refresh]: hourly stats: %v", err)
				errs = append(errs, fmt.Errorf("hourly stats: %w", err))
			}
			if err := store.RefreshTopNodes(ctx); err != nil {
				log.Printf("background[view_refresh]: top nodes: %v", err)
				errs = append(errs, fmt.Errorf("top nodes: %w", err))
			}
			if err := store.RefreshRadioPresets(ctx); err != nil {
				log.Printf("background[view_refresh]: radio presets: %v", err)
				errs = append(errs, fmt.Errorf("radio presets: %w", err))
			}
			return TaskResult{}, errors.Join(errs...)
		},
	}
}

// CleanupTask returns a Task that prunes old telemetry and packet rows.
func CleanupTask(store *db.Store, telemetryRetention, packetRetention, interval time.Duration) Task {
	return Task{
		Name:     "cleanup",
		Interval: interval,
		Run: func(ctx context.Context) (TaskResult, error) {
			telemetryRows, err := store.DeleteOldTelemetryCount(ctx, time.Now().Add(-telemetryRetention))
			if err != nil {
				return TaskResult{}, err
			}
			packetRows, err := store.DeleteOldPacketsCount(ctx, time.Now().Add(-packetRetention))
			if err != nil {
				return TaskResult{AffectedRows: telemetryRows}, err
			}
			return TaskResult{AffectedRows: telemetryRows + packetRows}, nil
		},
	}
}

// ReconfirmTask returns a Task that prunes stale and ambiguous resolved paths
// and neighbors. Runs after routes to ensure neighbors are cleaned against
// already-reconfirmed path data.
func ReconfirmTask(store *db.Store, dirty *DirtyIATAs, interval time.Duration) Task {
	return Task{
		Name:     "reconfirm",
		Interval: interval,
		Run: func(ctx context.Context) (TaskResult, error) {
			all, iatas := dirty.Take()
			if !all && len(iatas) == 0 {
				return TaskResult{Skipped: true}, nil
			}
			if all {
				iatas = nil
			}
			routeRows, err := store.ReconfirmRoutesForIATAs(ctx, iatas)
			if err != nil {
				dirty.Restore(all, iatas)
				return TaskResult{}, fmt.Errorf("routes: %w", err)
			}
			neighborRows, err := store.ReconfirmNeighborsForIATAs(ctx, iatas)
			if err != nil {
				dirty.Restore(all, iatas)
				return TaskResult{AffectedRows: routeRows}, fmt.Errorf("neighbors: %w", err)
			}
			return TaskResult{AffectedRows: routeRows + neighborRows}, nil
		},
	}
}
