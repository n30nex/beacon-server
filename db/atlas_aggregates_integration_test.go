// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAtlasAggregateCurrentSizeIntegration(t *testing.T) {
	if os.Getenv("BEACON_ATLAS_AGGREGATE_INTEGRATION") != "1" {
		t.Skip("set BEACON_ATLAS_AGGREGATE_INTEGRATION=1 for the live read-only probe")
	}
	dsn := os.Getenv("BEACON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("BEACON_TEST_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool)
	until := time.Now().Truncate(30 * time.Second)
	since := until.Add(-24 * time.Hour)
	if !store.atlasAggregatesAvailable(context.Background(), since, until, true) {
		t.Fatal("canonical current/previous aggregate coverage is incomplete")
	}

	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{"iata", func(ctx context.Context) error {
			_, err := store.getAtlasIATAsAggregated(ctx, since, until, "")
			return err
		}},
		{"previous_iata", func(ctx context.Context) error {
			_, err := store.getAtlasIATAsAggregated(ctx, since.Add(-24*time.Hour), since, "")
			return err
		}},
		{"mix", func(ctx context.Context) error {
			_, _, err := store.getAtlasPayloadAndRouteMixAggregated(ctx, since, until, "")
			return err
		}},
		{"nodes", func(ctx context.Context) error {
			_, err := store.getAtlasTopNodesAggregated(ctx, since, until, "", 8)
			return err
		}},
		{"observers", func(ctx context.Context) error {
			_, err := store.getAtlasTopObserversAggregated(ctx, since, until, "", 8)
			return err
		}},
		{"active_nodes", func(ctx context.Context) error {
			_, err := store.getAtlasActiveNodesByIATAAggregated(ctx, since, until)
			return err
		}},
		{"observer_health", func(ctx context.Context) error {
			_, err := store.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{StatsFilter: api.StatsFilter{Since: since, Until: until, Limit: 500}, StaleAfter: statsDefaultStaleAfter})
			return err
		}},
	}
	for _, step := range steps {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		started := time.Now()
		err := step.run(ctx)
		cancel()
		t.Logf("%s duration=%s err=%v", step.name, time.Since(started), err)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
	}
}
