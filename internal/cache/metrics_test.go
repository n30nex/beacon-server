// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"testing"
	"time"
)

func TestMetricsSnapshotRecordsCategoryTTLAndInvalidations(t *testing.T) {
	metrics := NewMetrics()
	metrics.ConfigureTTLs(CacheTTLs{Stats: time.Hour})
	metrics.recordMiss(CategoryStats, time.Hour)
	metrics.recordHit(CategoryStats, time.Hour)
	metrics.recordInvalidation(CategoryStats)
	metrics.recordError(CategoryStats, "set", time.Hour)

	snapshot := metrics.Snapshot()
	stats := snapshot[CategoryStats]
	if stats.Hits != 1 || stats.Misses != 1 || stats.Invalidations != 1 {
		t.Fatalf("unexpected stats counters: %#v", stats)
	}
	if stats.TTLSeconds != int64(time.Hour.Seconds()) {
		t.Fatalf("expected stats TTL seconds, got %d", stats.TTLSeconds)
	}
	if stats.Errors["set"] != 1 {
		t.Fatalf("expected set error count, got %#v", stats.Errors)
	}
}

func TestCategoryForKeyMapsHotPathPrefixes(t *testing.T) {
	cases := map[string]string{
		keyAtlasBriefingPrefix + "all":          CategoryAtlas,
		keyLiveSummaryPrefix + "all":            CategoryLive,
		keyStatsObserverHealthPrefix + "all":    CategoryStats,
		keyRadioPresetsPrefix + "default":       CategoryStats,
		keyKnownRoutesPrefix + "all":            CategoryNetgraph,
		keyNodeNeighborsPrefix + "node-id":      CategoryNodes,
		keyObserverScopesPrefix + "observer-id": CategoryObservers,
		keyScopeByNamePrefix + "scope":          CategoryReference,
		"beacon:unmapped":                       CategoryUnknown,
	}
	for key, want := range cases {
		if got := categoryForKey(key); got != want {
			t.Fatalf("categoryForKey(%q) = %q, want %q", key, got, want)
		}
	}
}
