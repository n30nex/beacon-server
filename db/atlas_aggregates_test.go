// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
	"time"
)

func TestAtlasBoundsUsesOverlappingCanonicalBuckets(t *testing.T) {
	since := time.Date(2026, 7, 10, 1, 16, 24, 0, time.UTC)
	until := since.Add(24 * time.Hour)
	bounds := atlasBounds(since, until)
	if want := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC); !bounds.fullStart.Equal(want) {
		t.Fatalf("fullStart = %s, want %s", bounds.fullStart, want)
	}
	if want := time.Date(2026, 7, 11, 2, 0, 0, 0, time.UTC); !bounds.fullEnd.Equal(want) {
		t.Fatalf("fullEnd = %s, want %s", bounds.fullEnd, want)
	}
	if got := bounds.fullEnd.Sub(bounds.fullStart) / time.Hour; got != 25 {
		t.Fatalf("overlapping aggregate hours = %d, want 25", got)
	}
}

func TestCanonicalAtlasWindowRejectsHistoricalCustomWindow(t *testing.T) {
	now := time.Date(2026, 7, 11, 1, 30, 0, 0, time.UTC)
	if !canonicalAtlasWindow(now.Add(-24*time.Hour), now, now) {
		t.Fatal("current 24-hour window should use aggregates")
	}
	if canonicalAtlasWindow(now.Add(-72*time.Hour), now.Add(-48*time.Hour), now) {
		t.Fatal("historical custom window must use raw queries")
	}
	if canonicalAtlasWindow(now.Add(-6*time.Hour), now, now) {
		t.Fatal("custom duration must use raw queries")
	}
}
