// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ratelimit

import (
	"testing"
	"time"
)

func TestLimiterPrunesIdleBuckets(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	limiter := New(Config{RequestsPerMinute: 60, Burst: 2, IdleTTL: time.Minute, MaxBuckets: 10})
	limiter.now = func() time.Time { return now }
	if !limiter.Allow("first") {
		t.Fatal("first request rejected")
	}
	now = now.Add(2 * time.Minute)
	if !limiter.Allow("second") {
		t.Fatal("second request rejected")
	}
	if got := limiter.Snapshot().ActiveBuckets; got != 1 {
		t.Fatalf("active buckets = %d, want 1 after idle pruning", got)
	}
}

func TestLimiterEvictsLeastRecentlyUsedBucketAtCapacity(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	limiter := New(Config{RequestsPerMinute: 60, Burst: 2, IdleTTL: time.Hour, MaxBuckets: 2})
	limiter.now = func() time.Time { return now }
	limiter.Allow("oldest")
	now = now.Add(time.Second)
	limiter.Allow("newer")
	now = now.Add(time.Second)
	limiter.Allow("newest")

	if got := limiter.Snapshot().ActiveBuckets; got != 2 {
		t.Fatalf("active buckets = %d, want hard cap 2", got)
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if _, ok := limiter.buckets["oldest"]; ok {
		t.Fatal("least recently used bucket was not evicted")
	}
}
