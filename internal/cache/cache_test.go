// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	redis "github.com/redis/go-redis/v9"
)

func TestResolveTTLsAtlasDefaultsToShortHotPath(t *testing.T) {
	ttls := ResolveTTLs(config.CacheConfig{})
	if ttls.Atlas != 30*time.Second {
		t.Fatalf("expected atlas ttl to default to 30s, got %s", ttls.Atlas)
	}
	if ttls.Live != 5*time.Second {
		t.Fatalf("expected live ttl to default to 5s, got %s", ttls.Live)
	}
	if ttls.Stats != time.Hour {
		t.Fatalf("expected stats ttl to keep 1h default, got %s", ttls.Stats)
	}
	if ttls.Netgraph != 10*time.Second {
		t.Fatalf("expected netgraph ttl to default to 10s, got %s", ttls.Netgraph)
	}
}

func newMemoryCacheClient(initial map[string][]byte) *Client {
	var mu sync.Mutex
	values := initial
	if values == nil {
		values = map[string][]byte{}
	}
	return &Client{
		metrics: NewMetrics(),
		getBytes: func(_ context.Context, key string) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			value, ok := values[key]
			if !ok {
				return nil, redis.Nil
			}
			return append([]byte(nil), value...), nil
		},
		setBytes: func(_ context.Context, key string, value []byte, _ time.Duration) error {
			mu.Lock()
			defer mu.Unlock()
			values[key] = append([]byte(nil), value...)
			return nil
		},
	}
}

func TestGetOrSetCoalescesIdenticalMisses(t *testing.T) {
	client := newMemoryCacheClient(nil)
	var fetches atomic.Int32
	const callers = 12
	values := make(chan int, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := getOrSet(context.Background(), client, keyAtlasBriefingPrefix+"all", 30*time.Second, func(context.Context) (int, error) {
				fetches.Add(1)
				time.Sleep(40 * time.Millisecond)
				return 42, nil
			})
			if err != nil {
				t.Errorf("getOrSet: %v", err)
				return
			}
			values <- value
		}()
	}
	wg.Wait()
	close(values)
	for value := range values {
		if value != 42 {
			t.Fatalf("value = %d, want 42", value)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1", got)
	}
	if client.metrics.Snapshot()[CategoryAtlas].Coalesced == 0 {
		t.Fatal("expected coalesced cache metric")
	}
}

func TestGetOrSetServesStaleWhenRefreshFails(t *testing.T) {
	key := keyLiveSummaryPrefix + "all"
	now := time.Now()
	envelope := cacheEnvelope[string]{
		Version:       cacheEnvelopeVersion,
		GeneratedAt:   now.Add(-10 * time.Second).UnixMilli(),
		FreshUntil:    now.Add(-5 * time.Second).UnixMilli(),
		HardExpiresAt: now.Add(5 * time.Second).UnixMilli(),
		Value:         "last-known-good",
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	client := newMemoryCacheClient(map[string][]byte{key: raw})
	refreshAttempted := make(chan struct{}, 1)

	value, err := getOrSet(context.Background(), client, key, 5*time.Second, func(context.Context) (string, error) {
		refreshAttempted <- struct{}{}
		return "", errors.New("database busy")
	})
	if err != nil || value != "last-known-good" {
		t.Fatalf("stale result = %q, %v", value, err)
	}
	select {
	case <-refreshAttempted:
	case <-time.After(time.Second):
		t.Fatal("bounded stale refresh did not run")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := client.metrics.Snapshot()[CategoryLive]
		if stats.RefreshFailures > 0 {
			if stats.StaleServed != 1 {
				t.Fatalf("stale served = %d, want 1", stats.StaleServed)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected failed refresh metric while stale value remained available")
}

func TestStatsPresetCacheKeyIsStableAndNormalizesQueryWindow(t *testing.T) {
	first := api.StatsFilter{
		IATAs:        []string{"YVR", "YYJ"},
		Since:        time.Unix(1, 0),
		Until:        time.Unix(2, 0),
		Bucket:       "1h",
		Limit:        25,
		WindowPreset: "24h",
	}
	second := first
	second.IATAs = []string{"YYJ", "YVR"}
	second.Since = time.Unix(3, 0)
	second.Until = time.Unix(4, 0)

	firstKey, firstNormalized := statsFilterCacheKey(keyStatsSummaryPrefix, first, time.Hour)
	secondKey, secondNormalized := statsFilterCacheKey(keyStatsSummaryPrefix, second, time.Hour)

	if firstKey != secondKey {
		t.Fatalf("preset cache keys differ: %q != %q", firstKey, secondKey)
	}
	if got := firstNormalized.Until.Sub(firstNormalized.Since); got != 24*time.Hour {
		t.Fatalf("normalized preset window = %s, want 24h", got)
	}
	if !firstNormalized.Since.Equal(secondNormalized.Since) || !firstNormalized.Until.Equal(secondNormalized.Until) {
		t.Fatalf("normalized preset queries differ: %#v != %#v", firstNormalized, secondNormalized)
	}
}

func TestStatsExactCacheKeyPreservesExplicitTimestamps(t *testing.T) {
	since := time.Date(2026, 7, 12, 10, 17, 23, 456000000, time.UTC)
	until := since.Add(37*time.Minute + 12*time.Second)
	key, normalized := statsFilterCacheKey(keyStatsSummaryPrefix, api.StatsFilter{
		Since: since, Until: until, Bucket: "1h", Limit: 25,
	}, time.Hour)

	if !normalized.Since.Equal(since) || !normalized.Until.Equal(until) {
		t.Fatalf("explicit timestamps changed: %#v", normalized)
	}
	wantWindow := "exact:" + strconv.FormatInt(since.UnixMilli(), 10) + ":" + strconv.FormatInt(until.UnixMilli(), 10)
	if !strings.Contains(key, wantWindow) {
		t.Fatalf("cache key %q does not contain %q", key, wantWindow)
	}
}

func TestStatsRefreshesAreGloballySerialized(t *testing.T) {
	client := newMemoryCacheClient(nil)
	var active atomic.Int32
	var maxActive atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := getOrSet(context.Background(), client, fmt.Sprintf("%s%d", keyStatsSummaryPrefix, i), time.Hour, func(context.Context) (int, error) {
				current := active.Add(1)
				for {
					previous := maxActive.Load()
					if current <= previous || maxActive.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(15 * time.Millisecond)
				active.Add(-1)
				return i, nil
			})
			if err != nil {
				t.Errorf("getOrSet: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent stats refreshes = %d, want 1", got)
	}
}
