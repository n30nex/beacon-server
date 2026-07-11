// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
