// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cache provides a Redis-backed caching layer for the Beacon API.
// It wraps api.Reader with a CachedReader that transparently caches responses
// for read-heavy, slow-changing endpoints. If Redis is unavailable, all
// operations degrade gracefully to the underlying reader.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const cacheEnvelopeVersion = 2

type cacheEnvelope[T any] struct {
	Version       int   `json:"version"`
	GeneratedAt   int64 `json:"generatedAt"`
	FreshUntil    int64 `json:"freshUntil"`
	HardExpiresAt int64 `json:"hardExpiresAt"`
	Value         T     `json:"value"`
}

type cachePolicy struct {
	Fresh          time.Duration
	Hard           time.Duration
	RefreshTimeout time.Duration
}

// Client wraps a Redis client with helper methods used by CachedReader.
type Client struct {
	rdb      *redis.Client
	metrics  *Metrics
	flights  singleflight.Group
	getBytes func(context.Context, string) ([]byte, error)
	setBytes func(context.Context, string, []byte, time.Duration) error
}

// NewClient creates a new Redis client from the given address, password, and
// database index. addr should be in "host:port" form e.g. "localhost:6379".
// password may be empty if the Redis instance requires no authentication.
// db is the Redis database index, typically 0.
func NewClient(addr, password string, db int) *Client {
	opts := redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	rdb := redis.NewClient(&opts)
	return &Client{
		rdb:     rdb,
		metrics: NewMetrics(),
		getBytes: func(ctx context.Context, key string) ([]byte, error) {
			return rdb.Get(ctx, key).Bytes()
		},
		setBytes: func(ctx context.Context, key string, value []byte, ttl time.Duration) error {
			return rdb.Set(ctx, key, value, ttl).Err()
		},
	}
}

// ResolveTTLs builds a CacheTTLs from the given config, applying the fallback
// chain: category TTL → global TTL → default (1h).
func ResolveTTLs(cfg config.CacheConfig) CacheTTLs {
	ttls := CacheTTLs{
		Atlas:     resolveAtlas(cfg.TTLs.Atlas.Duration, cfg.TTL.Duration),
		Live:      resolveLive(cfg.TTLs.Live.Duration),
		Stats:     resolve(cfg.TTLs.Stats.Duration, cfg.TTL.Duration),
		Netgraph:  resolveNetgraph(cfg.TTLs.Netgraph.Duration),
		Reference: resolve(cfg.TTLs.Reference.Duration, cfg.TTL.Duration),
		Nodes:     resolve(cfg.TTLs.Nodes.Duration, cfg.TTL.Duration),
		Observers: resolve(cfg.TTLs.Observers.Duration, cfg.TTL.Duration),
	}
	return ttls
}

// resolve returns the first non-zero duration from category, global, or 1h.
func resolve(category, global time.Duration) time.Duration {
	if category != 0 {
		return category
	}
	if global != 0 {
		return global
	}
	return time.Hour
}

func resolveAtlas(category, _ time.Duration) time.Duration {
	if category != 0 {
		return category
	}
	return 30 * time.Second
}

func resolveLive(category time.Duration) time.Duration {
	if category != 0 {
		return category
	}
	return 5 * time.Second
}

func resolveNetgraph(category time.Duration) time.Duration {
	if category != 0 {
		return category
	}
	return 10 * time.Second
}

// Ping checks connectivity to Redis. Call this on startup to verify the
// connection before serving requests.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close closes the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// MetricsSnapshot returns a copy of cache counters for health/status output.
func (c *Client) MetricsSnapshot() map[string]CategorySnapshot {
	if c == nil || c.metrics == nil {
		return nil
	}
	return c.metrics.Snapshot()
}

func (c *Client) del(ctx context.Context, keys ...string) {
	for _, key := range keys {
		c.metrics.recordInvalidation(categoryForKey(key))
	}
	if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
		for _, key := range keys {
			c.metrics.recordError(categoryForKey(key), "delete", 0)
		}
	}
}

// getOrSet retrieves a cached value by key, deserialising it into T.
// On a cache miss it calls fetch, stores the result under key with the given
// TTL, and returns it. If Redis is unavailable or returns an unexpected error,
// fetch is called directly and the result is not cached. If a cached entry
// fails to unmarshal (corrupt or schema-changed), the entry is overwritten
// with a fresh fetch. Errors from Set are ignored so a Redis hiccup never
// fails a request.
func getOrSet[T any](ctx context.Context, c *Client, key string, ttl time.Duration, fetch func(context.Context) (T, error)) (T, error) {
	category := categoryForKey(key)
	policy := policyForCategory(category, ttl)
	raw, err := c.getBytes(ctx, key)
	if err != nil && !errors.Is(err, redis.Nil) {
		// A Redis outage does not bypass coalescing or the database deadline.
		c.metrics.recordError(category, "get", ttl)
		return coalescedFetch(ctx, c, key, category, policy, false, fetch)
	}
	if errors.Is(err, redis.Nil) {
		c.metrics.recordMiss(category, ttl)
		return coalescedFetch(ctx, c, key, category, policy, true, fetch)
	}

	var envelope cacheEnvelope[T]
	if err = json.Unmarshal(raw, &envelope); err != nil || envelope.Version != cacheEnvelopeVersion || envelope.HardExpiresAt <= envelope.GeneratedAt {
		c.metrics.recordMiss(category, ttl)
		c.metrics.recordError(category, "unmarshal", ttl)
		return coalescedFetch(ctx, c, key, category, policy, true, fetch)
	}

	now := time.Now()
	if now.UnixMilli() >= envelope.HardExpiresAt {
		c.metrics.recordMiss(category, ttl)
		return coalescedFetch(ctx, c, key, category, policy, true, fetch)
	}
	if now.UnixMilli() >= envelope.FreshUntil {
		c.metrics.recordStale(category, envelope.GeneratedAt)
		refreshStale(c, key, category, policy, fetch)
		return envelope.Value, nil
	}

	c.metrics.recordHit(category, ttl)
	return envelope.Value, nil
}

func coalescedFetch[T any](ctx context.Context, c *Client, key, category string, policy cachePolicy, store bool, fetch func(context.Context) (T, error)) (T, error) {
	var zero T
	result := c.flights.DoChan(key, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(ctx, policy.RefreshTimeout)
		defer cancel()
		return fetchAndMaybeStore(fetchCtx, c, key, category, policy, store, fetch)
	})
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case item := <-result:
		if item.Shared {
			c.metrics.recordCoalesced(category)
		}
		if item.Err != nil {
			return zero, item.Err
		}
		value, ok := item.Val.(T)
		if !ok {
			return zero, fmt.Errorf("cache singleflight type mismatch for %s", category)
		}
		return value, nil
	}
}

func refreshStale[T any](c *Client, key, category string, policy cachePolicy, fetch func(context.Context) (T, error)) {
	result := c.flights.DoChan(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), policy.RefreshTimeout)
		defer cancel()
		return fetchAndMaybeStore(ctx, c, key, category, policy, true, fetch)
	})
	go func() {
		item := <-result
		if item.Shared {
			c.metrics.recordCoalesced(category)
		}
	}()
}

func fetchAndMaybeStore[T any](ctx context.Context, c *Client, key, category string, policy cachePolicy, store bool, fetch func(context.Context) (T, error)) (T, error) {
	var zero T
	value, err := fetch(ctx)
	if err != nil {
		c.metrics.recordError(category, "fetch", policy.Fresh)
		c.metrics.recordRefresh(category, err)
		return zero, err
	}
	if !store {
		c.metrics.recordRefresh(category, nil)
		return value, nil
	}
	now := time.Now()
	envelope := cacheEnvelope[T]{
		Version:       cacheEnvelopeVersion,
		GeneratedAt:   now.UnixMilli(),
		FreshUntil:    now.Add(policy.Fresh).UnixMilli(),
		HardExpiresAt: now.Add(policy.Hard).UnixMilli(),
		Value:         value,
	}
	data, marshalErr := json.Marshal(envelope)
	if marshalErr != nil {
		c.metrics.recordError(category, "marshal", policy.Fresh)
		c.metrics.recordRefresh(category, marshalErr)
		return value, nil
	}
	if err := c.setBytes(ctx, key, data, policy.Hard); err != nil {
		c.metrics.recordError(category, "set", policy.Fresh)
		c.metrics.recordRefresh(category, err)
		return value, nil
	}
	c.metrics.recordRefresh(category, nil)
	return value, nil
}

func policyForCategory(category string, configuredFresh time.Duration) cachePolicy {
	if configuredFresh <= 0 {
		configuredFresh = time.Hour
	}
	policy := cachePolicy{Fresh: configuredFresh, Hard: configuredFresh * 6, RefreshTimeout: 5 * time.Second}
	switch category {
	case CategoryAtlas:
		policy.Hard = 5 * time.Minute
		policy.RefreshTimeout = 5 * time.Second
	case CategoryStats:
		policy.Hard = 6 * time.Hour
		policy.RefreshTimeout = 15 * time.Second
	case CategoryNetgraph:
		policy.Hard = time.Minute
		policy.RefreshTimeout = 5 * time.Second
	case CategoryLive:
		policy.Hard = 15 * time.Second
		policy.RefreshTimeout = 2 * time.Second
	}
	if policy.Hard < policy.Fresh {
		policy.Hard = policy.Fresh
	}
	return policy
}
