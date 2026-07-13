// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	RequestsPerMinute int
	Burst             int
	IdleTTL           time.Duration
	MaxBuckets        int
}

type Snapshot struct {
	RequestsPerMinute int    `json:"requestsPerMinute"`
	Burst             int    `json:"burst"`
	ActiveBuckets     int    `json:"activeBuckets"`
	Allowed           uint64 `json:"allowed"`
	Rejected          uint64 `json:"rejected"`
}

type Limiter struct {
	mu                sync.Mutex
	buckets           map[string]*bucket
	requestsPerMinute int
	burst             int
	tokensPerSecond   float64
	allowed           atomic.Uint64
	rejected          atomic.Uint64
	now               func() time.Time
	idleTTL           time.Duration
	maxBuckets        int
	lastSweep         time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func New(cfg Config) *Limiter {
	if cfg.RequestsPerMinute <= 0 || cfg.Burst <= 0 {
		return &Limiter{}
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * time.Minute
	}
	if cfg.MaxBuckets <= 0 {
		cfg.MaxBuckets = 10_000
	}
	return &Limiter{
		buckets:           make(map[string]*bucket),
		requestsPerMinute: cfg.RequestsPerMinute,
		burst:             cfg.Burst,
		tokensPerSecond:   float64(cfg.RequestsPerMinute) / 60,
		now:               time.Now,
		idleTTL:           cfg.IdleTTL,
		maxBuckets:        cfg.MaxBuckets,
	}
}

func (l *Limiter) Enabled() bool {
	return l != nil && l.requestsPerMinute > 0 && l.burst > 0
}

func (l *Limiter) Allow(key string) bool {
	if !l.Enabled() {
		return true
	}
	if key == "" {
		key = "unknown"
	}

	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastSweep.IsZero() || now.Sub(l.lastSweep) >= l.idleTTL {
		l.pruneIdle(now)
		l.lastSweep = now
	}

	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= l.maxBuckets {
			l.pruneIdle(now)
		}
		if len(l.buckets) >= l.maxBuckets {
			l.evictOldest()
		}
		b = &bucket{tokens: float64(l.burst), last: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.tokensPerSecond
		if b.tokens > float64(l.burst) {
			b.tokens = float64(l.burst)
		}
		b.last = now
	}

	if b.tokens < 1 {
		l.rejected.Add(1)
		return false
	}
	b.tokens--
	l.allowed.Add(1)
	return true
}

func (l *Limiter) pruneIdle(now time.Time) {
	for key, candidate := range l.buckets {
		if now.Sub(candidate.last) >= l.idleTTL {
			delete(l.buckets, key)
		}
	}
}

func (l *Limiter) evictOldest() {
	var oldestKey string
	var oldest time.Time
	for key, candidate := range l.buckets {
		if oldestKey == "" || candidate.last.Before(oldest) {
			oldestKey = key
			oldest = candidate.last
		}
	}
	if oldestKey != "" {
		delete(l.buckets, oldestKey)
	}
}

func (l *Limiter) Snapshot() Snapshot {
	if l == nil {
		return Snapshot{}
	}
	l.mu.Lock()
	activeBuckets := len(l.buckets)
	l.mu.Unlock()
	return Snapshot{
		RequestsPerMinute: l.requestsPerMinute,
		Burst:             l.burst,
		ActiveBuckets:     activeBuckets,
		Allowed:           l.allowed.Load(),
		Rejected:          l.rejected.Load(),
	}
}
