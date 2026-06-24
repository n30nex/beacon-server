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
}

type bucket struct {
	tokens float64
	last   time.Time
}

func New(cfg Config) *Limiter {
	if cfg.RequestsPerMinute <= 0 || cfg.Burst <= 0 {
		return &Limiter{}
	}
	return &Limiter{
		buckets:           make(map[string]*bucket),
		requestsPerMinute: cfg.RequestsPerMinute,
		burst:             cfg.Burst,
		tokensPerSecond:   float64(cfg.RequestsPerMinute) / 60,
		now:               time.Now,
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

	b := l.buckets[key]
	if b == nil {
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
