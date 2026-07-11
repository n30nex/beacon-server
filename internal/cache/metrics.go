// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	CategoryAtlas     = "atlas"
	CategoryLive      = "live"
	CategoryStats     = "stats"
	CategoryNetgraph  = "netgraph"
	CategoryReference = "reference"
	CategoryNodes     = "nodes"
	CategoryObservers = "observers"
	CategoryUnknown   = "unknown"
)

// CategorySnapshot is the health-visible cache counter set for one category.
type CategorySnapshot struct {
	Hits             uint64            `json:"hits"`
	Misses           uint64            `json:"misses"`
	Invalidations    uint64            `json:"invalidations"`
	TTLSeconds       int64             `json:"ttlSeconds,omitempty"`
	Errors           map[string]uint64 `json:"errors,omitempty"`
	StaleServed      uint64            `json:"staleServed"`
	Refreshes        uint64            `json:"refreshes"`
	RefreshFailures  uint64            `json:"refreshFailures"`
	Coalesced        uint64            `json:"coalesced"`
	LastGeneratedAt  int64             `json:"lastGeneratedAt,omitempty"`
	LastRefreshAt    int64             `json:"lastRefreshAt,omitempty"`
	LastRefreshError string            `json:"lastRefreshError,omitempty"`
}

type categoryMetrics struct {
	hits             uint64
	misses           uint64
	invalidations    uint64
	ttl              time.Duration
	errors           map[string]uint64
	staleServed      uint64
	refreshes        uint64
	refreshFailures  uint64
	coalesced        uint64
	lastGeneratedAt  int64
	lastRefreshAt    time.Time
	lastRefreshError string
}

// Metrics records lightweight cache events for health/status reporting.
type Metrics struct {
	mu         sync.Mutex
	categories map[string]*categoryMetrics
}

// NewMetrics returns an empty cache metrics recorder.
func NewMetrics() *Metrics {
	return &Metrics{categories: map[string]*categoryMetrics{}}
}

// ConfigureTTLs records expected TTLs before traffic touches each category.
func (m *Metrics) ConfigureTTLs(ttls CacheTTLs) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.categoryLocked(CategoryAtlas).ttl = ttls.Atlas
	m.categoryLocked(CategoryLive).ttl = ttls.Live
	m.categoryLocked(CategoryStats).ttl = ttls.Stats
	m.categoryLocked(CategoryNetgraph).ttl = ttls.Netgraph
	m.categoryLocked(CategoryReference).ttl = ttls.Reference
	m.categoryLocked(CategoryNodes).ttl = ttls.Nodes
	m.categoryLocked(CategoryObservers).ttl = ttls.Observers
}

func (m *Metrics) recordHit(category string, ttl time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.categoryLocked(category)
	stats.hits++
	recordTTL(stats, ttl)
}

func (m *Metrics) recordMiss(category string, ttl time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.categoryLocked(category)
	stats.misses++
	recordTTL(stats, ttl)
}

func (m *Metrics) recordInvalidation(category string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.categoryLocked(category).invalidations++
}

func (m *Metrics) recordError(category, kind string, ttl time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.categoryLocked(category)
	if stats.errors == nil {
		stats.errors = map[string]uint64{}
	}
	stats.errors[kind]++
	recordTTL(stats, ttl)
}

func (m *Metrics) recordStale(category string, generatedAt int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.categoryLocked(category)
	stats.staleServed++
	stats.lastGeneratedAt = generatedAt
}

func (m *Metrics) recordRefresh(category string, err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.categoryLocked(category)
	stats.refreshes++
	stats.lastRefreshAt = time.Now()
	if err != nil {
		stats.refreshFailures++
		stats.lastRefreshError = err.Error()
		return
	}
	stats.lastRefreshError = ""
}

func (m *Metrics) recordCoalesced(category string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.categoryLocked(category).coalesced++
}

// Snapshot returns a copy of the current cache metrics grouped by category.
func (m *Metrics) Snapshot() map[string]CategorySnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.categories) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m.categories))
	for key := range m.categories {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make(map[string]CategorySnapshot, len(keys))
	for _, key := range keys {
		stats := m.categories[key]
		snap := CategorySnapshot{
			Hits:             stats.hits,
			Misses:           stats.misses,
			Invalidations:    stats.invalidations,
			TTLSeconds:       int64(stats.ttl.Seconds()),
			StaleServed:      stats.staleServed,
			Refreshes:        stats.refreshes,
			RefreshFailures:  stats.refreshFailures,
			Coalesced:        stats.coalesced,
			LastGeneratedAt:  stats.lastGeneratedAt,
			LastRefreshError: stats.lastRefreshError,
		}
		if !stats.lastRefreshAt.IsZero() {
			snap.LastRefreshAt = stats.lastRefreshAt.UnixMilli()
		}
		if len(stats.errors) > 0 {
			snap.Errors = make(map[string]uint64, len(stats.errors))
			for kind, count := range stats.errors {
				snap.Errors[kind] = count
			}
		}
		out[key] = snap
	}
	return out
}

func (m *Metrics) categoryLocked(category string) *categoryMetrics {
	category = normalizeCategory(category)
	stats := m.categories[category]
	if stats == nil {
		stats = &categoryMetrics{}
		m.categories[category] = stats
	}
	return stats
}

func normalizeCategory(category string) string {
	if category == "" {
		return CategoryUnknown
	}
	return category
}

func recordTTL(stats *categoryMetrics, ttl time.Duration) {
	if ttl > 0 {
		stats.ttl = ttl
	}
}

func categoryForKey(key string) string {
	switch {
	case strings.HasPrefix(key, keyAtlasRegionPrefix), strings.HasPrefix(key, keyAtlasBriefingPrefix):
		return CategoryAtlas
	case strings.HasPrefix(key, keyLiveSummaryPrefix):
		return CategoryLive
	case strings.HasPrefix(key, "beacon:v2:stats:"), strings.HasPrefix(key, keyRadioPresetsPrefix):
		return CategoryStats
	case strings.HasPrefix(key, keyKnownRoutesPrefix):
		return CategoryNetgraph
	case strings.HasPrefix(key, keyNodePrefix), strings.HasPrefix(key, keyNodeNeighborsPrefix), strings.HasPrefix(key, keyNodesByIDsPrefix):
		return CategoryNodes
	case strings.HasPrefix(key, keyObserverPrefix), strings.HasPrefix(key, keyObserverScopesPrefix):
		return CategoryObservers
	case strings.HasPrefix(key, keyIATAPrefix), key == keyIATAs,
		strings.HasPrefix(key, keyRegionPrefix), strings.HasPrefix(key, keyRegionSlugPrefix), key == keyRegions,
		key == keyScopeNames, key == keyScopeStats,
		strings.HasPrefix(key, keyScopesByIATAsPrefix), strings.HasPrefix(key, keyScopeByNamePrefix):
		return CategoryReference
	default:
		return CategoryUnknown
	}
}
