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
	CategoryReference = "reference"
	CategoryNodes     = "nodes"
	CategoryObservers = "observers"
	CategoryUnknown   = "unknown"
)

// CategorySnapshot is the health-visible cache counter set for one category.
type CategorySnapshot struct {
	Hits          uint64            `json:"hits"`
	Misses        uint64            `json:"misses"`
	Invalidations uint64            `json:"invalidations"`
	TTLSeconds    int64             `json:"ttlSeconds,omitempty"`
	Errors        map[string]uint64 `json:"errors,omitempty"`
}

type categoryMetrics struct {
	hits          uint64
	misses        uint64
	invalidations uint64
	ttl           time.Duration
	errors        map[string]uint64
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
			Hits:          stats.hits,
			Misses:        stats.misses,
			Invalidations: stats.invalidations,
			TTLSeconds:    int64(stats.ttl.Seconds()),
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
	case strings.HasPrefix(key, "beacon:stats:"), strings.HasPrefix(key, keyRadioPresetsPrefix):
		return CategoryStats
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
