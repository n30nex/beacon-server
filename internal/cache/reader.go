// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

const (
	keyIATAs                      = "beacon:v2:iatas"
	keyIATAPrefix                 = "beacon:v2:iata:"
	keyRegions                    = "beacon:v2:regions"
	keyRegionPrefix               = "beacon:v2:region:"
	keyRegionSlugPrefix           = "beacon:v2:region:slug:"
	keyAtlasRegionPrefix          = "beacon:v2:atlas:region:"
	keyAtlasBriefingPrefix        = "beacon:v2:atlas:briefing:"
	keyLiveSummaryPrefix          = "beacon:v2:live:summary:"
	keyScopeNames                 = "beacon:v2:scope:names"
	keyScopeStats                 = "beacon:v2:scope:stats"
	keyScopesByIATAsPrefix        = "beacon:v2:scopes:iatas:"
	keyScopeByNamePrefix          = "beacon:v2:scope:name:"
	keyStatsOverviewPrefix        = "beacon:v2:stats:overview:"
	keyStatsObservationsPrefix    = "beacon:v2:stats:observations:"
	keyStatsBreakdownPrefix       = "beacon:v2:stats:breakdown:"
	keyStatsTopNodesPrefix        = "beacon:v2:stats:top-nodes:"
	keyStatsTopObsPrefix          = "beacon:v2:stats:top-observers:"
	keyStatsNodeTypes             = "beacon:v2:stats:node-types:"
	keyStatsSummaryPrefix         = "beacon:v2:stats:summary:"
	keyStatsRegionsPrefix         = "beacon:v2:stats:regions:"
	keyStatsPayloadsPrefix        = "beacon:v2:stats:payloads:"
	keyStatsHashPrefix            = "beacon:v2:stats:hash:"
	keyStatsHashPrefixLookup      = "beacon:v2:stats:hash-prefix:"
	keyStatsTopologyPrefix        = "beacon:v2:stats:topology:"
	keyStatsSubpathsPrefix        = "beacon:v2:stats:subpaths:"
	keyStatsChannelsPrefix        = "beacon:v2:stats:channels:"
	keyStatsRFHealthPrefix        = "beacon:v2:stats:rf-health:"
	keyStatsObserverHealthPrefix  = "beacon:v2:stats:observer-health:"
	keyStatsObserverComparePrefix = "beacon:v2:stats:observer-compare:"
	keyRadioPresetsPrefix         = "beacon:v2:radio-presets:"
	keyKnownRoutesPrefix          = "beacon:v2:netgraph:known-routes:"
	keyNodePrefix                 = "beacon:v2:node:"
	keyNodeNeighborsPrefix        = "beacon:v2:node:neighbors:"
	keyNodesByIDsPrefix           = "beacon:v2:nodes:ids:"
	keyObserverPrefix             = "beacon:v2:observer:"
	keyObserverScopesPrefix       = "beacon:v2:observer:scopes:"
)

// CachedReader wraps an api.Reader with a Redis caching layer.
// It implements api.Reader and is a drop-in replacement for db.Store
// at the wiring point in main.go.
type CachedReader struct {
	inner api.Reader
	c     *Client
	ttl   CacheTTLs
}

// CacheTTLs holds the resolved per-category TTLs for the cache layer.
// All fields should be non-zero — use ResolveTTLs to build this from
// config with fallback to the global TTL and then the default.
type CacheTTLs struct {
	Atlas     time.Duration
	Live      time.Duration
	Stats     time.Duration
	Netgraph  time.Duration
	Reference time.Duration
	Nodes     time.Duration
	Observers time.Duration
}

// NewCachedReader returns an api.Reader that transparently caches responses
// using the provided Redis client and TTL configuration. inner is the
// underlying db.Store that is called on cache misses.
func NewCachedReader(inner api.Reader, c *Client, ttl CacheTTLs) api.Reader {
	if c != nil && c.metrics != nil {
		c.metrics.ConfigureTTLs(ttl)
	}
	return &CachedReader{
		inner: inner,
		c:     c,
		ttl:   ttl,
	}
}

func slugOrAll(slug string) string {
	if slug == "" {
		return "all"
	}
	return slug
}

func iataCacheSegment(iatas []string) string {
	if len(iatas) == 0 {
		return "all"
	}
	sorted := append([]string(nil), iatas...)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

func atlasCacheWindow(since, until time.Time, bucket time.Duration) (time.Time, time.Time) {
	if bucket <= 0 {
		bucket = 30 * time.Second
	}
	if until.IsZero() {
		until = time.Now()
	}
	until = until.Truncate(bucket)
	if since.IsZero() {
		since = until.Add(-24 * time.Hour)
	} else {
		since = since.Truncate(bucket)
	}
	return since, until
}

func liveCacheWindow(since, until time.Time, bucket time.Duration) (time.Time, time.Time) {
	if bucket <= 0 {
		bucket = 5 * time.Second
	}
	if until.IsZero() {
		until = time.Now()
	}
	until = until.Truncate(bucket)
	if since.IsZero() {
		since = until.Add(-15 * time.Minute)
	} else {
		since = since.Truncate(bucket)
	}
	return since, until
}

func statsCacheWindow(since, until time.Time, ttl time.Duration) (time.Time, time.Time) {
	if ttl <= 0 {
		ttl = time.Hour
	}
	if until.IsZero() {
		until = time.Now()
	}
	until = until.Truncate(ttl)
	if since.IsZero() {
		since = until.Add(-24 * time.Hour)
	} else {
		since = since.Truncate(ttl)
	}
	return since, until
}

func statsFilterCacheKey(prefix string, filter api.StatsFilter, cacheBucket time.Duration) string {
	since, until := statsCacheWindow(filter.Since, filter.Until, cacheBucket)
	return fmt.Sprintf(
		"%s%s:%d:%d:%s:%d",
		prefix,
		iataCacheSegment(filter.IATAs),
		since.UnixMilli(),
		until.UnixMilli(),
		filter.Bucket,
		filter.Limit,
	)
}

func statsObserverHealthCacheKey(prefix string, filter api.StatsObserverHealthFilter, cacheBucket time.Duration) string {
	base := statsFilterCacheKey(prefix, filter.StatsFilter, cacheBucket)
	return fmt.Sprintf("%s:%d", base, int64(filter.StaleAfter/time.Minute))
}

func statsObserverCompareCacheKey(prefix string, filter api.StatsObserverCompareFilter, cacheBucket time.Duration) string {
	base := statsObserverHealthCacheKey(prefix, filter.StatsObserverHealthFilter, cacheBucket)
	ids := make([]string, 0, len(filter.ObserverIDs))
	for _, id := range filter.ObserverIDs {
		ids = append(ids, id.String())
	}
	sort.Strings(ids)
	return fmt.Sprintf("%s:%s", base, strings.Join(ids, ","))
}

func statsHashPrefixCacheKey(prefix string, filter api.StatsHashPrefixFilter, cacheBucket time.Duration) string {
	base := statsFilterCacheKey(prefix, filter.StatsFilter, cacheBucket)
	return fmt.Sprintf("%s:%s:%d", base, filter.Prefix, filter.HashSize)
}

func knownRoutesCacheKey(iatas []string, hopCount int32, cursor time.Time, limit int32) string {
	cursorValue := int64(0)
	if !cursor.IsZero() {
		cursorValue = cursor.UnixMilli()
	}
	return fmt.Sprintf("%s%s:%d:%d:%d", keyKnownRoutesPrefix, iataCacheSegment(iatas), hopCount, cursorValue, limit)
}

// InvalidateNode removes the cached entries for a node by UUID.
// Should be called from the ingest path after a node upsert.
func (cr *CachedReader) InvalidateNode(ctx context.Context, nodeID uuid.UUID) {
	id := nodeID.String()
	cr.c.del(ctx, keyNodePrefix+id, keyNodeNeighborsPrefix+id)
}

// InvalidateObserver removes the cached entries for an observer by UUID.
// Should be called from the ingest path after an observer upsert.
func (cr *CachedReader) InvalidateObserver(ctx context.Context, observerID uuid.UUID) {
	id := observerID.String()
	cr.c.del(ctx, keyObserverPrefix+id, keyObserverScopesPrefix+id)
}

// ListIATAs implements [api.Reader].
func (cr *CachedReader) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	return getOrSet(ctx, cr.c, keyIATAs, cr.ttl.Reference, func(fetchCtx context.Context) ([]api.IATA, error) {
		return cr.inner.ListIATAs(fetchCtx)
	})
}

// GetIATA implements [api.Reader].
func (cr *CachedReader) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	return getOrSet(ctx, cr.c, keyIATAPrefix+iata, cr.ttl.Reference, func(fetchCtx context.Context) (*api.IATA, error) {
		return cr.inner.GetIATA(fetchCtx, iata)
	})
}

// ListRegions implements [api.Reader].
func (cr *CachedReader) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	return getOrSet(ctx, cr.c, keyRegions, cr.ttl.Reference, func(fetchCtx context.Context) ([]api.RegionSummary, error) {
		return cr.inner.ListRegions(fetchCtx)
	})
}

// GetRegion implements [api.Reader].
func (cr *CachedReader) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	return getOrSet(ctx, cr.c, fmt.Sprintf("%s%d", keyRegionPrefix, regionID), cr.ttl.Reference, func(fetchCtx context.Context) (*api.Region, error) {
		return cr.inner.GetRegion(fetchCtx, regionID)
	})
}

// GetRegionBySlug implements [api.Reader].
func (cr *CachedReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	return getOrSet(ctx, cr.c, keyRegionSlugPrefix+slug, cr.ttl.Reference, func(fetchCtx context.Context) (*api.Region, error) {
		return cr.inner.GetRegionBySlug(fetchCtx, slug)
	})
}

// GetRegionAtlasSummary implements [api.Reader].
func (cr *CachedReader) GetRegionAtlasSummary(ctx context.Context, slug string, since, until time.Time) (*api.RegionAtlasSummary, error) {
	since, until = atlasCacheWindow(since, until, cr.ttl.Atlas)
	key := fmt.Sprintf("%s%s:%d:%d", keyAtlasRegionPrefix, slugOrAll(slug), since.UnixMilli(), until.UnixMilli())
	return getOrSet(ctx, cr.c, key, cr.ttl.Atlas, func(fetchCtx context.Context) (*api.RegionAtlasSummary, error) {
		return cr.inner.GetRegionAtlasSummary(fetchCtx, slug, since, until)
	})
}

// GetAtlasBriefing implements [api.Reader].
func (cr *CachedReader) GetAtlasBriefing(ctx context.Context, regionSlug string, since, until time.Time) (*api.AtlasBriefing, error) {
	since, until = atlasCacheWindow(since, until, cr.ttl.Atlas)
	key := fmt.Sprintf("%s%s:%d:%d", keyAtlasBriefingPrefix, slugOrAll(regionSlug), since.UnixMilli(), until.UnixMilli())
	return getOrSet(ctx, cr.c, key, cr.ttl.Atlas, func(fetchCtx context.Context) (*api.AtlasBriefing, error) {
		return cr.inner.GetAtlasBriefing(fetchCtx, regionSlug, since, until)
	})
}

// ListAtlasReplay implements [api.Reader].
func (cr *CachedReader) ListAtlasReplay(ctx context.Context, regionSlug string, since, until time.Time, cursor int64, limit int32) (api.Page[api.AtlasReplayPacket], error) {
	return cr.inner.ListAtlasReplay(ctx, regionSlug, since, until, cursor, limit)
}

// GetScopeNames implements [api.Reader].
func (cr *CachedReader) GetScopeNames(ctx context.Context) ([]string, error) {
	return getOrSet(ctx, cr.c, keyScopeNames, cr.ttl.Reference, func(fetchCtx context.Context) ([]string, error) {
		return cr.inner.GetScopeNames(fetchCtx)
	})
}

// GetScopeStats implements [api.Reader].
func (cr *CachedReader) GetScopeStats(ctx context.Context) ([]api.ScopeStats, error) {
	return getOrSet(ctx, cr.c, keyScopeStats, cr.ttl.Reference, func(fetchCtx context.Context) ([]api.ScopeStats, error) {
		return cr.inner.GetScopeStats(fetchCtx)
	})
}

// GetScopesByIATAs implements [api.Reader].
func (cr *CachedReader) GetScopesByIATAs(ctx context.Context, iatas []string) ([]api.ScopeSummary, error) {
	sorted := make([]string, len(iatas))
	copy(sorted, iatas)
	sort.Strings(sorted)
	key := keyScopesByIATAsPrefix + strings.Join(sorted, ",")
	return getOrSet(ctx, cr.c, key, cr.ttl.Reference, func(fetchCtx context.Context) ([]api.ScopeSummary, error) {
		return cr.inner.GetScopesByIATAs(fetchCtx, iatas)
	})
}

// GetScopeByName implements [api.Reader].
func (cr *CachedReader) GetScopeByName(ctx context.Context, name string) (*api.ScopeDetail, error) {
	return getOrSet(ctx, cr.c, keyScopeByNamePrefix+name, cr.ttl.Reference, func(fetchCtx context.Context) (*api.ScopeDetail, error) {
		return cr.inner.GetScopeByName(fetchCtx, name)
	})
}

// GetStatsOverview implements [api.Reader].
func (cr *CachedReader) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s", keyStatsOverviewPrefix, segment)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsOverview, error) {
		return cr.inner.GetStatsOverview(fetchCtx, iatas)
	})
}

// GetStatsObservations implements [api.Reader].
func (cr *CachedReader) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%d", keyStatsObservationsPrefix, segment, since.UnixMilli())
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.ObservationPoint, error) {
		return cr.inner.GetStatsObservations(fetchCtx, iatas, since)
	})
}

// GetStatsPayloadBreakdown implements [api.Reader].
func (cr *CachedReader) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%d", keyStatsBreakdownPrefix, segment, since.UnixMilli())
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.PayloadBreakdownItem, error) {
		return cr.inner.GetStatsPayloadBreakdown(fetchCtx, iatas, since)
	})
}

// GetStatsTopNodes implements [api.Reader].
func (cr *CachedReader) GetStatsTopNodes(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%d", keyStatsTopNodesPrefix, segment, limit)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.TopNode, error) {
		return cr.inner.GetStatsTopNodes(fetchCtx, iatas, limit)
	})
}

// GetStatsNodeTypes implements [api.Reader].
func (cr *CachedReader) GetStatsNodeTypes(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s", keyStatsNodeTypes, segment)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.NodeTypeCount, error) {
		return cr.inner.GetStatsNodeTypes(fetchCtx, iatas)
	})
}

// GetStatsSummary implements [api.Reader].
func (cr *CachedReader) GetStatsSummary(ctx context.Context, filter api.StatsFilter) (*api.StatsSummary, error) {
	key := statsFilterCacheKey(keyStatsSummaryPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsSummary, error) {
		return cr.inner.GetStatsSummary(fetchCtx, filter)
	})
}

// GetStatsRegions implements [api.Reader].
func (cr *CachedReader) GetStatsRegions(ctx context.Context, filter api.StatsFilter) (*api.StatsRegions, error) {
	key := statsFilterCacheKey(keyStatsRegionsPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsRegions, error) {
		return cr.inner.GetStatsRegions(fetchCtx, filter)
	})
}

// GetStatsPayloads implements [api.Reader].
func (cr *CachedReader) GetStatsPayloads(ctx context.Context, filter api.StatsFilter) (*api.StatsPayloads, error) {
	key := statsFilterCacheKey(keyStatsPayloadsPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsPayloads, error) {
		return cr.inner.GetStatsPayloads(fetchCtx, filter)
	})
}

// GetStatsHashAnalytics implements [api.Reader].
func (cr *CachedReader) GetStatsHashAnalytics(ctx context.Context, filter api.StatsFilter) (*api.StatsHashAnalytics, error) {
	key := statsFilterCacheKey(keyStatsHashPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsHashAnalytics, error) {
		return cr.inner.GetStatsHashAnalytics(fetchCtx, filter)
	})
}

// GetStatsHashPrefixLookup implements [api.Reader].
func (cr *CachedReader) GetStatsHashPrefixLookup(ctx context.Context, filter api.StatsHashPrefixFilter) (*api.StatsHashPrefixLookup, error) {
	key := statsHashPrefixCacheKey(keyStatsHashPrefixLookup, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsHashPrefixLookup, error) {
		return cr.inner.GetStatsHashPrefixLookup(fetchCtx, filter)
	})
}

// GetStatsTopology implements [api.Reader].
func (cr *CachedReader) GetStatsTopology(ctx context.Context, filter api.StatsFilter) (*api.StatsTopology, error) {
	key := statsFilterCacheKey(keyStatsTopologyPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsTopology, error) {
		return cr.inner.GetStatsTopology(fetchCtx, filter)
	})
}

// GetStatsSubpaths implements [api.Reader].
func (cr *CachedReader) GetStatsSubpaths(ctx context.Context, filter api.StatsFilter) (*api.StatsSubpaths, error) {
	key := statsFilterCacheKey(keyStatsSubpathsPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsSubpaths, error) {
		return cr.inner.GetStatsSubpaths(fetchCtx, filter)
	})
}

// GetStatsChannels implements [api.Reader].
func (cr *CachedReader) GetStatsChannels(ctx context.Context, filter api.StatsFilter) (*api.StatsChannels, error) {
	key := statsFilterCacheKey(keyStatsChannelsPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsChannels, error) {
		return cr.inner.GetStatsChannels(fetchCtx, filter)
	})
}

// GetStatsRFHealth implements [api.Reader].
func (cr *CachedReader) GetStatsRFHealth(ctx context.Context, filter api.StatsObserverHealthFilter) (*api.StatsRFHealth, error) {
	key := statsObserverHealthCacheKey(keyStatsRFHealthPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsRFHealth, error) {
		return cr.inner.GetStatsRFHealth(fetchCtx, filter)
	})
}

// GetStatsObserverHealth implements [api.Reader].
func (cr *CachedReader) GetStatsObserverHealth(ctx context.Context, filter api.StatsObserverHealthFilter) (*api.StatsObserverHealthResponse, error) {
	key := statsObserverHealthCacheKey(keyStatsObserverHealthPrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsObserverHealthResponse, error) {
		return cr.inner.GetStatsObserverHealth(fetchCtx, filter)
	})
}

// GetStatsObserverCompare implements [api.Reader].
func (cr *CachedReader) GetStatsObserverCompare(ctx context.Context, filter api.StatsObserverCompareFilter) (*api.StatsObserverCompare, error) {
	key := statsObserverCompareCacheKey(keyStatsObserverComparePrefix, filter, cr.ttl.Stats)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.StatsObserverCompare, error) {
		return cr.inner.GetStatsObserverCompare(fetchCtx, filter)
	})
}

// GetStatsTopObservers implements [api.Reader].
func (cr *CachedReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%d:%d", keyStatsTopObsPrefix, segment, since.UnixMilli(), limit)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.TopObserver, error) {
		return cr.inner.GetStatsTopObservers(fetchCtx, iatas, since, limit)
	})
}

// GetRadioPresets implements [api.Reader].
func (cr *CachedReader) GetRadioPresets(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%s", keyRadioPresetsPrefix, preset, segment)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) ([]api.RadioPreset, error) {
		return cr.inner.GetRadioPresets(fetchCtx, preset, iatas)
	})
}

// GetNode implements [api.Reader].
func (cr *CachedReader) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	return getOrSet(ctx, cr.c, keyNodePrefix+nodeID.String(), cr.ttl.Nodes, func(fetchCtx context.Context) (*api.Node, error) {
		return cr.inner.GetNode(fetchCtx, nodeID)
	})
}

// GetNodeNeighbors implements [api.Reader].
func (cr *CachedReader) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	return getOrSet(ctx, cr.c, keyNodeNeighborsPrefix+nodeID.String(), cr.ttl.Nodes, func(fetchCtx context.Context) ([]api.NodeNeighbor, error) {
		return cr.inner.GetNodeNeighbors(fetchCtx, nodeID)
	})
}

// GetNodesByIDs implements [api.Reader].
func (cr *CachedReader) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	sort.Strings(strs)
	key := keyNodesByIDsPrefix + strings.Join(strs, ",")
	return getOrSet(ctx, cr.c, key, cr.ttl.Nodes, func(fetchCtx context.Context) (map[uuid.UUID]*api.ResolvedNode, error) {
		return cr.inner.GetNodesByIDs(fetchCtx, ids)
	})
}

// GetObserver implements [api.Reader].
func (cr *CachedReader) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	return getOrSet(ctx, cr.c, keyObserverPrefix+observerID.String(), cr.ttl.Observers, func(fetchCtx context.Context) (*api.Observer, error) {
		return cr.inner.GetObserver(fetchCtx, observerID)
	})
}

// GetObserverScopes implements [api.Reader].
func (cr *CachedReader) GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error) {
	return getOrSet(ctx, cr.c, keyObserverScopesPrefix+observerID.String(), cr.ttl.Observers, func(fetchCtx context.Context) ([]string, error) {
		return cr.inner.GetObserverScopes(fetchCtx, observerID)
	})
}

// GetObserverTelemetry implements [api.Reader].
func (cr *CachedReader) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	return cr.inner.GetObserverTelemetry(ctx, observerID, since, until, afterID)
}

// GetObserverTelemetryBucketed implements [api.Reader].
func (cr *CachedReader) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error) {
	return cr.inner.GetObserverTelemetryBucketed(ctx, observerID, since, until, bucketHours)
}

// GetPacket implements [api.Reader].
func (cr *CachedReader) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	return cr.inner.GetPacket(ctx, packetHash)
}

// GetChannel implements [api.Reader].
func (cr *CachedReader) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	return cr.inner.GetChannel(ctx, channelID)
}

// GetTraceByTag implements [api.Reader].
func (cr *CachedReader) GetTraceByTag(ctx context.Context, tag string, iatas []string, scope string, since, until time.Time) (*api.TraceDetail, error) {
	return cr.inner.GetTraceByTag(ctx, tag, iatas, scope, since, until)
}

// GetKnownRoutesByNode implements [api.Reader].
func (cr *CachedReader) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID, limit int32) ([]api.KnownRoute, error) {
	return cr.inner.GetKnownRoutesByNode(ctx, iata, nodeID, limit)
}

// GetCrossIATANeighbors implements [api.Reader].
func (cr *CachedReader) GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error) {
	return cr.inner.GetCrossIATANeighbors(ctx, nodeID, iata)
}

// ListChannels implements [api.Reader].
func (cr *CachedReader) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	return cr.inner.ListChannels(ctx, limit, hash, iata, cursor)
}

// ListChannelMessages implements [api.Reader].
func (cr *CachedReader) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return cr.inner.ListChannelMessages(ctx, channelID, since, limit, iatas, scope, cursor)
}

// ListChannelMessagesByHash implements [api.Reader].
func (cr *CachedReader) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return cr.inner.ListChannelMessagesByHash(ctx, hash, since, limit, iatas, scope, cursor)
}

// ListMessagesAfterID implements [api.Reader].
func (cr *CachedReader) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
	return cr.inner.ListMessagesAfterID(ctx, afterID, iatas, scope, limit)
}

// ListNodes implements [api.Reader].
func (cr *CachedReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	return cr.inner.ListNodes(ctx, nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, name, scope, cursor, limit)
}

// ListNodeObservations implements [api.Reader].
func (cr *CachedReader) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
	return cr.inner.ListNodeObservations(ctx, nodeID, cursor, limit)
}

// ListNodeAdverts implements [api.Reader].
func (cr *CachedReader) ListNodeAdverts(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.NodeAdvertObservation], error) {
	return cr.inner.ListNodeAdverts(ctx, nodeID, cursor, limit)
}

// GetNodeAnalytics implements [api.Reader]. Node analytics is detail-panel scoped and bounded by
// window filters, so the first parity pass leaves it uncached.
func (cr *CachedReader) GetNodeAnalytics(ctx context.Context, nodeID uuid.UUID, filter api.NodeAnalyticsFilter) (*api.NodeAnalytics, error) {
	return cr.inner.GetNodeAnalytics(ctx, nodeID, filter)
}

// ListObservers implements [api.Reader].
func (cr *CachedReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	return cr.inner.ListObservers(ctx, iatas, observerType, broker, status, name, scope, cursor, limit)
}

// ListObserverAdverts implements [api.Reader].
func (cr *CachedReader) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	return cr.inner.ListObserverAdverts(ctx, observerID, cursor, limit)
}

// ListPackets implements [api.Reader].
func (cr *CachedReader) ListPackets(ctx context.Context, payloadType, routeType int16, iatas []string, scope string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	return cr.inner.ListPackets(ctx, payloadType, routeType, iatas, scope, since, until, cursor, limit)
}

// ListPacketsAfterID implements [api.Reader].
func (cr *CachedReader) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	return cr.inner.ListPacketsAfterID(ctx, afterObservationID, payloadType, routeType, iatas, scope, limit)
}

// ListLiveBackfill implements [api.Reader]. Backfill is intentionally uncached.
func (cr *CachedReader) ListLiveBackfill(ctx context.Context, filter api.LiveBackfillFilter) (api.Page[api.LivePacketObservation], error) {
	return cr.inner.ListLiveBackfill(ctx, filter)
}

// GetLiveSummary implements [api.Reader].
func (cr *CachedReader) GetLiveSummary(ctx context.Context, filter api.LiveSummaryFilter) (*api.LiveSummary, error) {
	filter.Since, filter.Until = liveCacheWindow(filter.Since, filter.Until, cr.ttl.Live)
	key := fmt.Sprintf("%s%s:%d:%d", keyLiveSummaryPrefix, iataCacheSegment(filter.IATAs), filter.Since.Unix(), filter.Until.Unix())
	return getOrSet(ctx, cr.c, key, cr.ttl.Live, func(fetchCtx context.Context) (*api.LiveSummary, error) {
		return cr.inner.GetLiveSummary(fetchCtx, filter)
	})
}

// ListKnownRoutes implements [api.Reader].
func (cr *CachedReader) ListKnownRoutes(ctx context.Context, iatas []string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	key := knownRoutesCacheKey(iatas, hopCount, cursor, limit)
	return getOrSet(ctx, cr.c, key, cr.ttl.Netgraph, func(fetchCtx context.Context) ([]api.KnownRoute, error) {
		return cr.inner.ListKnownRoutes(fetchCtx, iatas, hopCount, cursor, limit)
	})
}

// GetKnownRoute implements [api.Reader].
func (cr *CachedReader) GetKnownRoute(ctx context.Context, routeID int64) (*api.KnownRoute, error) {
	return cr.inner.GetKnownRoute(ctx, routeID)
}

// SearchKnownRoutes implements [api.Reader].
func (cr *CachedReader) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	return cr.inner.SearchKnownRoutes(ctx, iata, fromHash, toHash)
}

// SearchCrossIATARoutes implements [api.Reader].
func (cr *CachedReader) SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error) {
	return cr.inner.SearchCrossIATARoutes(ctx, fromHash, fromIATA, toHash, toIATA)
}

// ListTraceTags implements [api.Reader].
func (cr *CachedReader) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	return cr.inner.ListTraceTags(ctx, iatas, scope, traceType, since, until, cursor, limit)
}

// GetObserverTopology implements [api.Reader].
func (cr *CachedReader) GetObserverTopology(ctx context.Context, observerID uuid.UUID, filter api.StatsFilter) (*api.ObserverTopologySummary, error) {
	key := fmt.Sprintf("%s%s:%s", keyObserverPrefix, observerID.String(), statsFilterCacheKey("topology:", filter, cr.ttl.Stats))
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(fetchCtx context.Context) (*api.ObserverTopologySummary, error) {
		return cr.inner.GetObserverTopology(fetchCtx, observerID, filter)
	})
}
