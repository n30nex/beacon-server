// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const atlasDefaultWindow = 24 * time.Hour

func atlasWindow(since, until time.Time) (time.Time, time.Time) {
	if until.IsZero() {
		until = time.Now()
	}
	if since.IsZero() {
		since = until.Add(-atlasDefaultWindow)
	}
	return since, until
}

func (s *Store) atlasRegion(ctx context.Context, slug string) (*api.Region, []string, error) {
	if slug == "" || slug == "all" {
		iatas, err := s.ListIATAs(ctx)
		if err != nil {
			return nil, nil, err
		}
		codes := make([]string, 0, len(iatas))
		for _, iata := range iatas {
			codes = append(codes, iata.IATA)
		}
		return &api.Region{
			RegionSummary: api.RegionSummary{ID: 0, Slug: "all", Name: "All Regions"},
			IATAs:         codes,
		}, nil, nil
	}
	region, err := s.GetRegionBySlug(ctx, slug)
	if err != nil {
		return nil, nil, err
	}
	return region, region.IATAs, nil
}

func (s *Store) GetRegionAtlasSummary(ctx context.Context, slug string, since, until time.Time) (*api.RegionAtlasSummary, error) {
	since, until = atlasWindow(since, until)
	region, iatas, err := s.atlasRegion(ctx, slug)
	if err != nil {
		return nil, err
	}
	iataFilter := strings.Join(iatas, ",")

	kpis, err := s.getAtlasKPIs(ctx, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	iataRows, err := s.getAtlasIATAs(ctx, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	hourly, err := s.getAtlasHourly(ctx, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	payload, err := s.getAtlasPayloadMix(ctx, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	topNodes, err := s.getAtlasTopNodes(ctx, since, until, iataFilter, 8)
	if err != nil {
		return nil, err
	}
	topObservers, err := s.getAtlasTopObservers(ctx, since, until, iataFilter, 8)
	if err != nil {
		return nil, err
	}
	nodeTypes, err := s.GetStatsNodeTypes(ctx, iatas)
	if err != nil {
		return nil, err
	}
	radioPresets, err := s.GetRadioPresets(ctx, "", iatas)
	if err != nil {
		return nil, err
	}
	scopes, err := s.getAtlasScopeSummaries(ctx, len(iatas))
	if err != nil {
		return nil, err
	}

	summary := &api.RegionAtlasSummary{
		Region:       *region,
		Window:       api.AtlasWindow{Since: since.UnixMilli(), Until: until.UnixMilli()},
		KPIs:         *kpis,
		IATAs:        iataRows,
		Hourly:       hourly,
		NodeTypes:    nodeTypes,
		PayloadMix:   payload,
		TopNodes:     topNodes,
		TopObservers: topObservers,
		RadioPresets: radioPresets,
		Scopes:       scopes,
	}
	summary.StoryBeats = atlasStoryBeats(summary)
	return summary, nil
}

func (s *Store) GetAtlasBriefing(ctx context.Context, regionSlug string, since, until time.Time) (*api.AtlasBriefing, error) {
	since, until = atlasWindow(since, until)
	useAggregates := s.atlasAggregatesAvailable(ctx, since, until, true)
	if regionSlug == "" {
		regionSlug = "all"
	}
	region, iatas, err := s.atlasRegion(ctx, regionSlug)
	if err != nil {
		return nil, err
	}
	iataFilter := strings.Join(iatas, ",")
	var (
		allIATARows      []api.AtlasIATA
		previousIATARows []api.AtlasIATA
		payload          []api.PayloadBreakdownItem
		routeMix         []api.LiveRouteMixItem
		topNodes         []api.TopNode
		topObservers     []api.TopObserver
		scopes           []api.ScopeSummary
		health           *api.StatsObserverHealthResponse
		notableRoutes    []api.AtlasNotableRoute
	)
	if err := runStoreParallelTasks(ctx,
		storeParallelTask{
			name: "atlas current/previous iata rollups",
			run: func(ctx context.Context) error {
				var err error
				if useAggregates {
					window := until.Sub(since)
					if err = runStoreParallelTasks(ctx,
						storeParallelTask{name: "current aggregate", run: func(ctx context.Context) error {
							rows, aggregateErr := s.getAtlasIATAsAggregated(ctx, since, until, "")
							allIATARows = rows
							return aggregateErr
						}},
						storeParallelTask{name: "previous aggregate", run: func(ctx context.Context) error {
							rows, aggregateErr := s.getAtlasIATAsAggregated(ctx, since.Add(-window), since, "")
							previousIATARows = rows
							return aggregateErr
						}},
					); err != nil {
						return err
					}
					return nil
				}
				allIATARows, previousIATARows, err = s.getAtlasCurrentAndPreviousIATAs(ctx, since, until, "")
				return err
			},
		},
		storeParallelTask{
			name: "atlas payload and route mix",
			run: func(ctx context.Context) error {
				var err error
				if useAggregates {
					payload, routeMix, err = s.getAtlasPayloadAndRouteMixAggregated(ctx, since, until, iataFilter)
				} else {
					payload, routeMix, err = s.getAtlasPayloadAndRouteMix(ctx, since, until, iataFilter)
				}
				return err
			},
		},
		storeParallelTask{
			name: "atlas top nodes",
			run: func(ctx context.Context) error {
				var err error
				if useAggregates {
					topNodes, err = s.getAtlasTopNodesAggregated(ctx, since, until, iataFilter, 8)
				} else {
					topNodes, err = s.getAtlasTopNodes(ctx, since, until, iataFilter, 8)
				}
				return err
			},
		},
		storeParallelTask{
			name: "atlas top observers",
			run: func(ctx context.Context) error {
				var err error
				if useAggregates {
					topObservers, err = s.getAtlasTopObserversAggregated(ctx, since, until, iataFilter, 8)
				} else {
					topObservers, err = s.getAtlasTopObservers(ctx, since, until, iataFilter, 8)
				}
				return err
			},
		},
		storeParallelTask{
			name: "atlas scope summaries",
			run: func(ctx context.Context) error {
				var err error
				scopes, err = s.getAtlasScopeSummaries(ctx, len(iatas))
				return err
			},
		},
		storeParallelTask{
			name: "atlas observer health",
			run: func(ctx context.Context) error {
				var err error
				health, err = s.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
					StatsFilter: api.StatsFilter{
						IATAs: iatas,
						Since: since,
						Until: until,
						Limit: 500,
					},
					StaleAfter: statsDefaultStaleAfter,
				})
				return err
			},
		},
		storeParallelTask{
			name: "atlas notable routes",
			run: func(ctx context.Context) error {
				var err error
				notableRoutes, err = s.getAtlasNotableRoutes(ctx, iatas)
				return err
			},
		},
	); err != nil {
		return nil, err
	}
	iataRows := atlasFilterIATAs(allIATARows, iatas)
	kpis := atlasKPIsFromIATAs(iataRows, since, until)
	summary := &api.RegionAtlasSummary{
		Region:       *region,
		Window:       api.AtlasWindow{Since: since.UnixMilli(), Until: until.UnixMilli()},
		KPIs:         kpis,
		IATAs:        iataRows,
		PayloadMix:   payload,
		TopNodes:     topNodes,
		TopObservers: topObservers,
		Scopes:       scopes,
	}
	regions, err := s.getAtlasBriefingRegions(ctx, since, until, allIATARows, previousIATARows, useAggregates)
	if err != nil {
		return nil, err
	}

	hotspots := atlasBriefingHotspots(summary.IATAs)
	degradedObservers := atlasDegradedObservers(health.Items, 8)
	briefingHealth := atlasBriefingHealth(summary.KPIs, health.Summary)
	priorities := atlasBriefingPriorities(summary, briefingHealth, health.Summary, regions, hotspots, notableRoutes, degradedObservers)

	return &api.AtlasBriefing{
		ServerTime:        time.Now().UnixMilli(),
		Region:            summary.Region,
		Window:            summary.Window,
		Health:            briefingHealth,
		Regions:           regions,
		Priorities:        priorities,
		Hotspots:          hotspots,
		DegradedObservers: degradedObservers,
		NotableRoutes:     notableRoutes,
		TopNodes:          summary.TopNodes,
		TopObservers:      summary.TopObservers,
		PayloadMix:        summary.PayloadMix,
		RouteMix:          routeMix,
		Scopes:            summary.Scopes,
	}, nil
}

type storeParallelTask struct {
	name string
	run  func(context.Context) error
}

func runStoreParallelTasks(ctx context.Context, tasks ...storeParallelTask) error {
	return runStoreParallelTasksLimited(ctx, len(tasks), tasks...)
}

func runStoreParallelTasksLimited(ctx context.Context, limit int, tasks ...storeParallelTask) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if limit < 1 {
		limit = 1
	}
	slots := make(chan struct{}, limit)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, task := range tasks {
		task := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-taskCtx.Done():
				return
			}
			if err := task.run(taskCtx); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("%s: %w", task.name, err)
					cancel()
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (s *Store) getAtlasBriefingRegions(ctx context.Context, since, until time.Time, currentRows, previousRows []api.AtlasIATA, useAggregates bool) ([]api.AtlasBriefingRegion, error) {
	slugs := []string{"all", "western-canada", "eastern-canada"}
	rows := make([]api.AtlasBriefingRegion, 0, len(slugs))
	var activeNodesByIATA map[string]int64
	var err error
	if useAggregates {
		activeNodesByIATA, err = s.getAtlasActiveNodesByIATAAggregated(ctx, since, until)
	} else {
		activeNodesByIATA, err = s.getAtlasActiveNodesByIATA(ctx, since, until)
	}
	if err != nil {
		return nil, err
	}
	routeCountsByIATA, err := s.getAtlasRouteCountsByIATA(ctx, since, until)
	if err != nil {
		return nil, err
	}
	for _, slug := range slugs {
		region, iatas, err := s.atlasRegion(ctx, slug)
		if err != nil {
			if slug == "all" {
				return nil, err
			}
			continue
		}
		iataRows := atlasFilterIATAs(currentRows, iatas)
		prevRows := atlasFilterIATAs(previousRows, iatas)
		kpis := atlasKPIsFromIATAs(iataRows, since, until)
		prevKPIs := atlasKPIsFromIATAs(prevRows, since.Add(-until.Sub(since)), since)
		activeNodes := atlasSumByIATA(activeNodesByIATA, iatas)
		routeCount := atlasSumByIATA(routeCountsByIATA, iatas)
		topIATA := ""
		if len(iataRows) > 0 {
			topIATA = iataRows[0].IATA
		}
		rows = append(rows, api.AtlasBriefingRegion{
			Slug:                region.Slug,
			Name:                region.Name,
			IATACount:           len(region.IATAs),
			PacketCount:         kpis.TotalPackets,
			ObservationCount:    kpis.TotalObservations,
			ActiveObservers:     kpis.ActiveObservers,
			ActiveIATAs:         kpis.ActiveIATAs,
			ActiveNodes:         activeNodes,
			RouteCount:          routeCount,
			ObservationDeltaPct: atlasDeltaPct(kpis.TotalObservations, prevKPIs.TotalObservations),
			TopIATA:             topIATA,
			HealthScore:         atlasRegionHealthScore(kpis, activeNodes, routeCount),
			URL:                 atlasBriefingURL("Stats", region.Slug, "", map[string]string{"statsTab": "regions", "range": "24h"}),
		})
	}
	return rows, nil
}

func (s *Store) getAtlasScopeSummaries(ctx context.Context, iataCount int) ([]api.ScopeSummary, error) {
	stats, err := s.GetScopeStats(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]api.ScopeSummary, 0, len(stats))
	for _, scope := range stats {
		items = append(items, api.ScopeSummary{
			Name:          scope.Name,
			ObserverCount: scope.ObserverCount,
			NodeCount:     scope.NodeCount,
			IATACount:     int64(iataCount),
		})
	}
	return items, nil
}

func atlasFilterIATAs(rows []api.AtlasIATA, iatas []string) []api.AtlasIATA {
	if len(iatas) == 0 {
		return rows
	}
	allowed := make(map[string]struct{}, len(iatas))
	for _, iata := range iatas {
		allowed[iata] = struct{}{}
	}
	filtered := make([]api.AtlasIATA, 0, len(iatas))
	for _, row := range rows {
		if _, ok := allowed[row.IATA]; ok {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func atlasKPIsFromIATAs(rows []api.AtlasIATA, since, until time.Time) api.StatsOverview {
	var out api.StatsOverview
	for _, row := range rows {
		out.TotalPackets += row.UniquePackets
		out.TotalObservations += row.ObservationCount
		out.ActiveObservers += row.ActiveObservers
		if row.ObservationCount > 0 {
			out.ActiveIATAs++
		}
	}
	out.WindowHours = int(until.Sub(since).Hours())
	if out.WindowHours < 1 {
		out.WindowHours = 1
	}
	return out
}

func atlasSumByIATA(values map[string]int64, iatas []string) int64 {
	if len(iatas) == 0 {
		var total int64
		for _, value := range values {
			total += value
		}
		return total
	}
	var total int64
	for _, iata := range iatas {
		total += values[iata]
	}
	return total
}

func (s *Store) getAtlasKPIs(ctx context.Context, since, until time.Time, iatas string) (*api.StatsOverview, error) {
	const q = `
SELECT
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint,
  COUNT(DISTINCT po.iata)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))`
	var out api.StatsOverview
	if err := s.pool.QueryRow(ctx, q, since, until, iatas).Scan(&out.TotalPackets, &out.TotalObservations, &out.ActiveObservers, &out.ActiveIATAs); err != nil {
		return nil, err
	}
	out.WindowHours = int(until.Sub(since).Hours())
	if out.WindowHours < 1 {
		out.WindowHours = 1
	}
	return &out, nil
}

func (s *Store) getAtlasIATAs(ctx context.Context, since, until time.Time, iatas string) ([]api.AtlasIATA, error) {
	const q = `
WITH counts AS MATERIALIZED (
  SELECT
    po.iata,
    COUNT(*)::bigint AS observation_count,
    COUNT(DISTINCT po.packet_hash)::bigint AS unique_packets,
    COUNT(DISTINCT po.observer_id)::bigint AS active_observers
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.iata
)
SELECT
  i.iata,
  i.display_name,
  i.approx_lat,
  i.approx_lng,
  COALESCE(c.observation_count, 0)::bigint AS observation_count,
  COALESCE(c.unique_packets, 0)::bigint AS unique_packets,
  COALESCE(c.active_observers, 0)::bigint AS active_observers
FROM iata_codes i
LEFT JOIN counts c ON c.iata = i.iata
WHERE ($3::text = '' OR i.iata = ANY(string_to_array($3::text, ',')))
ORDER BY observation_count DESC, i.iata`
	rows, err := s.pool.Query(ctx, q, since, until, iatas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.AtlasIATA{}
	for rows.Next() {
		var item api.AtlasIATA
		if err := rows.Scan(&item.IATA, &item.DisplayName, &item.Lat, &item.Lng, &item.ObservationCount, &item.UniquePackets, &item.ActiveObservers); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) getAtlasCurrentAndPreviousIATAs(ctx context.Context, since, until time.Time, iatas string) ([]api.AtlasIATA, []api.AtlasIATA, error) {
	var current []api.AtlasIATA
	var previous []api.AtlasIATA
	window := until.Sub(since)
	if err := runStoreParallelTasks(ctx,
		storeParallelTask{
			name: "current atlas iata rollup",
			run: func(ctx context.Context) error {
				var err error
				current, err = s.getAtlasIATAs(ctx, since, until, iatas)
				return err
			},
		},
		storeParallelTask{
			name: "previous atlas iata rollup",
			run: func(ctx context.Context) error {
				var err error
				previous, err = s.getAtlasIATAs(ctx, since.Add(-window), since, iatas)
				return err
			},
		},
	); err != nil {
		return nil, nil, err
	}
	return current, previous, nil
}

func (s *Store) getAtlasHourly(ctx context.Context, since, until time.Time, iatas string) ([]api.ObservationPoint, error) {
	const q = `
SELECT
  date_trunc('hour', po.heard_at)::timestamptz AS hour,
  po.iata,
  COUNT(*)::bigint AS observation_count,
  COUNT(DISTINCT po.packet_hash)::bigint AS unique_packets,
  COUNT(DISTINCT po.observer_id)::bigint AS active_observers
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY hour, po.iata
ORDER BY hour, po.iata`
	rows, err := s.pool.Query(ctx, q, since, until, iatas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.ObservationPoint{}
	for rows.Next() {
		var hour time.Time
		var item api.ObservationPoint
		if err := rows.Scan(&hour, &item.IATA, &item.ObservationCount, &item.UniquePackets, &item.ActiveObservers); err != nil {
			return nil, err
		}
		item.Hour = hour.UnixMilli()
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) getAtlasPayloadMix(ctx context.Context, since, until time.Time, iatas string) ([]api.PayloadBreakdownItem, error) {
	const q = `
SELECT p.payload_type, COUNT(*)::bigint AS count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY p.payload_type
ORDER BY count DESC`
	rows, err := s.pool.Query(ctx, q, since, until, iatas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.PayloadBreakdownItem{}
	for rows.Next() {
		var item api.PayloadBreakdownItem
		if err := rows.Scan(&item.PayloadType, &item.Count); err != nil {
			return nil, err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) getAtlasRouteMix(ctx context.Context, since, until time.Time, iatas string) ([]api.LiveRouteMixItem, error) {
	const q = `
SELECT p.route_type, COUNT(*)::bigint AS count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY p.route_type
ORDER BY count DESC, p.route_type
LIMIT 8`
	rows, err := s.pool.Query(ctx, q, since, until, iatas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.LiveRouteMixItem{}
	for rows.Next() {
		var item api.LiveRouteMixItem
		if err := rows.Scan(&item.RouteType, &item.Count); err != nil {
			return nil, err
		}
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) getAtlasPayloadAndRouteMix(ctx context.Context, since, until time.Time, iatas string) ([]api.PayloadBreakdownItem, []api.LiveRouteMixItem, error) {
	const q = `
WITH base AS MATERIALIZED (
  SELECT p.payload_type, p.route_type
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
payload_mix AS (
  SELECT payload_type AS code, COUNT(*)::bigint AS count
  FROM base
  GROUP BY payload_type
),
route_mix AS (
  SELECT route_type AS code, COUNT(*)::bigint AS count
  FROM base
  GROUP BY route_type
  ORDER BY count DESC, route_type ASC
  LIMIT 8
)
SELECT 'payload' AS kind, code, count
FROM payload_mix
UNION ALL
SELECT 'route' AS kind, code, count
FROM route_mix
ORDER BY kind, count DESC, code ASC`
	rows, err := s.pool.Query(ctx, q, since, until, iatas)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	payload := []api.PayloadBreakdownItem{}
	routes := []api.LiveRouteMixItem{}
	for rows.Next() {
		var kind string
		var code int16
		var count int64
		if err := rows.Scan(&kind, &code, &count); err != nil {
			return nil, nil, err
		}
		switch kind {
		case "payload":
			payload = append(payload, api.PayloadBreakdownItem{
				PayloadType:     code,
				PayloadTypeName: api.PayloadTypeName(code),
				Count:           count,
			})
		case "route":
			routes = append(routes, api.LiveRouteMixItem{
				RouteType:     code,
				RouteTypeName: api.RouteTypeName(code),
				Count:         count,
			})
		}
	}
	return payload, routes, rows.Err()
}

func (s *Store) getAtlasActiveNodes(ctx context.Context, since, until time.Time, iatas string) (int64, error) {
	const q = `
WITH active_origins AS (
  SELECT p.origin_pubkey
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND p.origin_pubkey IS NOT NULL
  GROUP BY p.origin_pubkey
)
SELECT COUNT(*)::bigint
FROM active_origins ao
JOIN nodes n ON n.public_key = ao.origin_pubkey`
	var count int64
	if err := s.pool.QueryRow(ctx, q, since, until, iatas).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) getAtlasRouteCount(ctx context.Context, since, until time.Time, iatas string) (int64, error) {
	const q = `
SELECT COUNT(*)::bigint
FROM known_routes kr
WHERE kr.last_seen >= $1
  AND kr.last_seen <= $2
  AND ($3::text = '' OR kr.iata = ANY(string_to_array($3::text, ',')))`
	var count int64
	if err := s.pool.QueryRow(ctx, q, since, until, iatas).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) getAtlasActiveNodesByIATA(ctx context.Context, since, until time.Time) (map[string]int64, error) {
	const q = `
WITH active_origins AS (
  SELECT po.iata, p.origin_pubkey
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND p.origin_pubkey IS NOT NULL
  GROUP BY po.iata, p.origin_pubkey
)
SELECT ao.iata, COUNT(*)::bigint
FROM active_origins ao
JOIN nodes n ON n.public_key = ao.origin_pubkey
GROUP BY ao.iata`
	rows, err := s.pool.Query(ctx, q, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var iata string
		var count int64
		if err := rows.Scan(&iata, &count); err != nil {
			return nil, err
		}
		result[iata] = count
	}
	return result, rows.Err()
}

func (s *Store) getAtlasRouteCountsByIATA(ctx context.Context, since, until time.Time) (map[string]int64, error) {
	const q = `
SELECT kr.iata, COUNT(*)::bigint
FROM known_routes kr
WHERE kr.last_seen >= $1
  AND kr.last_seen <= $2
GROUP BY kr.iata`
	rows, err := s.pool.Query(ctx, q, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var iata string
		var count int64
		if err := rows.Scan(&iata, &count); err != nil {
			return nil, err
		}
		result[iata] = count
	}
	return result, rows.Err()
}

func (s *Store) getAtlasTopNodes(ctx context.Context, since, until time.Time, iatas string, limit int32) ([]api.TopNode, error) {
	const q = `
WITH node_counts AS (
  SELECT
    p.origin_pubkey,
    COALESCE((array_agg(DISTINCT po.iata ORDER BY po.iata))[1], '')::text AS iata,
    COUNT(*)::bigint AS observation_count,
    MAX(po.heard_at)::timestamptz AS last_heard
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND p.origin_pubkey IS NOT NULL
  GROUP BY p.origin_pubkey
)
SELECT
  n.id,
  n.name,
  n.node_type,
  nc.iata,
  nc.observation_count,
  nc.last_heard
FROM node_counts nc
JOIN nodes n ON n.public_key = nc.origin_pubkey
ORDER BY nc.observation_count DESC, nc.last_heard DESC
LIMIT $4`
	rows, err := s.pool.Query(ctx, q, since, until, iatas, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.TopNode{}
	for rows.Next() {
		var item api.TopNode
		var lastHeard time.Time
		if err := rows.Scan(&item.NodeID, &item.NodeName, &item.NodeType, &item.IATA, &item.ObservationCount, &lastHeard); err != nil {
			return nil, err
		}
		item.NodeTypeName = api.NodeTypeName(item.NodeType)
		item.LastHeard = lastHeard.UnixMilli()
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) getAtlasTopObservers(ctx context.Context, since, until time.Time, iatas string, limit int32) ([]api.TopObserver, error) {
	const q = `
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE((array_agg(DISTINCT po.iata ORDER BY po.iata))[1], '')::text AS iata,
  COUNT(*)::bigint AS observation_count
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY o.id
ORDER BY observation_count DESC
LIMIT $4`
	rows, err := s.pool.Query(ctx, q, since, until, iatas, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []api.TopObserver{}
	for rows.Next() {
		var item api.TopObserver
		if err := rows.Scan(&item.ObserverID, &item.DisplayName, &item.ObserverType, &item.IATA, &item.ObservationCount); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func atlasStoryBeats(summary *api.RegionAtlasSummary) []api.AtlasStoryBeat {
	beats := []api.AtlasStoryBeat{
		{
			ID:     "traffic",
			Kind:   "traffic",
			Title:  "Traffic window",
			Detail: fmt.Sprintf("%d observations across %d packets", summary.KPIs.TotalObservations, summary.KPIs.TotalPackets),
			Value:  summary.KPIs.TotalObservations,
		},
	}
	if len(summary.IATAs) > 0 {
		top := summary.IATAs[0]
		beats = append(beats, api.AtlasStoryBeat{
			ID:     "iata-" + strings.ToLower(top.IATA),
			Kind:   "hotspot",
			Title:  "Busiest IATA",
			Detail: fmt.Sprintf("%s carried %d observations", top.IATA, top.ObservationCount),
			IATA:   top.IATA,
			Value:  top.ObservationCount,
		})
	}
	if len(summary.TopObservers) > 0 {
		top := summary.TopObservers[0]
		name := top.ObserverID.String()[:8]
		if top.DisplayName != nil && *top.DisplayName != "" {
			name = *top.DisplayName
		}
		beats = append(beats, api.AtlasStoryBeat{
			ID:     "observer-" + top.ObserverID.String(),
			Kind:   "observer",
			Title:  "Lead observer",
			Detail: fmt.Sprintf("%s heard %d observations", name, top.ObservationCount),
			IATA:   top.IATA,
			Value:  top.ObservationCount,
		})
	}
	if len(summary.TopNodes) > 0 {
		top := summary.TopNodes[0]
		name := top.NodeID.String()[:8]
		if top.NodeName != nil && *top.NodeName != "" {
			name = *top.NodeName
		}
		at := top.LastHeard
		beats = append(beats, api.AtlasStoryBeat{
			ID:     "node-" + top.NodeID.String(),
			Kind:   "node",
			Title:  "Most-heard node",
			Detail: fmt.Sprintf("%s led with %d observations", name, top.ObservationCount),
			IATA:   top.IATA,
			Value:  top.ObservationCount,
			At:     &at,
		})
	}
	if len(summary.PayloadMix) > 0 {
		top := summary.PayloadMix[0]
		beats = append(beats, api.AtlasStoryBeat{
			ID:     "payload-" + top.PayloadTypeName,
			Kind:   "payload",
			Title:  "Dominant payload",
			Detail: fmt.Sprintf("%s made up %d observations", top.PayloadTypeName, top.Count),
			Value:  top.Count,
		})
	}
	return beats
}

func (s *Store) getAtlasNotableRoutes(ctx context.Context, iatas []string) ([]api.AtlasNotableRoute, error) {
	routes, err := s.ListKnownRoutes(ctx, iatas, 0, time.Time{}, 8)
	if err != nil {
		return nil, err
	}
	items := make([]api.AtlasNotableRoute, 0, len(routes))
	for _, route := range routes {
		items = append(items, api.AtlasNotableRoute{
			RouteID:          route.ID,
			IATA:             route.IATA,
			HopCount:         route.HopCount,
			NodeNames:        atlasRouteNodeNames(route.Hops),
			ObservationCount: route.ObservationCount,
			LastSeen:         route.LastSeen,
			URL:              atlasBriefingURL("Map", "", "", map[string]string{"routeId": fmt.Sprintf("%d", route.ID), "routeReplay": "1"}),
		})
	}
	return items, nil
}

func atlasRouteNodeNames(hops []api.RouteHop) []string {
	names := make([]string, 0, len(hops))
	for _, hop := range hops {
		if hop.Node != nil && hop.Node.Name != nil && *hop.Node.Name != "" {
			names = append(names, *hop.Node.Name)
			continue
		}
		if hop.HashBytes != "" {
			names = append(names, strings.ToUpper(hop.HashBytes))
			continue
		}
		names = append(names, hop.NodeID.String()[:8])
	}
	return names
}

func atlasBriefingHotspots(iatas []api.AtlasIATA) []api.AtlasHotspot {
	limit := len(iatas)
	if limit > 8 {
		limit = 8
	}
	items := make([]api.AtlasHotspot, 0, limit)
	for _, row := range iatas[:limit] {
		items = append(items, api.AtlasHotspot{
			IATA:             row.IATA,
			DisplayName:      row.DisplayName,
			Lat:              row.Lat,
			Lng:              row.Lng,
			ObservationCount: row.ObservationCount,
			UniquePackets:    row.UniquePackets,
			ActiveObservers:  row.ActiveObservers,
			URL:              atlasBriefingURL("Live", "", row.IATA, nil),
		})
	}
	return items
}

func atlasDegradedObservers(items []api.StatsObserverHealth, limit int) []api.StatsObserverHealth {
	out := make([]api.StatsObserverHealth, 0, limit)
	for _, item := range items {
		if !(item.Flags.Stale || item.Flags.LowBattery || item.Flags.HighNoise || item.Flags.HighAirtime || item.Flags.QueueBacklog || item.Flags.ReceiveErrors || item.Flags.NoTelemetry) {
			continue
		}
		out = append(out, item)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func atlasBriefingHealth(kpis api.StatsOverview, summary api.StatsHealthSummary) api.AtlasBriefingHealth {
	degraded := summary.StaleObservers + summary.LowBattery + summary.HighNoise + summary.HighAirtime + summary.QueueBacklog + summary.ReceiveErrors
	score := 100
	score -= int(minInt64(summary.StaleObservers*5, 45))
	score -= int(minInt64((summary.LowBattery+summary.HighNoise+summary.HighAirtime+summary.QueueBacklog+summary.ReceiveErrors)*4, 35))
	score -= int(minInt64(summary.NoTelemetry*2, 20))
	if kpis.ActiveObservers == 0 {
		score = 0
	}
	if score < 0 {
		score = 0
	}
	status := "ok"
	if score < 50 {
		status = "critical"
	} else if score < 80 || degraded > 0 || summary.NoTelemetry > 0 {
		status = "degraded"
	}
	return api.AtlasBriefingHealth{
		Status:            status,
		ServerTime:        time.Now().UnixMilli(),
		StaleObservers:    summary.StaleObservers,
		DegradedObservers: degraded,
		NoTelemetry:       summary.NoTelemetry,
		HealthScore:       score,
	}
}

func atlasRegionHealthScore(kpis api.StatsOverview, activeNodes, routeCount int64) int {
	if kpis.TotalObservations == 0 {
		return 35
	}
	score := 70
	if kpis.ActiveObservers > 0 {
		score += 10
	}
	if activeNodes > 0 {
		score += 10
	}
	if routeCount > 0 {
		score += 10
	}
	if score > 100 {
		return 100
	}
	return score
}

func atlasDeltaPct(current, previous int64) float64 {
	if previous == 0 {
		if current > 0 {
			return 100
		}
		return 0
	}
	return (float64(current-previous) / float64(previous)) * 100
}

func atlasBriefingPriorities(
	summary *api.RegionAtlasSummary,
	health api.AtlasBriefingHealth,
	healthSummary api.StatsHealthSummary,
	regions []api.AtlasBriefingRegion,
	hotspots []api.AtlasHotspot,
	routes []api.AtlasNotableRoute,
	degradedObservers []api.StatsObserverHealth,
) []api.AtlasPriorityItem {
	items := []api.AtlasPriorityItem{
		{
			ID:       "health",
			Kind:     "broker_or_cache",
			Severity: atlasHealthSeverity(health.Status),
			Title:    "Network health snapshot",
			Detail:   fmt.Sprintf("%s / score %d / %d active observers", health.Status, health.HealthScore, summary.KPIs.ActiveObservers),
			Region:   summary.Region.Slug,
			Value:    int64(health.HealthScore),
			URL:      atlasBriefingURL("Stats", summary.Region.Slug, "", map[string]string{"statsTab": "overview", "range": "24h"}),
		},
	}
	selectedRegion := atlasSelectedBriefingRegion(summary.Region.Slug, regions)
	if selectedRegion != nil && selectedRegion.ObservationDeltaPct >= 25 {
		items = append(items, api.AtlasPriorityItem{
			ID:       "traffic-spike-" + selectedRegion.Slug,
			Kind:     "traffic_spike",
			Severity: "warn",
			Title:    "Traffic surge",
			Detail:   fmt.Sprintf("%s observations are up %.0f%% over the previous window", selectedRegion.Name, selectedRegion.ObservationDeltaPct),
			Region:   selectedRegion.Slug,
			Value:    selectedRegion.ObservationCount,
			URL:      atlasBriefingURL("Live", selectedRegion.Slug, "", map[string]string{"range": "24h"}),
		})
	}
	if healthSummary.StaleObservers > 0 {
		items = append(items, api.AtlasPriorityItem{
			ID:       "stale-observers",
			Kind:     "stale_observers",
			Severity: "warn",
			Title:    "Stale observers",
			Detail:   fmt.Sprintf("%d observers are stale in this briefing window", healthSummary.StaleObservers),
			Region:   summary.Region.Slug,
			Value:    healthSummary.StaleObservers,
			URL:      atlasBriefingURL("Stats", summary.Region.Slug, "", map[string]string{"statsTab": "rf", "range": "24h"}),
		})
	}
	rfIssues := healthSummary.LowBattery + healthSummary.HighNoise + healthSummary.HighAirtime + healthSummary.QueueBacklog + healthSummary.ReceiveErrors
	if rfIssues > 0 {
		items = append(items, api.AtlasPriorityItem{
			ID:       "rf-degraded",
			Kind:     "rf_degraded",
			Severity: "warn",
			Title:    "RF degradation",
			Detail:   fmt.Sprintf("%d observer health flags need review", rfIssues),
			Region:   summary.Region.Slug,
			Value:    rfIssues,
			URL:      atlasBriefingURL("Stats", summary.Region.Slug, "", map[string]string{"statsTab": "rf", "range": "24h"}),
		})
	}
	if len(hotspots) > 0 && hotspots[0].ObservationCount > 0 {
		top := hotspots[0]
		items = append(items, api.AtlasPriorityItem{
			ID:       "hot-iata-" + strings.ToLower(top.IATA),
			Kind:     "hot_iata",
			Severity: "info",
			Title:    "Busiest IATA",
			Detail:   fmt.Sprintf("%s carried %d observations", top.IATA, top.ObservationCount),
			Region:   summary.Region.Slug,
			IATA:     top.IATA,
			Value:    top.ObservationCount,
			URL:      top.URL,
		})
	}
	if len(routes) > 0 {
		route := routes[0]
		at := route.LastSeen
		routeID := route.RouteID
		items = append(items, api.AtlasPriorityItem{
			ID:       fmt.Sprintf("route-%d", route.RouteID),
			Kind:     "new_route",
			Severity: "info",
			Title:    "Verified route corridor",
			Detail:   fmt.Sprintf("%s / %d hops / %d observations", route.IATA, route.HopCount, route.ObservationCount),
			Region:   summary.Region.Slug,
			IATA:     route.IATA,
			RouteID:  &routeID,
			Value:    route.ObservationCount,
			At:       &at,
			URL:      route.URL,
		})
	}
	if len(summary.TopNodes) > 0 {
		node := summary.TopNodes[0]
		nodeID := node.NodeID
		at := node.LastHeard
		items = append(items, api.AtlasPriorityItem{
			ID:       "top-node-" + node.NodeID.String(),
			Kind:     "top_node",
			Severity: "info",
			Title:    "Top node",
			Detail:   fmt.Sprintf("%s produced %d observations", atlasNodeName(node), node.ObservationCount),
			Region:   summary.Region.Slug,
			IATA:     node.IATA,
			NodeID:   &nodeID,
			Value:    node.ObservationCount,
			At:       &at,
			URL:      atlasBriefingURL("Nodes", summary.Region.Slug, "", map[string]string{"nodeId": node.NodeID.String()}),
		})
	}
	if len(summary.TopObservers) > 0 {
		observer := summary.TopObservers[0]
		observerID := observer.ObserverID
		items = append(items, api.AtlasPriorityItem{
			ID:         "top-observer-" + observer.ObserverID.String(),
			Kind:       "top_observer",
			Severity:   "info",
			Title:      "Top observer",
			Detail:     fmt.Sprintf("%s heard %d observations", atlasObserverName(observer), observer.ObservationCount),
			Region:     summary.Region.Slug,
			IATA:       observer.IATA,
			ObserverID: &observerID,
			Value:      observer.ObservationCount,
			URL:        atlasBriefingURL("Observers", summary.Region.Slug, "", map[string]string{"observerId": observer.ObserverID.String()}),
		})
	}
	if len(degradedObservers) > 0 {
		observer := degradedObservers[0]
		observerID := observer.ObserverID
		at := observer.LastHeard
		items = append(items, api.AtlasPriorityItem{
			ID:         "degraded-observer-" + observer.ObserverID.String(),
			Kind:       "rf_degraded",
			Severity:   "warn",
			Title:      "Observer needs review",
			Detail:     fmt.Sprintf("%s / health %d / %s", atlasHealthObserverName(observer), observer.HealthScore, observer.Status),
			Region:     summary.Region.Slug,
			IATA:       observer.IATA,
			ObserverID: &observerID,
			Value:      int64(100 - observer.HealthScore),
			At:         &at,
			URL:        atlasBriefingURL("Stats", summary.Region.Slug, "", map[string]string{"statsTab": "observers", "observerId": observer.ObserverID.String(), "range": "24h"}),
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		wi := atlasSeverityWeight(items[i].Severity)
		wj := atlasSeverityWeight(items[j].Severity)
		if wi != wj {
			return wi < wj
		}
		ai := int64(0)
		if items[i].At != nil {
			ai = *items[i].At
		}
		aj := int64(0)
		if items[j].At != nil {
			aj = *items[j].At
		}
		if ai != aj {
			return ai > aj
		}
		if items[i].Value != items[j].Value {
			return items[i].Value > items[j].Value
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func atlasSelectedBriefingRegion(slug string, regions []api.AtlasBriefingRegion) *api.AtlasBriefingRegion {
	if slug == "" {
		slug = "all"
	}
	for i := range regions {
		if regions[i].Slug == slug {
			return &regions[i]
		}
	}
	return nil
}

func atlasHealthSeverity(status string) string {
	if status == "critical" {
		return "critical"
	}
	if status == "degraded" {
		return "warn"
	}
	return "good"
}

func atlasSeverityWeight(severity string) int {
	switch severity {
	case "critical":
		return 0
	case "warn":
		return 1
	case "info":
		return 2
	case "good":
		return 3
	default:
		return 4
	}
}

func atlasNodeName(node api.TopNode) string {
	if node.NodeName != nil && *node.NodeName != "" {
		return *node.NodeName
	}
	return node.NodeID.String()[:8]
}

func atlasObserverName(observer api.TopObserver) string {
	if observer.DisplayName != nil && *observer.DisplayName != "" {
		return *observer.DisplayName
	}
	return observer.ObserverID.String()[:8]
}

func atlasHealthObserverName(observer api.StatsObserverHealth) string {
	if observer.DisplayName != nil && *observer.DisplayName != "" {
		return *observer.DisplayName
	}
	return observer.ObserverID.String()[:8]
}

func atlasBriefingURL(tab, regionSlug, iata string, extra map[string]string) string {
	params := url.Values{}
	params.Set("tab", tab)
	if iata != "" {
		params.Set("iata", iata)
	} else if regionSlug != "" && regionSlug != "all" {
		params.Set("regions", regionSlug)
	}
	for key, value := range extra {
		if value != "" {
			params.Set(key, value)
		}
	}
	return "/?" + params.Encode()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type atlasPointRow struct {
	Kind  string  `json:"kind"`
	Label *string `json:"label"`
	IATA  string  `json:"iata"`
	Lat   float64 `json:"lat"`
	Lng   float64 `json:"lng"`
	At    *int64  `json:"at"`
}

func (s *Store) ListAtlasReplay(ctx context.Context, regionSlug string, since, until time.Time, cursor int64, limit int32) (api.Page[api.AtlasReplayPacket], error) {
	since, until = atlasWindow(since, until)
	_, iatas, err := s.atlasRegion(ctx, regionSlug)
	if err != nil {
		return api.Page[api.AtlasReplayPacket]{}, err
	}
	iataFilter := strings.Join(iatas, ",")
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	const q = `
WITH page AS (
  SELECT
    p.packet_hash,
    p.payload_type,
    p.route_type,
    p.first_heard_at,
    p.last_heard_at,
    ts.name AS scope_name,
    origin.id AS origin_id,
    origin.name AS origin_name,
    origin.public_key AS origin_public_key,
    origin.latitude AS origin_latitude,
    origin.longitude AS origin_longitude
  FROM packets p
  LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
  LEFT JOIN nodes origin ON origin.public_key = p.origin_pubkey
  WHERE p.first_heard_at >= $2
    AND p.first_heard_at <= $3
    AND ($4::timestamptz IS NULL OR p.last_heard_at < $4)
    AND ($1::text = '' OR EXISTS (
      SELECT 1 FROM packet_observations po
      WHERE po.packet_hash = p.packet_hash
        AND po.iata = ANY(string_to_array($1::text, ','))
    ))
  ORDER BY p.last_heard_at DESC, p.packet_hash DESC
  LIMIT $5
),
obs AS (
  SELECT
    po.packet_hash,
    COUNT(*)::bigint AS observation_count,
    array_agg(DISTINCT po.iata ORDER BY po.iata)::text[] AS iatas
  FROM packet_observations po
  JOIN page pg ON pg.packet_hash = po.packet_hash
  WHERE $1::text = '' OR po.iata = ANY(string_to_array($1::text, ','))
  GROUP BY po.packet_hash
),
latest AS (
  SELECT DISTINCT ON (po.packet_hash)
    po.packet_hash,
    po.observer_id,
    o.display_name,
    po.iata
  FROM packet_observations po
  JOIN page pg ON pg.packet_hash = po.packet_hash
  LEFT JOIN observers o ON o.id = po.observer_id
  WHERE $1::text = '' OR po.iata = ANY(string_to_array($1::text, ','))
  ORDER BY po.packet_hash, po.heard_at DESC, po.id DESC
),
points AS (
  SELECT
    point.packet_hash,
    jsonb_agg(jsonb_build_object(
      'kind', 'observer',
      'label', COALESCE(point.node_name, point.observer_name),
      'iata', point.iata,
      'lat', point.latitude,
      'lng', point.longitude,
      'at', (extract(epoch from point.heard_at) * 1000)::bigint
    ) ORDER BY point.heard_at) AS observer_points
  FROM (
    SELECT DISTINCT ON (po.packet_hash, COALESCE(onode.id::text, po.observer_id::text), po.iata)
      po.packet_hash,
      po.iata,
      po.heard_at,
      o.display_name AS observer_name,
      onode.name AS node_name,
      onode.latitude,
      onode.longitude
    FROM packet_observations po
    JOIN page pg ON pg.packet_hash = po.packet_hash
    LEFT JOIN observers o ON o.id = po.observer_id
    LEFT JOIN nodes onode ON onode.public_key = o.public_key
    WHERE ($1::text = '' OR po.iata = ANY(string_to_array($1::text, ',')))
      AND onode.latitude IS NOT NULL
      AND onode.longitude IS NOT NULL
    ORDER BY po.packet_hash, COALESCE(onode.id::text, po.observer_id::text), po.iata, po.heard_at
  ) point
  GROUP BY point.packet_hash
)
SELECT
  page.packet_hash,
  page.payload_type,
  page.route_type,
  page.first_heard_at,
  page.last_heard_at,
  page.scope_name,
  COALESCE(obs.observation_count, 0)::bigint AS observation_count,
  latest.observer_id,
  latest.display_name,
  latest.iata,
  page.origin_id,
  page.origin_name,
  page.origin_public_key,
  page.origin_latitude,
  page.origin_longitude,
  COALESCE(obs.iatas, ARRAY[]::text[]) AS iatas,
  COALESCE(points.observer_points, '[]'::jsonb) AS observer_points
FROM page
LEFT JOIN obs ON obs.packet_hash = page.packet_hash
LEFT JOIN latest ON latest.packet_hash = page.packet_hash
LEFT JOIN points ON points.packet_hash = page.packet_hash
ORDER BY page.last_heard_at DESC, page.packet_hash DESC`
	rows, err := s.pool.Query(ctx, q, iataFilter, since, until, cursorTS, limit+1)
	if err != nil {
		return api.Page[api.AtlasReplayPacket]{}, err
	}
	defer rows.Close()
	items := []api.AtlasReplayPacket{}
	for rows.Next() {
		var item api.AtlasReplayPacket
		var hash []byte
		var firstHeard, lastHeard time.Time
		var observationCount int64
		var latestID pgtype.UUID
		var latestName *string
		var latestIATA *string
		var originID pgtype.UUID
		var originName *string
		var originPubkey []byte
		var originLat, originLng *float64
		var pointJSON []byte
		if err := rows.Scan(
			&hash,
			&item.PayloadType,
			&item.RouteType,
			&firstHeard,
			&lastHeard,
			&item.Scope,
			&observationCount,
			&latestID,
			&latestName,
			&latestIATA,
			&originID,
			&originName,
			&originPubkey,
			&originLat,
			&originLng,
			&item.IATAs,
			&pointJSON,
		); err != nil {
			return api.Page[api.AtlasReplayPacket]{}, err
		}
		item.PacketHash = hex.EncodeToString(hash)
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		item.FirstHeardAt = firstHeard.UnixMilli()
		item.LastHeardAt = lastHeard.UnixMilli()
		item.ObservationCount = int32(observationCount)
		if latestID.Valid {
			iata := ""
			if latestIATA != nil {
				iata = *latestIATA
			}
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          uuid.UUID(latestID.Bytes),
				DisplayName: latestName,
				IATA:        iata,
			}
		}
		if originID.Valid {
			item.Origin = &api.ResolvedNode{
				ID:        uuid.UUID(originID.Bytes),
				Name:      originName,
				PublicKey: hex.EncodeToString(originPubkey),
				Latitude:  originLat,
				Longitude: originLng,
			}
		}
		item.Path = atlasReplayPath(item.Origin, pointJSON)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return api.Page[api.AtlasReplayPacket]{}, err
	}
	hasMore := len(items) > int(limit)
	if hasMore {
		items = items[:limit]
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1].LastHeardAt
		nextCursor = &last
	}
	return api.Page[api.AtlasReplayPacket]{Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

func atlasReplayPath(origin *api.ResolvedNode, pointJSON []byte) []api.AtlasPathPoint {
	points := []api.AtlasPathPoint{}
	if origin != nil && origin.Latitude != nil && origin.Longitude != nil {
		points = append(points, api.AtlasPathPoint{
			Kind:  "origin",
			Label: origin.Name,
			Lat:   *origin.Latitude,
			Lng:   *origin.Longitude,
		})
	}
	var observerPoints []atlasPointRow
	if len(pointJSON) > 0 {
		_ = json.Unmarshal(pointJSON, &observerPoints)
	}
	for _, p := range observerPoints {
		points = append(points, api.AtlasPathPoint{
			Kind:  p.Kind,
			Label: p.Label,
			IATA:  p.IATA,
			Lat:   p.Lat,
			Lng:   p.Lng,
			At:    p.At,
		})
	}
	return points
}
