// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

const (
	statsDefaultWindow     = 24 * time.Hour
	statsDefaultBucket     = "1h"
	statsDefaultLimit      = int32(25)
	statsDefaultStaleAfter = 30 * time.Minute
	statsMaxSubpathNodes   = int32(6)
)

func normalizeStatsFilter(filter api.StatsFilter) api.StatsFilter {
	now := time.Now()
	if filter.Until.IsZero() {
		filter.Until = now
	}
	if filter.Since.IsZero() {
		filter.Since = filter.Until.Add(-statsDefaultWindow)
	}
	if filter.Bucket == "" {
		filter.Bucket = defaultStatsBucket(filter.Since, filter.Until)
	}
	if filter.Limit < 1 {
		filter.Limit = statsDefaultLimit
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	return filter
}

func normalizeObserverHealthFilter(filter api.StatsObserverHealthFilter) api.StatsObserverHealthFilter {
	filter.StatsFilter = normalizeStatsFilter(filter.StatsFilter)
	if filter.StaleAfter <= 0 {
		filter.StaleAfter = statsDefaultStaleAfter
	}
	if filter.StaleAfter > 7*24*time.Hour {
		filter.StaleAfter = 7 * 24 * time.Hour
	}
	return filter
}

func defaultStatsBucket(since, until time.Time) string {
	window := until.Sub(since)
	switch {
	case window <= 30*time.Hour:
		return "1h"
	case window <= 8*24*time.Hour:
		return "6h"
	default:
		return "24h"
	}
}

func bucketHours(bucket string) float64 {
	switch bucket {
	case "6h":
		return 6
	case "24h":
		return 24
	default:
		return 1
	}
}

func statsWindow(filter api.StatsFilter) api.StatsWindow {
	return api.StatsWindow{
		Since:  filter.Since.UnixMilli(),
		Until:  filter.Until.UnixMilli(),
		Bucket: filter.Bucket,
	}
}

func statsIATAFilter(iatas []string) string {
	if len(iatas) == 0 {
		return ""
	}
	return strings.Join(iatas, ",")
}

func (s *Store) GetStatsSummary(ctx context.Context, filter api.StatsFilter) (*api.StatsSummary, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	summary := &api.StatsSummary{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	liveSince := filter.Until.Add(-15 * time.Minute)
	if liveSince.Before(filter.Since) {
		liveSince = filter.Since
	}
	var (
		overview     api.StatsOverview
		live         *api.LiveSummary
		nodeTypes    []api.NodeTypeCount
		payloads     *api.StatsPayloads
		topIATAs     []api.LiveIATACount
		topObservers []api.TopObserver
		topNodes     []api.TopNode
		presets      []api.RadioPreset
		scopes       []api.ScopeStats
		health       *api.StatsObserverHealthResponse
	)
	if err := runStoreParallelTasks(ctx,
		storeParallelTask{
			name: "stats overview",
			run: func(ctx context.Context) error {
				var err error
				overview, err = s.getStatsOverviewWindow(ctx, filter, iataFilter)
				return err
			},
		},
		storeParallelTask{
			name: "stats live summary",
			run: func(ctx context.Context) error {
				var err error
				live, err = s.GetLiveSummary(ctx, api.LiveSummaryFilter{IATAs: filter.IATAs, Since: liveSince, Until: filter.Until})
				return err
			},
		},
		storeParallelTask{
			name: "stats node types",
			run: func(ctx context.Context) error {
				var err error
				nodeTypes, err = s.GetStatsNodeTypes(ctx, filter.IATAs)
				return err
			},
		},
		storeParallelTask{
			name: "stats payloads",
			run: func(ctx context.Context) error {
				var err error
				payloads, err = s.GetStatsPayloads(ctx, filter)
				return err
			},
		},
		storeParallelTask{
			name: "stats top iatas",
			run: func(ctx context.Context) error {
				var err error
				topIATAs, err = s.getStatsTopIATAs(ctx, filter, iataFilter)
				return err
			},
		},
		storeParallelTask{
			name: "stats top observers",
			run: func(ctx context.Context) error {
				var err error
				topObservers, err = s.getStatsTopObserversWindow(ctx, filter, iataFilter, 10)
				return err
			},
		},
		storeParallelTask{
			name: "stats top nodes",
			run: func(ctx context.Context) error {
				var err error
				topNodes, err = s.getStatsTopNodesWindow(ctx, filter, iataFilter, 10)
				return err
			},
		},
		storeParallelTask{
			name: "stats radio presets",
			run: func(ctx context.Context) error {
				var err error
				presets, err = s.GetRadioPresets(ctx, "", filter.IATAs)
				return err
			},
		},
		storeParallelTask{
			name: "stats scopes",
			run: func(ctx context.Context) error {
				var err error
				scopes, err = s.GetScopeStats(ctx)
				return err
			},
		},
		storeParallelTask{
			name: "stats observer health",
			run: func(ctx context.Context) error {
				var err error
				health, err = s.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
					StatsFilter: api.StatsFilter{
						IATAs:  filter.IATAs,
						Since:  filter.Since,
						Until:  filter.Until,
						Bucket: filter.Bucket,
						Limit:  500,
					},
					StaleAfter: statsDefaultStaleAfter,
				})
				return err
			},
		},
	); err != nil {
		return nil, err
	}
	summary.Overview = overview
	if live != nil {
		summary.Live = *live
	}
	summary.NodeTypes = nodeTypes
	if payloads != nil {
		summary.PayloadMix = payloads.Totals
		summary.RouteMix = payloads.RouteTotals
	}
	summary.TopIATAs = topIATAs
	summary.TopObservers = topObservers
	summary.TopNodes = topNodes
	summary.RadioPresets = presets
	summary.Scopes = scopes
	if health != nil {
		summary.Health = health.Summary
	}

	return summary, nil
}

func (s *Store) getStatsOverviewWindow(ctx context.Context, filter api.StatsFilter, iataFilter string) (api.StatsOverview, error) {
	var overview api.StatsOverview
	err := s.pool.QueryRow(ctx, `
SELECT
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint,
  COUNT(DISTINCT po.iata)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))`,
		filter.Since, filter.Until, iataFilter,
	).Scan(
		&overview.TotalPackets,
		&overview.TotalObservations,
		&overview.ActiveObservers,
		&overview.ActiveIATAs,
	)
	overview.WindowHours = int(math.Ceil(filter.Until.Sub(filter.Since).Hours()))
	if overview.WindowHours < 1 {
		overview.WindowHours = 1
	}
	return overview, err
}

func (s *Store) getStatsTopIATAs(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.LiveIATACount, error) {
	rows, err := s.pool.Query(ctx, `
SELECT po.iata, COUNT(*)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.iata
ORDER BY COUNT(*) DESC, po.iata ASC
LIMIT 12`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]api.LiveIATACount, 0, 12)
	for rows.Next() {
		var item api.LiveIATACount
		if err := rows.Scan(&item.IATA, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopObserversWindow(ctx context.Context, filter api.StatsFilter, iataFilter string, limit int32) ([]api.TopObserver, error) {
	rows, err := s.pool.Query(ctx, `
WITH window_counts AS MATERIALIZED (
  SELECT po.observer_id, COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id
),
latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata
  FROM observer_iatas oi
  JOIN window_counts wc ON wc.observer_id = oi.observer_id
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
)
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE(li.iata, '') AS latest_iata,
  wc.observation_count
FROM window_counts wc
JOIN observers o ON o.id = wc.observer_id
LEFT JOIN latest_iata li ON li.observer_id = wc.observer_id
ORDER BY wc.observation_count DESC, latest_iata ASC
LIMIT $4`, filter.Since, filter.Until, iataFilter, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]api.TopObserver, 0, limit)
	for rows.Next() {
		var item api.TopObserver
		if err := rows.Scan(&item.ObserverID, &item.DisplayName, &item.ObserverType, &item.IATA, &item.ObservationCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopNodesWindow(ctx context.Context, filter api.StatsFilter, iataFilter string, limit int32) ([]api.TopNode, error) {
	rows, err := s.pool.Query(ctx, `
WITH node_counts AS (
  SELECT
    p.origin_pubkey,
    po.iata,
    COUNT(*)::bigint AS observation_count,
    MAX(po.heard_at)::timestamptz AS last_heard
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND p.origin_pubkey IS NOT NULL
  GROUP BY p.origin_pubkey, po.iata
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
LIMIT $4`, filter.Since, filter.Until, iataFilter, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]api.TopNode, 0, limit)
	for rows.Next() {
		var item api.TopNode
		var lastHeard time.Time
		if err := rows.Scan(&item.NodeID, &item.NodeName, &item.NodeType, &item.IATA, &item.ObservationCount, &lastHeard); err != nil {
			return nil, err
		}
		item.NodeTypeName = api.NodeTypeName(item.NodeType)
		item.LastHeard = lastHeard.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetStatsRegions(ctx context.Context, filter api.StatsFilter) (*api.StatsRegions, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsRegions{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	rows, err := s.pool.Query(ctx, `
WITH base AS (
  SELECT po.iata, po.packet_hash, po.observer_id, po.heard_at
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
)
SELECT
  b.iata,
  COUNT(DISTINCT b.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT b.observer_id)::bigint,
  COALESCE((
    SELECT COUNT(DISTINCT ni.node_id)::bigint
    FROM node_iatas ni
    WHERE ni.iata = b.iata
      AND ni.last_heard >= $1
      AND ni.last_heard <= $2
  ), 0)::bigint,
  MAX(b.heard_at)
FROM base b
GROUP BY b.iata
ORDER BY COUNT(*) DESC, b.iata ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	rowByIATA := map[string]*api.StatsRegionRow{}
	for rows.Next() {
		item := api.StatsRegionRow{}
		var lastHeard time.Time
		if err := rows.Scan(
			&item.IATA,
			&item.PacketCount,
			&item.ObservationCount,
			&item.ActiveObservers,
			&item.ActiveNodes,
			&lastHeard,
		); err != nil {
			rows.Close()
			return nil, err
		}
		item.LastHeard = lastHeard.UnixMilli()
		rowByIATA[item.IATA] = &item
		response.Items = append(response.Items, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range response.Items {
		rowByIATA[response.Items[i].IATA] = &response.Items[i]
	}

	if err := s.fillStatsRegionTopPayloads(ctx, filter, iataFilter, rowByIATA); err != nil {
		return nil, err
	}
	if err := s.fillStatsRegionTopRoutes(ctx, filter, iataFilter, rowByIATA); err != nil {
		return nil, err
	}
	if err := s.fillStatsRegionTrends(ctx, filter, iataFilter, rowByIATA); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Store) fillStatsRegionTopPayloads(ctx context.Context, filter api.StatsFilter, iataFilter string, rowsByIATA map[string]*api.StatsRegionRow) error {
	rows, err := s.pool.Query(ctx, `
SELECT iata, payload_type, count
FROM (
  SELECT
    po.iata,
    p.payload_type,
    COUNT(*)::bigint AS count,
    ROW_NUMBER() OVER (PARTITION BY po.iata ORDER BY COUNT(*) DESC, p.payload_type ASC) AS rn
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.iata, p.payload_type
) ranked
WHERE rn = 1`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var iata string
		var payloadType int16
		var count int64
		if err := rows.Scan(&iata, &payloadType, &count); err != nil {
			return err
		}
		if row := rowsByIATA[iata]; row != nil {
			row.TopPayloadType = payloadType
			row.TopPayloadTypeName = api.PayloadTypeName(payloadType)
			row.TopPayloadCount = count
		}
	}
	return rows.Err()
}

func (s *Store) fillStatsRegionTopRoutes(ctx context.Context, filter api.StatsFilter, iataFilter string, rowsByIATA map[string]*api.StatsRegionRow) error {
	rows, err := s.pool.Query(ctx, `
SELECT iata, route_type, count
FROM (
  SELECT
    po.iata,
    p.route_type,
    COUNT(*)::bigint AS count,
    ROW_NUMBER() OVER (PARTITION BY po.iata ORDER BY COUNT(*) DESC, p.route_type ASC) AS rn
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.iata, p.route_type
) ranked
WHERE rn = 1`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var iata string
		var routeType int16
		var count int64
		if err := rows.Scan(&iata, &routeType, &count); err != nil {
			return err
		}
		if row := rowsByIATA[iata]; row != nil {
			row.TopRouteType = routeType
			row.TopRouteTypeName = api.RouteTypeName(routeType)
			row.TopRouteCount = count
		}
	}
	return rows.Err()
}

func (s *Store) fillStatsRegionTrends(ctx context.Context, filter api.StatsFilter, iataFilter string, rowsByIATA map[string]*api.StatsRegionRow) error {
	rows, err := s.pool.Query(ctx, `
SELECT
  po.iata,
  to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.iata, bucket
ORDER BY po.iata ASC, bucket ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var iata string
		var bucket time.Time
		var point api.StatsTrendPoint
		if err := rows.Scan(&iata, &bucket, &point.PacketCount, &point.ObservationCount, &point.ActiveObservers); err != nil {
			return err
		}
		point.T = bucket.UnixMilli()
		if row := rowsByIATA[iata]; row != nil {
			row.Trend = append(row.Trend, point)
		}
	}
	return rows.Err()
}

func (s *Store) GetStatsPayloads(ctx context.Context, filter api.StatsFilter) (*api.StatsPayloads, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsPayloads{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	rows, err := s.pool.Query(ctx, `
SELECT
  CASE
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.route_type) = 1 THEN 'payload_total'
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.payload_type) = 1 THEN 'route_total'
    WHEN GROUPING(p.route_type) = 1 THEN 'payload_timeline'
    ELSE 'route_timeline'
  END AS kind,
  b.bucket,
  CASE WHEN GROUPING(p.route_type) = 1 THEN p.payload_type ELSE p.route_type END AS code,
  COUNT(*)::bigint AS count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
CROSS JOIN LATERAL (
  SELECT to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket
) b
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY GROUPING SETS ((p.payload_type), (p.route_type), (b.bucket, p.payload_type), (b.bucket, p.route_type))
ORDER BY
  CASE
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.route_type) = 1 THEN 1
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.payload_type) = 1 THEN 2
    WHEN GROUPING(p.route_type) = 1 THEN 3
    ELSE 4
  END,
  b.bucket ASC NULLS FIRST,
  count DESC,
  code ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var bucket *time.Time
		var code int16
		var count int64
		if err := rows.Scan(&kind, &bucket, &code, &count); err != nil {
			return nil, err
		}
		switch kind {
		case "payload_total":
			response.Totals = append(response.Totals, api.PayloadBreakdownItem{
				PayloadType:     code,
				PayloadTypeName: api.PayloadTypeName(code),
				Count:           count,
			})
		case "route_total":
			response.RouteTotals = append(response.RouteTotals, api.LiveRouteMixItem{
				RouteType:     code,
				RouteTypeName: api.RouteTypeName(code),
				Count:         count,
			})
		case "payload_timeline":
			if bucket == nil {
				continue
			}
			response.PayloadTimeline = append(response.PayloadTimeline, api.StatsPayloadBucket{
				T:               bucket.UnixMilli(),
				PayloadType:     code,
				PayloadTypeName: api.PayloadTypeName(code),
				Count:           count,
			})
		case "route_timeline":
			if bucket == nil {
				continue
			}
			response.RouteTimeline = append(response.RouteTimeline, api.StatsRouteBucket{
				T:             bucket.UnixMilli(),
				RouteType:     code,
				RouteTypeName: api.RouteTypeName(code),
				Count:         count,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return response, nil
}

func (s *Store) GetStatsHashAnalytics(ctx context.Context, filter api.StatsFilter) (*api.StatsHashAnalytics, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsHashAnalytics{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	if err := s.pool.QueryRow(ctx, `
WITH inconsistent AS (
  SELECT po.packet_hash
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.packet_hash
  HAVING COUNT(DISTINCT po.hash_size) > 1
),
short_prefixes AS (
  SELECT
    encode(
      CASE sizes.hash_size
        WHEN 1 THEN ns.prefix_1
        WHEN 2 THEN ns.prefix_2
        WHEN 3 THEN ns.prefix_3
        ELSE ns.prefix_4
      END,
      'hex'
    ) AS prefix,
    sizes.hash_size::smallint AS hash_size,
    ns.iata::text AS iata,
    ns.node_id
  FROM node_short_ids ns
  JOIN node_iatas ni ON ni.node_id = ns.node_id AND ni.iata = ns.iata
  CROSS JOIN (VALUES (1), (2), (3), (4)) AS sizes(hash_size)
  WHERE ni.last_heard >= $1
    AND ni.last_heard <= $2
    AND ($3::text = '' OR ns.iata = ANY(string_to_array($3::text, ',')))
),
risky AS (
  SELECT 1
  FROM short_prefixes
  GROUP BY prefix, hash_size, iata
  HAVING COUNT(DISTINCT node_id) > 1
)
SELECT
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(*) FILTER (WHERE po.hash_size > 1)::bigint,
  (SELECT COUNT(*)::bigint FROM inconsistent),
  (SELECT COUNT(*)::bigint FROM risky)
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))`,
		filter.Since, filter.Until, iataFilter).Scan(
		&response.TotalPackets,
		&response.TotalObservations,
		&response.MultibyteObservations,
		&response.InconsistentPacketCount,
		&response.CollisionPrefixCount,
	); err != nil {
		return nil, err
	}

	sizeMix, err := s.getStatsHashSizeMix(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.SizeMix = sizeMix

	timeline, err := s.getStatsHashTimeline(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.Timeline = timeline

	risky, err := s.getStatsHashRiskyPrefixes(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.RiskyPrefixes = risky

	matrix, err := s.getStatsHashCollisionMatrix(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.CollisionMatrix = matrix

	inconsistent, err := s.getStatsHashInconsistentPackets(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.InconsistentPacketSamples = inconsistent
	return response, nil
}

func (s *Store) getStatsHashSizeMix(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashSizeCount, error) {
	rows, err := s.pool.Query(ctx, `
SELECT po.hash_size, COUNT(*)::bigint, COUNT(DISTINCT po.packet_hash)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.hash_size
ORDER BY po.hash_size ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsHashSizeCount{}
	for rows.Next() {
		var item api.StatsHashSizeCount
		if err := rows.Scan(&item.HashSize, &item.ObservationCount, &item.PacketCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsHashTimeline(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashTimelinePoint, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
  po.hash_size,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.packet_hash)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY bucket, po.hash_size
ORDER BY bucket ASC, po.hash_size ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := []api.StatsHashTimelinePoint{}
	for rows.Next() {
		var point api.StatsHashTimelinePoint
		var bucket time.Time
		if err := rows.Scan(&bucket, &point.HashSize, &point.ObservationCount, &point.PacketCount); err != nil {
			return nil, err
		}
		point.T = bucket.UnixMilli()
		points = append(points, point)
	}
	return points, rows.Err()
}

func statsHashShortIDCollisionCTE() string {
	return `
WITH short_prefixes AS (
  SELECT
    encode(
      CASE sizes.hash_size
        WHEN 1 THEN ns.prefix_1
        WHEN 2 THEN ns.prefix_2
        WHEN 3 THEN ns.prefix_3
        ELSE ns.prefix_4
      END,
      'hex'
    ) AS prefix,
    sizes.hash_size::smallint AS hash_size,
    ns.iata::text AS iata,
    ns.node_id,
    COALESCE(ni.observation_count, 0)::bigint AS observation_count,
    ni.first_heard,
    ni.last_heard
  FROM node_short_ids ns
  JOIN node_iatas ni ON ni.node_id = ns.node_id AND ni.iata = ns.iata
  CROSS JOIN (VALUES (1), (2), (3), (4)) AS sizes(hash_size)
  WHERE ni.last_heard >= $1
    AND ni.last_heard <= $2
    AND ($3::text = '' OR ns.iata = ANY(string_to_array($3::text, ',')))
),
risky_keys AS (
  SELECT prefix, hash_size, iata
  FROM short_prefixes
  GROUP BY prefix, hash_size, iata
  HAVING COUNT(DISTINCT node_id) > 1
)`
}

func (s *Store) getStatsHashRiskyPrefixes(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashCollisionPrefix, error) {
	rows, err := s.pool.Query(ctx, statsHashShortIDCollisionCTE()+`
SELECT
  sp.prefix,
  sp.hash_size,
  sp.iata,
  COUNT(DISTINCT sp.node_id)::bigint AS packet_count,
  COUNT(DISTINCT sp.node_id)::bigint AS node_count,
  COALESCE(SUM(sp.observation_count), 0)::bigint AS observation_count,
  0::bigint AS observer_count,
  MIN(sp.first_heard),
  MAX(sp.last_heard)
FROM short_prefixes sp
JOIN risky_keys rk USING (prefix, hash_size, iata)
GROUP BY sp.prefix, sp.hash_size, sp.iata
ORDER BY COUNT(DISTINCT sp.node_id) DESC, COALESCE(SUM(sp.observation_count), 0) DESC, MAX(sp.last_heard) DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsHashCollisionPrefix{}
	for rows.Next() {
		var item api.StatsHashCollisionPrefix
		var firstHeard, lastHeard time.Time
		if err := rows.Scan(
			&item.Prefix,
			&item.HashSize,
			&item.IATA,
			&item.PacketCount,
			&item.NodeCount,
			&item.ObservationCount,
			&item.ObserverCount,
			&firstHeard,
			&lastHeard,
		); err != nil {
			return nil, err
		}
		item.FirstHeard = firstHeard.UnixMilli()
		item.LastHeard = lastHeard.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsHashCollisionMatrix(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashCollisionCell, error) {
	rows, err := s.pool.Query(ctx, statsHashShortIDCollisionCTE()+`
SELECT
  sp.hash_size,
  sp.iata,
  COUNT(DISTINCT sp.prefix)::bigint,
  COUNT(DISTINCT sp.node_id)::bigint AS packet_count,
  COUNT(DISTINCT sp.node_id)::bigint AS node_count,
  COALESCE(SUM(sp.observation_count), 0)::bigint AS observation_count,
  0::bigint AS observer_count,
  MIN(sp.first_heard),
  MAX(sp.last_heard)
FROM short_prefixes sp
JOIN risky_keys rk USING (prefix, hash_size, iata)
GROUP BY sp.hash_size, sp.iata
ORDER BY sp.hash_size ASC, COUNT(DISTINCT sp.prefix) DESC, sp.iata ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsHashCollisionCell{}
	for rows.Next() {
		var item api.StatsHashCollisionCell
		var firstHeard, lastHeard time.Time
		if err := rows.Scan(
			&item.HashSize,
			&item.IATA,
			&item.PrefixCount,
			&item.PacketCount,
			&item.NodeCount,
			&item.ObservationCount,
			&item.ObserverCount,
			&firstHeard,
			&lastHeard,
		); err != nil {
			return nil, err
		}
		item.FirstHeard = firstHeard.UnixMilli()
		item.LastHeard = lastHeard.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsHashInconsistentPackets(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashInconsistentPacket, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  encode(po.packet_hash, 'hex') AS packet_hash,
  MIN(po.hash_size),
  MAX(po.hash_size),
  array_agg(DISTINCT po.hash_size ORDER BY po.hash_size),
  array_agg(DISTINCT po.iata ORDER BY po.iata),
  COUNT(*)::bigint,
  MIN(po.heard_at),
  MAX(po.heard_at)
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.packet_hash
HAVING COUNT(DISTINCT po.hash_size) > 1
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsHashInconsistentPacket{}
	for rows.Next() {
		var item api.StatsHashInconsistentPacket
		var firstHeard, lastHeard time.Time
		if err := rows.Scan(
			&item.PacketHash,
			&item.MinHashSize,
			&item.MaxHashSize,
			&item.HashSizes,
			&item.IATAs,
			&item.ObservationCount,
			&firstHeard,
			&lastHeard,
		); err != nil {
			return nil, err
		}
		item.FirstHeard = firstHeard.UnixMilli()
		item.LastHeard = lastHeard.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetStatsHashPrefixLookup(ctx context.Context, filter api.StatsHashPrefixFilter) (*api.StatsHashPrefixLookup, error) {
	filter.StatsFilter = normalizeStatsFilter(filter.StatsFilter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsHashPrefixLookup{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter.StatsFilter),
		Prefix:     filter.Prefix,
	}
	if filter.HashSize > 0 {
		hashSize := filter.HashSize
		response.HashSize = &hashSize
	}

	if err := s.pool.QueryRow(ctx, statsHashPrefixBaseSQL()+`
SELECT
  COUNT(*)::bigint,
  COUNT(DISTINCT packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT observer_id)::bigint,
  COALESCE(array_agg(DISTINCT iata ORDER BY iata), ARRAY[]::text[])
FROM matched`, filter.Since, filter.Until, iataFilter, filter.Prefix, filter.HashSize).Scan(
		&response.MatchCount,
		&response.PacketCount,
		&response.ObservationCount,
		&response.ObserverCount,
		&response.IATAs,
	); err != nil {
		return nil, err
	}

	items, err := s.getStatsHashPrefixPackets(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.Items = items
	return response, nil
}

func statsHashPrefixBaseSQL() string {
	return `
WITH hop_hashes AS (
  SELECT
    po.packet_hash,
    po.observer_id,
    po.iata,
    po.heard_at,
    po.hash_size,
    h.hop_index::int AS hop_index,
    encode(substring(po.path_bytes from (h.hop_index::int * po.hash_size::int) + 1 for po.hash_size::int), 'hex') AS path_hash
  FROM packet_observations po
  CROSS JOIN LATERAL generate_series(
    0,
    GREATEST(COALESCE((octet_length(po.path_bytes) / NULLIF(po.hash_size::int, 0)) - 1, -1), -1)
  ) AS h(hop_index)
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND po.path_bytes IS NOT NULL
    AND po.hash_size > 0
),
matched AS (
  SELECT *
  FROM hop_hashes
  WHERE path_hash LIKE $4::text || '%'
    AND ($5::smallint = 0 OR hash_size = $5::smallint)
)`
}

func (s *Store) getStatsHashPrefixPackets(ctx context.Context, filter api.StatsHashPrefixFilter, iataFilter string) ([]api.StatsHashPrefixPacket, error) {
	rows, err := s.pool.Query(ctx, statsHashPrefixBaseSQL()+`
SELECT
  encode(m.packet_hash, 'hex') AS packet_hash,
  m.path_hash,
  m.hash_size,
  m.hop_index,
  p.payload_type,
  p.route_type,
  ts.name AS scope_name,
  array_agg(DISTINCT m.iata ORDER BY m.iata) AS iatas,
  COUNT(*)::bigint AS observation_count,
  COUNT(DISTINCT m.observer_id)::bigint AS observer_count,
  (array_agg(m.observer_id ORDER BY m.heard_at DESC))[1] AS latest_observer_id,
  (array_agg(o.display_name ORDER BY m.heard_at DESC))[1] AS latest_observer,
  MIN(m.heard_at) AS first_heard,
  MAX(m.heard_at) AS last_heard
FROM matched m
JOIN packets p ON p.packet_hash = m.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
LEFT JOIN observers o ON o.id = m.observer_id
GROUP BY m.packet_hash, m.path_hash, m.hash_size, m.hop_index, p.payload_type, p.route_type, ts.name
ORDER BY COUNT(*) DESC, COUNT(DISTINCT m.observer_id) DESC, MAX(m.heard_at) DESC
LIMIT $6`, filter.Since, filter.Until, iataFilter, filter.Prefix, filter.HashSize, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsHashPrefixPacket{}
	for rows.Next() {
		var item api.StatsHashPrefixPacket
		var latestObserverID uuid.UUID
		var firstHeard, lastHeard time.Time
		if err := rows.Scan(
			&item.PacketHash,
			&item.PathHash,
			&item.HashSize,
			&item.HopIndex,
			&item.PayloadType,
			&item.RouteType,
			&item.Scope,
			&item.IATAs,
			&item.ObservationCount,
			&item.ObserverCount,
			&latestObserverID,
			&item.LatestObserver,
			&firstHeard,
			&lastHeard,
		); err != nil {
			return nil, err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		item.LatestObserverID = &latestObserverID
		item.FirstHeard = firstHeard.UnixMilli()
		item.LastHeard = lastHeard.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetStatsTopology(ctx context.Context, filter api.StatsFilter) (*api.StatsTopology, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsTopology{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	if err := s.pool.QueryRow(ctx, `
SELECT
  COUNT(*)::bigint,
  COALESCE(SUM(observation_count), 0)::bigint,
  COUNT(DISTINCT iata)::bigint,
  COALESCE(AVG(hop_count), 0)::float8
FROM known_routes
WHERE last_seen >= $1
  AND last_seen <= $2
  AND ($3::text = '' OR iata = ANY(string_to_array($3::text, ',')))`,
		filter.Since, filter.Until, iataFilter).Scan(
		&response.RouteCount,
		&response.ObservationCount,
		&response.ActiveIATAs,
		&response.AverageHopCount,
	); err != nil {
		return nil, err
	}

	hops, err := s.getStatsTopologyHopBuckets(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.HopBuckets = hops

	repeaters, err := s.getStatsTopologyRepeaters(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopRepeaters = repeaters

	pairs, err := s.getStatsTopologyPairs(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopPairs = pairs

	paths, err := s.getStatsTopologyBestPaths(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.BestPaths = paths
	return response, nil
}

func (s *Store) getStatsTopologyHopBuckets(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsTopologyHopBucket, error) {
	rows, err := s.pool.Query(ctx, `
SELECT hop_count, COUNT(*)::bigint, COALESCE(SUM(observation_count), 0)::bigint
FROM known_routes
WHERE last_seen >= $1
  AND last_seen <= $2
  AND ($3::text = '' OR iata = ANY(string_to_array($3::text, ',')))
GROUP BY hop_count
ORDER BY hop_count ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsTopologyHopBucket{}
	for rows.Next() {
		var item api.StatsTopologyHopBucket
		if err := rows.Scan(&item.HopCount, &item.RouteCount, &item.ObservationCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopologyRepeaters(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsTopologyRepeater, error) {
	rows, err := s.pool.Query(ctx, `
WITH route_nodes AS (
  SELECT kr.id, kr.iata, kr.observation_count, kr.last_seen, node_id
  FROM known_routes kr
  CROSS JOIN LATERAL unnest(kr.node_ids) AS u(node_id)
  WHERE kr.last_seen >= $1
    AND kr.last_seen <= $2
    AND ($3::text = '' OR kr.iata = ANY(string_to_array($3::text, ',')))
)
SELECT
  rn.node_id,
  n.name,
  n.node_type,
  array_agg(DISTINCT rn.iata ORDER BY rn.iata),
  COUNT(DISTINCT rn.id)::bigint,
  COALESCE(SUM(rn.observation_count), 0)::bigint,
  MAX(rn.last_seen)
FROM route_nodes rn
JOIN nodes n ON n.id = rn.node_id
GROUP BY rn.node_id, n.name, n.node_type
ORDER BY COALESCE(SUM(rn.observation_count), 0) DESC, COUNT(DISTINCT rn.id) DESC, MAX(rn.last_seen) DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsTopologyRepeater{}
	for rows.Next() {
		var item api.StatsTopologyRepeater
		var lastSeen time.Time
		if err := rows.Scan(&item.NodeID, &item.NodeName, &item.NodeType, &item.IATAs, &item.RouteCount, &item.ObservationCount, &lastSeen); err != nil {
			return nil, err
		}
		item.NodeTypeName = api.NodeTypeName(item.NodeType)
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopologyPairs(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsTopologyPair, error) {
	rows, err := s.pool.Query(ctx, `
WITH route_nodes AS (
  SELECT kr.id, kr.iata, kr.observation_count, kr.last_seen, u.node_id, u.ord
  FROM known_routes kr
  CROSS JOIN LATERAL unnest(kr.node_ids) WITH ORDINALITY AS u(node_id, ord)
  WHERE kr.last_seen >= $1
    AND kr.last_seen <= $2
    AND ($3::text = '' OR kr.iata = ANY(string_to_array($3::text, ',')))
),
pairs AS (
  SELECT a.node_id AS from_node_id, b.node_id AS to_node_id, a.iata, a.id, a.observation_count, a.last_seen
  FROM route_nodes a
  JOIN route_nodes b ON b.id = a.id AND b.ord = a.ord + 1
)
SELECT
  p.from_node_id,
  nf.name,
  p.to_node_id,
  nt.name,
  p.iata,
  COUNT(DISTINCT p.id)::bigint,
  COALESCE(SUM(p.observation_count), 0)::bigint,
  MAX(p.last_seen)
FROM pairs p
JOIN nodes nf ON nf.id = p.from_node_id
JOIN nodes nt ON nt.id = p.to_node_id
GROUP BY p.from_node_id, nf.name, p.to_node_id, nt.name, p.iata
ORDER BY COALESCE(SUM(p.observation_count), 0) DESC, COUNT(DISTINCT p.id) DESC, MAX(p.last_seen) DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsTopologyPair{}
	for rows.Next() {
		var item api.StatsTopologyPair
		var lastSeen time.Time
		if err := rows.Scan(&item.FromNodeID, &item.FromNodeName, &item.ToNodeID, &item.ToNodeName, &item.IATA, &item.RouteCount, &item.ObservationCount, &lastSeen); err != nil {
			return nil, err
		}
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopologyBestPaths(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsTopologyPath, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  kr.id,
  kr.iata,
  kr.hop_count,
  kr.node_ids,
  COALESCE(names.node_names, ARRAY[]::text[]),
  kr.observation_count,
  kr.first_seen,
  kr.last_seen
FROM known_routes kr
LEFT JOIN LATERAL (
  SELECT array_agg(COALESCE(n.name, encode(n.public_key, 'hex')) ORDER BY u.ord) AS node_names
  FROM unnest(kr.node_ids) WITH ORDINALITY AS u(node_id, ord)
  JOIN nodes n ON n.id = u.node_id
) names ON true
WHERE kr.last_seen >= $1
  AND kr.last_seen <= $2
  AND ($3::text = '' OR kr.iata = ANY(string_to_array($3::text, ',')))
ORDER BY kr.observation_count DESC, kr.last_seen DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsTopologyPath{}
	for rows.Next() {
		var item api.StatsTopologyPath
		var firstSeen, lastSeen time.Time
		if err := rows.Scan(
			&item.RouteID,
			&item.IATA,
			&item.HopCount,
			&item.NodeIDs,
			&item.NodeNames,
			&item.ObservationCount,
			&firstSeen,
			&lastSeen,
		); err != nil {
			return nil, err
		}
		item.FirstSeen = firstSeen.UnixMilli()
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetStatsSubpaths(ctx context.Context, filter api.StatsFilter) (*api.StatsSubpaths, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsSubpaths{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	if err := s.pool.QueryRow(ctx, `
WITH routes AS (
  SELECT id, iata, node_ids, observation_count, last_seen
  FROM known_routes
  WHERE last_seen >= $1
    AND last_seen <= $2
    AND array_length(node_ids, 1) >= 2
    AND ($3::text = '' OR iata = ANY(string_to_array($3::text, ',')))
),
subpaths AS (
  SELECT
    r.id,
    r.node_ids[start_ord:end_ord] AS node_ids,
    (end_ord - start_ord + 1)::int AS node_count,
    r.observation_count
  FROM routes r
  CROSS JOIN LATERAL generate_subscripts(r.node_ids, 1) AS s(start_ord)
  CROSS JOIN LATERAL generate_subscripts(r.node_ids, 1) AS e(end_ord)
  WHERE end_ord > start_ord
    AND (end_ord - start_ord + 1) <= $4
)
SELECT
  (SELECT COUNT(*)::bigint FROM routes),
  COUNT(*)::bigint,
  COUNT(DISTINCT subpaths.node_ids::text)::bigint,
  COALESCE(SUM(subpaths.observation_count), 0)::bigint,
  COALESCE(AVG(subpaths.node_count), 0)::float8
FROM subpaths`, filter.Since, filter.Until, iataFilter, statsMaxSubpathNodes).Scan(
		&response.RouteCount,
		&response.SubpathCount,
		&response.UniqueSubpathCount,
		&response.ObservationCount,
		&response.AverageNodeCount,
	); err != nil {
		return nil, err
	}

	lengths, err := s.getStatsSubpathLengthBuckets(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.LengthBuckets = lengths

	subpaths, err := s.getStatsTopSubpaths(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopSubpaths = subpaths

	endpoints, err := s.getStatsSubpathEndpointPairs(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopEndpointPairs = endpoints

	timeline, err := s.getStatsSubpathTimeline(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.Timeline = timeline
	return response, nil
}

func statsSubpathSQLBase() string {
	return `
WITH routes AS (
  SELECT id, iata, node_ids, observation_count, first_seen, last_seen
  FROM known_routes
  WHERE last_seen >= $1
    AND last_seen <= $2
    AND array_length(node_ids, 1) >= 2
    AND ($3::text = '' OR iata = ANY(string_to_array($3::text, ',')))
),
subpaths AS (
  SELECT
    r.id,
    r.iata,
    r.node_ids[start_ord:end_ord] AS node_ids,
    (end_ord - start_ord + 1)::int AS node_count,
    r.observation_count,
    r.first_seen,
    r.last_seen
  FROM routes r
  CROSS JOIN LATERAL generate_subscripts(r.node_ids, 1) AS s(start_ord)
  CROSS JOIN LATERAL generate_subscripts(r.node_ids, 1) AS e(end_ord)
  WHERE end_ord > start_ord
    AND (end_ord - start_ord + 1) <= $4
)`
}

func (s *Store) getStatsSubpathLengthBuckets(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsSubpathLengthBucket, error) {
	rows, err := s.pool.Query(ctx, statsSubpathSQLBase()+`
SELECT
  node_count,
  COUNT(DISTINCT id)::bigint,
  COUNT(*)::bigint,
  COALESCE(SUM(observation_count), 0)::bigint
FROM subpaths
GROUP BY node_count
ORDER BY node_count ASC`, filter.Since, filter.Until, iataFilter, statsMaxSubpathNodes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsSubpathLengthBucket{}
	for rows.Next() {
		var item api.StatsSubpathLengthBucket
		if err := rows.Scan(&item.NodeCount, &item.RouteCount, &item.SubpathCount, &item.ObservationCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsTopSubpaths(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsSubpathRow, error) {
	rows, err := s.pool.Query(ctx, statsSubpathSQLBase()+`
, grouped AS (
  SELECT
    node_ids,
    MAX(node_count)::int AS node_count,
    array_agg(DISTINCT iata ORDER BY iata) AS iatas,
    COUNT(DISTINCT id)::bigint AS route_count,
    COALESCE(SUM(observation_count), 0)::bigint AS observation_count,
    MIN(first_seen) AS first_seen,
    MAX(last_seen) AS last_seen
  FROM subpaths
  GROUP BY node_ids
)
SELECT
  g.node_count,
  g.node_ids,
  COALESCE(names.node_names, ARRAY[]::text[]),
  g.iatas,
  g.route_count,
  g.observation_count,
  g.first_seen,
  g.last_seen
FROM grouped g
LEFT JOIN LATERAL (
  SELECT array_agg(COALESCE(n.name, encode(n.public_key, 'hex')) ORDER BY u.ord) AS node_names
  FROM unnest(g.node_ids) WITH ORDINALITY AS u(node_id, ord)
  JOIN nodes n ON n.id = u.node_id
) names ON true
ORDER BY g.observation_count DESC, g.route_count DESC, g.last_seen DESC
LIMIT $5`, filter.Since, filter.Until, iataFilter, statsMaxSubpathNodes, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsSubpathRow{}
	for rows.Next() {
		var item api.StatsSubpathRow
		var firstSeen, lastSeen time.Time
		if err := rows.Scan(
			&item.NodeCount,
			&item.NodeIDs,
			&item.NodeNames,
			&item.IATAs,
			&item.RouteCount,
			&item.ObservationCount,
			&firstSeen,
			&lastSeen,
		); err != nil {
			return nil, err
		}
		item.FirstSeen = firstSeen.UnixMilli()
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsSubpathEndpointPairs(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsSubpathEndpointPair, error) {
	rows, err := s.pool.Query(ctx, statsSubpathSQLBase()+`
, endpoints AS (
  SELECT
    node_ids[1] AS from_node_id,
    node_ids[node_count] AS to_node_id,
    iata,
    id,
    node_count,
    observation_count,
    last_seen
  FROM subpaths
)
SELECT
  e.from_node_id,
  nf.name,
  e.to_node_id,
  nt.name,
  array_agg(DISTINCT e.iata ORDER BY e.iata),
  MIN(e.node_count)::int,
  MAX(e.node_count)::int,
  COUNT(DISTINCT e.id)::bigint,
  COALESCE(SUM(e.observation_count), 0)::bigint,
  MAX(e.last_seen)
FROM endpoints e
JOIN nodes nf ON nf.id = e.from_node_id
JOIN nodes nt ON nt.id = e.to_node_id
GROUP BY e.from_node_id, nf.name, e.to_node_id, nt.name
ORDER BY COALESCE(SUM(e.observation_count), 0) DESC, COUNT(DISTINCT e.id) DESC, MAX(e.last_seen) DESC
LIMIT $5`, filter.Since, filter.Until, iataFilter, statsMaxSubpathNodes, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsSubpathEndpointPair{}
	for rows.Next() {
		var item api.StatsSubpathEndpointPair
		var lastSeen time.Time
		if err := rows.Scan(
			&item.FromNodeID,
			&item.FromNodeName,
			&item.ToNodeID,
			&item.ToNodeName,
			&item.IATAs,
			&item.MinNodeCount,
			&item.MaxNodeCount,
			&item.RouteCount,
			&item.ObservationCount,
			&lastSeen,
		); err != nil {
			return nil, err
		}
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsSubpathTimeline(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsSubpathTimelinePoint, error) {
	rows, err := s.pool.Query(ctx, statsSubpathSQLBase()+`
SELECT
  to_timestamp(floor(extract(epoch from last_seen) / ($5::double precision * 3600)) * ($5::double precision * 3600)) AS bucket,
  node_count,
  COUNT(DISTINCT id)::bigint,
  COUNT(*)::bigint,
  COALESCE(SUM(observation_count), 0)::bigint
FROM subpaths
GROUP BY bucket, node_count
ORDER BY bucket ASC, node_count ASC`, filter.Since, filter.Until, iataFilter, statsMaxSubpathNodes, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := []api.StatsSubpathTimelinePoint{}
	for rows.Next() {
		var point api.StatsSubpathTimelinePoint
		var bucket time.Time
		if err := rows.Scan(&bucket, &point.NodeCount, &point.RouteCount, &point.SubpathCount, &point.ObservationCount); err != nil {
			return nil, err
		}
		point.T = bucket.UnixMilli()
		points = append(points, point)
	}
	return points, rows.Err()
}

func (s *Store) GetStatsChannels(ctx context.Context, filter api.StatsFilter) (*api.StatsChannels, error) {
	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	response := &api.StatsChannels{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter),
	}

	if err := s.pool.QueryRow(ctx, `
WITH channel_packets AS (
  SELECT p.channel_hash, po.packet_hash, po.observer_id, po.iata, po.heard_at
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE p.channel_hash IS NOT NULL
    AND po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
channel_rows AS (
  SELECT DISTINCT ON (c.channel_hash)
    c.channel_hash,
    COALESCE(c.key_known, false) AS key_known,
    COALESCE(c.is_hashtag, false) AS is_hashtag,
    COALESCE(c.is_public, false) AS is_public
  FROM channels c
  ORDER BY c.channel_hash, COALESCE(c.key_known, false) DESC, COALESCE(c.is_public, false) DESC, c.last_seen DESC
),
active_channels AS (
  SELECT
    cp.channel_hash,
    COALESCE(cr.key_known, false) AS key_known,
    COALESCE(cr.is_hashtag, false) AS is_hashtag,
    COALESCE(cr.is_public, false) AS is_public
  FROM channel_packets cp
  LEFT JOIN channel_rows cr ON cr.channel_hash = cp.channel_hash
  GROUP BY cp.channel_hash, cr.key_known, cr.is_hashtag, cr.is_public
),
message_rows AS (
  SELECT DISTINCT cm.id
  FROM channel_messages cm
  JOIN channel_packets cp ON cp.packet_hash = cm.packet_hash
)
SELECT
  COUNT(*)::bigint,
  COUNT(*) FILTER (WHERE key_known)::bigint,
  COUNT(*) FILTER (WHERE NOT key_known)::bigint,
  COUNT(*) FILTER (WHERE is_hashtag)::bigint,
  COUNT(*) FILTER (WHERE is_public)::bigint,
  (SELECT COUNT(*)::bigint FROM message_rows),
  (SELECT COUNT(DISTINCT packet_hash)::bigint FROM channel_packets),
  (SELECT COUNT(*)::bigint FROM channel_packets),
  (SELECT COUNT(DISTINCT iata)::bigint FROM channel_packets)
FROM active_channels`, filter.Since, filter.Until, iataFilter).Scan(
		&response.TotalChannels,
		&response.KnownChannels,
		&response.UnknownChannels,
		&response.HashtagChannels,
		&response.PublicChannels,
		&response.MessageCount,
		&response.PacketCount,
		&response.ObservationCount,
		&response.ActiveIATAs,
	); err != nil {
		return nil, err
	}

	keyMix, err := s.getStatsChannelKeyMix(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.KeyMix = keyMix

	timeline, err := s.getStatsChannelTimeline(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.Timeline = timeline

	topChannels, err := s.getStatsTopChannels(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopChannels = topChannels

	topSenders, err := s.getStatsChannelTopSenders(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopSenders = topSenders

	topIATAs, err := s.getStatsChannelTopIATAs(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	response.TopIATAs = topIATAs
	return response, nil
}

func channelKeyState(isPublic, isHashtag, keyKnown bool) string {
	switch {
	case isPublic:
		return "public"
	case isHashtag:
		return "hashtag"
	case keyKnown:
		return "known"
	default:
		return "unknown"
	}
}

func (s *Store) getStatsChannelKeyMix(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsChannelKeyBucket, error) {
	rows, err := s.pool.Query(ctx, `
WITH channel_packets AS (
  SELECT p.channel_hash, po.packet_hash, po.heard_at
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE p.channel_hash IS NOT NULL
    AND po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
channel_rows AS (
  SELECT DISTINCT ON (c.channel_hash)
    c.channel_hash,
    COALESCE(c.key_known, false) AS key_known,
    COALESCE(c.is_hashtag, false) AS is_hashtag,
    COALESCE(c.is_public, false) AS is_public
  FROM channels c
  ORDER BY c.channel_hash, COALESCE(c.key_known, false) DESC, COALESCE(c.is_public, false) DESC, c.last_seen DESC
),
bucketed AS (
  SELECT
    CASE WHEN COALESCE(cr.is_public, false) THEN 'public'
         WHEN COALESCE(cr.is_hashtag, false) THEN 'hashtag'
         WHEN COALESCE(cr.key_known, false) THEN 'known'
         ELSE 'unknown' END AS key_state,
    cp.channel_hash,
    cp.packet_hash
  FROM channel_packets cp
  LEFT JOIN channel_rows cr ON cr.channel_hash = cp.channel_hash
),
message_rows AS (
  SELECT DISTINCT
    CASE WHEN COALESCE(cr.is_public, false) THEN 'public'
         WHEN COALESCE(cr.is_hashtag, false) THEN 'hashtag'
         WHEN COALESCE(cr.key_known, false) THEN 'known'
         ELSE 'unknown' END AS key_state,
    cm.id
  FROM channel_messages cm
  JOIN channel_packets cp ON cp.packet_hash = cm.packet_hash
  LEFT JOIN channel_rows cr ON cr.channel_hash = cp.channel_hash
)
SELECT
  b.key_state,
  COUNT(DISTINCT b.channel_hash)::bigint,
  COALESCE((SELECT COUNT(*)::bigint FROM message_rows mr WHERE mr.key_state = b.key_state), 0)::bigint,
  COUNT(DISTINCT b.packet_hash)::bigint,
  COUNT(*)::bigint
FROM bucketed b
GROUP BY b.key_state
ORDER BY COUNT(*) DESC, b.key_state ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsChannelKeyBucket{}
	for rows.Next() {
		var item api.StatsChannelKeyBucket
		if err := rows.Scan(&item.KeyState, &item.ChannelCount, &item.MessageCount, &item.PacketCount, &item.ObservationCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsChannelTimeline(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsChannelTimelinePoint, error) {
	rows, err := s.pool.Query(ctx, `
WITH channel_rows AS (
  SELECT DISTINCT ON (c.channel_hash)
    c.channel_hash,
    COALESCE(c.key_known, false) AS key_known,
    COALESCE(c.is_hashtag, false) AS is_hashtag,
    COALESCE(c.is_public, false) AS is_public
  FROM channels c
  ORDER BY c.channel_hash, COALESCE(c.key_known, false) DESC, COALESCE(c.is_public, false) DESC, c.last_seen DESC
),
base AS (
  SELECT
    to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
    CASE WHEN COALESCE(cr.is_public, false) THEN 'public'
         WHEN COALESCE(cr.is_hashtag, false) THEN 'hashtag'
         WHEN COALESCE(cr.key_known, false) THEN 'known'
         ELSE 'unknown' END AS key_state,
    po.packet_hash,
    cm.id AS message_id
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  LEFT JOIN channel_rows cr ON cr.channel_hash = p.channel_hash
  LEFT JOIN channel_messages cm ON cm.packet_hash = po.packet_hash
  WHERE p.channel_hash IS NOT NULL
    AND po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
)
SELECT
  bucket,
  key_state,
  COUNT(DISTINCT message_id)::bigint,
  COUNT(DISTINCT packet_hash)::bigint,
  COUNT(*)::bigint
FROM base
GROUP BY bucket, key_state
ORDER BY bucket ASC, key_state ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := []api.StatsChannelTimelinePoint{}
	for rows.Next() {
		var point api.StatsChannelTimelinePoint
		var bucket time.Time
		if err := rows.Scan(&bucket, &point.KeyState, &point.MessageCount, &point.PacketCount, &point.ObservationCount); err != nil {
			return nil, err
		}
		point.T = bucket.UnixMilli()
		points = append(points, point)
	}
	return points, rows.Err()
}

func (s *Store) getStatsTopChannels(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsChannelRow, error) {
	rows, err := s.pool.Query(ctx, `
WITH channel_rows AS (
  SELECT DISTINCT ON (c.channel_hash)
    c.channel_hash,
    c.id,
    c.name,
    COALESCE(c.key_known, false) AS key_known,
    COALESCE(c.is_hashtag, false) AS is_hashtag,
    COALESCE(c.is_public, false) AS is_public
  FROM channels c
  ORDER BY c.channel_hash, COALESCE(c.key_known, false) DESC, COALESCE(c.is_public, false) DESC, c.last_seen DESC
),
base AS (
  SELECT
    p.channel_hash,
    po.packet_hash,
    po.observer_id,
    po.iata,
    po.heard_at,
    cm.id AS message_id
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  LEFT JOIN channel_messages cm ON cm.packet_hash = po.packet_hash
  WHERE p.channel_hash IS NOT NULL
    AND po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
latest AS (
  SELECT DISTINCT ON (channel_hash)
    channel_hash,
    iata AS latest_iata,
    heard_at AS last_seen
  FROM base
  ORDER BY channel_hash, heard_at DESC
)
SELECT
  cr.id,
  b.channel_hash,
  cr.name,
  COALESCE(cr.is_public, false),
  COALESCE(cr.is_hashtag, false),
  COALESCE(cr.key_known, false),
  COUNT(DISTINCT b.message_id)::bigint,
  COUNT(DISTINCT b.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT b.iata)::bigint,
  COUNT(DISTINCT b.observer_id)::bigint,
  COALESCE(l.latest_iata, '') AS latest_iata,
  l.last_seen
FROM base b
LEFT JOIN channel_rows cr ON cr.channel_hash = b.channel_hash
LEFT JOIN latest l ON l.channel_hash = b.channel_hash
GROUP BY cr.id, b.channel_hash, cr.name, cr.is_public, cr.is_hashtag, cr.key_known, l.latest_iata, l.last_seen
ORDER BY COUNT(*) DESC, l.last_seen DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsChannelRow{}
	for rows.Next() {
		var item api.StatsChannelRow
		var channelID *int32
		var channelHash []byte
		var isPublic bool
		var lastSeen time.Time
		if err := rows.Scan(
			&channelID,
			&channelHash,
			&item.Name,
			&isPublic,
			&item.IsHashtag,
			&item.KeyKnown,
			&item.MessageCount,
			&item.PacketCount,
			&item.ObservationCount,
			&item.ActiveIATAs,
			&item.ActiveObservers,
			&item.LatestIATA,
			&lastSeen,
		); err != nil {
			return nil, err
		}
		item.ChannelID = channelID
		item.ChannelHash = hex.EncodeToString(channelHash)
		item.KeyState = channelKeyState(isPublic, item.IsHashtag, item.KeyKnown)
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsChannelTopSenders(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsChannelSender, error) {
	rows, err := s.pool.Query(ctx, `
WITH base AS (
  SELECT DISTINCT
    cm.id,
    cm.channel_id,
    cm.packet_hash,
    cm.sender_name,
    cm.sender_pubkey,
    cm.sent_at,
    c.channel_hash,
    c.name AS channel_name
  FROM channel_messages cm
  JOIN channels c ON c.id = cm.channel_id
  JOIN packet_observations po ON po.packet_hash = cm.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
)
SELECT
  COALESCE(NULLIF(b.sender_name, ''), 'UNKNOWN') AS sender_name,
  b.sender_pubkey,
  b.channel_id,
  b.channel_hash,
  b.channel_name,
  COUNT(DISTINCT b.id)::bigint,
  COUNT(po.id)::bigint,
  MIN(b.sent_at),
  MAX(b.sent_at)
FROM base b
JOIN packet_observations po ON po.packet_hash = b.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY COALESCE(NULLIF(b.sender_name, ''), 'UNKNOWN'), b.sender_pubkey, b.channel_id, b.channel_hash, b.channel_name
ORDER BY COUNT(DISTINCT b.id) DESC, COUNT(po.id) DESC, MAX(b.sent_at) DESC
LIMIT $4`, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsChannelSender{}
	for rows.Next() {
		var item api.StatsChannelSender
		var senderPubkey []byte
		var channelHash []byte
		var firstSeen, lastSeen time.Time
		if err := rows.Scan(
			&item.SenderName,
			&senderPubkey,
			&item.ChannelID,
			&channelHash,
			&item.ChannelName,
			&item.MessageCount,
			&item.ObservationCount,
			&firstSeen,
			&lastSeen,
		); err != nil {
			return nil, err
		}
		if len(senderPubkey) > 0 {
			encoded := hex.EncodeToString(senderPubkey)
			item.SenderPubkey = &encoded
		}
		item.ChannelHash = hex.EncodeToString(channelHash)
		item.FirstSeen = firstSeen.UnixMilli()
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) getStatsChannelTopIATAs(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsChannelIATA, error) {
	rows, err := s.pool.Query(ctx, `
WITH base AS (
  SELECT po.iata, p.channel_hash, po.packet_hash, cm.id AS message_id
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  LEFT JOIN channel_messages cm ON cm.packet_hash = po.packet_hash
  WHERE p.channel_hash IS NOT NULL
    AND po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
)
SELECT
  iata,
  COUNT(DISTINCT channel_hash)::bigint,
  COUNT(DISTINCT message_id)::bigint,
  COUNT(DISTINCT packet_hash)::bigint,
  COUNT(*)::bigint
FROM base
GROUP BY iata
ORDER BY COUNT(*) DESC, iata ASC
LIMIT 12`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.StatsChannelIATA{}
	for rows.Next() {
		var item api.StatsChannelIATA
		if err := rows.Scan(&item.IATA, &item.ChannelCount, &item.MessageCount, &item.PacketCount, &item.ObservationCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetStatsObserverHealth(ctx context.Context, filter api.StatsObserverHealthFilter) (*api.StatsObserverHealthResponse, error) {
	filter = normalizeObserverHealthFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	staleCutoff := filter.Until.Add(-filter.StaleAfter)
	response := &api.StatsObserverHealthResponse{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter.StatsFilter),
	}

	rows, err := s.pool.Query(ctx, `
WITH latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata,
    oi.last_heard AS heard_at
  FROM observer_iatas oi
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
),
window_counts AS (
  SELECT po.observer_id, COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id
),
latest_tel AS (
  SELECT DISTINCT ON (ot.observer_id)
    ot.observer_id,
    ot.reported_at,
    ot.battery_voltage_mv,
    ot.airtime_tx_pct,
    ot.airtime_rx_pct,
    ot.noise_floor_db,
    ot.queue_length,
    ot.receive_errors
  FROM observer_telemetry ot
  ORDER BY ot.observer_id, ot.reported_at DESC
)
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE(li.iata, '') AS iata,
  CASE WHEN COALESCE(o.last_status_at, o.last_seen) >= $4 THEN 'online' ELSE 'offline' END AS status,
  COALESCE(li.heard_at, o.last_status_at, o.last_seen) AS last_heard,
  COALESCE(wc.observation_count, 0)::bigint,
  lt.reported_at,
  lt.battery_voltage_mv,
  lt.noise_floor_db,
  lt.airtime_tx_pct,
  lt.airtime_rx_pct,
  lt.queue_length,
  lt.receive_errors
FROM observers o
LEFT JOIN latest_iata li ON li.observer_id = o.id
LEFT JOIN window_counts wc ON wc.observer_id = o.id
LEFT JOIN latest_tel lt ON lt.observer_id = o.id
WHERE ($3::text = '' OR li.iata IS NOT NULL)
ORDER BY COALESCE(wc.observation_count, 0) DESC, COALESCE(li.heard_at, o.last_status_at, o.last_seen) DESC
LIMIT $5`, filter.Since, filter.Until, iataFilter, staleCutoff, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var item api.StatsObserverHealth
		var lastHeard time.Time
		var telemetryAt *time.Time
		if err := rows.Scan(
			&item.ObserverID,
			&item.DisplayName,
			&item.ObserverType,
			&item.IATA,
			&item.Status,
			&lastHeard,
			&item.ObservationCount,
			&telemetryAt,
			&item.BatteryMV,
			&item.NoiseFloorDB,
			&item.AirtimeTxPct,
			&item.AirtimeRxPct,
			&item.QueueLength,
			&item.ReceiveErrors,
		); err != nil {
			return nil, err
		}
		item.LastHeard = lastHeard.UnixMilli()
		if telemetryAt != nil {
			t := telemetryAt.UnixMilli()
			item.TelemetryAt = &t
		}
		classifyObserverHealth(&item, staleCutoff)
		response.Items = append(response.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	response.Summary = summarizeObserverHealth(response.Items)
	return response, nil
}

func (s *Store) GetStatsObserverCompare(ctx context.Context, filter api.StatsObserverCompareFilter) (*api.StatsObserverCompare, error) {
	filter.StatsObserverHealthFilter = normalizeObserverHealthFilter(filter.StatsObserverHealthFilter)
	iataFilter := statsIATAFilter(filter.IATAs)
	observerIDs := observerIDSegment(filter.ObserverIDs)
	response := &api.StatsObserverCompare{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter.StatsFilter),
	}
	if observerIDs == "" {
		return response, nil
	}

	items, err := s.getStatsObserverCompareItems(ctx, filter, iataFilter, observerIDs)
	if err != nil {
		return nil, err
	}
	response.Items = items
	if err := s.fillObserverCompareMix(ctx, filter, iataFilter, observerIDs, response.Items); err != nil {
		return nil, err
	}
	shared, err := s.getObserverCompareSharedIATAs(ctx, filter, iataFilter, observerIDs)
	if err != nil {
		return nil, err
	}
	response.SharedIATAs = shared
	series, err := s.getObserverCompareSeries(ctx, filter, iataFilter, observerIDs)
	if err != nil {
		return nil, err
	}
	response.Series = series
	return response, nil
}

func observerIDSegment(ids []uuid.UUID) string {
	if len(ids) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, id.String())
	}
	return strings.Join(parts, ",")
}

func (s *Store) getStatsObserverCompareItems(ctx context.Context, filter api.StatsObserverCompareFilter, iataFilter, observerIDs string) ([]api.StatsObserverCompareItem, error) {
	staleCutoff := filter.Until.Add(-filter.StaleAfter)
	rows, err := s.pool.Query(ctx, `
WITH selected AS (
  SELECT unnest(string_to_array($5::text, ',')::uuid[]) AS observer_id
),
latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata,
    oi.last_heard AS heard_at
  FROM observer_iatas oi
  JOIN selected s ON s.observer_id = oi.observer_id
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
),
window_counts AS (
  SELECT
    po.observer_id,
    COUNT(DISTINCT po.packet_hash)::bigint AS packet_count,
    COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  JOIN selected s ON s.observer_id = po.observer_id
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id
),
latest_tel AS (
  SELECT DISTINCT ON (ot.observer_id)
    ot.observer_id,
    ot.reported_at,
    ot.battery_voltage_mv,
    ot.airtime_tx_pct,
    ot.airtime_rx_pct,
    ot.noise_floor_db,
    ot.queue_length,
    ot.receive_errors
  FROM observer_telemetry ot
  JOIN selected s ON s.observer_id = ot.observer_id
  ORDER BY ot.observer_id, ot.reported_at DESC
),
window_tel AS (
  SELECT
    ot.observer_id,
    AVG(ot.noise_floor_db)::real AS avg_noise_floor_db,
    AVG(ot.airtime_tx_pct)::real AS avg_airtime_tx_pct,
    AVG(ot.airtime_rx_pct)::real AS avg_airtime_rx_pct,
    AVG(ot.battery_voltage_mv)::int AS avg_battery_mv,
    MAX(ot.queue_length)::int AS max_queue_length,
    SUM(COALESCE(ot.receive_errors, 0))::bigint AS receive_errors_sum
  FROM observer_telemetry ot
  JOIN selected s ON s.observer_id = ot.observer_id
  WHERE ot.reported_at >= $1
    AND ot.reported_at <= $2
  GROUP BY ot.observer_id
)
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE(li.iata, '') AS iata,
  CASE WHEN COALESCE(o.last_status_at, o.last_seen) >= $4 THEN 'online' ELSE 'offline' END AS status,
  COALESCE(li.heard_at, o.last_status_at, o.last_seen) AS last_heard,
  COALESCE(wc.observation_count, 0)::bigint,
  COALESCE(wc.packet_count, 0)::bigint,
  lt.reported_at,
  lt.battery_voltage_mv,
  lt.noise_floor_db,
  lt.airtime_tx_pct,
  lt.airtime_rx_pct,
  lt.queue_length,
  lt.receive_errors,
  wt.avg_noise_floor_db,
  wt.avg_airtime_tx_pct,
  wt.avg_airtime_rx_pct,
  wt.avg_battery_mv,
  wt.max_queue_length,
  COALESCE(wt.receive_errors_sum, 0)::bigint
FROM selected s
JOIN observers o ON o.id = s.observer_id
LEFT JOIN latest_iata li ON li.observer_id = o.id
LEFT JOIN window_counts wc ON wc.observer_id = o.id
LEFT JOIN latest_tel lt ON lt.observer_id = o.id
LEFT JOIN window_tel wt ON wt.observer_id = o.id
WHERE ($3::text = '' OR li.iata IS NOT NULL OR wc.observation_count IS NOT NULL)
ORDER BY array_position(string_to_array($5::text, ',')::uuid[], o.id)`, filter.Since, filter.Until, iataFilter, staleCutoff, observerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []api.StatsObserverCompareItem{}
	for rows.Next() {
		var item api.StatsObserverCompareItem
		var lastHeard time.Time
		var telemetryAt *time.Time
		if err := rows.Scan(
			&item.ObserverID,
			&item.DisplayName,
			&item.ObserverType,
			&item.IATA,
			&item.Status,
			&lastHeard,
			&item.ObservationCount,
			&item.PacketCount,
			&telemetryAt,
			&item.BatteryMV,
			&item.NoiseFloorDB,
			&item.AirtimeTxPct,
			&item.AirtimeRxPct,
			&item.QueueLength,
			&item.ReceiveErrors,
			&item.AvgNoiseFloorDB,
			&item.AvgAirtimeTxPct,
			&item.AvgAirtimeRxPct,
			&item.AvgBatteryMV,
			&item.MaxQueueLength,
			&item.ReceiveErrorsSum,
		); err != nil {
			return nil, err
		}
		item.LastHeard = lastHeard.UnixMilli()
		if telemetryAt != nil {
			t := telemetryAt.UnixMilli()
			item.TelemetryAt = &t
		}
		classifyObserverHealth(&item.StatsObserverHealth, staleCutoff)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) fillObserverCompareMix(ctx context.Context, filter api.StatsObserverCompareFilter, iataFilter, observerIDs string, items []api.StatsObserverCompareItem) error {
	byID := make(map[uuid.UUID]*api.StatsObserverCompareItem, len(items))
	for i := range items {
		byID[items[i].ObserverID] = &items[i]
	}
	payloadRows, err := s.pool.Query(ctx, `
SELECT po.observer_id, p.payload_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.observer_id = ANY(string_to_array($4::text, ',')::uuid[])
  AND po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.observer_id, p.payload_type
ORDER BY po.observer_id, COUNT(*) DESC, p.payload_type ASC`, filter.Since, filter.Until, iataFilter, observerIDs)
	if err != nil {
		return err
	}
	for payloadRows.Next() {
		var observerID uuid.UUID
		var item api.PayloadBreakdownItem
		if err := payloadRows.Scan(&observerID, &item.PayloadType, &item.Count); err != nil {
			payloadRows.Close()
			return err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		if target := byID[observerID]; target != nil {
			target.PayloadMix = append(target.PayloadMix, item)
		}
	}
	payloadRows.Close()
	if err := payloadRows.Err(); err != nil {
		return err
	}

	routeRows, err := s.pool.Query(ctx, `
SELECT po.observer_id, p.route_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.observer_id = ANY(string_to_array($4::text, ',')::uuid[])
  AND po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.observer_id, p.route_type
ORDER BY po.observer_id, COUNT(*) DESC, p.route_type ASC`, filter.Since, filter.Until, iataFilter, observerIDs)
	if err != nil {
		return err
	}
	defer routeRows.Close()
	for routeRows.Next() {
		var observerID uuid.UUID
		var item api.LiveRouteMixItem
		if err := routeRows.Scan(&observerID, &item.RouteType, &item.Count); err != nil {
			return err
		}
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		if target := byID[observerID]; target != nil {
			target.RouteMix = append(target.RouteMix, item)
		}
	}
	return routeRows.Err()
}

func (s *Store) getObserverCompareSharedIATAs(ctx context.Context, filter api.StatsObserverCompareFilter, iataFilter, observerIDs string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
SELECT po.iata
FROM packet_observations po
WHERE po.observer_id = ANY(string_to_array($5::text, ',')::uuid[])
  AND po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.iata
HAVING COUNT(DISTINCT po.observer_id) = $4
ORDER BY po.iata ASC`, filter.Since, filter.Until, iataFilter, int64(len(filter.ObserverIDs)), observerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []string{}
	for rows.Next() {
		var iata string
		if err := rows.Scan(&iata); err != nil {
			return nil, err
		}
		items = append(items, iata)
	}
	return items, rows.Err()
}

func (s *Store) getObserverCompareSeries(ctx context.Context, filter api.StatsObserverCompareFilter, iataFilter, observerIDs string) ([]api.StatsObserverComparePoint, error) {
	rows, err := s.pool.Query(ctx, `
WITH selected AS (
  SELECT unnest(string_to_array($5::text, ',')::uuid[]) AS observer_id
),
eligible AS (
  SELECT observer_id FROM selected s
  WHERE $3::text = '' OR EXISTS (
    SELECT 1 FROM packet_observations po
    WHERE po.observer_id = s.observer_id
      AND po.iata = ANY(string_to_array($3::text, ','))
  )
),
obs AS (
  SELECT
    po.observer_id,
    to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
    COUNT(DISTINCT po.packet_hash)::bigint AS packet_count,
    COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  JOIN eligible e ON e.observer_id = po.observer_id
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id, bucket
),
tel AS (
  SELECT
    ot.observer_id,
    to_timestamp(floor(extract(epoch from ot.reported_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
    AVG(ot.noise_floor_db)::real AS noise_floor_db,
    AVG(ot.airtime_tx_pct)::real AS airtime_tx_pct,
    AVG(ot.airtime_rx_pct)::real AS airtime_rx_pct,
    MAX(ot.queue_length)::int AS queue_length,
    SUM(COALESCE(ot.receive_errors, 0))::bigint AS receive_errors,
    AVG(ot.battery_voltage_mv)::int AS battery_mv
  FROM observer_telemetry ot
  JOIN eligible e ON e.observer_id = ot.observer_id
  WHERE ot.reported_at >= $1
    AND ot.reported_at <= $2
  GROUP BY ot.observer_id, bucket
)
SELECT
  COALESCE(obs.bucket, tel.bucket) AS bucket,
  COALESCE(obs.observer_id, tel.observer_id) AS observer_id,
  COALESCE(obs.packet_count, 0)::bigint,
  COALESCE(obs.observation_count, 0)::bigint,
  tel.noise_floor_db,
  tel.airtime_tx_pct,
  tel.airtime_rx_pct,
  tel.queue_length,
  COALESCE(tel.receive_errors, 0)::bigint,
  tel.battery_mv
FROM obs
FULL OUTER JOIN tel ON tel.observer_id = obs.observer_id AND tel.bucket = obs.bucket
ORDER BY bucket ASC, observer_id ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket), observerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := []api.StatsObserverComparePoint{}
	for rows.Next() {
		var point api.StatsObserverComparePoint
		var bucket time.Time
		if err := rows.Scan(
			&bucket,
			&point.ObserverID,
			&point.PacketCount,
			&point.ObservationCount,
			&point.NoiseFloorDB,
			&point.AirtimeTxPct,
			&point.AirtimeRxPct,
			&point.QueueLength,
			&point.ReceiveErrors,
			&point.BatteryMV,
		); err != nil {
			return nil, err
		}
		point.T = bucket.UnixMilli()
		points = append(points, point)
	}
	return points, rows.Err()
}

func classifyObserverHealth(item *api.StatsObserverHealth, staleCutoff time.Time) {
	lastHeard := time.UnixMilli(item.LastHeard)
	item.Flags.Stale = item.LastHeard == 0 || lastHeard.Before(staleCutoff)
	item.HasTelemetry = hasStatsTelemetry(item)
	item.Flags.NoTelemetry = !item.HasTelemetry
	if item.BatteryMV != nil && *item.BatteryMV > 0 && *item.BatteryMV < 3300 {
		item.Flags.LowBattery = true
	}
	if item.NoiseFloorDB != nil && *item.NoiseFloorDB > -95 {
		item.Flags.HighNoise = true
	}
	if (item.AirtimeTxPct != nil && *item.AirtimeTxPct > 80) || (item.AirtimeRxPct != nil && *item.AirtimeRxPct > 80) {
		item.Flags.HighAirtime = true
	}
	if item.QueueLength != nil && *item.QueueLength > 10 {
		item.Flags.QueueBacklog = true
	}
	if item.ReceiveErrors != nil && *item.ReceiveErrors > 0 {
		item.Flags.ReceiveErrors = true
	}

	score := 100
	if item.Flags.Stale {
		score -= 35
	}
	if item.Flags.NoTelemetry {
		score -= 10
	}
	if item.Flags.LowBattery {
		score -= 15
	}
	if item.Flags.HighNoise {
		score -= 15
	}
	if item.Flags.HighAirtime {
		score -= 15
	}
	if item.Flags.QueueBacklog {
		score -= 10
	}
	if item.Flags.ReceiveErrors {
		score -= 10
	}
	if score < 0 {
		score = 0
	}
	item.HealthScore = score
}

func hasStatsTelemetry(item *api.StatsObserverHealth) bool {
	return (item.TelemetryAt != nil) && ((item.BatteryMV != nil && *item.BatteryMV != 0) ||
		(item.NoiseFloorDB != nil && *item.NoiseFloorDB != 0) ||
		(item.AirtimeTxPct != nil && *item.AirtimeTxPct != 0) ||
		(item.AirtimeRxPct != nil && *item.AirtimeRxPct != 0) ||
		(item.QueueLength != nil && *item.QueueLength != 0) ||
		(item.ReceiveErrors != nil && *item.ReceiveErrors != 0))
}

func summarizeObserverHealth(items []api.StatsObserverHealth) api.StatsHealthSummary {
	summary := api.StatsHealthSummary{TotalObservers: int64(len(items))}
	for _, item := range items {
		if item.Flags.Stale {
			summary.StaleObservers++
		}
		if item.Flags.LowBattery {
			summary.LowBattery++
		}
		if item.Flags.HighNoise {
			summary.HighNoise++
		}
		if item.Flags.HighAirtime {
			summary.HighAirtime++
		}
		if item.Flags.QueueBacklog {
			summary.QueueBacklog++
		}
		if item.Flags.ReceiveErrors {
			summary.ReceiveErrors++
		}
		if item.Flags.NoTelemetry {
			summary.NoTelemetry++
		}
	}
	return summary
}

func (s *Store) GetStatsRFHealth(ctx context.Context, filter api.StatsObserverHealthFilter) (*api.StatsRFHealth, error) {
	filter = normalizeObserverHealthFilter(filter)
	allHealth, err := s.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
		StatsFilter: api.StatsFilter{
			IATAs:  filter.IATAs,
			Since:  filter.Since,
			Until:  filter.Until,
			Bucket: filter.Bucket,
			Limit:  500,
		},
		StaleAfter: filter.StaleAfter,
	})
	if err != nil {
		return nil, err
	}
	response := &api.StatsRFHealth{
		ServerTime: time.Now().UnixMilli(),
		Window:     statsWindow(filter.StatsFilter),
		Summary:    allHealth.Summary,
	}
	response.ByIATA = summarizeRFByIATA(allHealth.Items)

	offenders := append([]api.StatsObserverHealth(nil), allHealth.Items...)
	sort.SliceStable(offenders, func(i, j int) bool {
		if offenders[i].HealthScore == offenders[j].HealthScore {
			return offenders[i].ObservationCount > offenders[j].ObservationCount
		}
		return offenders[i].HealthScore < offenders[j].HealthScore
	})
	if len(offenders) > 10 {
		offenders = offenders[:10]
	}
	response.TopOffenders = offenders

	series, err := s.getStatsRFSeries(ctx, filter)
	if err != nil {
		return nil, err
	}
	response.Series = series
	return response, nil
}

type rfIATAAccumulator struct {
	item       api.StatsRFHealthIATA
	noiseSum   float64
	noiseCount int64
	scoreSum   int64
}

func summarizeRFByIATA(items []api.StatsObserverHealth) []api.StatsRFHealthIATA {
	byIATA := map[string]*rfIATAAccumulator{}
	for _, observer := range items {
		iata := observer.IATA
		if iata == "" {
			iata = "UNKNOWN"
		}
		acc := byIATA[iata]
		if acc == nil {
			acc = &rfIATAAccumulator{item: api.StatsRFHealthIATA{IATA: iata}}
			byIATA[iata] = acc
		}
		acc.item.ActiveObservers++
		acc.scoreSum += int64(observer.HealthScore)
		if observer.Flags.Stale {
			acc.item.StaleObservers++
		}
		if observer.Flags.LowBattery {
			acc.item.LowBattery++
		}
		if observer.Flags.ReceiveErrors && observer.ReceiveErrors != nil {
			acc.item.ReceiveErrors += int64(*observer.ReceiveErrors)
		}
		if observer.NoiseFloorDB != nil {
			acc.noiseSum += float64(*observer.NoiseFloorDB)
			acc.noiseCount++
		}
		maxAirtime := observer.AirtimeTxPct
		if observer.AirtimeRxPct != nil && (maxAirtime == nil || *observer.AirtimeRxPct > *maxAirtime) {
			maxAirtime = observer.AirtimeRxPct
		}
		if maxAirtime != nil && (acc.item.MaxAirtimePct == nil || *maxAirtime > *acc.item.MaxAirtimePct) {
			v := *maxAirtime
			acc.item.MaxAirtimePct = &v
		}
		if observer.QueueLength != nil && (acc.item.MaxQueueLength == nil || *observer.QueueLength > *acc.item.MaxQueueLength) {
			v := *observer.QueueLength
			acc.item.MaxQueueLength = &v
		}
	}
	out := make([]api.StatsRFHealthIATA, 0, len(byIATA))
	for _, acc := range byIATA {
		if acc.noiseCount > 0 {
			v := float32(acc.noiseSum / float64(acc.noiseCount))
			acc.item.AvgNoiseFloorDB = &v
		}
		if acc.item.ActiveObservers > 0 {
			acc.item.HealthScore = int(acc.scoreSum / acc.item.ActiveObservers)
		}
		out = append(out, acc.item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].HealthScore == out[j].HealthScore {
			return out[i].ActiveObservers > out[j].ActiveObservers
		}
		return out[i].HealthScore < out[j].HealthScore
	})
	return out
}

func (s *Store) getStatsRFSeries(ctx context.Context, filter api.StatsObserverHealthFilter) ([]api.StatsRFHealthPoint, error) {
	iataFilter := statsIATAFilter(filter.IATAs)
	rows, err := s.pool.Query(ctx, `
WITH latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata
  FROM observer_iatas oi
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
)
SELECT
  to_timestamp(floor(extract(epoch from ot.reported_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
  COALESCE(li.iata, 'UNKNOWN') AS iata,
  AVG(ot.noise_floor_db)::real,
  AVG(ot.airtime_tx_pct)::real,
  AVG(ot.airtime_rx_pct)::real,
  MAX(ot.queue_length)::int,
  SUM(COALESCE(ot.receive_errors, 0))::bigint,
  AVG(ot.battery_voltage_mv)::int
FROM observer_telemetry ot
LEFT JOIN latest_iata li ON li.observer_id = ot.observer_id
WHERE ot.reported_at >= $1
  AND ot.reported_at <= $2
  AND ($3::text = '' OR li.iata IS NOT NULL)
GROUP BY bucket, COALESCE(li.iata, 'UNKNOWN')
ORDER BY bucket ASC, iata ASC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := make([]api.StatsRFHealthPoint, 0)
	for rows.Next() {
		var point api.StatsRFHealthPoint
		var bucket time.Time
		if err := rows.Scan(
			&bucket,
			&point.IATA,
			&point.NoiseFloorDB,
			&point.AirtimeTxPct,
			&point.AirtimeRxPct,
			&point.QueueLength,
			&point.ReceiveErrors,
			&point.BatteryMV,
		); err != nil {
			return nil, err
		}
		point.T = bucket.UnixMilli()
		points = append(points, point)
	}
	return points, rows.Err()
}
