// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
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

	overview, err := s.getStatsOverviewWindow(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	summary.Overview = overview

	liveSince := filter.Until.Add(-15 * time.Minute)
	if liveSince.Before(filter.Since) {
		liveSince = filter.Since
	}
	live, err := s.GetLiveSummary(ctx, api.LiveSummaryFilter{IATAs: filter.IATAs, Since: liveSince, Until: filter.Until})
	if err != nil {
		return nil, err
	}
	summary.Live = *live

	nodeTypes, err := s.GetStatsNodeTypes(ctx, filter.IATAs)
	if err != nil {
		return nil, err
	}
	summary.NodeTypes = nodeTypes

	payloads, err := s.GetStatsPayloads(ctx, filter)
	if err != nil {
		return nil, err
	}
	summary.PayloadMix = payloads.Totals
	summary.RouteMix = payloads.RouteTotals

	topIATAs, err := s.getStatsTopIATAs(ctx, filter, iataFilter)
	if err != nil {
		return nil, err
	}
	summary.TopIATAs = topIATAs

	topObservers, err := s.getStatsTopObserversWindow(ctx, filter, iataFilter, 10)
	if err != nil {
		return nil, err
	}
	summary.TopObservers = topObservers

	topNodes, err := s.getStatsTopNodesWindow(ctx, filter, iataFilter, 10)
	if err != nil {
		return nil, err
	}
	summary.TopNodes = topNodes

	presets, err := s.GetRadioPresets(ctx, "", filter.IATAs)
	if err != nil {
		return nil, err
	}
	summary.RadioPresets = presets

	scopes, err := s.GetScopeStats(ctx)
	if err != nil {
		return nil, err
	}
	summary.Scopes = scopes

	health, err := s.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
		StatsFilter: api.StatsFilter{
			IATAs:  filter.IATAs,
			Since:  filter.Since,
			Until:  filter.Until,
			Bucket: filter.Bucket,
			Limit:  500,
		},
		StaleAfter: statsDefaultStaleAfter,
	})
	if err != nil {
		return nil, err
	}
	summary.Health = health.Summary

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
SELECT
  po.observer_id,
  o.display_name,
  o.observer_type,
  (array_agg(po.iata ORDER BY po.heard_at DESC))[1] AS latest_iata,
  COUNT(*)::bigint
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.observer_id, o.display_name, o.observer_type
ORDER BY COUNT(*) DESC, latest_iata ASC
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
SELECT
  n.id,
  n.name,
  n.node_type,
  po.iata,
  COUNT(*)::bigint,
  MAX(po.heard_at)
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY n.id, n.name, n.node_type, po.iata
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
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

	payloadRows, err := s.pool.Query(ctx, `
SELECT p.payload_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY p.payload_type
ORDER BY COUNT(*) DESC, p.payload_type ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	for payloadRows.Next() {
		var item api.PayloadBreakdownItem
		if err := payloadRows.Scan(&item.PayloadType, &item.Count); err != nil {
			payloadRows.Close()
			return nil, err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		response.Totals = append(response.Totals, item)
	}
	payloadRows.Close()
	if err := payloadRows.Err(); err != nil {
		return nil, err
	}

	routeRows, err := s.pool.Query(ctx, `
SELECT p.route_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY p.route_type
ORDER BY COUNT(*) DESC, p.route_type ASC`, filter.Since, filter.Until, iataFilter)
	if err != nil {
		return nil, err
	}
	for routeRows.Next() {
		var item api.LiveRouteMixItem
		if err := routeRows.Scan(&item.RouteType, &item.Count); err != nil {
			routeRows.Close()
			return nil, err
		}
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		response.RouteTotals = append(response.RouteTotals, item)
	}
	routeRows.Close()
	if err := routeRows.Err(); err != nil {
		return nil, err
	}

	payloadTimeline, err := s.pool.Query(ctx, `
SELECT
  to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
  p.payload_type,
  COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY bucket, p.payload_type
ORDER BY bucket ASC, COUNT(*) DESC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	for payloadTimeline.Next() {
		var item api.StatsPayloadBucket
		var bucket time.Time
		if err := payloadTimeline.Scan(&bucket, &item.PayloadType, &item.Count); err != nil {
			payloadTimeline.Close()
			return nil, err
		}
		item.T = bucket.UnixMilli()
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		response.PayloadTimeline = append(response.PayloadTimeline, item)
	}
	payloadTimeline.Close()
	if err := payloadTimeline.Err(); err != nil {
		return nil, err
	}

	routeTimeline, err := s.pool.Query(ctx, `
SELECT
  to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket,
  p.route_type,
  COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY bucket, p.route_type
ORDER BY bucket ASC, COUNT(*) DESC`, filter.Since, filter.Until, iataFilter, bucketHours(filter.Bucket))
	if err != nil {
		return nil, err
	}
	for routeTimeline.Next() {
		var item api.StatsRouteBucket
		var bucket time.Time
		if err := routeTimeline.Scan(&bucket, &item.RouteType, &item.Count); err != nil {
			routeTimeline.Close()
			return nil, err
		}
		item.T = bucket.UnixMilli()
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		response.RouteTimeline = append(response.RouteTimeline, item)
	}
	routeTimeline.Close()
	if err := routeTimeline.Err(); err != nil {
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
risky AS (
  SELECT 1
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND po.path_bytes IS NOT NULL
    AND po.hash_size > 0
  GROUP BY encode(substring(po.path_bytes from 1 for LEAST(po.hash_size::int, 2)), 'hex'), po.hash_size, po.iata
  HAVING COUNT(DISTINCT po.packet_hash) > 1
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

func (s *Store) getStatsHashRiskyPrefixes(ctx context.Context, filter api.StatsFilter, iataFilter string) ([]api.StatsHashCollisionPrefix, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  encode(substring(po.path_bytes from 1 for LEAST(po.hash_size::int, 2)), 'hex') AS prefix,
  po.hash_size,
  po.iata,
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint,
  MIN(po.heard_at),
  MAX(po.heard_at)
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  AND po.path_bytes IS NOT NULL
  AND po.hash_size > 0
GROUP BY prefix, po.hash_size, po.iata
HAVING COUNT(DISTINCT po.packet_hash) > 1
ORDER BY COUNT(DISTINCT po.packet_hash) DESC, COUNT(*) DESC, MAX(po.heard_at) DESC
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
  SELECT DISTINCT ON (po.observer_id)
    po.observer_id,
    po.iata,
    po.heard_at
  FROM packet_observations po
  WHERE ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY po.observer_id, po.heard_at DESC
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
  SELECT DISTINCT ON (po.observer_id)
    po.observer_id,
    po.iata,
    po.heard_at
  FROM packet_observations po
  JOIN selected s ON s.observer_id = po.observer_id
  WHERE ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY po.observer_id, po.heard_at DESC
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
  SELECT DISTINCT ON (po.observer_id)
    po.observer_id,
    po.iata
  FROM packet_observations po
  WHERE ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY po.observer_id, po.heard_at DESC
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
