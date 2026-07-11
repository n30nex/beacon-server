// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

const (
	atlasAggregateRetention = 7 * 24 * time.Hour
	atlasAggregateBatchSize = 168
)

type atlasAggregateBounds struct {
	fullStart time.Time
	fullEnd   time.Time
}

func atlasBounds(since, until time.Time) atlasAggregateBounds {
	return atlasAggregateBounds{
		fullStart: since.Truncate(time.Hour),
		fullEnd:   until.Truncate(time.Hour).Add(time.Hour),
	}
}

func canonicalAtlasWindow(since, until, now time.Time) bool {
	duration := until.Sub(since)
	if math.Abs(float64(duration-atlasDefaultWindow)) > float64(time.Minute) {
		return false
	}
	age := now.Sub(until)
	return age >= -time.Minute && age <= 10*time.Minute
}

// atlasAggregatesAvailable verifies every complete hour needed by the current
// window and, when requested, its previous comparison window. Canonical
// briefing counts intentionally use overlapping hourly bucket semantics;
// custom windows retain exact raw-query semantics.
func (s *Store) atlasAggregatesAvailable(ctx context.Context, since, until time.Time, includePrevious bool) bool {
	if !canonicalAtlasWindow(since, until, time.Now()) {
		return false
	}
	earliest := since
	if includePrevious {
		earliest = since.Add(-until.Sub(since))
	}
	bounds := atlasBounds(earliest, until)
	if !bounds.fullEnd.After(bounds.fullStart) {
		return false
	}
	expected := int64(bounds.fullEnd.Sub(bounds.fullStart) / time.Hour)
	var actual int64
	if err := s.pool.QueryRow(ctx, `
SELECT COUNT(*)::bigint
FROM atlas_aggregate_hours
WHERE hour >= $1 AND hour < $2`, bounds.fullStart, bounds.fullEnd).Scan(&actual); err != nil {
		return false
	}
	return actual == expected
}

// RefreshAtlasHourlyAggregates refreshes the current/prior hour and backfills
// missing hours over seven days. Each hour commits independently, bounding
// locks, WAL, and retry work even during the one-time backfill.
func (s *Store) RefreshAtlasHourlyAggregates(ctx context.Context, now time.Time) (int64, error) {
	current := now.Truncate(time.Hour)
	hours := []time.Time{current, current.Add(-time.Hour)}
	seen := map[time.Time]struct{}{current: {}, current.Add(-time.Hour): {}}

	rows, err := s.pool.Query(ctx, `
SELECT candidate.hour
FROM generate_series($1::timestamptz, $2::timestamptz, interval '1 hour') AS candidate(hour)
LEFT JOIN atlas_aggregate_hours done ON done.hour = candidate.hour
WHERE done.hour IS NULL
ORDER BY candidate.hour DESC
LIMIT $3`, current.Add(-atlasAggregateRetention), current, atlasAggregateBatchSize)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var hour time.Time
		if err := rows.Scan(&hour); err != nil {
			rows.Close()
			return 0, err
		}
		hour = hour.Truncate(time.Hour)
		if _, ok := seen[hour]; ok {
			continue
		}
		seen[hour] = struct{}{}
		hours = append(hours, hour)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	var affected int64
	for _, hour := range hours {
		count, err := s.refreshAtlasAggregateHour(ctx, hour)
		if err != nil {
			return affected, err
		}
		affected += count
	}

	cutoff := current.Add(-atlasAggregateRetention)
	for _, table := range []string{
		"atlas_hourly_iata_aggregates",
		"atlas_hourly_mix_aggregates",
		"atlas_hourly_node_aggregates",
		"atlas_hourly_observer_aggregates",
		"atlas_aggregate_hours",
	} {
		tag, err := s.pool.Exec(ctx, "DELETE FROM "+table+" WHERE hour < $1", cutoff)
		if err != nil {
			return affected, err
		}
		affected += tag.RowsAffected()
	}
	return affected, nil
}

func (s *Store) refreshAtlasAggregateHour(ctx context.Context, hour time.Time) (affected int64, err error) {
	hour = hour.Truncate(time.Hour)
	until := hour.Add(time.Hour)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, table := range []string{
		"atlas_hourly_iata_aggregates",
		"atlas_hourly_mix_aggregates",
		"atlas_hourly_node_aggregates",
		"atlas_hourly_observer_aggregates",
	} {
		tag, execErr := tx.Exec(ctx, "DELETE FROM "+table+" WHERE hour = $1", hour)
		if execErr != nil {
			return affected, execErr
		}
		affected += tag.RowsAffected()
	}

	statements := []string{
		`INSERT INTO atlas_hourly_iata_aggregates (hour, iata, observation_count, packet_hashes, observer_ids, unique_packet_count, active_observer_count)
SELECT $1, po.iata, COUNT(*)::bigint,
       array_agg(DISTINCT po.packet_hash ORDER BY po.packet_hash),
       array_agg(DISTINCT po.observer_id ORDER BY po.observer_id),
       COUNT(DISTINCT po.packet_hash)::bigint,
       COUNT(DISTINCT po.observer_id)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1 AND po.heard_at < $2
GROUP BY po.iata`,
		`INSERT INTO atlas_hourly_mix_aggregates (hour, iata, payload_type, route_type, observation_count)
SELECT $1, po.iata, p.payload_type, p.route_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1 AND po.heard_at < $2
GROUP BY po.iata, p.payload_type, p.route_type`,
		`INSERT INTO atlas_hourly_node_aggregates (hour, iata, origin_pubkey, observation_count, last_heard)
SELECT $1, po.iata, p.origin_pubkey, COUNT(*)::bigint, MAX(po.heard_at)
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1 AND po.heard_at < $2 AND p.origin_pubkey IS NOT NULL
GROUP BY po.iata, p.origin_pubkey`,
		`INSERT INTO atlas_hourly_observer_aggregates (hour, iata, observer_id, observation_count, last_heard)
SELECT $1, po.iata, po.observer_id, COUNT(*)::bigint, MAX(po.heard_at)
FROM packet_observations po
WHERE po.heard_at >= $1 AND po.heard_at < $2
GROUP BY po.iata, po.observer_id`,
	}
	for _, statement := range statements {
		tag, execErr := tx.Exec(ctx, statement, hour, until)
		if execErr != nil {
			return affected, execErr
		}
		affected += tag.RowsAffected()
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO atlas_aggregate_hours (hour, refreshed_at) VALUES ($1, NOW())
ON CONFLICT (hour) DO UPDATE SET refreshed_at = EXCLUDED.refreshed_at`, hour); err != nil {
		return affected, err
	}
	if err = tx.Commit(ctx); err != nil {
		return affected, err
	}
	return affected, nil
}

func (s *Store) getAtlasIATAsAggregated(ctx context.Context, since, until time.Time, iatas string) ([]api.AtlasIATA, error) {
	bounds := atlasBounds(since, until)
	rows, err := s.pool.Query(ctx, `
WITH counts AS (
  SELECT a.iata,
         SUM(a.observation_count)::bigint AS observation_count,
         SUM(a.unique_packet_count)::bigint AS unique_packets,
         SUM(a.active_observer_count)::bigint AS active_observers
  FROM atlas_hourly_iata_aggregates a
  WHERE a.hour >= $2 AND a.hour < $3
    AND ($1::text = '' OR a.iata = ANY(string_to_array($1::text, ',')))
  GROUP BY a.iata
)
SELECT i.iata, i.display_name, i.approx_lat, i.approx_lng,
       COALESCE(c.observation_count, 0)::bigint,
       COALESCE(c.unique_packets, 0)::bigint,
       COALESCE(c.active_observers, 0)::bigint
FROM iata_codes i
LEFT JOIN counts c ON c.iata = i.iata
WHERE ($1::text = '' OR i.iata = ANY(string_to_array($1::text, ',')))
ORDER BY COALESCE(c.observation_count, 0) DESC, i.iata`, iatas, bounds.fullStart, bounds.fullEnd)
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

func (s *Store) getAtlasPayloadAndRouteMixAggregated(ctx context.Context, since, until time.Time, iatas string) ([]api.PayloadBreakdownItem, []api.LiveRouteMixItem, error) {
	bounds := atlasBounds(since, until)
	rows, err := s.pool.Query(ctx, `
WITH base AS MATERIALIZED (
  SELECT a.payload_type, a.route_type, SUM(a.observation_count)::bigint AS count
  FROM atlas_hourly_mix_aggregates a
  WHERE a.hour >= $2 AND a.hour < $3
    AND ($1::text = '' OR a.iata = ANY(string_to_array($1::text, ',')))
  GROUP BY a.payload_type, a.route_type
), payload_mix AS (
  SELECT payload_type AS code, SUM(count)::bigint AS count FROM base GROUP BY payload_type
), route_mix AS (
  SELECT route_type AS code, SUM(count)::bigint AS count FROM base GROUP BY route_type
  ORDER BY count DESC, route_type LIMIT 8
)
SELECT 'payload', code, count FROM payload_mix
UNION ALL
SELECT 'route', code, count FROM route_mix
ORDER BY 1, 3 DESC, 2`, iatas, bounds.fullStart, bounds.fullEnd)
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
		if kind == "payload" {
			payload = append(payload, api.PayloadBreakdownItem{PayloadType: code, PayloadTypeName: api.PayloadTypeName(code), Count: count})
		} else {
			routes = append(routes, api.LiveRouteMixItem{RouteType: code, RouteTypeName: api.RouteTypeName(code), Count: count})
		}
	}
	return payload, routes, rows.Err()
}

func (s *Store) getAtlasTopNodesAggregated(ctx context.Context, since, until time.Time, iatas string, limit int32) ([]api.TopNode, error) {
	bounds := atlasBounds(since, until)
	rows, err := s.pool.Query(ctx, `
WITH source AS (
  SELECT a.iata, a.origin_pubkey, a.observation_count, a.last_heard
  FROM atlas_hourly_node_aggregates a
  WHERE a.hour >= $2 AND a.hour < $3
    AND ($1::text = '' OR a.iata = ANY(string_to_array($1::text, ',')))
), combined AS (
  SELECT origin_pubkey, MIN(iata)::text AS iata,
         SUM(observation_count)::bigint AS observation_count, MAX(last_heard) AS last_heard
  FROM source GROUP BY origin_pubkey
)
SELECT n.id, n.name, n.node_type, c.iata, c.observation_count, c.last_heard
FROM combined c JOIN nodes n ON n.public_key = c.origin_pubkey
ORDER BY c.observation_count DESC, c.last_heard DESC
LIMIT $4`, iatas, bounds.fullStart, bounds.fullEnd, limit)
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

func (s *Store) getAtlasTopObserversAggregated(ctx context.Context, since, until time.Time, iatas string, limit int32) ([]api.TopObserver, error) {
	bounds := atlasBounds(since, until)
	rows, err := s.pool.Query(ctx, `
WITH source AS (
  SELECT a.iata, a.observer_id, a.observation_count
  FROM atlas_hourly_observer_aggregates a
  WHERE a.hour >= $2 AND a.hour < $3
    AND ($1::text = '' OR a.iata = ANY(string_to_array($1::text, ',')))
), combined AS (
  SELECT observer_id, MIN(iata)::text AS iata, SUM(observation_count)::bigint AS observation_count
  FROM source GROUP BY observer_id
)
SELECT o.id, o.display_name, o.observer_type, c.iata, c.observation_count
FROM combined c JOIN observers o ON o.id = c.observer_id
ORDER BY c.observation_count DESC
LIMIT $4`, iatas, bounds.fullStart, bounds.fullEnd, limit)
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

func (s *Store) getAtlasActiveNodesByIATAAggregated(ctx context.Context, since, until time.Time) (map[string]int64, error) {
	bounds := atlasBounds(since, until)
	rows, err := s.pool.Query(ctx, `
WITH active AS (
  SELECT a.iata, a.origin_pubkey
  FROM atlas_hourly_node_aggregates a
  WHERE a.hour >= $1 AND a.hour < $2
  GROUP BY a.iata, a.origin_pubkey
)
SELECT active.iata, COUNT(*)::bigint
FROM active JOIN nodes n ON n.public_key = active.origin_pubkey
GROUP BY active.iata`, bounds.fullStart, bounds.fullEnd)
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
		result[strings.TrimSpace(iata)] = count
	}
	return result, rows.Err()
}
