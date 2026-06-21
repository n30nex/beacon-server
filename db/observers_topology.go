// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) GetObserverTopology(ctx context.Context, observerID uuid.UUID, filter api.StatsFilter) (*api.ObserverTopologySummary, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM observers WHERE id = $1)`, observerID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	filter = normalizeStatsFilter(filter)
	iataFilter := statsIATAFilter(filter.IATAs)
	out := &api.ObserverTopologySummary{
		ServerTime:    time.Now().UnixMilli(),
		Window:        statsWindow(filter),
		ObserverID:    observerID,
		PayloadMix:    []api.PayloadBreakdownItem{},
		RouteMix:      []api.LiveRouteMixItem{},
		TopNodes:      []api.ObserverTopologyNode{},
		TopTraceTags:  []api.ObserverTopologyTraceTag{},
		TopScopes:     []api.ObserverTopologyScope{},
		RecentAdverts: []api.AdvertObservation{},
	}

	var avgSNR pgtype.Float8
	if err := s.pool.QueryRow(ctx, `
SELECT
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.iata)::bigint,
  AVG(po.snr)::float8
FROM packet_observations po
WHERE po.observer_id = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))`,
		observerID, filter.Since, filter.Until, iataFilter,
	).Scan(&out.PacketCount, &out.ObservationCount, &out.ActiveIATAs, &avgSNR); err != nil {
		return nil, err
	}
	if avgSNR.Valid {
		out.AvgSNR = &avgSNR.Float64
	}

	if err := s.fillObserverTopologyMix(ctx, out, filter, iataFilter); err != nil {
		return nil, err
	}
	if err := s.fillObserverTopologyNodes(ctx, out, filter, iataFilter); err != nil {
		return nil, err
	}
	if err := s.fillObserverTopologyTraceTags(ctx, out, filter, iataFilter); err != nil {
		return nil, err
	}
	if err := s.fillObserverTopologyScopes(ctx, out, filter, iataFilter); err != nil {
		return nil, err
	}
	if err := s.fillObserverTopologyAdverts(ctx, out, filter, iataFilter); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) fillObserverTopologyMix(ctx context.Context, out *api.ObserverTopologySummary, filter api.StatsFilter, iataFilter string) error {
	payloadRows, err := s.pool.Query(ctx, `
SELECT p.payload_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.observer_id = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
GROUP BY p.payload_type
ORDER BY COUNT(*) DESC, p.payload_type ASC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return err
	}
	defer payloadRows.Close()
	for payloadRows.Next() {
		var item api.PayloadBreakdownItem
		if err := payloadRows.Scan(&item.PayloadType, &item.Count); err != nil {
			return err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		out.PayloadMix = append(out.PayloadMix, item)
	}
	if err := payloadRows.Err(); err != nil {
		return err
	}

	routeRows, err := s.pool.Query(ctx, `
SELECT p.route_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.observer_id = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
GROUP BY p.route_type
ORDER BY COUNT(*) DESC, p.route_type ASC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return err
	}
	defer routeRows.Close()
	for routeRows.Next() {
		var item api.LiveRouteMixItem
		if err := routeRows.Scan(&item.RouteType, &item.Count); err != nil {
			return err
		}
		item.RouteTypeName = api.RouteTypeName(item.RouteType)
		out.RouteMix = append(out.RouteMix, item)
	}
	return routeRows.Err()
}

func (s *Store) fillObserverTopologyNodes(ctx context.Context, out *api.ObserverTopologySummary, filter api.StatsFilter, iataFilter string) error {
	rows, err := s.pool.Query(ctx, `
SELECT
  n.id,
  n.name,
  encode(n.public_key, 'hex'),
  ARRAY_AGG(DISTINCT po.iata::text ORDER BY po.iata::text),
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  MAX(po.heard_at),
  AVG(po.snr)::float8
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
GROUP BY n.id, n.name, n.public_key
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item api.ObserverTopologyNode
		var last pgtype.Timestamptz
		var avgSNR pgtype.Float8
		if err := rows.Scan(
			&item.ID,
			&item.Name,
			&item.PublicKey,
			&item.IATAs,
			&item.PacketCount,
			&item.ObservationCount,
			&last,
			&avgSNR,
		); err != nil {
			return err
		}
		if last.Valid {
			item.LastHeard = last.Time.UnixMilli()
		}
		if avgSNR.Valid {
			item.AvgSNR = &avgSNR.Float64
		}
		out.TopNodes = append(out.TopNodes, item)
	}
	return rows.Err()
}

func (s *Store) fillObserverTopologyTraceTags(ctx context.Context, out *api.ObserverTopologySummary, filter api.StatsFilter, iataFilter string) error {
	rows, err := s.pool.Query(ctx, `
SELECT
  encode(p.trace_tag, 'hex'),
  COALESCE(NULLIF(p.parsed_payload->>'type', ''), 'TRACE'),
  ARRAY_AGG(DISTINCT po.iata::text ORDER BY po.iata::text),
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  MAX(po.heard_at)
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.observer_id = $1
  AND p.trace_tag IS NOT NULL
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
GROUP BY p.trace_tag, COALESCE(NULLIF(p.parsed_payload->>'type', ''), 'TRACE')
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item api.ObserverTopologyTraceTag
		var last pgtype.Timestamptz
		if err := rows.Scan(
			&item.TraceTag,
			&item.TraceType,
			&item.IATAs,
			&item.PacketCount,
			&item.ObservationCount,
			&last,
		); err != nil {
			return err
		}
		if last.Valid {
			item.LastHeard = last.Time.UnixMilli()
		}
		out.TopTraceTags = append(out.TopTraceTags, item)
	}
	return rows.Err()
}

func (s *Store) fillObserverTopologyScopes(ctx context.Context, out *api.ObserverTopologySummary, filter api.StatsFilter, iataFilter string) error {
	rows, err := s.pool.Query(ctx, `
SELECT
  COALESCE(ts.name, 'none'),
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  MAX(po.heard_at)
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE po.observer_id = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
GROUP BY COALESCE(ts.name, 'none')
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, filter.Limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item api.ObserverTopologyScope
		var last pgtype.Timestamptz
		if err := rows.Scan(&item.Scope, &item.PacketCount, &item.ObservationCount, &last); err != nil {
			return err
		}
		if last.Valid {
			item.LastHeard = last.Time.UnixMilli()
		}
		out.TopScopes = append(out.TopScopes, item)
	}
	return rows.Err()
}

func (s *Store) fillObserverTopologyAdverts(ctx context.Context, out *api.ObserverTopologySummary, filter api.StatsFilter, iataFilter string) error {
	limit := filter.Limit
	if limit > 20 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
SELECT
  po.id,
  encode(po.packet_hash, 'hex'),
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  n.name,
  p.origin_pubkey
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND p.payload_type = 4
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
ORDER BY po.heard_at DESC, po.id DESC
LIMIT $5`, out.ObserverID, filter.Since, filter.Until, iataFilter, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item api.AdvertObservation
		var heard pgtype.Timestamptz
		var publicKey []byte
		if err := rows.Scan(
			&item.ID,
			&item.PacketHash,
			&item.PayloadType,
			&item.IATA,
			&heard,
			&item.RSSI,
			&item.SNR,
			&item.HopCount,
			&item.NodeName,
			&publicKey,
		); err != nil {
			return err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		if heard.Valid {
			item.HeardAt = heard.Time.UnixMilli()
		}
		if len(publicKey) > 0 {
			encoded := hex.EncodeToString(publicKey)
			item.NodePublicKey = &encoded
		}
		out.RecentAdverts = append(out.RecentAdverts, item)
	}
	return rows.Err()
}
