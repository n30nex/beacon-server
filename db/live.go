// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

const liveDefaultWindow = 15 * time.Minute

func liveWindow(since, until time.Time) (time.Time, time.Time) {
	if until.IsZero() {
		until = time.Now()
	}
	if since.IsZero() {
		since = until.Add(-liveDefaultWindow)
	}
	return since, until
}

// ListLiveBackfill returns durable observation-shaped packet events after a cursor.
func (s *Store) ListLiveBackfill(ctx context.Context, filter api.LiveBackfillFilter) (api.Page[api.LivePacketObservation], error) {
	limit := filter.Limit
	if limit < 1 {
		limit = 1
	}
	if limit > 250 {
		limit = 250
	}
	iataFilter := strings.Join(filter.IATAs, ",")
	rows, err := s.pool.Query(ctx, `
SELECT
  po.id,
  encode(p.packet_hash, 'hex') AS packet_hash,
  p.payload_type,
  p.route_type,
  p.raw_header,
  p.raw_payload,
  ts.name AS scope_name,
  po.observer_id,
  o.display_name,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.source_broker,
  po.path_bytes,
  po.path_length_byte,
  po.hash_size,
  po.hop_count,
  po.propagation_time_ms,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = p.packet_hash) AS observation_count,
  po.id = (SELECT MIN(po3.id) FROM packet_observations po3 WHERE po3.packet_hash = p.packet_hash) AS is_first_observation
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE po.id > $1
  AND ($2::smallint = -1 OR p.payload_type = $2::smallint)
  AND ($3::smallint = -1 OR p.route_type = $3::smallint)
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
  AND ($5::text = '' OR ts.name = $5::text)
ORDER BY po.id ASC
LIMIT $6`, filter.AfterObservationID, filter.PayloadType, filter.RouteType, iataFilter, filter.Scope, limit+1)
	if err != nil {
		return api.Page[api.LivePacketObservation]{}, err
	}
	defer rows.Close()

	items := make([]api.LivePacketObservation, 0, limit)
	for rows.Next() {
		var item api.LivePacketObservation
		var observerID uuid.UUID
		var observerName *string
		var heardAt time.Time
		var rssi *int16
		var snr *float32
		var sourceBroker *string
		var pathBytes []byte
		var pathLengthByte int16
		var hashSize int16
		var hopCount int16
		var propagationTimeMs *int32
		var rawHeader []byte
		var rawPayload []byte
		var payloadType int16
		var routeType int16
		if err := rows.Scan(
			&item.Observation.ID,
			&item.PacketHash,
			&payloadType,
			&routeType,
			&rawHeader,
			&rawPayload,
			&item.Packet.Scope,
			&observerID,
			&observerName,
			&item.Observation.IATA,
			&heardAt,
			&rssi,
			&snr,
			&sourceBroker,
			&pathBytes,
			&pathLengthByte,
			&hashSize,
			&hopCount,
			&propagationTimeMs,
			&item.Packet.ObservationCount,
			&item.Packet.IsFirstObservation,
		); err != nil {
			return api.Page[api.LivePacketObservation]{}, err
		}
		item.Packet.PayloadType = uint8(payloadType)
		item.Packet.PayloadTypeName = api.PayloadTypeName(payloadType)
		item.Packet.RouteType = uint8(routeType)
		item.Packet.RouteTypeName = api.RouteTypeName(routeType)
		if len(rawHeader)+len(pathBytes)+len(rawPayload) > 0 {
			raw := make([]byte, 0, len(rawHeader)+len(pathBytes)+len(rawPayload))
			raw = append(raw, rawHeader...)
			raw = append(raw, pathBytes...)
			raw = append(raw, rawPayload...)
			item.Packet.RawHex = hex.EncodeToString(raw)
		}
		item.Observation.ObserverID = observerID.String()
		if observerName != nil {
			item.Observation.ObserverName = *observerName
		}
		item.Observation.HeardAt = heardAt.UnixMilli()
		if rssi != nil {
			item.Observation.RSSI = *rssi
		}
		if snr != nil {
			item.Observation.SNR = *snr
		}
		if sourceBroker != nil {
			item.Observation.SourceBroker = *sourceBroker
		}
		if len(pathBytes) > 0 {
			item.Observation.PathBytes = hex.EncodeToString(pathBytes)
		}
		item.Observation.PathLength = api.PacketPathLength{
			Raw:      fmt.Sprintf("%02x", uint8(pathLengthByte)),
			HashSize: hashSize,
			HopCount: hopCount,
		}
		if propagationTimeMs != nil {
			item.Observation.PropagationTimeMs = *propagationTimeMs
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return api.Page[api.LivePacketObservation]{}, err
	}

	hasMore := len(items) > int(limit)
	if hasMore {
		items = items[:limit]
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1].Observation.ID
		nextCursor = &last
	}
	return api.Page[api.LivePacketObservation]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// GetLiveSummary returns a compact, short-window summary for the Live page.
func (s *Store) GetLiveSummary(ctx context.Context, filter api.LiveSummaryFilter) (*api.LiveSummary, error) {
	since, until := liveWindow(filter.Since, filter.Until)
	iataFilter := strings.Join(filter.IATAs, ",")
	summary := &api.LiveSummary{
		ServerTime: time.Now().UnixMilli(),
		Since:      since.UnixMilli(),
		Until:      until.UnixMilli(),
	}

	err := s.pool.QueryRow(ctx, `
SELECT
  COALESCE(MAX(po.id), 0)::bigint,
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))`, since, until, iataFilter).Scan(
		&summary.LatestObservationID,
		&summary.PacketCount,
		&summary.ObservationCount,
		&summary.ActiveObservers,
	)
	if err != nil {
		return nil, err
	}

	payloadRows, err := s.pool.Query(ctx, `
SELECT p.payload_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY p.payload_type
ORDER BY COUNT(*) DESC, p.payload_type ASC
LIMIT 8`, since, until, iataFilter)
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
		summary.PayloadMix = append(summary.PayloadMix, item)
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
ORDER BY COUNT(*) DESC, p.route_type ASC`, since, until, iataFilter)
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
		summary.RouteMix = append(summary.RouteMix, item)
	}
	routeRows.Close()
	if err := routeRows.Err(); err != nil {
		return nil, err
	}

	iataRows, err := s.pool.Query(ctx, `
SELECT po.iata, COUNT(*)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.iata
ORDER BY COUNT(*) DESC, po.iata ASC
LIMIT 8`, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	for iataRows.Next() {
		var item api.LiveIATACount
		if err := iataRows.Scan(&item.IATA, &item.Count); err != nil {
			iataRows.Close()
			return nil, err
		}
		summary.TopIATAs = append(summary.TopIATAs, item)
	}
	iataRows.Close()
	if err := iataRows.Err(); err != nil {
		return nil, err
	}

	observerRows, err := s.pool.Query(ctx, `
SELECT
  po.observer_id,
  o.display_name,
  o.observer_type,
  (array_agg(po.iata ORDER BY po.id DESC))[1] AS latest_iata,
  COUNT(*)::bigint
FROM packet_observations po
LEFT JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY po.observer_id, o.display_name, o.observer_type
ORDER BY COUNT(*) DESC, latest_iata ASC
LIMIT 8`, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	for observerRows.Next() {
		var item api.TopObserver
		if err := observerRows.Scan(&item.ObserverID, &item.DisplayName, &item.ObserverType, &item.IATA, &item.ObservationCount); err != nil {
			observerRows.Close()
			return nil, err
		}
		summary.TopObservers = append(summary.TopObservers, item)
	}
	observerRows.Close()
	if err := observerRows.Err(); err != nil {
		return nil, err
	}

	return summary, nil
}
