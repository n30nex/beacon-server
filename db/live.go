// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
	const liveBackfillSelect = `
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
WHERE ($2::smallint = -1 OR p.payload_type = $2::smallint)
  AND ($3::smallint = -1 OR p.route_type = $3::smallint)
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
  AND ($5::text = '' OR ts.name = $5::text)`
	query := liveBackfillSelect + `
  AND po.id > $1
ORDER BY po.id ASC
LIMIT $6`
	if filter.AfterObservationID <= 0 {
		// Initial Live page load has no cursor yet. Seed from the newest durable
		// observations, then restore ascending order so the client can animate them
		// naturally and advance its high-water mark.
		query = `SELECT * FROM (` + liveBackfillSelect + `
  AND $1::bigint <= 0
ORDER BY po.id DESC
LIMIT $6
) recent_live_observations
ORDER BY id ASC`
	}
	rows, err := s.pool.Query(ctx, query, filter.AfterObservationID, filter.PayloadType, filter.RouteType, iataFilter, filter.Scope, limit+1)
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

	var payloadMixJSON, routeMixJSON, topIATAsJSON, topObserversJSON []byte
	err := s.pool.QueryRow(ctx, `
WITH base AS MATERIALIZED (
  SELECT
    po.id,
    po.packet_hash,
    po.observer_id,
    po.iata,
    p.payload_type,
    p.route_type
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
summary AS (
  SELECT
    COALESCE(MAX(id), 0)::bigint AS latest_observation_id,
    COUNT(DISTINCT packet_hash)::bigint AS packet_count,
    COUNT(*)::bigint AS observation_count,
    COUNT(DISTINCT observer_id)::bigint AS active_observers
  FROM base
),
payload_mix AS (
  SELECT payload_type, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY payload_type
  ORDER BY observation_count DESC, payload_type ASC
  LIMIT 8
),
route_mix AS (
  SELECT route_type, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY route_type
  ORDER BY observation_count DESC, route_type ASC
),
iata_mix AS (
  SELECT iata, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY iata
  ORDER BY observation_count DESC, iata ASC
  LIMIT 8
),
observer_counts AS (
  SELECT
    b.observer_id,
    o.display_name,
    o.observer_type,
    (array_agg(b.iata ORDER BY b.id DESC))[1] AS latest_iata,
    COUNT(*)::bigint AS observation_count
  FROM base b
  LEFT JOIN observers o ON o.id = b.observer_id
  GROUP BY b.observer_id, o.display_name, o.observer_type
),
observer_mix AS (
  SELECT observer_id, display_name, observer_type, latest_iata, observation_count
  FROM observer_counts
  ORDER BY observation_count DESC, latest_iata ASC
  LIMIT 8
)
SELECT
  summary.latest_observation_id,
  summary.packet_count,
  summary.observation_count,
  summary.active_observers,
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'payloadType', payload_type,
      'count', observation_count
    ) ORDER BY observation_count DESC, payload_type ASC)
    FROM payload_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'routeType', route_type,
      'count', observation_count
    ) ORDER BY observation_count DESC, route_type ASC)
    FROM route_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'iata', iata,
      'count', observation_count
    ) ORDER BY observation_count DESC, iata ASC)
    FROM iata_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'observerId', observer_id,
      'displayName', display_name,
      'observerType', observer_type,
      'iata', latest_iata,
      'observationCount', observation_count
    ) ORDER BY observation_count DESC, latest_iata ASC)
    FROM observer_mix
  ), '[]'::jsonb)
FROM summary`, since, until, iataFilter).Scan(
		&summary.LatestObservationID,
		&summary.PacketCount,
		&summary.ObservationCount,
		&summary.ActiveObservers,
		&payloadMixJSON,
		&routeMixJSON,
		&topIATAsJSON,
		&topObserversJSON,
	)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(payloadMixJSON, &summary.PayloadMix); err != nil {
		return nil, err
	}
	for i := range summary.PayloadMix {
		summary.PayloadMix[i].PayloadTypeName = api.PayloadTypeName(summary.PayloadMix[i].PayloadType)
	}
	if err := json.Unmarshal(routeMixJSON, &summary.RouteMix); err != nil {
		return nil, err
	}
	for i := range summary.RouteMix {
		summary.RouteMix[i].RouteTypeName = api.RouteTypeName(summary.RouteMix[i].RouteType)
	}
	if err := json.Unmarshal(topIATAsJSON, &summary.TopIATAs); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(topObserversJSON, &summary.TopObservers); err != nil {
		return nil, err
	}

	return summary, nil
}
