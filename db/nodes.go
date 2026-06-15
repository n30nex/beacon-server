// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) UpsertNode(ctx context.Context, n ingest.UpsertNodeParams, radio ingest.RadioSettings) (uuid.UUID, error) {
	params := sqlc.UpsertNodeParams{
		PublicKey: n.PublicKey,
		NodeType:  int16(n.NodeType),
		Name:      &n.Name,
		Latitude:  n.Latitude,
		Longitude: n.Longitude,
	}
	if radio.FreqMHz != 0 {
		params.RadioFreqMhz = &radio.FreqMHz
		params.RadioSf = &radio.SF
		params.RadioBwKhz = &radio.BWKHz
	}
	row, err := s.q.UpsertNode(ctx, params)
	if err != nil {
		return uuid.Nil, err
	}
	return row.ID, nil
}

func (s *Store) UpsertNodeIATA(ctx context.Context, nodeID uuid.UUID, iata string) error {
	params := sqlc.UpsertNodeIATAParams{NodeID: nodeID, Iata: iata}
	return s.q.UpsertNodeIATA(ctx, params)
}

func (s *Store) UpsertNodeShortID(ctx context.Context, nodeID uuid.UUID, iata string, prefix4 []byte) error {
	return s.q.UpsertNodeShortID(ctx, sqlc.UpsertNodeShortIDParams{
		NodeID:  nodeID,
		Iata:    iata,
		Prefix4: prefix4,
	})
}

func (s *Store) UpsertNodeNeighbor(ctx context.Context, nodeID, neighborID uuid.UUID, iata string) error {
	return s.q.UpsertNodeNeighbor(ctx, sqlc.UpsertNodeNeighborParams{
		NodeID:     nodeID,
		NeighborID: neighborID,
		Iata:       iata,
	})
}

func (s *Store) SetNodeCapability(ctx context.Context, nodeID uuid.UUID, paths, traces bool) error {
	var errs []error
	if paths {
		errs = append(errs, s.q.SetNodeMultibytePaths(ctx, nodeID))
	}
	if traces {
		errs = append(errs, s.q.SetNodeMultibyteTraces(ctx, nodeID))
	}
	return errors.Join(errs...)
}

func (s *Store) SetNodeDefaultScope(ctx context.Context, nodeID uuid.UUID, scopeID int32) error {
	return s.q.SetNodeDefaultScope(ctx, sqlc.SetNodeDefaultScopeParams{
		ID:             nodeID,
		DefaultScopeID: &scopeID,
	})
}

func (s *Store) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.ListNodes(ctx, sqlc.ListNodesParams{
		Column1: nodeType,
		Column2: iataFilter,
		Column3: tristate(supportsMultibytePaths),
		Column4: tristate(supportsMultibyteTraces),
		Column5: pubkey,
		Column6: name,
		Column7: cursorTS,
		Limit:   limit + 1,
		Column9: scope,
	})
	if err != nil {
		return api.Page[api.NodeSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.NodeSummary, 0, len(rows))
	for _, v := range rows {
		node := api.NodeSummary{
			ID:                 v.ID,
			PublicKey:          hex.EncodeToString(v.PublicKey),
			NodeType:           v.NodeType,
			NodeTypeName:       api.NodeTypeName(v.NodeType),
			Name:               v.Name,
			Latitude:           v.Latitude,
			Longitude:          v.Longitude,
			IsObserver:         v.IsObserver,
			ObserverID:         nullableUUID(v.ObserverID),
			KnownNeighborCount: v.KnownNeighborCount,
		}
		if len(v.Iatas) > 0 {
			if err := json.Unmarshal(v.Iatas, &node.IATAs); err != nil {
				log.Printf("store: failed to unmarshal node iatas: %v", err)
				node.IATAs = []api.NodeIATA{}
			}
		}
		if v.RadioFreqMhz != nil && v.RadioSf != nil && v.RadioBwKhz != nil {
			s := fmt.Sprintf("%.1f,%g,%d", *v.RadioFreqMhz, *v.RadioBwKhz, *v.RadioSf)
			node.Radio = &s
		}
		items = append(items, node)
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		ms := rows[len(rows)-1].LastSeen.Time.UnixMilli()
		nextCursor = &ms
	}
	return api.Page[api.NodeSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	row, err := s.q.GetNodeByID(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	node := &api.Node{
		NodeSummary: api.NodeSummary{
			ID:                 row.ID,
			PublicKey:          hex.EncodeToString(row.PublicKey),
			NodeType:           row.NodeType,
			NodeTypeName:       api.NodeTypeName(row.NodeType),
			Name:               row.Name,
			Latitude:           row.Latitude,
			Longitude:          row.Longitude,
			IsObserver:         row.IsObserver,
			ObserverID:         nullableUUID(row.ObserverID),
			DefaultScope:       row.DefaultScopeName,
			KnownNeighborCount: row.KnownNeighborCount,
		},
		LocationSource:          row.LocationSource,
		SupportsMultibytePaths:  row.SupportsMultibytePaths,
		SupportsMultibyteTraces: row.SupportsMultibyteTraces,
		MinFirmwareVersion:      row.MinFirmwareVersion,
		FirstSeen:               row.FirstSeen.Time.UnixMilli(),
		LastSeen:                row.LastSeen.Time.UnixMilli(),
		Metadata:                row.Metadata,
	}
	neighbors, err := s.GetNodeNeighbors(ctx, nodeID)
	if err != nil {
		log.Printf("store: GetNodeNeighbors failed for %s: %v", nodeID, err)
		neighbors = []api.NodeNeighbor{}
	}
	node.Neighbors = neighbors
	if len(row.Iatas) > 0 {
		if err := json.Unmarshal(row.Iatas, &node.IATAs); err != nil {
			log.Printf("store: failed to unmarshal node iatas: %v", err)
			node.IATAs = []api.NodeIATA{}
		}
	}
	if row.RadioFreqMhz != nil && row.RadioSf != nil && row.RadioBwKhz != nil {
		s := fmt.Sprintf("%.1f,%g,%d", *row.RadioFreqMhz, *row.RadioBwKhz, *row.RadioSf)
		node.Radio = &s
	}
	if row.LastAdvertAt.Valid {
		ms := row.LastAdvertAt.Time.UnixMilli()
		node.LastAdvertAt = &ms
	}
	return node, nil
}

func (s *Store) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	rows, err := s.q.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := make(map[uuid.UUID]*api.ResolvedNode, len(rows))
	for _, r := range rows {
		result[r.ID] = &api.ResolvedNode{
			ID:        r.ID,
			Name:      r.Name,
			PublicKey: hex.EncodeToString(r.PublicKey),
			Latitude:  r.Latitude,
			Longitude: r.Longitude,
		}
	}
	return result, nil
}

func (s *Store) GetNodeAnalytics(ctx context.Context, nodeID uuid.UUID, filter api.NodeAnalyticsFilter) (*api.NodeAnalytics, error) {
	since, until, iataFilter := normalizeNodeAnalyticsFilter(filter)
	out := &api.NodeAnalytics{
		NodeID: nodeID,
		Since:  since.UnixMilli(),
		Until:  until.UnixMilli(),
	}

	var firstSeen, lastSeen pgtype.Timestamptz
	var avgSNR, avgRSSI, avgHop pgtype.Float8
	err := s.pool.QueryRow(ctx, `
WITH target AS (SELECT public_key FROM nodes WHERE id = $1),
filtered AS (
  SELECT po.packet_hash, po.observer_id, po.iata, po.heard_at, po.rssi, po.snr, po.hop_count
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  JOIN target t ON t.public_key = p.origin_pubkey
  WHERE po.heard_at >= $2
    AND po.heard_at <= $3
    AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
)
SELECT
  COUNT(DISTINCT packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT observer_id)::bigint,
  COUNT(DISTINCT iata)::bigint,
  MIN(heard_at),
  MAX(heard_at),
  AVG(snr)::double precision,
  AVG(rssi)::double precision,
  AVG(hop_count)::double precision
FROM filtered`, nodeID, since, until, iataFilter).Scan(
		&out.KPIs.PacketCount,
		&out.KPIs.ObservationCount,
		&out.KPIs.ActiveObservers,
		&out.KPIs.ActiveIATAs,
		&firstSeen,
		&lastSeen,
		&avgSNR,
		&avgRSSI,
		&avgHop,
	)
	if err != nil {
		return nil, err
	}
	if firstSeen.Valid {
		ms := firstSeen.Time.UnixMilli()
		out.KPIs.FirstHeardAt = &ms
	}
	if lastSeen.Valid {
		ms := lastSeen.Time.UnixMilli()
		out.KPIs.LastHeardAt = &ms
	}
	if avgSNR.Valid {
		v := avgSNR.Float64
		out.KPIs.AvgSNR = &v
	}
	if avgRSSI.Valid {
		v := avgRSSI.Float64
		out.KPIs.AvgRSSI = &v
	}
	if avgHop.Valid {
		v := avgHop.Float64
		out.KPIs.AvgHopCount = &v
	}

	var errMix error
	out.PayloadMix, errMix = s.nodeAnalyticsCounts(ctx, nodeID, since, until, iataFilter, `
SELECT p.payload_type::text AS key, p.payload_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
GROUP BY p.payload_type
ORDER BY COUNT(*) DESC, p.payload_type ASC
LIMIT 8`, api.PayloadTypeName)
	if errMix != nil {
		return nil, errMix
	}
	out.RouteMix, errMix = s.nodeAnalyticsCounts(ctx, nodeID, since, until, iataFilter, `
SELECT p.route_type::text AS key, p.route_type, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
GROUP BY p.route_type
ORDER BY COUNT(*) DESC, p.route_type ASC
LIMIT 8`, api.RouteTypeName)
	if errMix != nil {
		return nil, errMix
	}
	out.IATAMix, errMix = s.nodeAnalyticsTextCounts(ctx, nodeID, since, until, iataFilter, `
SELECT po.iata, po.iata, COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
GROUP BY po.iata
ORDER BY COUNT(*) DESC, po.iata ASC
LIMIT 8`)
	if errMix != nil {
		return nil, errMix
	}
	out.TopObservers, errMix = s.nodeAnalyticsTextCounts(ctx, nodeID, since, until, iataFilter, `
SELECT o.id::text, COALESCE(o.display_name, left(encode(o.public_key, 'hex'), 8)), COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
JOIN observers o ON o.id = po.observer_id
WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
GROUP BY o.id, o.display_name, o.public_key
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT 8`)
	if errMix != nil {
		return nil, errMix
	}
	if out.Hourly, errMix = s.nodeActivityTimeline(ctx, nodeID, since, until, iataFilter); errMix != nil {
		return nil, errMix
	}
	if out.SNRBuckets, errMix = s.nodeSignalBuckets(ctx, nodeID, since, until, iataFilter, "snr"); errMix != nil {
		return nil, errMix
	}
	if out.RSSIBuckets, errMix = s.nodeSignalBuckets(ctx, nodeID, since, until, iataFilter, "rssi"); errMix != nil {
		return nil, errMix
	}
	if out.HopBuckets, errMix = s.nodeSignalBuckets(ctx, nodeID, since, until, iataFilter, "hop"); errMix != nil {
		return nil, errMix
	}
	if out.TopPeers, errMix = s.nodeAnalyticsPeers(ctx, nodeID, since, until, iataFilter); errMix != nil {
		return nil, errMix
	}
	return out, nil
}

func (s *Store) ListNodeAdverts(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.NodeAdvertObservation], error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  po.id,
  encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  NULLIF(p.parsed_payload #>> '{appData,name}', '') AS advertised_name,
  NULLIF(p.parsed_payload #>> '{appData,flags,deviceRole}', '')::smallint AS advertised_node_type,
  NULLIF(p.parsed_payload #>> '{appData,flags,deviceRoleName}', '') AS advertised_node_type_name,
  NULLIF(p.parsed_payload #>> '{appData,latitude}', '')::double precision AS advertised_lat,
  NULLIF(p.parsed_payload #>> '{appData,longitude}', '')::double precision AS advertised_lng,
  NULLIF(p.parsed_payload #>> '{appData,flags,raw}', '') AS flags_raw,
  NULLIF(p.parsed_payload #>> '{appData,flags,hasLocation}', '')::boolean AS has_location,
  NULLIF(p.parsed_payload #>> '{appData,flags,hasName}', '')::boolean AS has_name
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1
  AND p.payload_type = 4
  AND ($2 = 0 OR po.id > $2)
ORDER BY po.id ASC
LIMIT $3`, nodeID, cursor, limit+1)
	if err != nil {
		log.Printf("api: ListNodeAdverts failed: %v", err)
		return api.Page[api.NodeAdvertObservation]{}, err
	}
	defer rows.Close()

	items := make([]api.NodeAdvertObservation, 0, limit)
	for rows.Next() {
		var item api.NodeAdvertObservation
		var heardAt time.Time
		var rssi pgtype.Int2
		var snr pgtype.Float4
		var hopCount pgtype.Int2
		var advertisedName pgtype.Text
		var advertisedNodeType pgtype.Int2
		var advertisedNodeTypeName pgtype.Text
		var advertisedLat pgtype.Float8
		var advertisedLng pgtype.Float8
		var flagsRaw pgtype.Text
		var hasLocation pgtype.Bool
		var hasName pgtype.Bool
		if err := rows.Scan(
			&item.ID,
			&item.PacketHash,
			&item.PayloadType,
			&item.IATA,
			&heardAt,
			&rssi,
			&snr,
			&hopCount,
			&advertisedName,
			&advertisedNodeType,
			&advertisedNodeTypeName,
			&advertisedLat,
			&advertisedLng,
			&flagsRaw,
			&hasLocation,
			&hasName,
		); err != nil {
			return api.Page[api.NodeAdvertObservation]{}, err
		}
		item.PayloadTypeName = api.PayloadTypeName(item.PayloadType)
		item.HeardAt = heardAt.UnixMilli()
		if rssi.Valid {
			v := rssi.Int16
			item.RSSI = &v
		}
		if snr.Valid {
			v := snr.Float32
			item.SNR = &v
		}
		if hopCount.Valid {
			v := hopCount.Int16
			item.HopCount = &v
		}
		if advertisedName.Valid {
			v := advertisedName.String
			item.AdvertisedName = &v
		}
		if advertisedNodeType.Valid {
			v := advertisedNodeType.Int16
			item.AdvertisedNodeType = &v
			if !advertisedNodeTypeName.Valid {
				name := api.NodeTypeName(v)
				item.AdvertisedNodeTypeName = &name
			}
		}
		if advertisedNodeTypeName.Valid {
			v := advertisedNodeTypeName.String
			item.AdvertisedNodeTypeName = &v
		}
		if advertisedLat.Valid {
			v := advertisedLat.Float64
			item.AdvertisedLat = &v
		}
		if advertisedLng.Valid {
			v := advertisedLng.Float64
			item.AdvertisedLng = &v
		}
		if flagsRaw.Valid {
			v := flagsRaw.String
			item.FlagsRaw = &v
		}
		if hasLocation.Valid {
			v := hasLocation.Bool
			item.HasLocation = &v
		}
		if hasName.Valid {
			v := hasName.Bool
			item.HasName = &v
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return api.Page[api.NodeAdvertObservation]{}, err
	}

	hasMore := len(items) > int(limit)
	if hasMore {
		items = items[:limit]
	}
	var nextCursor *int64
	if hasMore {
		last := items[len(items)-1].ID
		nextCursor = &last
	}
	return api.Page[api.NodeAdvertObservation]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func normalizeNodeAnalyticsFilter(filter api.NodeAnalyticsFilter) (time.Time, time.Time, string) {
	until := filter.Until
	if until.IsZero() {
		until = time.Now()
	}
	since := filter.Since
	if since.IsZero() {
		since = until.Add(-24 * time.Hour)
	}
	return since, until, strings.Join(filter.IATAs, ",")
}

func (s *Store) nodeAnalyticsCounts(ctx context.Context, nodeID uuid.UUID, since, until time.Time, iataFilter, query string, labelFn func(int16) string) ([]api.NodeAnalyticsCount, error) {
	rows, err := s.pool.Query(ctx, query, nodeID, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NodeAnalyticsCount{}
	for rows.Next() {
		var key string
		var value int16
		var count int64
		if err := rows.Scan(&key, &value, &count); err != nil {
			return nil, err
		}
		items = append(items, api.NodeAnalyticsCount{Key: key, Label: labelFn(value), Count: count})
	}
	return items, rows.Err()
}

func (s *Store) nodeAnalyticsTextCounts(ctx context.Context, nodeID uuid.UUID, since, until time.Time, iataFilter, query string) ([]api.NodeAnalyticsCount, error) {
	rows, err := s.pool.Query(ctx, query, nodeID, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NodeAnalyticsCount{}
	for rows.Next() {
		var key, label string
		var count int64
		if err := rows.Scan(&key, &label, &count); err != nil {
			return nil, err
		}
		items = append(items, api.NodeAnalyticsCount{Key: key, Label: label, Count: count})
	}
	return items, rows.Err()
}

func (s *Store) nodeActivityTimeline(ctx context.Context, nodeID uuid.UUID, since, until time.Time, iataFilter string) ([]api.NodeActivityPoint, error) {
	rows, err := s.pool.Query(ctx, `
SELECT date_trunc('hour', po.heard_at) AS bucket,
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
  AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
GROUP BY bucket
ORDER BY bucket ASC`, nodeID, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NodeActivityPoint{}
	for rows.Next() {
		var bucket time.Time
		var packets, observations int64
		if err := rows.Scan(&bucket, &packets, &observations); err != nil {
			return nil, err
		}
		items = append(items, api.NodeActivityPoint{Timestamp: bucket.UnixMilli(), Packets: packets, Observations: observations})
	}
	return items, rows.Err()
}

func (s *Store) nodeSignalBuckets(ctx context.Context, nodeID uuid.UUID, since, until time.Time, iataFilter, metric string) ([]api.NodeSignalBucket, error) {
	var bucketExpr, nullCheck string
	switch metric {
	case "snr":
		nullCheck = "po.snr IS NOT NULL"
		bucketExpr = `CASE
  WHEN po.snr < -15 THEN '< -15'
  WHEN po.snr < -10 THEN '-15..-10'
  WHEN po.snr < -5 THEN '-10..-5'
  WHEN po.snr < 0 THEN '-5..0'
  WHEN po.snr < 5 THEN '0..5'
  WHEN po.snr < 10 THEN '5..10'
  ELSE '>= 10'
END`
	case "rssi":
		nullCheck = "po.rssi IS NOT NULL"
		bucketExpr = `CASE
  WHEN po.rssi < -120 THEN '< -120'
  WHEN po.rssi < -110 THEN '-120..-110'
  WHEN po.rssi < -100 THEN '-110..-100'
  WHEN po.rssi < -90 THEN '-100..-90'
  ELSE '>= -90'
END`
	default:
		nullCheck = "po.hop_count IS NOT NULL"
		bucketExpr = `CASE
  WHEN po.hop_count = 0 THEN '0'
  WHEN po.hop_count = 1 THEN '1'
  WHEN po.hop_count = 2 THEN '2'
  WHEN po.hop_count = 3 THEN '3'
  ELSE '4+'
END`
	}
	query := fmt.Sprintf(`
SELECT bucket, COUNT(*)::bigint FROM (
  SELECT %s AS bucket
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  JOIN nodes n ON n.public_key = p.origin_pubkey
  WHERE n.id = $1 AND po.heard_at >= $2 AND po.heard_at <= $3
    AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
    AND %s
) b
GROUP BY bucket
ORDER BY bucket ASC`, bucketExpr, nullCheck)
	rows, err := s.pool.Query(ctx, query, nodeID, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NodeSignalBucket{}
	for rows.Next() {
		var bucket string
		var count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			return nil, err
		}
		items = append(items, api.NodeSignalBucket{Bucket: bucket, Count: count})
	}
	return items, rows.Err()
}

func (s *Store) nodeAnalyticsPeers(ctx context.Context, nodeID uuid.UUID, since, until time.Time, iataFilter string) ([]api.NodeAnalyticsPeer, error) {
	rows, err := s.pool.Query(ctx, `
SELECT n.id, n.name, encode(n.public_key, 'hex'), n.node_type, nn.iata, nn.observation_count, nn.last_seen
FROM node_neighbors nn
JOIN nodes n ON n.id = nn.neighbor_id
WHERE nn.node_id = $1
  AND nn.last_seen >= $2
  AND nn.last_seen <= $3
  AND ($4::text = '' OR nn.iata = ANY(string_to_array(upper($4::text), ',')))
ORDER BY nn.observation_count DESC, nn.last_seen DESC
LIMIT 8`, nodeID, since, until, iataFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NodeAnalyticsPeer{}
	for rows.Next() {
		var item api.NodeAnalyticsPeer
		var nodeType int16
		var lastSeen time.Time
		if err := rows.Scan(&item.ID, &item.Name, &item.PublicKey, &nodeType, &item.IATA, &item.ObservationCount, &lastSeen); err != nil {
			return nil, err
		}
		item.NodeTypeName = api.NodeTypeName(nodeType)
		item.LastSeen = lastSeen.UnixMilli()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	rows, err := s.q.GetNodeNeighbors(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	seen := make(map[uuid.UUID]int)
	items := make([]api.NodeNeighbor, 0, len(rows))
	for _, r := range rows {
		if idx, ok := seen[r.ID]; ok {
			items[idx].ObservationCount += r.ObservationCount
			if r.LastSeen.Time.After(time.UnixMilli(items[idx].LastSeen)) {
				items[idx].LastSeen = r.LastSeen.Time.UnixMilli()
				items[idx].IATA = r.Iata
			}
			if r.FirstSeen.Time.Before(time.UnixMilli(items[idx].FirstSeen)) {
				items[idx].FirstSeen = r.FirstSeen.Time.UnixMilli()
			}
			continue
		}
		seen[r.ID] = len(items)
		items = append(items, api.NodeNeighbor{
			ID:               r.ID,
			Name:             r.Name,
			PublicKey:        hex.EncodeToString(r.PublicKey),
			NodeType:         r.NodeType,
			NodeTypeName:     api.NodeTypeName(r.NodeType),
			Latitude:         r.Latitude,
			Longitude:        r.Longitude,
			IATA:             r.Iata,
			ObservationCount: r.ObservationCount,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) ReconfirmNeighbors(ctx context.Context) error {
	return s.q.ReconfirmNeighbors(ctx)
}
