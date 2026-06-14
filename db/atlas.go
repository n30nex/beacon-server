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
	scopes, err := s.GetScopesByIATAs(ctx, iatas)
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
SELECT
  i.iata,
  i.display_name,
  i.approx_lat,
  i.approx_lng,
  COUNT(po.id)::bigint AS observation_count,
  COUNT(DISTINCT po.packet_hash)::bigint AS unique_packets,
  COUNT(DISTINCT po.observer_id)::bigint AS active_observers
FROM iata_codes i
LEFT JOIN packet_observations po
  ON po.iata = i.iata
 AND po.heard_at >= $1
 AND po.heard_at <= $2
WHERE ($3::text = '' OR i.iata = ANY(string_to_array($3::text, ',')))
GROUP BY i.iata, i.display_name, i.approx_lat, i.approx_lng
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

func (s *Store) getAtlasTopNodes(ctx context.Context, since, until time.Time, iatas string, limit int32) ([]api.TopNode, error) {
	const q = `
SELECT
  n.id,
  n.name,
  n.node_type,
  COALESCE((array_agg(DISTINCT po.iata ORDER BY po.iata))[1], '')::text AS iata,
  COUNT(*)::bigint AS observation_count,
  MAX(po.heard_at)::timestamptz AS last_heard
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY n.id
ORDER BY observation_count DESC
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
