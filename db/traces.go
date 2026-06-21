// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

type tracePayload struct {
	PathHashes []string  `json:"pathHashes"`
	Flags      byte      `json:"flags"`
	SNRValues  []float32 `json:"snrValues"`
}

func (s *Store) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	iataFilter := strings.Join(iatas, ",")
	var sinceTS, untilTS, cursorTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	if !cursor.IsZero() {
		cursorTS = pgtype.Timestamptz{Time: cursor, Valid: true}
	}
	rows, err := s.q.ListTraceTags(ctx, sqlc.ListTraceTagsParams{
		Column1: iataFilter,
		Column2: scope,
		Column3: sinceTS,
		Column4: untilTS,
		Column5: cursorTS,
		Limit:   limit,
		Column7: traceType,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TraceTagSummary, 0, len(rows))
	for _, r := range rows {
		var best tracePayload
		if len(r.BestPayload) > 0 {
			_ = json.Unmarshal(r.BestPayload, &best)
		}
		items = append(items, api.TraceTagSummary{
			TraceTag:     r.TraceTag,
			FirstHeardAt: r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:  r.LastHeardAt.Time.UnixMilli(),
			PacketCount:  r.PacketCount,
			IATACount:    r.IataCount,
			TraceType:    r.TraceType,
			PathHashes:   best.PathHashes,
			SNRValues:    best.SNRValues,
		})
	}
	return items, nil
}

func (s *Store) GetTraceByTag(ctx context.Context, tag string, iatas []string, scope string, since, until time.Time) (*api.TraceDetail, error) {
	iataFilter := strings.Join(iatas, ",")
	queryRows, err := s.pool.Query(ctx, `
SELECT encode(p.packet_hash, 'hex') AS packet_hash_hex,
    p.route_type,
    p.first_heard_at,
    p.last_heard_at,
    p.parsed_payload,
    p.scope_id,
    ts.name AS scope_name
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE p.trace_tag = decode($1, 'hex')
  AND ($2::text = '' OR EXISTS (
    SELECT 1 FROM packet_observations po
    WHERE po.packet_hash = p.packet_hash
      AND po.iata = ANY(string_to_array($2::text, ','))
  ))
  AND ($3::text = '' OR ts.name = $3)
  AND ($4::timestamptz IS NULL OR p.first_heard_at >= $4)
  AND ($5::timestamptz IS NULL OR p.first_heard_at <= $5)
ORDER BY p.first_heard_at ASC`,
		tag,
		iataFilter,
		scope,
		nullableTime(since),
		nullableTime(until),
	)
	if err != nil {
		return nil, err
	}
	defer queryRows.Close()

	type tracePacketRow struct {
		PacketHashHex string
		RouteType     int16
		FirstHeardAt  pgtype.Timestamptz
		LastHeardAt   pgtype.Timestamptz
		ParsedPayload []byte
		ScopeID       *int32
		ScopeName     *string
	}
	rows := []tracePacketRow{}
	for queryRows.Next() {
		var row tracePacketRow
		if err := queryRows.Scan(
			&row.PacketHashHex,
			&row.RouteType,
			&row.FirstHeardAt,
			&row.LastHeardAt,
			&row.ParsedPayload,
			&row.ScopeID,
			&row.ScopeName,
		); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := queryRows.Err(); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	detail := &api.TraceDetail{
		TraceTag: tag,
		Packets:  make([]api.TracePacket, 0, len(rows)),
	}
	for _, r := range rows {
		packet := api.TracePacket{
			PacketHash:    r.PacketHashHex,
			RouteType:     r.RouteType,
			RouteTypeName: api.RouteTypeName(r.RouteType),
			Scope:         r.ScopeName,
			FirstHeardAt:  r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:   r.LastHeardAt.Time.UnixMilli(),
		}
		var parsed tracePayload
		if err := json.Unmarshal(r.ParsedPayload, &parsed); err == nil {
			// build raw path
			rawPath := make([]api.RawHop, 0, len(parsed.PathHashes))
			for i, h := range parsed.PathHashes {
				hop := api.RawHop{Hash: h}
				if i < len(parsed.SNRValues) {
					snr := parsed.SNRValues[i]
					hop.SNR = &snr
				}
				rawPath = append(rawPath, hop)
			}
			packet.RawPath = rawPath
		}
		// Resolve paths within the requested IATA/region when provided; otherwise
		// derive the IATAs that actually observed this packet.
		resolveIATAs := append([]string(nil), iatas...)
		if len(resolveIATAs) == 0 {
			packetHashBytes, err := hex.DecodeString(r.PacketHashHex)
			if err == nil {
				obsRows, err := s.q.ListObservationsForPacket(ctx, packetHashBytes)
				if err == nil && len(obsRows) > 0 {
					seen := make(map[string]struct{})
					for _, v := range obsRows {
						if _, ok := seen[v.Iata]; !ok {
							seen[v.Iata] = struct{}{}
							resolveIATAs = append(resolveIATAs, v.Iata)
						}
					}
				}
			}
		}
		packet.ResolvedRoute = s.resolveTraceRoute(ctx, &parsed, resolveIATAs)
		detail.Packets = append(detail.Packets, packet)
	}
	return detail, nil
}

func (s *Store) resolveTraceRoute(ctx context.Context, payload *tracePayload, iatas []string) []api.ResolvedHop {
	if payload == nil || len(payload.PathHashes) == 0 {
		return nil
	}
	hashSize := int(1 << (payload.Flags & 0x03))
	hashes := make([][]byte, 0, len(payload.PathHashes))
	for _, h := range payload.PathHashes {
		b, err := hex.DecodeString(h)
		if err == nil {
			hashes = append(hashes, b)
		}
	}
	confidenceRank := map[string]int{"none": 0, "ambiguous": 1, "high": 2}
	type hopResult struct {
		confidence string
		entries    []api.ResolvedPathEntry
	}
	merged := make([]hopResult, len(hashes))
	for i := range merged {
		merged[i] = hopResult{confidence: "none"}
	}
	for _, iata := range iatas {
		resolved, err := s.ResolvePathHashes(ctx, iata, hashes)
		if err != nil {
			continue
		}
		for i, hash := range hashes {
			key := hex.EncodeToString(hash[:hashSize])
			entries := resolved[key]
			var confidence string
			switch len(entries) {
			case 0:
				confidence = "none"
			case 1:
				confidence = "high"
			default:
				confidence = "ambiguous"
			}
			if confidenceRank[confidence] > confidenceRank[merged[i].confidence] {
				merged[i] = hopResult{confidence: confidence, entries: entries}
			}
		}
	}
	route := make([]api.ResolvedHop, 0, len(hashes))
	for i, hr := range merged {
		hop := api.ResolvedHop{
			Confidence: hr.confidence,
			Nodes:      make([]api.ResolvedNode, 0, len(hr.entries)),
		}
		if i < len(payload.SNRValues) {
			snr := payload.SNRValues[i]
			hop.SNR = &snr
		}
		for _, e := range hr.entries {
			hop.Nodes = append(hop.Nodes, api.ResolvedNode{
				ID:        e.NodeID,
				Name:      e.Name,
				Latitude:  e.Latitude,
				Longitude: e.Longitude,
				PublicKey: hex.EncodeToString(e.PublicKey),
			})
		}
		route = append(route, hop)
	}
	return route
}
