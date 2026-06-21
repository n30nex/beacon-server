// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

func (s *Store) UpsertKnownRoute(ctx context.Context, nodeIDs []uuid.UUID, hashPrefix [][]byte, iata string, hopCount int32) error {
	return s.q.UpsertKnownRoute(ctx, sqlc.UpsertKnownRouteParams{
		NodeIds:    nodeIDs,
		HashPrefix: hashPrefix,
		Iata:       iata,
		HopCount:   int32(hopCount),
	})
}

func (s *Store) ListKnownRoutes(ctx context.Context, iatas []string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	rows, err := s.queryKnownRoutes(ctx, `
		SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
		FROM known_routes
		WHERE ($1::text = '' OR iata = ANY(string_to_array($1::text, ',')))
		  AND ($2::int = 0 OR hop_count = $2)
		  AND ($3::timestamptz IS NULL OR last_seen < $3)
		ORDER BY last_seen DESC
		LIMIT $4
	`, strings.Join(iatas, ","), hopCount, nullableTime(cursor), limit)
	if err != nil {
		return nil, err
	}
	ids := collectNodeIDs(rows)
	nodes, err := s.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	return toKnownRoutes(rows, nodes), nil
}

func (s *Store) GetKnownRoute(ctx context.Context, routeID int64) (*api.KnownRoute, error) {
	rows, err := s.queryKnownRoutes(ctx, `
		SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
		FROM known_routes
		WHERE id = $1
	`, routeID)
	if err != nil {
		return nil, err
	}
	nodes, err := s.GetNodesByIDs(ctx, collectNodeIDs(rows))
	if err != nil {
		return nil, err
	}
	routes := toKnownRoutes(rows, nodes)
	if len(routes) == 0 {
		return nil, nil
	}
	return &routes[0], nil
}

func (s *Store) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	fromBytes, err := hex.DecodeString(fromHash)
	if err != nil {
		return nil, err
	}
	toBytes, err := hex.DecodeString(toHash)
	if err != nil {
		return nil, err
	}
	rows, err := s.q.SearchKnownRoutes(ctx, sqlc.SearchKnownRoutesParams{
		Iata:    iata,
		Column2: fromBytes,
		Column3: toBytes,
	})
	if err != nil {
		return nil, err
	}
	ids := collectNodeIDs(rows)
	nodes, err := s.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	items := make([]api.KnownRoute, 0, len(rows))
	for _, r := range rows {
		fromPos, toPos := -1, -1
		for i, h := range r.HashPrefix {
			if fromPos == -1 && hex.EncodeToString(h) == fromHash {
				fromPos = i
			}
			if fromPos != -1 && hex.EncodeToString(h) == toHash {
				toPos = i
				break
			}
		}
		if fromPos == -1 || toPos == -1 {
			continue
		}
		nodeIDs := r.NodeIds[fromPos : toPos+1]
		hashPrefix := r.HashPrefix[fromPos : toPos+1]
		hops := make([]api.RouteHop, 0, len(nodeIDs))
		for i, nodeID := range nodeIDs {
			hop := api.RouteHop{
				NodeID: nodeID,
				Node:   nodes[nodeID],
			}
			if i < len(hashPrefix) {
				hop.HashBytes = hex.EncodeToString(hashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			ID:               r.ID,
			IATA:             r.Iata,
			HopCount:         int32(len(hops)),
			Hops:             hops,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
			ObservationCount: r.ObservationCount,
		})
	}
	return items, nil
}

func (s *Store) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error) {
	rows, err := s.queryKnownRoutes(ctx, `
		SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
		FROM known_routes
		WHERE ($1 = '' OR iata = $1)
		  AND $2::uuid = ANY(node_ids)
		ORDER BY hop_count ASC, last_seen DESC
	`, iata, nodeID)
	if err != nil {
		return nil, err
	}
	ids := collectNodeIDs(rows)
	nodes, err := s.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	return toKnownRoutes(rows, nodes), nil
}

func (s *Store) queryKnownRoutes(ctx context.Context, query string, args ...any) ([]sqlc.KnownRoute, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []sqlc.KnownRoute{}
	for rows.Next() {
		var row sqlc.KnownRoute
		if err := rows.Scan(
			&row.ID,
			&row.NodeIds,
			&row.HashPrefix,
			&row.Iata,
			&row.HopCount,
			&row.FirstSeen,
			&row.LastSeen,
			&row.ObservationCount,
		); err != nil {
			return nil, err
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error) {
	rows, err := s.q.GetCrossIATANeighbors(ctx, sqlc.GetCrossIATANeighborsParams{
		NodeID: nodeID,
		Iata:   iata,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.NodeNeighbor, 0, len(rows))
	for _, r := range rows {
		items = append(items, api.NodeNeighbor{
			ID:               r.ID,
			Name:             r.Name,
			NodeType:         r.NodeType,
			NodeTypeName:     api.NodeTypeName(r.NodeType),
			Latitude:         r.Latitude,
			Longitude:        r.Longitude,
			IATA:             r.NeighborIata,
			ObservationCount: r.ObservationCount,
			LastSeen:         r.LastSeen.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error) {
	// 1. resolve fromHash in fromIATA
	fromBytes, err := hex.DecodeString(fromHash)
	if err != nil {
		return nil, err
	}
	fromResolved, err := s.ResolvePathHashes(ctx, fromIATA, [][]byte{fromBytes})
	if err != nil {
		return nil, err
	}
	fromEntries := fromResolved[fromHash]
	if len(fromEntries) != 1 {
		return nil, nil // not found or ambiguous
	}
	fromNodeID := fromEntries[0].NodeID

	// 2. resolve toHash in toIATA
	toBytes, err := hex.DecodeString(toHash)
	if err != nil {
		return nil, err
	}
	toResolved, err := s.ResolvePathHashes(ctx, toIATA, [][]byte{toBytes})
	if err != nil {
		return nil, err
	}
	toEntries := toResolved[toHash]
	if len(toEntries) != 1 {
		return nil, nil // not found or ambiguous
	}
	toNodeID := toEntries[0].NodeID

	// 3. find routes in source IATA containing fromNode
	sourceRoutes, err := s.GetKnownRoutesByNode(ctx, fromIATA, fromNodeID)
	if err != nil {
		return nil, err
	}

	// 4. find routes in target IATA containing toNode
	targetRoutes, err := s.GetKnownRoutesByNode(ctx, toIATA, toNodeID)
	if err != nil {
		return nil, err
	}

	if len(sourceRoutes) == 0 || len(targetRoutes) == 0 {
		return nil, nil
	}

	// 5. find cross-IATA links — nodes at the boundary of source routes
	//    that have neighbors in the target IATA at the start of target routes
	var results []api.CrossIATARoute

	// build a set of node IDs that appear in target routes
	targetNodeSet := make(map[uuid.UUID][]api.RouteHop)
	for _, tr := range targetRoutes {
		for _, hop := range tr.Hops {
			if _, ok := targetNodeSet[hop.NodeID]; !ok {
				targetNodeSet[hop.NodeID] = tr.Hops
			}
		}
	}

	// for each source route, check if any node has a cross-IATA neighbor in targetNodeSet
	for _, sr := range sourceRoutes {
		for i, hop := range sr.Hops {
			crossNeighbors, err := s.GetCrossIATANeighbors(ctx, hop.NodeID, fromIATA)
			if err != nil {
				continue
			}
			for _, neighbor := range crossNeighbors {
				if neighbor.IATA != toIATA {
					continue
				}
				if targetHops, ok := targetNodeSet[neighbor.ID]; ok {
					// found a cross-IATA link — build the route
					sourceSegment := sr.Hops[:i+1]
					targetSegment := extractFromNode(targetHops, neighbor.ID)

					fromNode := api.ResolvedNode{
						ID:        hop.NodeID,
						Latitude:  fromEntries[0].Latitude,
						Longitude: fromEntries[0].Longitude,
						PublicKey: hex.EncodeToString(fromEntries[0].PublicKey),
					}
					toNode := api.ResolvedNode{
						ID:        neighbor.ID,
						Name:      neighbor.Name,
						Latitude:  neighbor.Latitude,
						Longitude: neighbor.Longitude,
					}

					results = append(results, api.CrossIATARoute{
						SourceSegment: sourceSegment,
						CrossHop: api.CrossIATAHop{
							FromNode: fromNode,
							ToNode:   toNode,
							FromIATA: fromIATA,
							ToIATA:   toIATA,
							LastSeen: neighbor.LastSeen,
						},
						TargetSegment: targetSegment,
						TotalHops:     len(sourceSegment) + 1 + len(targetSegment),
					})
				}
			}
		}
	}

	return results, nil
}

func (s *Store) ReconfirmRoutes(ctx context.Context) error {
	return s.q.ReconfirmRoutes(ctx)
}

// extractFromNode returns the portion of a route starting at the given node.
func extractFromNode(hops []api.RouteHop, nodeID uuid.UUID) []api.RouteHop {
	for i, hop := range hops {
		if hop.NodeID == nodeID {
			return hops[i:]
		}
	}
	return hops
}

func toKnownRoutes(rows []sqlc.KnownRoute, nodes map[uuid.UUID]*api.ResolvedNode) []api.KnownRoute {
	items := make([]api.KnownRoute, 0, len(rows))
	for _, r := range rows {
		hops := make([]api.RouteHop, 0, len(r.NodeIds))
		for i, nodeID := range r.NodeIds {
			hop := api.RouteHop{
				NodeID: nodeID,
				Node:   nodes[nodeID],
			}
			if i < len(r.HashPrefix) {
				hop.HashBytes = hex.EncodeToString(r.HashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			ID:               r.ID,
			IATA:             r.Iata,
			HopCount:         r.HopCount,
			Hops:             hops,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
			ObservationCount: r.ObservationCount,
		})
	}
	return items
}

func collectNodeIDs(rows []sqlc.KnownRoute) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	var ids []uuid.UUID
	for _, r := range rows {
		for _, id := range r.NodeIds {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	return ids
}
