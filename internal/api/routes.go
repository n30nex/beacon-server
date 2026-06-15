// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// RouteHop is a single resolved hop in a known route.
type RouteHop struct {
	NodeID    uuid.UUID     `json:"nodeId"`
	HashBytes string        `json:"hashBytes"`      // hex-encoded hash prefix
	Node      *ResolvedNode `json:"node,omitempty"` // populated when node details are available
}

// KnownRoute is a fully resolved path through the mesh where all hops
// have been confirmed as high confidence.
type KnownRoute struct {
	ID               int64      `json:"id"`
	IATA             string     `json:"iata"`
	HopCount         int32      `json:"hopCount"`
	Hops             []RouteHop `json:"hops"`
	FirstSeen        int64      `json:"firstSeen"` // epoch ms
	LastSeen         int64      `json:"lastSeen"`  // epoch ms
	ObservationCount int64      `json:"observationCount"`
}

// RouteNeighborhoodNode is a map-ready node included in a verified route neighborhood.
type RouteNeighborhoodNode struct {
	ID          uuid.UUID `json:"id"`
	Name        *string   `json:"name,omitempty"`
	PublicKey   string    `json:"publicKey"`
	Latitude    float64   `json:"lat"`
	Longitude   float64   `json:"lng"`
	HopDistance int32     `json:"hopDistance"`
}

// RouteNeighborhoodEdge is an adjacent-node link observed in one or more fully resolved known routes.
type RouteNeighborhoodEdge struct {
	FromNodeID       uuid.UUID `json:"fromNodeId"`
	ToNodeID         uuid.UUID `json:"toNodeId"`
	IATA             string    `json:"iata"`
	RouteIDs         []int64   `json:"routeIds"`
	HopDistance      int32     `json:"hopDistance"`
	LastSeen         int64     `json:"lastSeen"`
	ObservationCount int64     `json:"observationCount"`
}

// NodeRouteNeighborhood is a verified local route graph around a selected node.
type NodeRouteNeighborhood struct {
	NodeID  uuid.UUID               `json:"nodeId"`
	MaxHops int32                   `json:"maxHops"`
	Nodes   []RouteNeighborhoodNode `json:"nodes"`
	Edges   []RouteNeighborhoodEdge `json:"edges"`
}

// CrossIATAHop represents the boundary hop between two IATAs in a cross-IATA route.
type CrossIATAHop struct {
	FromNode ResolvedNode `json:"fromNode"` // last node in source IATA
	ToNode   ResolvedNode `json:"toNode"`   // first node in target IATA
	FromIATA string       `json:"fromIata"`
	ToIATA   string       `json:"toIata"`
	LastSeen int64        `json:"lastSeen"` // epoch ms
}

// CrossIATARoute is a route that crosses IATA boundaries.
type CrossIATARoute struct {
	SourceSegment []RouteHop   `json:"sourceSegment"` // route segment in source IATA
	CrossHop      CrossIATAHop `json:"crossHop"`      // the boundary hop
	TargetSegment []RouteHop   `json:"targetSegment"` // route segment in target IATA
	TotalHops     int          `json:"totalHops"`
}
