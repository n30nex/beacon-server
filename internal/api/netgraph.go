// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// NetgraphSnapshot is the render-ready verified-route topology used by the
// experimental 3D Netgraph page.
type NetgraphSnapshot struct {
	ServerTime int64          `json:"serverTime"`
	Stats      NetgraphStats  `json:"stats"`
	Limits     NetgraphLimits `json:"limits"`
	Nodes      []NetgraphNode `json:"nodes"`
	Edges      []NetgraphEdge `json:"edges"`
}

type NetgraphStats struct {
	SourceRouteCount int64 `json:"sourceRouteCount"`
	MappedRouteCount int64 `json:"mappedRouteCount"`
	NodeCount        int64 `json:"nodeCount"`
	EdgeCount        int64 `json:"edgeCount"`
	ObservationCount int64 `json:"observationCount"`
	ActiveIATAs      int64 `json:"activeIatas"`
	TruncatedRoutes  bool  `json:"truncatedRoutes"`
	TruncatedNodes   bool  `json:"truncatedNodes"`
	TruncatedEdges   bool  `json:"truncatedEdges"`
}

type NetgraphLimits struct {
	RouteLimit int32 `json:"routeLimit"`
	NodeLimit  int32 `json:"nodeLimit"`
	EdgeLimit  int32 `json:"edgeLimit"`
}

type NetgraphNode struct {
	ID               uuid.UUID `json:"id"`
	Name             *string   `json:"name,omitempty"`
	PublicKey        string    `json:"publicKey"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	Latitude         *float64  `json:"lat,omitempty"`
	Longitude        *float64  `json:"lng,omitempty"`
	IsObserver       bool      `json:"isObserver"`
	IATAs            []string  `json:"iatas"`
	RouteIDs         []int64   `json:"routeIds"`
	RouteCount       int64     `json:"routeCount"`
	ObservationCount int64     `json:"observationCount"`
	FirstSeen        int64     `json:"firstSeen"`
	LastSeen         int64     `json:"lastSeen"`
}

type NetgraphEdge struct {
	ID               string    `json:"id"`
	FromNodeID       uuid.UUID `json:"fromNodeId"`
	ToNodeID         uuid.UUID `json:"toNodeId"`
	IATAs            []string  `json:"iatas"`
	RouteIDs         []int64   `json:"routeIds"`
	RouteCount       int64     `json:"routeCount"`
	ObservationCount int64     `json:"observationCount"`
	FirstSeen        int64     `json:"firstSeen"`
	LastSeen         int64     `json:"lastSeen"`
}
