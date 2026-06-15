// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// NodeNeighbor represents a neighboring node relationship observed in a given IATA.
type NodeNeighbor struct {
	ID               uuid.UUID `json:"id"`
	Name             *string   `json:"name,omitempty"`
	PublicKey        string    `json:"publicKey"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	Latitude         *float64  `json:"lat,omitempty"`
	Longitude        *float64  `json:"lng,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	FirstSeen        int64     `json:"firstSeen"` // epoch ms
	LastSeen         int64     `json:"lastSeen"`  // epoch ms
}

// NodeIATA represents a single IATA code and the last time the node was heard there.
type NodeIATA struct {
	IATA      string `json:"iata"`
	LastHeard int64  `json:"lastHeard"` // epoch ms
}

// NodeSummary is the minimal node representation used in list responses.
type NodeSummary struct {
	ID                 uuid.UUID  `json:"id"`
	PublicKey          string     `json:"publicKey"` // hex-encoded Ed25519 public key
	NodeType           int16      `json:"nodeType"`  // 1=companion, 2=repeater, 3=room_server, 4=sensor
	NodeTypeName       string     `json:"nodeTypeName"`
	Name               *string    `json:"name,omitempty"`
	IsObserver         bool       `json:"isObserver"`             // true if this node is also a known observer
	ObserverID         *uuid.UUID `json:"observerId,omitempty"`   // UUID of the associated observer row, if any
	Latitude           *float64   `json:"lat,omitempty"`          // decimal degrees, from advert AppData
	Longitude          *float64   `json:"lng,omitempty"`          // decimal degrees, from advert AppData
	Radio              *string    `json:"radio,omitempty"`        // shorthand: "freqMhz,bwKhz,sf" e.g. "910.5,62.5,7"
	IATAs              []NodeIATA `json:"iatas"`                  // IATAs where this node has been heard, with last heard timestamps
	DefaultScope       *string    `json:"defaultScope,omitempty"` // most recently matched transport scope name e.g. "#bc"
	KnownNeighborCount int64      `json:"knownNeighborCount"`
}

// Node is the full node representation including firmware capability flags,
// location source, and timing metadata.
type Node struct {
	NodeSummary
	LocationSource          *string        `json:"locationSource,omitempty"`     // "advert" or "manual"
	LastAdvertAt            *int64         `json:"lastAdvertAt,omitempty"`       // epoch ms, nil if no advert received
	SupportsMultibytePaths  bool           `json:"supportsMultibytePaths"`       // firmware >= 1.14.0; detected via path hash size
	SupportsMultibyteTraces bool           `json:"supportsMultibyteTraces"`      // firmware >= 1.11.0; detected via trace hash size
	MinFirmwareVersion      *string        `json:"minFirmwareVersion,omitempty"` // derived from capability flags
	FirstSeen               int64          `json:"firstSeen"`                    // epoch ms
	LastSeen                int64          `json:"lastSeen"`                     // epoch ms
	Metadata                any            `json:"metadata,omitempty"`           // raw JSONB metadata
	Neighbors               []NodeNeighbor `json:"neighbors"`
}

// NodeAnalyticsFilter scopes node analytics to a time window and optional IATA set.
type NodeAnalyticsFilter struct {
	Since time.Time
	Until time.Time
	IATAs []string
}

// NodeAnalyticsKPI is the compact top-line activity summary for a node.
type NodeAnalyticsKPI struct {
	PacketCount      int64    `json:"packetCount"`
	ObservationCount int64    `json:"observationCount"`
	ActiveObservers  int64    `json:"activeObservers"`
	ActiveIATAs      int64    `json:"activeIatas"`
	FirstHeardAt     *int64   `json:"firstHeardAt,omitempty"`
	LastHeardAt      *int64   `json:"lastHeardAt,omitempty"`
	AvgSNR           *float64 `json:"avgSnr,omitempty"`
	AvgRSSI          *float64 `json:"avgRssi,omitempty"`
	AvgHopCount      *float64 `json:"avgHopCount,omitempty"`
}

// NodeAnalyticsCount is a labeled aggregate bucket.
type NodeAnalyticsCount struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Count int64  `json:"count"`
}

// NodeActivityPoint is an hourly node activity bucket.
type NodeActivityPoint struct {
	Timestamp    int64 `json:"timestamp"`
	Packets      int64 `json:"packets"`
	Observations int64 `json:"observations"`
}

// NodeSignalBucket is a small distribution bucket for SNR/RSSI/hops.
type NodeSignalBucket struct {
	Bucket string `json:"bucket"`
	Count  int64  `json:"count"`
}

// NodeAnalyticsPeer is a neighboring node ranked for analytics.
type NodeAnalyticsPeer struct {
	ID               uuid.UUID `json:"id"`
	Name             *string   `json:"name,omitempty"`
	PublicKey        string    `json:"publicKey"`
	NodeTypeName     string    `json:"nodeTypeName"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	LastSeen         int64     `json:"lastSeen"`
}

// NodeAnalytics is the Beacon-native CoreScope parity payload for per-node analytics.
type NodeAnalytics struct {
	NodeID       uuid.UUID            `json:"nodeId"`
	Since        int64                `json:"since"`
	Until        int64                `json:"until"`
	KPIs         NodeAnalyticsKPI     `json:"kpis"`
	PayloadMix   []NodeAnalyticsCount `json:"payloadMix"`
	RouteMix     []NodeAnalyticsCount `json:"routeMix"`
	IATAMix      []NodeAnalyticsCount `json:"iataMix"`
	Hourly       []NodeActivityPoint  `json:"hourly"`
	SNRBuckets   []NodeSignalBucket   `json:"snrBuckets"`
	RSSIBuckets  []NodeSignalBucket   `json:"rssiBuckets"`
	HopBuckets   []NodeSignalBucket   `json:"hopBuckets"`
	TopObservers []NodeAnalyticsCount `json:"topObservers"`
	TopPeers     []NodeAnalyticsPeer  `json:"topPeers"`
}

// NodeReachHopBucket summarizes verified-route reach at one graph distance.
type NodeReachHopBucket struct {
	HopDistance      int32 `json:"hopDistance"`
	NodeCount        int64 `json:"nodeCount"`
	EdgeCount        int64 `json:"edgeCount"`
	RouteCount       int64 `json:"routeCount"`
	ObservationCount int64 `json:"observationCount"`
}

// NodeReachNode is a ranked reachable node from verified known_routes edges.
type NodeReachNode struct {
	ID               uuid.UUID `json:"id"`
	Name             *string   `json:"name,omitempty"`
	PublicKey        string    `json:"publicKey"`
	HopDistance      int32     `json:"hopDistance"`
	IATAs            []string  `json:"iatas"`
	RouteCount       int64     `json:"routeCount"`
	ObservationCount int64     `json:"observationCount"`
	LastSeen         int64     `json:"lastSeen"`
}

// NodeReachIATA summarizes verified-route reach contribution by IATA.
type NodeReachIATA struct {
	IATA             string `json:"iata"`
	NodeCount        int64  `json:"nodeCount"`
	EdgeCount        int64  `json:"edgeCount"`
	RouteCount       int64  `json:"routeCount"`
	ObservationCount int64  `json:"observationCount"`
	LastSeen         int64  `json:"lastSeen"`
}

// NodeReach is a CoreScope-style verified route-reach summary for a selected node.
type NodeReach struct {
	NodeID           uuid.UUID            `json:"nodeId"`
	MaxHops          int32                `json:"maxHops"`
	GeneratedAt      int64                `json:"generatedAt"`
	ReachableNodes   int64                `json:"reachableNodes"`
	VerifiedEdges    int64                `json:"verifiedEdges"`
	RouteCount       int64                `json:"routeCount"`
	ObservationCount int64                `json:"observationCount"`
	HopBuckets       []NodeReachHopBucket `json:"hopBuckets"`
	TopNodes         []NodeReachNode      `json:"topNodes"`
	TopIATAs         []NodeReachIATA      `json:"topIatas"`
}

// NodeTypeName returns a human-readable name for a node type integer.
// NOTE: truncation is fine here until there are at least over 200 types of node
func NodeTypeName(t int16) string {
	switch byte(t) {
	case meshcore.AdvertTypeChat:
		return "companion"
	case meshcore.AdvertTypeRepeater:
		return "repeater"
	case meshcore.AdvertTypeRoom:
		return "room_server"
	case meshcore.AdvertTypeSensor:
		return "sensor"
	default:
		return "unknown"
	}
}

// NodeTypeFromString returns the integer node type for a given name.
// Returns 0 (no filter) if the string is empty or unrecognized.
func NodeTypeFromString(s string) int16 {
	switch strings.ToLower(s) {
	case "companion", "chat":
		return int16(meshcore.AdvertTypeChat)
	case "repeater":
		return int16(meshcore.AdvertTypeRepeater)
	case "room_server", "roomserver", "room-server", "room":
		return int16(meshcore.AdvertTypeRoom)
	case "sensor":
		return int16(meshcore.AdvertTypeSensor)
	default:
		return 0
	}
}
