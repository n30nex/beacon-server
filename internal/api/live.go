// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "time"

// LivePacketObservation is the REST shape used by /live/backfill. It mirrors
// the WebSocket packetObservation data object so the frontend can replay missed
// observations through the same normalization path.
type LivePacketObservation struct {
	PacketHash  string                  `json:"packetHash"`
	Packet      LivePacketEnvelope      `json:"packet"`
	Observation LiveObservationEnvelope `json:"observation"`
}

type LivePacketEnvelope struct {
	PayloadType        uint8   `json:"payloadType"`
	PayloadTypeName    string  `json:"payloadTypeName"`
	RouteType          uint8   `json:"routeType"`
	RouteTypeName      string  `json:"routeTypeName"`
	RawHex             string  `json:"rawHex,omitempty"`
	IsFirstObservation bool    `json:"isFirstObservation"`
	ObservationCount   int64   `json:"observationCount"`
	Scope              *string `json:"scope,omitempty"`
}

type LiveObservationEnvelope struct {
	ID                int64            `json:"id"`
	ObserverID        string           `json:"observerId"`
	ObserverName      string           `json:"observerName"`
	IATA              string           `json:"iata"`
	HeardAt           int64            `json:"heardAt"`
	RSSI              int16            `json:"rssi"`
	SNR               float32          `json:"snr"`
	SourceBroker      string           `json:"sourceBroker"`
	PathBytes         string           `json:"pathBytes,omitempty"`
	PathLength        PacketPathLength `json:"pathLength"`
	PropagationTimeMs int32            `json:"propagationTimeMs"`
	ResolvedPath      []ResolvedHop    `json:"resolvedPath,omitempty"`
}

// LiveSummary is a compact, short-TTL overview for the Live console.
type LiveSummary struct {
	ServerTime          int64                  `json:"serverTime"`
	Since               int64                  `json:"since"`
	Until               int64                  `json:"until"`
	LatestObservationID int64                  `json:"latestObservationId"`
	PacketCount         int64                  `json:"packetCount"`
	ObservationCount    int64                  `json:"observationCount"`
	ActiveObservers     int64                  `json:"activeObservers"`
	PayloadMix          []PayloadBreakdownItem `json:"payloadMix"`
	RouteMix            []LiveRouteMixItem     `json:"routeMix"`
	TopIATAs            []LiveIATACount        `json:"topIatas"`
	TopObservers        []TopObserver          `json:"topObservers"`
}

type LiveRouteMixItem struct {
	RouteType     int16  `json:"routeType"`
	RouteTypeName string `json:"routeTypeName"`
	Count         int64  `json:"count"`
}

type LiveIATACount struct {
	IATA  string `json:"iata"`
	Count int64  `json:"count"`
}

// LiveBackfillFilter carries all filters for the durable observation backfill.
type LiveBackfillFilter struct {
	AfterObservationID int64
	PayloadType        int16
	RouteType          int16
	IATAs              []string
	Scope              string
	Limit              int32
}

// LiveSummaryFilter carries the aggregate filter for /live/summary.
type LiveSummaryFilter struct {
	IATAs []string
	Since time.Time
	Until time.Time
}
