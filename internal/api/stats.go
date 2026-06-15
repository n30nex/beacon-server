// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"time"

	"github.com/google/uuid"
)

// RadioPreset represents a unique radio configuration observed in a given IATA,
// aggregated from both observer status messages and node adverts.
type RadioPreset struct {
	Preset     string `json:"preset"` // "freqMhz,bwKhz,sf" e.g. "910.525,62.5,7"
	IATA       string `json:"iata"`
	SourceType string `json:"sourceType"` // "observer" or "node"
	Count      int64  `json:"count"`      // number of observers or nodes on this preset in this IATA
}

// StatsOverview is the top-level network summary for the overview endpoint.
type StatsOverview struct {
	TotalPackets      int64 `json:"totalPackets"`
	TotalObservations int64 `json:"totalObservations"`
	ActiveObservers   int64 `json:"activeObservers"`
	ActiveIATAs       int64 `json:"activeIatas"`
	WindowHours       int   `json:"windowHours"` // always 24 for now
}

// ObservationPoint is a single time-bucketed observation count for charting.
type ObservationPoint struct {
	Hour             int64  `json:"hour"` // epoch ms, start of the 1-hour bucket
	IATA             string `json:"iata"`
	ObservationCount int64  `json:"observationCount"`
	UniquePackets    int64  `json:"uniquePackets"`
	ActiveObservers  int64  `json:"activeObservers"`
}

// PayloadBreakdownItem is a single payload type with its observation count.
type PayloadBreakdownItem struct {
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	Count           int64  `json:"count"`
}

// ScopeStats represents aggregate statistics for a single transport scope.
type ScopeStats struct {
	Name          string `json:"name"`          // normalized scope name e.g. "#bc"
	PacketCount   int64  `json:"packetCount"`   // distinct packets matched to this scope
	ObserverCount int64  `json:"observerCount"` // distinct observers that forwarded packets in this scope
	NodeCount     int64  `json:"nodeCount"`     // distinct nodes with this as their default scope
}

// TopNode is a node ranked by observation count from the mv_top_nodes_by_iata materialized view.
type TopNode struct {
	NodeID           uuid.UUID `json:"nodeId"`
	NodeName         *string   `json:"nodeName,omitempty"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	LastHeard        int64     `json:"lastHeard"` // epoch ms
}

// TopObserver is an observer ranked by observation count.
type TopObserver struct {
	ObserverID       uuid.UUID `json:"observerId"`
	DisplayName      *string   `json:"displayName,omitempty"`
	ObserverType     *string   `json:"observerType,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
}

// NodeTypeCount shows the count of nodes of a given type with the type name
type NodeTypeCount struct {
	NodeType     int16  `json:"nodeType"`
	NodeTypeName string `json:"nodeTypeName"`
	Count        int64  `json:"count"`
}

// StatsFilter carries the common geography and time-window inputs used by the
// operator console aggregate endpoints.
type StatsFilter struct {
	IATAs  []string
	Since  time.Time
	Until  time.Time
	Bucket string
	Limit  int32
}

// StatsObserverHealthFilter extends StatsFilter with the freshness threshold
// used to classify stale observers.
type StatsObserverHealthFilter struct {
	StatsFilter
	StaleAfter time.Duration
}

// StatsObserverCompareFilter scopes an observer comparison payload.
type StatsObserverCompareFilter struct {
	StatsObserverHealthFilter
	ObserverIDs []uuid.UUID
}

// StatsWindow echoes the normalized time window used to build a response.
type StatsWindow struct {
	Since  int64  `json:"since"`
	Until  int64  `json:"until"`
	Bucket string `json:"bucket"`
}

// StatsHealthSummary is a compact count of current operator-health flags.
type StatsHealthSummary struct {
	TotalObservers int64 `json:"totalObservers"`
	StaleObservers int64 `json:"staleObservers"`
	LowBattery     int64 `json:"lowBattery"`
	HighNoise      int64 `json:"highNoise"`
	HighAirtime    int64 `json:"highAirtime"`
	QueueBacklog   int64 `json:"queueBacklog"`
	ReceiveErrors  int64 `json:"receiveErrors"`
	NoTelemetry    int64 `json:"noTelemetry"`
}

// StatsSummary is the prepared data model for the Stats overview tab.
type StatsSummary struct {
	ServerTime   int64                  `json:"serverTime"`
	Window       StatsWindow            `json:"window"`
	Overview     StatsOverview          `json:"overview"`
	Live         LiveSummary            `json:"live"`
	NodeTypes    []NodeTypeCount        `json:"nodeTypes"`
	PayloadMix   []PayloadBreakdownItem `json:"payloadMix"`
	RouteMix     []LiveRouteMixItem     `json:"routeMix"`
	TopIATAs     []LiveIATACount        `json:"topIatas"`
	TopObservers []TopObserver          `json:"topObservers"`
	TopNodes     []TopNode              `json:"topNodes"`
	RadioPresets []RadioPreset          `json:"radioPresets"`
	Scopes       []ScopeStats           `json:"scopes"`
	Health       StatsHealthSummary     `json:"health"`
}

// StatsTrendPoint is a bucketed activity point for a region/IATA row.
type StatsTrendPoint struct {
	T                int64 `json:"t"`
	PacketCount      int64 `json:"packetCount"`
	ObservationCount int64 `json:"observationCount"`
	ActiveObservers  int64 `json:"activeObservers"`
}

// StatsRegionRow represents one IATA in the Regions tab comparison table.
type StatsRegionRow struct {
	IATA               string            `json:"iata"`
	PacketCount        int64             `json:"packetCount"`
	ObservationCount   int64             `json:"observationCount"`
	ActiveObservers    int64             `json:"activeObservers"`
	ActiveNodes        int64             `json:"activeNodes"`
	TopPayloadType     int16             `json:"topPayloadType"`
	TopPayloadTypeName string            `json:"topPayloadTypeName"`
	TopPayloadCount    int64             `json:"topPayloadCount"`
	TopRouteType       int16             `json:"topRouteType"`
	TopRouteTypeName   string            `json:"topRouteTypeName"`
	TopRouteCount      int64             `json:"topRouteCount"`
	LastHeard          int64             `json:"lastHeard"`
	Trend              []StatsTrendPoint `json:"trend"`
}

// StatsRegions is the response envelope for /stats/regions.
type StatsRegions struct {
	ServerTime int64            `json:"serverTime"`
	Window     StatsWindow      `json:"window"`
	Items      []StatsRegionRow `json:"items"`
}

// StatsPayloadBucket is a time-bucketed payload count.
type StatsPayloadBucket struct {
	T               int64  `json:"t"`
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	Count           int64  `json:"count"`
}

// StatsRouteBucket is a time-bucketed route count.
type StatsRouteBucket struct {
	T             int64  `json:"t"`
	RouteType     int16  `json:"routeType"`
	RouteTypeName string `json:"routeTypeName"`
	Count         int64  `json:"count"`
}

// StatsPayloads is the response envelope for /stats/payloads.
type StatsPayloads struct {
	ServerTime      int64                  `json:"serverTime"`
	Window          StatsWindow            `json:"window"`
	Totals          []PayloadBreakdownItem `json:"totals"`
	RouteTotals     []LiveRouteMixItem     `json:"routeTotals"`
	PayloadTimeline []StatsPayloadBucket   `json:"payloadTimeline"`
	RouteTimeline   []StatsRouteBucket     `json:"routeTimeline"`
}

// StatsObserverHealthFlags exposes the operator-health classification for one observer.
type StatsObserverHealthFlags struct {
	Stale         bool `json:"stale"`
	LowBattery    bool `json:"lowBattery"`
	HighNoise     bool `json:"highNoise"`
	HighAirtime   bool `json:"highAirtime"`
	QueueBacklog  bool `json:"queueBacklog"`
	ReceiveErrors bool `json:"receiveErrors"`
	NoTelemetry   bool `json:"noTelemetry"`
}

// StatsObserverHealth is one observer row enriched with latest telemetry and flags.
type StatsObserverHealth struct {
	ObserverID       uuid.UUID                `json:"observerId"`
	DisplayName      *string                  `json:"displayName,omitempty"`
	ObserverType     *string                  `json:"observerType,omitempty"`
	IATA             string                   `json:"iata"`
	Status           string                   `json:"status"`
	LastHeard        int64                    `json:"lastHeard"`
	ObservationCount int64                    `json:"observationCount"`
	TelemetryAt      *int64                   `json:"telemetryAt,omitempty"`
	HasTelemetry     bool                     `json:"hasTelemetry"`
	BatteryMV        *int32                   `json:"batteryMv,omitempty"`
	NoiseFloorDB     *float32                 `json:"noiseFloorDb,omitempty"`
	AirtimeTxPct     *float32                 `json:"airtimeTxPct,omitempty"`
	AirtimeRxPct     *float32                 `json:"airtimeRxPct,omitempty"`
	QueueLength      *int32                   `json:"queueLength,omitempty"`
	ReceiveErrors    *int32                   `json:"receiveErrors,omitempty"`
	HealthScore      int                      `json:"healthScore"`
	Flags            StatsObserverHealthFlags `json:"flags"`
}

// StatsObserverHealthResponse is the response envelope for /stats/observer-health.
type StatsObserverHealthResponse struct {
	ServerTime int64                 `json:"serverTime"`
	Window     StatsWindow           `json:"window"`
	Summary    StatsHealthSummary    `json:"summary"`
	Items      []StatsObserverHealth `json:"items"`
}

// StatsObserverCompareItem is a prepared per-observer comparison row.
type StatsObserverCompareItem struct {
	StatsObserverHealth
	PacketCount      int64                  `json:"packetCount"`
	PayloadMix       []PayloadBreakdownItem `json:"payloadMix"`
	RouteMix         []LiveRouteMixItem     `json:"routeMix"`
	AvgNoiseFloorDB  *float32               `json:"avgNoiseFloorDb,omitempty"`
	AvgAirtimeTxPct  *float32               `json:"avgAirtimeTxPct,omitempty"`
	AvgAirtimeRxPct  *float32               `json:"avgAirtimeRxPct,omitempty"`
	AvgBatteryMV     *int32                 `json:"avgBatteryMv,omitempty"`
	MaxQueueLength   *int32                 `json:"maxQueueLength,omitempty"`
	ReceiveErrorsSum int64                  `json:"receiveErrorsSum"`
}

// StatsObserverComparePoint is a bucketed compare point for one observer.
type StatsObserverComparePoint struct {
	T                int64     `json:"t"`
	ObserverID       uuid.UUID `json:"observerId"`
	PacketCount      int64     `json:"packetCount"`
	ObservationCount int64     `json:"observationCount"`
	NoiseFloorDB     *float32  `json:"noiseFloorDb,omitempty"`
	AirtimeTxPct     *float32  `json:"airtimeTxPct,omitempty"`
	AirtimeRxPct     *float32  `json:"airtimeRxPct,omitempty"`
	QueueLength      *int32    `json:"queueLength,omitempty"`
	ReceiveErrors    int64     `json:"receiveErrors"`
	BatteryMV        *int32    `json:"batteryMv,omitempty"`
}

// StatsObserverCompare is the response envelope for /stats/observer-compare.
type StatsObserverCompare struct {
	ServerTime  int64                       `json:"serverTime"`
	Window      StatsWindow                 `json:"window"`
	SharedIATAs []string                    `json:"sharedIatas"`
	Items       []StatsObserverCompareItem  `json:"items"`
	Series      []StatsObserverComparePoint `json:"series"`
}

// StatsRFHealthIATA summarizes RF health for one IATA.
type StatsRFHealthIATA struct {
	IATA            string   `json:"iata"`
	ActiveObservers int64    `json:"activeObservers"`
	StaleObservers  int64    `json:"staleObservers"`
	AvgNoiseFloorDB *float32 `json:"avgNoiseFloorDb,omitempty"`
	MaxAirtimePct   *float32 `json:"maxAirtimePct,omitempty"`
	MaxQueueLength  *int32   `json:"maxQueueLength,omitempty"`
	ReceiveErrors   int64    `json:"receiveErrors"`
	LowBattery      int64    `json:"lowBattery"`
	HealthScore     int      `json:"healthScore"`
}

// StatsRFHealthPoint is a telemetry bucket for RF-health charts.
type StatsRFHealthPoint struct {
	T             int64    `json:"t"`
	IATA          string   `json:"iata"`
	NoiseFloorDB  *float32 `json:"noiseFloorDb,omitempty"`
	AirtimeTxPct  *float32 `json:"airtimeTxPct,omitempty"`
	AirtimeRxPct  *float32 `json:"airtimeRxPct,omitempty"`
	QueueLength   *int32   `json:"queueLength,omitempty"`
	ReceiveErrors int64    `json:"receiveErrors"`
	BatteryMV     *int32   `json:"batteryMv,omitempty"`
}

// StatsRFHealth is the response envelope for /stats/rf-health.
type StatsRFHealth struct {
	ServerTime   int64                 `json:"serverTime"`
	Window       StatsWindow           `json:"window"`
	Summary      StatsHealthSummary    `json:"summary"`
	ByIATA       []StatsRFHealthIATA   `json:"byIata"`
	TopOffenders []StatsObserverHealth `json:"topOffenders"`
	Series       []StatsRFHealthPoint  `json:"series"`
}
