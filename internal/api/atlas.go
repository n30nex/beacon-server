// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// AtlasWindow describes the time window used to build a regional story.
type AtlasWindow struct {
	Since int64 `json:"since"` // epoch ms
	Until int64 `json:"until"` // epoch ms
}

// AtlasIATA is an airport/location row with coordinates and activity for the
// selected window.
type AtlasIATA struct {
	IATA             string   `json:"iata"`
	DisplayName      *string  `json:"displayName,omitempty"`
	Lat              *float64 `json:"lat,omitempty"`
	Lng              *float64 `json:"lng,omitempty"`
	ObservationCount int64    `json:"observationCount"`
	UniquePackets    int64    `json:"uniquePackets"`
	ActiveObservers  int64    `json:"activeObservers"`
}

// AtlasStoryBeat is a compact narrative item shown in the Atlas story rail.
type AtlasStoryBeat struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	IATA   string `json:"iata,omitempty"`
	Value  int64  `json:"value,omitempty"`
	At     *int64 `json:"at,omitempty"`
}

// RegionAtlasSummary is the single payload used to paint the Atlas region
// overview without making the frontend stitch many independent calls together.
type RegionAtlasSummary struct {
	Region       Region                 `json:"region"`
	Window       AtlasWindow            `json:"window"`
	KPIs         StatsOverview          `json:"kpis"`
	IATAs        []AtlasIATA            `json:"iatas"`
	Hourly       []ObservationPoint     `json:"hourly"`
	NodeTypes    []NodeTypeCount        `json:"nodeTypes"`
	PayloadMix   []PayloadBreakdownItem `json:"payloadMix"`
	TopNodes     []TopNode              `json:"topNodes"`
	TopObservers []TopObserver          `json:"topObservers"`
	RadioPresets []RadioPreset          `json:"radioPresets"`
	Scopes       []ScopeSummary         `json:"scopes"`
	StoryBeats   []AtlasStoryBeat       `json:"storyBeats"`
}

// AtlasBriefingHealth summarizes the operator health signals that should be
// visible before an operator drills into Stats or Observers.
type AtlasBriefingHealth struct {
	Status            string `json:"status"`
	ServerTime        int64  `json:"serverTime"`
	StaleObservers    int64  `json:"staleObservers"`
	DegradedObservers int64  `json:"degradedObservers"`
	NoTelemetry       int64  `json:"noTelemetry"`
	HealthScore       int    `json:"healthScore"`
}

// AtlasBriefingRegion is one command-board comparison row for All/Western/Eastern.
type AtlasBriefingRegion struct {
	Slug                string  `json:"slug"`
	Name                string  `json:"name"`
	IATACount           int     `json:"iataCount"`
	PacketCount         int64   `json:"packetCount"`
	ObservationCount    int64   `json:"observationCount"`
	ActiveObservers     int64   `json:"activeObservers"`
	ActiveIATAs         int64   `json:"activeIatas"`
	ActiveNodes         int64   `json:"activeNodes"`
	RouteCount          int64   `json:"routeCount"`
	ObservationDeltaPct float64 `json:"observationDeltaPct"`
	TopIATA             string  `json:"topIata,omitempty"`
	HealthScore         int     `json:"healthScore"`
	URL                 string  `json:"url"`
}

// AtlasPriorityItem is a sorted investigation prompt for the Atlas briefing.
type AtlasPriorityItem struct {
	ID         string     `json:"id"`
	Kind       string     `json:"kind"`
	Severity   string     `json:"severity"`
	Title      string     `json:"title"`
	Detail     string     `json:"detail"`
	Region     string     `json:"region"`
	IATA       string     `json:"iata,omitempty"`
	NodeID     *uuid.UUID `json:"nodeId,omitempty"`
	ObserverID *uuid.UUID `json:"observerId,omitempty"`
	RouteID    *int64     `json:"routeId,omitempty"`
	Value      int64      `json:"value,omitempty"`
	At         *int64     `json:"at,omitempty"`
	URL        string     `json:"url"`
}

// AtlasHotspot is a map-ready IATA activity marker for the compact briefing map.
type AtlasHotspot struct {
	IATA             string   `json:"iata"`
	DisplayName      *string  `json:"displayName,omitempty"`
	Lat              *float64 `json:"lat,omitempty"`
	Lng              *float64 `json:"lng,omitempty"`
	ObservationCount int64    `json:"observationCount"`
	UniquePackets    int64    `json:"uniquePackets"`
	ActiveObservers  int64    `json:"activeObservers"`
	URL              string   `json:"url"`
}

// AtlasNotableRoute highlights a verified route corridor worth inspecting.
type AtlasNotableRoute struct {
	RouteID          int64    `json:"routeId"`
	IATA             string   `json:"iata"`
	HopCount         int32    `json:"hopCount"`
	NodeNames        []string `json:"nodeNames"`
	ObservationCount int64    `json:"observationCount"`
	LastSeen         int64    `json:"lastSeen"`
	URL              string   `json:"url"`
}

// AtlasBriefing is Beacon's prepared command-board payload for the Atlas tab.
type AtlasBriefing struct {
	ServerTime        int64                  `json:"serverTime"`
	Region            Region                 `json:"region"`
	Window            AtlasWindow            `json:"window"`
	Health            AtlasBriefingHealth    `json:"health"`
	Regions           []AtlasBriefingRegion  `json:"regions"`
	Priorities        []AtlasPriorityItem    `json:"priorities"`
	Hotspots          []AtlasHotspot         `json:"hotspots"`
	DegradedObservers []StatsObserverHealth  `json:"degradedObservers"`
	NotableRoutes     []AtlasNotableRoute    `json:"notableRoutes"`
	TopNodes          []TopNode              `json:"topNodes"`
	TopObservers      []TopObserver          `json:"topObservers"`
	PayloadMix        []PayloadBreakdownItem `json:"payloadMix"`
	RouteMix          []LiveRouteMixItem     `json:"routeMix"`
	Scopes            []ScopeSummary         `json:"scopes"`
}

// AtlasPathPoint is a map-ready coordinate in a replay packet path.
type AtlasPathPoint struct {
	Kind  string  `json:"kind"` // origin or observer
	Label *string `json:"label,omitempty"`
	IATA  string  `json:"iata,omitempty"`
	Lat   float64 `json:"lat"`
	Lng   float64 `json:"lng"`
	At    *int64  `json:"at,omitempty"`
}

// AtlasReplayPacket is a packet list item enriched with regional IATA and
// coordinate data for cinematic map playback.
type AtlasReplayPacket struct {
	PacketHash       string                `json:"packetHash"`
	PayloadType      int16                 `json:"payloadType"`
	PayloadTypeName  string                `json:"payloadTypeName"`
	RouteType        int16                 `json:"routeType"`
	RouteTypeName    string                `json:"routeTypeName"`
	Scope            *string               `json:"scope,omitempty"`
	FirstHeardAt     int64                 `json:"firstHeardAt"`
	LastHeardAt      int64                 `json:"lastHeardAt"`
	ObservationCount int32                 `json:"observationCount"`
	IATAs            []string              `json:"iatas"`
	LatestObserver   *PacketLatestObserver `json:"latestObserver,omitempty"`
	Origin           *ResolvedNode         `json:"origin,omitempty"`
	Path             []AtlasPathPoint      `json:"path"`
}
