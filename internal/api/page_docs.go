// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// AtlasReplayPage documents the generic Page[AtlasReplayPacket] response for Swagger.
type AtlasReplayPage struct {
	Items      []AtlasReplayPacket `json:"items"`
	NextCursor *int64              `json:"nextCursor,omitempty"`
	HasMore    bool                `json:"hasMore"`
}

// LiveBackfillPage documents the generic Page[LivePacketObservation] response for Swagger.
type LiveBackfillPage struct {
	Items      []LivePacketObservation `json:"items"`
	NextCursor *int64                  `json:"nextCursor,omitempty"`
	HasMore    bool                    `json:"hasMore"`
}

// ChannelSummaryPage documents the generic Page[ChannelSummary] response for Swagger.
type ChannelSummaryPage struct {
	Items      []ChannelSummary `json:"items"`
	NextCursor *int64           `json:"nextCursor,omitempty"`
	HasMore    bool             `json:"hasMore"`
}

// NodeSummaryPage documents the generic Page[NodeSummary] response for Swagger.
type NodeSummaryPage struct {
	Items      []NodeSummary `json:"items"`
	NextCursor *int64        `json:"nextCursor,omitempty"`
	HasMore    bool          `json:"hasMore"`
}

// PacketObservationSummaryPage documents the generic Page[PacketObservationSummary] response for Swagger.
type PacketObservationSummaryPage struct {
	Items      []PacketObservationSummary `json:"items"`
	NextCursor *int64                     `json:"nextCursor,omitempty"`
	HasMore    bool                       `json:"hasMore"`
}

// ObserverSummaryPage documents the generic Page[ObserverSummary] response for Swagger.
type ObserverSummaryPage struct {
	Items      []ObserverSummary `json:"items"`
	NextCursor *int64            `json:"nextCursor,omitempty"`
	HasMore    bool              `json:"hasMore"`
}

// AdvertObservationPage documents the generic Page[AdvertObservation] response for Swagger.
type AdvertObservationPage struct {
	Items      []AdvertObservation `json:"items"`
	NextCursor *int64              `json:"nextCursor,omitempty"`
	HasMore    bool                `json:"hasMore"`
}

// PacketDoc documents Packet for Swagger without exposing json.RawMessage,
// which the generator cannot resolve as a schema type.
type PacketDoc struct {
	PacketHash       string                    `json:"packetHash"`
	Header           PacketHeader              `json:"header"`
	TransportCodes   *PacketTransportCodes     `json:"transportCodes,omitempty"`
	OriginPubkey     *string                   `json:"originPubkey,omitempty"`
	ParsedPayload    map[string]interface{}    `json:"parsedPayload,omitempty"`
	RawPayload       string                    `json:"rawPayload"`
	Decrypted        bool                      `json:"decrypted"`
	ChannelHash      *string                   `json:"channelHash,omitempty"`
	Scope            *string                   `json:"scope,omitempty"`
	FirstHeardAt     int64                     `json:"firstHeardAt"`
	LastHeardAt      int64                     `json:"lastHeardAt"`
	FirstToLastMs    int64                     `json:"firstToLastMs"`
	ObservationCount int32                     `json:"observationCount"`
	ResolvedRoute    []ResolvedHop             `json:"resolvedRoute,omitempty"`
	Observations     []PacketObservationDetail `json:"observations"`
}
