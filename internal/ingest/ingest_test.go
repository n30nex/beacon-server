// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

func TestParseEnvelopeTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "UTC designator", value: "2026-07-11T23:31:39.661465Z", want: "2026-07-11T23:31:39.661465Z"},
		{name: "numeric offset", value: "2026-07-11T19:31:39.661465-04:00", want: "2026-07-11T23:31:39.661465Z"},
		{name: "naive microseconds", value: "2026-07-11T23:31:39.661465", want: "2026-07-11T23:31:39.661465Z"},
		{name: "naive seconds", value: "2026-07-11T23:31:39", want: "2026-07-11T23:31:39Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEnvelopeTimestamp(tt.value)
			if err != nil {
				t.Fatalf("parseEnvelopeTimestamp() error = %v", err)
			}
			if got.UTC().Format(time.RFC3339Nano) != tt.want {
				t.Fatalf("parseEnvelopeTimestamp() = %s, want %s", got.UTC().Format(time.RFC3339Nano), tt.want)
			}
		})
	}
}

func TestParseEnvelopeTimestampRejectsInvalidValue(t *testing.T) {
	if _, err := parseEnvelopeTimestamp("not-a-timestamp"); err == nil {
		t.Fatal("parseEnvelopeTimestamp() accepted an invalid timestamp")
	}
}

func TestParseNumber_Float(t *testing.T) {
	raw := json.RawMessage(`3.14`)
	if parseNumber(raw) != 3.14 {
		t.Errorf("expected 3.14, got %f", parseNumber(raw))
	}
}

func TestParseNumber_QuotedString(t *testing.T) {
	raw := json.RawMessage(`"42.5"`)
	if parseNumber(raw) != 42.5 {
		t.Errorf("expected 42.5, got %f", parseNumber(raw))
	}
}

func TestParseNumber_Empty(t *testing.T) {
	if parseNumber(json.RawMessage(``)) != 0 {
		t.Error("expected 0 for empty input")
	}
}

func TestParseNumber_Invalid(t *testing.T) {
	if parseNumber(json.RawMessage(`"notanumber"`)) != 0 {
		t.Error("expected 0 for unparseable string")
	}
}

func TestParseNumber_Integer(t *testing.T) {
	raw := json.RawMessage(`7`)
	if parseNumber(raw) != 7 {
		t.Errorf("expected 7, got %f", parseNumber(raw))
	}
}

func TestNormalizeObserverType_OrgPrefix(t *testing.T) {
	if normalizeObserverType("meshcore-dev/meshcore-ha") != "meshcore-ha" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcore-dev/meshcore-ha"))
	}
}

func TestNormalizeObserverType_VersionSuffix(t *testing.T) {
	if normalizeObserverType("meshcoretomqtt:1.1.0") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcoretomqtt:1.1.0"))
	}
}

func TestNormalizeObserverType_BuildSuffix(t *testing.T) {
	// org/name format — LastIndex strips to just the name portion
	if normalizeObserverType("meshcore-dev/meshcoretomqtt") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcore-dev/meshcoretomqtt"))
	}
}

func TestNormalizeObserverType_Plain(t *testing.T) {
	if normalizeObserverType("meshcoretomqtt") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcoretomqtt"))
	}
}

func TestNormalizeObserverType_Empty(t *testing.T) {
	if normalizeObserverType("") != "" {
		t.Error("expected empty string for empty input")
	}
}

func TestInferObserverType_SourceTakesPriority(t *testing.T) {
	got := inferObserverType("meshcore-dev/meshcore-ha", "some-version")
	if got != "meshcore-ha" {
		t.Errorf("expected meshcore-ha, got %s", got)
	}
}

func TestInferObserverType_FallsBackToClientVersion(t *testing.T) {
	got := inferObserverType("", "custom-firmware-1.0")
	if got != "custom-firmware-1.0" {
		t.Errorf("expected custom-firmware-1.0, got %s", got)
	}
}

func TestInferObserverType_BothEmpty(t *testing.T) {
	if inferObserverType("", "") != "" {
		t.Error("expected empty string when both inputs are empty")
	}
}

func TestUint32ToBytes_KnownValue(t *testing.T) {
	b := uint32ToBytes(0x01020304)
	// little-endian: least significant byte first
	expected := []byte{0x04, 0x03, 0x02, 0x01}
	for i, v := range expected {
		if b[i] != v {
			t.Errorf("byte %d: expected %02x, got %02x", i, v, b[i])
		}
	}
}

func TestUint32ToBytes_Zero(t *testing.T) {
	b := uint32ToBytes(0)
	if len(b) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(b))
	}
	for _, v := range b {
		if v != 0 {
			t.Error("expected all zero bytes")
		}
	}
}

func TestUint32ToBytes_RoundTrip(t *testing.T) {
	v := uint32(0xDEADBEEF)
	b := uint32ToBytes(v)
	got := binary.LittleEndian.Uint32(b)
	if got != v {
		t.Errorf("round trip failed: expected %x, got %x", v, got)
	}
}

func TestAdvertLocationCoordinates_RequiresLocationFlag(t *testing.T) {
	lat, lng := advertLocationCoordinates(meshcore.AdvertAppData{
		Lat: 1188916984,
		Lon: -122239380,
	}, meshcore.AdvertTypeRepeater)
	if lat != nil || lng != nil {
		t.Fatalf("expected no coordinates without AdvertLatLonMask, got %v %v", lat, lng)
	}
}

func TestAdvertLocationCoordinates_DropsOutOfRangeValues(t *testing.T) {
	lat, lng := advertLocationCoordinates(meshcore.AdvertAppData{
		Lat: 1188916984,
		Lon: -122239380,
	}, meshcore.AdvertTypeRepeater|meshcore.AdvertLatLonMask)
	if lat != nil || lng != nil {
		t.Fatalf("expected out-of-range coordinates to be dropped, got %v %v", lat, lng)
	}
}

func TestAdvertLocationCoordinates_DecodesValidLocation(t *testing.T) {
	lat, lng := advertLocationCoordinates(meshcore.AdvertAppData{
		Lat: 49420000,
		Lon: -123120000,
	}, meshcore.AdvertTypeRepeater|meshcore.AdvertLatLonMask)
	if lat == nil || lng == nil {
		t.Fatal("expected valid coordinates")
	}
	if *lat != 49.42 || *lng != -123.12 {
		t.Fatalf("unexpected coordinates: %f %f", *lat, *lng)
	}
}

func TestResolvedPathHops(t *testing.T) {
	name := "relay"
	lat := 45.42
	lng := -75.69
	nodeID := uuid.New()
	otherID := uuid.New()
	hops := resolvedPathHops(
		[][]byte{{0xaa}, {0xbb}, {0xcc}},
		map[string][]api.ResolvedPathEntry{
			"aa": {
				{NodeID: nodeID, Name: &name, PublicKey: []byte{0xaa, 0x01}, Latitude: &lat, Longitude: &lng},
			},
			"bb": {
				{NodeID: nodeID, PublicKey: []byte{0xbb, 0x01}},
				{NodeID: otherID, PublicKey: []byte{0xbb, 0x02}},
			},
		},
	)

	if got := len(hops); got != 3 {
		t.Fatalf("expected 3 hops, got %d", got)
	}
	if hops[0].Confidence != "high" || len(hops[0].Nodes) != 1 {
		t.Fatalf("expected first hop high confidence, got %+v", hops[0])
	}
	if hops[0].Nodes[0].ID != nodeID || hops[0].Nodes[0].PublicKey != "aa01" || hops[0].Nodes[0].Name == nil {
		t.Fatalf("unexpected first node: %+v", hops[0].Nodes[0])
	}
	if hops[1].Confidence != "ambiguous" || len(hops[1].Nodes) != 2 {
		t.Fatalf("expected second hop ambiguous, got %+v", hops[1])
	}
	if hops[2].Confidence != "none" || len(hops[2].Nodes) != 0 {
		t.Fatalf("expected third hop none, got %+v", hops[2])
	}
}
