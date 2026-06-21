// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

func TestWebSocketEventDiscriminatorsStable(t *testing.T) {
	expected := map[hub.EventType]string{
		hub.EventPacketObservation: "packetObservation",
		hub.EventChannelMessage:    "channelMessage",
		hub.EventObserverStatus:    "observerStatus",
		hub.EventNodeUpdate:        "nodeUpdate",
	}

	for eventType, want := range expected {
		if got := string(eventType); got != want {
			t.Fatalf("event discriminator changed: got %q, want %q", got, want)
		}
	}
}

func TestClientMessageContractJSONFields(t *testing.T) {
	raw := []byte(`{
		"v": 1,
		"type": "subscribe",
		"id": "sub-1",
		"subscriptionId": "server-sub",
		"scope": {
			"iatas": ["YVR"],
			"regionIds": ["1"],
			"regionSlugs": ["pacific"],
			"payloadTypes": [4],
			"routeTypes": [2],
			"channelHashes": ["abcd"],
			"observerIds": ["observer-1"],
			"events": ["packetObservation", "channelMessage"]
		}
	}`)

	var msg clientMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal client message: %v", err)
	}
	if msg.V != 1 || msg.Type != "subscribe" || msg.ID != "sub-1" || msg.SubscriptionID != "server-sub" {
		t.Fatalf("unexpected client message header: %+v", msg)
	}
	if msg.Scope == nil {
		t.Fatal("scope was not decoded")
	}

	assertStringSlice(t, "iatas", msg.Scope.IATAs, []string{"YVR"})
	assertStringSlice(t, "regionIds", msg.Scope.RegionIDs, []string{"1"})
	assertStringSlice(t, "regionSlugs", msg.Scope.RegionSlugs, []string{"pacific"})
	assertUint8Slice(t, "payloadTypes", msg.Scope.PayloadTypes, []uint8{4})
	assertUint8Slice(t, "routeTypes", msg.Scope.RouteTypes, []uint8{2})
	assertStringSlice(t, "channelHashes", msg.Scope.ChannelHashes, []string{"abcd"})
	assertStringSlice(t, "observerIds", msg.Scope.ObserverIDs, []string{"observer-1"})
	if got, want := msg.Scope.Events, []hub.EventType{hub.EventPacketObservation, hub.EventChannelMessage}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestWebSocketProtocolDocMentionsContractTokens(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	docPath := filepath.Join(filepath.Dir(filename), "..", "..", "docs", "ws-protocol.md")
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read websocket protocol doc: %v", err)
	}
	doc := string(data)

	required := []string{
		"GET /ws",
		"`subscribe`",
		"`unsubscribe`",
		"`ping`",
		"`hello`",
		"`subscribed`",
		"`unsubscribed`",
		"`pong`",
		"`event`",
		"`lagged`",
		"`error`",
		"`packetObservation`",
		"`channelMessage`",
		"`observerStatus`",
		"`nodeUpdate`",
		"`iatas`",
		"`regionIds`",
		"`regionSlugs`",
		"`payloadTypes`",
		"`routeTypes`",
		"`channelHashes`",
		"`observerIds`",
		"`events`",
		"`/api/v1/live/backfill`",
	}
	for _, token := range required {
		if !strings.Contains(doc, token) {
			t.Fatalf("protocol doc missing %s", token)
		}
	}
}

func assertStringSlice(t *testing.T, name string, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s len = %d, want %d", name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q", name, i, got[i], want[i])
		}
	}
}

func assertUint8Slice(t *testing.T, name string, got []uint8, want []uint8) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s len = %d, want %d", name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %d, want %d", name, i, got[i], want[i])
		}
	}
}
