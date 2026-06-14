// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"encoding/json"
	"testing"
)

// TestFrameEvent verifies the wire frame produced once by the hub is valid
// JSON and carries exactly the envelope the WS protocol promises:
// {"v":1,"type":"event","event":<type>,"data":<payload>}.
func TestFrameEvent(t *testing.T) {
	payload := json.RawMessage(`{"packetHash":"ab","observationCount":3}`)
	framed := frameEvent(EventPacketObservation, payload)

	var got struct {
		V     int             `json:"v"`
		Type  string          `json:"type"`
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(framed, &got); err != nil {
		t.Fatalf("framed event is not valid JSON: %v (%s)", err, framed)
	}
	if got.V != 1 || got.Type != "event" {
		t.Errorf("unexpected envelope: v=%d type=%q", got.V, got.Type)
	}
	if got.Event != string(EventPacketObservation) {
		t.Errorf("event = %q, want %q", got.Event, EventPacketObservation)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("data = %s, want %s", got.Data, payload)
	}
}

// TestFrameEvent_NilPayload ensures a missing payload still yields valid JSON.
func TestFrameEvent_NilPayload(t *testing.T) {
	framed := frameEvent(EventObserverStatus, nil)
	if !json.Valid(framed) {
		t.Fatalf("framed event with nil payload is invalid JSON: %s", framed)
	}
}

func TestScopeMatches_EmptyScope(t *testing.T) {
	// empty scope matches everything — no filters means no restrictions
	s := Scope{}
	e := Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 4}
	if !scopeMatches(s, e) {
		t.Error("empty scope should match all events")
	}
}

func TestScopeMatches_EventFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventNodeUpdate}}
	if !scopeMatches(s, Event{Type: EventNodeUpdate, IATA: "YVR"}) {
		t.Error("expected nodeUpdate to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR"}) {
		t.Error("expected packetObservation not to match")
	}
}

func TestScopeMatches_IATAFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventPacketObservation}, IATAs: []string{"YVR", "YYJ"}}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR"}) {
		t.Error("expected YVR to match")
	}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYJ"}) {
		t.Error("expected YYJ to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYC"}) {
		t.Error("expected YYC not to match")
	}
}

func TestScopeMatches_PayloadTypeFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventPacketObservation}, PayloadTypes: []uint8{4}}
	if !scopeMatches(s, Event{Type: EventPacketObservation, PayloadType: 4}) {
		t.Error("expected payload type 4 to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, PayloadType: 5}) {
		t.Error("expected payload type 5 not to match")
	}
}

func TestScopeMatches_ChannelHashFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventChannelMessage}, ChannelHashes: []string{"ab"}}
	if !scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "ab"}) {
		t.Error("expected channel hash ab to match")
	}
	if scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "cd"}) {
		t.Error("expected channel hash cd not to match")
	}
}

func TestScopeMatches_AllFiltersPass(t *testing.T) {
	s := Scope{
		Events:       []EventType{EventPacketObservation},
		IATAs:        []string{"YVR"},
		PayloadTypes: []uint8{4},
	}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 4}) {
		t.Error("expected all-matching event to pass")
	}
}

func TestScopeMatches_OneFilterFails(t *testing.T) {
	s := Scope{
		Events:       []EventType{EventPacketObservation},
		IATAs:        []string{"YVR"},
		PayloadTypes: []uint8{4},
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYC", PayloadType: 4}) {
		t.Error("expected wrong IATA to fail")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 5}) {
		t.Error("expected wrong payload type to fail")
	}
}
