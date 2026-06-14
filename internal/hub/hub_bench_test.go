// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"
)

// samplePayload mimics a real packetObservation payload size.
var samplePayload = json.RawMessage(`{"packetHash":"a1b2c3d4","packet":{"payloadType":4,"payloadTypeName":"ADVERT","routeType":1,"observationCount":7},"observation":{"observerId":"6f1c","iata":"YVR","heardAt":1700000000000,"rssi":-92,"snr":7.5}}`)

// BenchmarkFramePerClient models the OLD behaviour: each connected client
// re-serialised the envelope with a reflection-based map[string]any.
func BenchmarkFramePerClient(b *testing.B) {
	const clients = 200
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for c := 0; c < clients; c++ {
			msg := map[string]any{
				"v":     1,
				"type":  "event",
				"event": EventPacketObservation,
				"data":  json.RawMessage(samplePayload),
			}
			_, _ = json.Marshal(msg)
		}
	}
}

// BenchmarkFrameOnce models the NEW behaviour: the hub frames the event a
// single time and shares the bytes across all clients.
func BenchmarkFrameOnce(b *testing.B) {
	const clients = 200
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		framed := frameEvent(EventPacketObservation, samplePayload)
		for c := 0; c < clients; c++ {
			_ = framed // each client writes the shared frame verbatim
		}
	}
}

// largeScope builds a region-sized subscription (many IATAs), the worst case
// for the old linear slices.Contains matcher.
func largeScope() Scope {
	iatas := make([]string, 300)
	for i := range iatas {
		iatas[i] = "X" + strconv.Itoa(i)
	}
	return Scope{Events: []EventType{EventPacketObservation}, IATAs: iatas}
}

// linearMatch replicates the old slices.Contains-based matcher for comparison.
func linearMatch(s Scope, e Event) bool {
	if len(s.Events) > 0 && !slices.Contains(s.Events, e.Type) {
		return false
	}
	if len(s.IATAs) > 0 && !slices.Contains(s.IATAs, e.IATA) {
		return false
	}
	return true
}

func BenchmarkMatchLinear(b *testing.B) {
	s := largeScope()
	e := Event{Type: EventPacketObservation, IATA: "X299"} // worst case: last entry
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = linearMatch(s, e)
	}
}

func BenchmarkMatchCompiled(b *testing.B) {
	cs := compileScope(largeScope())
	e := Event{Type: EventPacketObservation, IATA: "X299"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cs.matches(e)
	}
}
