// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

type liveRegionLookupReader struct {
	stubReader
	called bool
}

type liveBackfillCaptureReader struct {
	stubReader
	filter api.LiveBackfillFilter
	called bool
	page   api.Page[api.LivePacketObservation]
}

type liveSummaryFixtureReader struct {
	stubReader
	filter api.LiveSummaryFilter
}

func (r *liveRegionLookupReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	r.called = true
	return nil, nil
}

func (r *liveBackfillCaptureReader) ListLiveBackfill(ctx context.Context, filter api.LiveBackfillFilter) (api.Page[api.LivePacketObservation], error) {
	r.called = true
	r.filter = filter
	return r.page, nil
}

func (r *liveSummaryFixtureReader) GetLiveSummary(ctx context.Context, filter api.LiveSummaryFilter) (*api.LiveSummary, error) {
	r.filter = filter
	return &api.LiveSummary{
		ServerTime:          1710000005000,
		Since:               filter.Since.UnixMilli(),
		Until:               filter.Until.UnixMilli(),
		LatestObservationID: 902,
		PacketCount:         3,
		ObservationCount:    7,
		ActiveObservers:     2,
		PayloadMix: []api.PayloadBreakdownItem{{
			PayloadType:     4,
			PayloadTypeName: "advert",
			Count:           5,
		}},
		RouteMix: []api.LiveRouteMixItem{{
			RouteType:     1,
			RouteTypeName: "FLOOD",
			Count:         7,
		}},
		TopIATAs: []api.LiveIATACount{{IATA: "YVR", Count: 7}},
		TopObservers: []api.TopObserver{{
			ObserverID:       uuid.MustParse("00000000-0000-0000-0000-000000000901"),
			DisplayName:      strPtr("West Roof"),
			ObserverType:     strPtr("mqtt"),
			IATA:             "YVR",
			ObservationCount: 7,
		}},
	}, nil
}

func TestListLiveBackfill_MissingAfterID(t *testing.T) {
	r := LiveRouter(stubReader{})
	req := httptest.NewRequest(http.MethodGet, "/backfill", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestListLiveBackfill_InvalidRouteType(t *testing.T) {
	r := LiveRouter(stubReader{})
	req := httptest.NewRequest(http.MethodGet, "/backfill?afterObservationId=1&routeType=nope", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestListLiveBackfill_ZeroCursorSeedsLatest(t *testing.T) {
	reader := &liveBackfillCaptureReader{}
	r := LiveRouter(reader)
	req := httptest.NewRequest(http.MethodGet, "/backfill?afterObservationId=0&limit=12", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !reader.called {
		t.Fatal("expected reader to be called")
	}
	if reader.filter.AfterObservationID != 0 {
		t.Fatalf("expected cursor 0, got %d", reader.filter.AfterObservationID)
	}
	if reader.filter.Limit != 12 {
		t.Fatalf("expected limit 12, got %d", reader.filter.Limit)
	}
}

func TestListLiveBackfill_ResponseShapeFixture(t *testing.T) {
	next := int64(901)
	scope := "#bc"
	reader := &liveBackfillCaptureReader{
		page: api.Page[api.LivePacketObservation]{
			Items: []api.LivePacketObservation{{
				PacketHash: "aabbcc",
				Packet: api.LivePacketEnvelope{
					PayloadType:        4,
					PayloadTypeName:    "advert",
					RouteType:          1,
					RouteTypeName:      "FLOOD",
					RawHex:             "11aabb",
					IsFirstObservation: true,
					ObservationCount:   2,
					Scope:              &scope,
				},
				Observation: api.LiveObservationEnvelope{
					ID:           900,
					ObserverID:   "00000000-0000-0000-0000-000000000901",
					ObserverName: "West Roof",
					IATA:         "YVR",
					HeardAt:      1710000001000,
					RSSI:         -71,
					SNR:          8.5,
					SourceBroker: "mqtt1",
					PathBytes:    "aabb",
					PathLength: api.PacketPathLength{
						Raw:      "11",
						HashSize: 1,
						HopCount: 1,
					},
					PropagationTimeMs: 25,
				},
			}},
			NextCursor: &next,
			HasMore:    true,
		},
	}
	r := LiveRouter(reader)
	req := httptest.NewRequest(http.MethodGet, "/backfill?afterObservationId=899&limit=10&payloadType=4&routeType=1&iatas=yvr&scope=%23bc", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if reader.filter.AfterObservationID != 899 || reader.filter.PayloadType != 4 || reader.filter.RouteType != 1 || reader.filter.Scope != "#bc" {
		t.Fatalf("unexpected filter: %#v", reader.filter)
	}
	var body api.Page[api.LivePacketObservation]
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.HasMore || body.NextCursor == nil || *body.NextCursor != next {
		t.Fatalf("unexpected page envelope: %#v", body)
	}
	if len(body.Items) != 1 || body.Items[0].PacketHash != "aabbcc" || body.Items[0].Packet.Scope == nil || *body.Items[0].Packet.Scope != "#bc" {
		t.Fatalf("unexpected live backfill item: %#v", body.Items)
	}
	if body.Items[0].Observation.PathLength.HashSize != 1 || body.Items[0].Observation.SourceBroker != "mqtt1" {
		t.Fatalf("unexpected observation shape: %#v", body.Items[0].Observation)
	}
}

func TestGetLiveSummary_InvalidWindow(t *testing.T) {
	r := LiveRouter(stubReader{})
	req := httptest.NewRequest(http.MethodGet, "/summary?since=2000&until=1000", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestGetLiveSummary_AllRegionBypassesRegionLookup(t *testing.T) {
	reader := &liveRegionLookupReader{}
	r := LiveRouter(reader)
	req := httptest.NewRequest(http.MethodGet, "/summary?region=all", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if reader.called {
		t.Fatal("expected region=all to skip region lookup")
	}
}

func TestGetLiveSummary_ResponseShapeFixture(t *testing.T) {
	reader := &liveSummaryFixtureReader{}
	r := LiveRouter(reader)
	req := httptest.NewRequest(http.MethodGet, "/summary?since=1710000000000&until=1710000300000&iatas=yvr,yyj", nil)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1710000000000)) || !reader.filter.Until.Equal(time.UnixMilli(1710000300000)) {
		t.Fatalf("unexpected window: %#v", reader.filter)
	}
	if len(reader.filter.IATAs) != 2 || reader.filter.IATAs[0] != "YVR" || reader.filter.IATAs[1] != "YYJ" {
		t.Fatalf("unexpected IATA filter: %#v", reader.filter.IATAs)
	}
	var body api.LiveSummary
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.LatestObservationID != 902 || body.ObservationCount != 7 || len(body.PayloadMix) != 1 || len(body.TopObservers) != 1 {
		t.Fatalf("unexpected live summary body: %#v", body)
	}
}
