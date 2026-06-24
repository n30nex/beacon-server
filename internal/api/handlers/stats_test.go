// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type statsObserverCompareReader struct {
	stubReader
	filter api.StatsObserverCompareFilter
}

type statsHashReader struct {
	stubReader
	filter api.StatsFilter
}

type statsHashPrefixReader struct {
	stubReader
	filter api.StatsHashPrefixFilter
}

type statsTopologyReader struct {
	stubReader
	filter api.StatsFilter
}

type statsHomeReader struct {
	stubReader
	summaryCalled bool
}

type statsObserverHealthReader struct {
	stubReader
	filter api.StatsObserverHealthFilter
}

func (r *statsHomeReader) GetStatsSummary(ctx context.Context, filter api.StatsFilter) (*api.StatsSummary, error) {
	r.summaryCalled = true
	return nil, nil
}

func (r *statsHomeReader) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	return &api.StatsOverview{TotalPackets: 100, TotalObservations: 250, ActiveObservers: 7, ActiveIATAs: 3, WindowHours: 24}, nil
}

func (r *statsHomeReader) GetLiveSummary(ctx context.Context, filter api.LiveSummaryFilter) (*api.LiveSummary, error) {
	return &api.LiveSummary{
		ServerTime:          123,
		Since:               filter.Since.UnixMilli(),
		Until:               filter.Until.UnixMilli(),
		LatestObservationID: 42,
		PacketCount:         10,
		ObservationCount:    30,
		ActiveObservers:     4,
		TopIATAs: []api.LiveIATACount{
			{IATA: "YVR", Count: 20},
			{IATA: "YYJ", Count: 10},
		},
	}, nil
}

func (r *statsHomeReader) GetStatsTopNodes(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
	return []api.TopNode{{
		NodeID:           uuid.MustParse("00000000-0000-0000-0000-000000000101"),
		NodeName:         strPtr("Node One"),
		NodeType:         1,
		NodeTypeName:     "Repeater",
		IATA:             "YVR",
		ObservationCount: 12,
		LastHeard:        456,
	}}, nil
}

func (r *statsHomeReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	return []api.TopObserver{{
		ObserverID:       uuid.MustParse("00000000-0000-0000-0000-000000000201"),
		DisplayName:      strPtr("Observer One"),
		ObserverType:     strPtr("mqtt"),
		IATA:             "YVR",
		ObservationCount: 15,
	}}, nil
}

type statsSubpathsReader struct {
	stubReader
	filter api.StatsFilter
}

type statsChannelsReader struct {
	stubReader
	filter api.StatsFilter
}

func (r *statsObserverCompareReader) GetStatsObserverCompare(ctx context.Context, filter api.StatsObserverCompareFilter) (*api.StatsObserverCompare, error) {
	r.filter = filter
	return &api.StatsObserverCompare{
		ServerTime: 123,
		Window:     api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		Items: []api.StatsObserverCompareItem{{
			StatsObserverHealth: api.StatsObserverHealth{ObserverID: filter.ObserverIDs[0], IATA: "YVR", ObservationCount: 8},
			PacketCount:         4,
		}},
	}, nil
}

func (r *statsHashReader) GetStatsHashAnalytics(ctx context.Context, filter api.StatsFilter) (*api.StatsHashAnalytics, error) {
	r.filter = filter
	return &api.StatsHashAnalytics{
		ServerTime:              123,
		Window:                  api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		TotalPackets:            3,
		TotalObservations:       7,
		CollisionPrefixCount:    1,
		InconsistentPacketCount: 1,
		CollisionMatrix: []api.StatsHashCollisionCell{{
			HashSize:         1,
			IATA:             "YOW",
			PrefixCount:      2,
			PacketCount:      3,
			ObservationCount: 7,
			ObserverCount:    2,
		}},
	}, nil
}

func (r *statsHashPrefixReader) GetStatsHashPrefixLookup(ctx context.Context, filter api.StatsHashPrefixFilter) (*api.StatsHashPrefixLookup, error) {
	r.filter = filter
	return &api.StatsHashPrefixLookup{
		ServerTime:       123,
		Window:           api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		Prefix:           filter.Prefix,
		PacketCount:      2,
		ObservationCount: 7,
		Items: []api.StatsHashPrefixPacket{{
			PacketHash:       "aabb",
			PathHash:         "11",
			HashSize:         1,
			ObservationCount: 7,
		}},
	}, nil
}

func (r *statsTopologyReader) GetStatsTopology(ctx context.Context, filter api.StatsFilter) (*api.StatsTopology, error) {
	r.filter = filter
	return &api.StatsTopology{
		ServerTime:       123,
		Window:           api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		RouteCount:       3,
		ObservationCount: 9,
		ActiveIATAs:      2,
	}, nil
}

func (r *statsSubpathsReader) GetStatsSubpaths(ctx context.Context, filter api.StatsFilter) (*api.StatsSubpaths, error) {
	r.filter = filter
	return &api.StatsSubpaths{
		ServerTime:         123,
		Window:             api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		RouteCount:         5,
		SubpathCount:       22,
		UniqueSubpathCount: 9,
		ObservationCount:   44,
	}, nil
}

func (r *statsChannelsReader) GetStatsChannels(ctx context.Context, filter api.StatsFilter) (*api.StatsChannels, error) {
	r.filter = filter
	return &api.StatsChannels{
		ServerTime:       123,
		Window:           api.StatsWindow{Since: filter.Since.UnixMilli(), Until: filter.Until.UnixMilli(), Bucket: filter.Bucket},
		TotalChannels:    4,
		MessageCount:     8,
		ObservationCount: 24,
	}, nil
}

func (r *statsObserverHealthReader) GetStatsObserverHealth(ctx context.Context, filter api.StatsObserverHealthFilter) (*api.StatsObserverHealthResponse, error) {
	r.filter = filter
	telemetryAt := filter.Until.Add(-2 * time.Minute).UnixMilli()
	battery := int32(3600)
	noise := float32(-116.5)
	tx := float32(12.5)
	rx := float32(18.25)
	queue := int32(2)
	errors := int32(1)
	return &api.StatsObserverHealthResponse{
		ServerTime: 1710000005000,
		Window: api.StatsWindow{
			Since:  filter.Since.UnixMilli(),
			Until:  filter.Until.UnixMilli(),
			Bucket: filter.Bucket,
		},
		Summary: api.StatsHealthSummary{
			TotalObservers: 2,
			ReceiveErrors:  1,
			NoTelemetry:    1,
		},
		Items: []api.StatsObserverHealth{{
			ObserverID:       uuid.MustParse("00000000-0000-0000-0000-000000000301"),
			DisplayName:      strPtr("West Roof"),
			ObserverType:     strPtr("mqtt"),
			IATA:             "YVR",
			Status:           "online",
			LastHeard:        filter.Until.Add(-time.Minute).UnixMilli(),
			ObservationCount: 42,
			TelemetryAt:      &telemetryAt,
			HasTelemetry:     true,
			BatteryMV:        &battery,
			NoiseFloorDB:     &noise,
			AirtimeTxPct:     &tx,
			AirtimeRxPct:     &rx,
			QueueLength:      &queue,
			ReceiveErrors:    &errors,
			HealthScore:      88,
			Flags: api.StatsObserverHealthFlags{
				ReceiveErrors: true,
			},
		}},
	}, nil
}

func TestGetStatsObservations_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observations", getStatsObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observations?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloadBreakdown_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payload-breakdown", getStatsPayloadBreakdown(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payload-breakdown?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-nodes", getStatsTopNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsSummary_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/summary", getStatsSummary(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/summary?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHome_UsesCompactReaderCalls(t *testing.T) {
	reader := &statsHomeReader{}
	r := chi.NewRouter()
	r.Get("/stats/home", getStatsHome(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/home?range=24h&iatas=YVR,YYJ", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if reader.summaryCalled {
		t.Fatal("home endpoint must not call the heavy stats summary")
	}
	var body api.StatsHome
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Overview.TotalPackets != 100 || body.Live.ObservationCount != 30 {
		t.Fatalf("unexpected home payload: %+v", body)
	}
	if len(body.TopNodes) != 1 || len(body.TopObservers) != 1 || len(body.TopIATAs) != 2 {
		t.Fatalf("expected compact rankings in response: %+v", body)
	}
}

func TestGetStatsObserverHealth_ResponseShapeFixture(t *testing.T) {
	reader := &statsObserverHealthReader{}
	r := chi.NewRouter()
	r.Get("/stats/observer-health", getStatsObserverHealth(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-health?range=24h&bucket=1h&limit=25&iatas=yvr&staleAfterMinutes=15", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if reader.filter.Limit != 25 || reader.filter.StaleAfter != 15*time.Minute {
		t.Fatalf("unexpected observer-health filter: %#v", reader.filter)
	}
	if len(reader.filter.IATAs) != 1 || reader.filter.IATAs[0] != "YVR" {
		t.Fatalf("unexpected IATA filter: %#v", reader.filter.IATAs)
	}
	var body api.StatsObserverHealthResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Summary.TotalObservers != 2 || body.Summary.ReceiveErrors != 1 || len(body.Items) != 1 {
		t.Fatalf("unexpected observer-health body: %#v", body)
	}
	if body.Items[0].Status != "online" || !body.Items[0].HasTelemetry || !body.Items[0].Flags.ReceiveErrors {
		t.Fatalf("unexpected observer-health item: %#v", body.Items[0])
	}
}

func TestGetStatsRegions_InvalidWindow(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/regions", getStatsRegions(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/regions?since=2000&until=1000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloads_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payloads", getStatsPayloads(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payloads?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHashAnalytics_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/hash", getStatsHashAnalytics(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHashAnalytics_FilterContract(t *testing.T) {
	reader := &statsHashReader{}
	r := chi.NewRouter()
	r.Get("/stats/hash", getStatsHashAnalytics(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash?since=1000&until=5000&bucket=1h&iatas=yvr,YOW&limit=12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if reader.filter.Bucket != "1h" || reader.filter.Limit != 12 {
		t.Fatalf("unexpected bucket/limit %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsHashAnalytics
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TotalPackets != 3 || body.CollisionPrefixCount != 1 || len(body.CollisionMatrix) != 1 {
		t.Fatalf("unexpected response %#v", body)
	}
	if body.CollisionMatrix[0].IATA != "YOW" || body.CollisionMatrix[0].PrefixCount != 2 {
		t.Fatalf("unexpected collision matrix %#v", body.CollisionMatrix)
	}
}

func TestGetStatsHashPrefixLookup_RequiresPrefix(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/hash-prefix", getStatsHashPrefixLookup(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash-prefix", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHashPrefixLookup_InvalidPrefix(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/hash-prefix", getStatsHashPrefixLookup(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash-prefix?prefix=not-hex", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHashPrefixLookup_InvalidHashSize(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/hash-prefix", getStatsHashPrefixLookup(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash-prefix?prefix=11&hashSize=9", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsHashPrefixLookup_FilterContract(t *testing.T) {
	reader := &statsHashPrefixReader{}
	r := chi.NewRouter()
	r.Get("/stats/hash-prefix", getStatsHashPrefixLookup(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/hash-prefix?prefix=0xAB&hashSize=1&since=1000&until=5000&bucket=1h&iatas=yvr,YOW&limit=12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.filter.Prefix != "ab" || reader.filter.HashSize != 1 {
		t.Fatalf("unexpected prefix filter %#v", reader.filter)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if reader.filter.Bucket != "1h" || reader.filter.Limit != 12 {
		t.Fatalf("unexpected bucket/limit %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsHashPrefixLookup
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Prefix != "ab" || body.PacketCount != 2 || len(body.Items) != 1 {
		t.Fatalf("unexpected response %#v", body)
	}
}

func TestGetStatsTopology_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/topology", getStatsTopology(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/topology?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopology_FilterContract(t *testing.T) {
	reader := &statsTopologyReader{}
	r := chi.NewRouter()
	r.Get("/stats/topology", getStatsTopology(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/topology?since=1000&until=5000&bucket=1h&iatas=yvr,YOW&limit=12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if reader.filter.Bucket != "1h" || reader.filter.Limit != 12 {
		t.Fatalf("unexpected bucket/limit %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsTopology
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RouteCount != 3 || body.ActiveIATAs != 2 {
		t.Fatalf("unexpected response %#v", body)
	}
}

func TestGetStatsSubpaths_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/subpaths", getStatsSubpaths(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/subpaths?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsSubpaths_FilterContract(t *testing.T) {
	reader := &statsSubpathsReader{}
	r := chi.NewRouter()
	r.Get("/stats/subpaths", getStatsSubpaths(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/subpaths?since=1000&until=5000&bucket=1h&iatas=yvr,YOW&limit=12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if reader.filter.Bucket != "1h" || reader.filter.Limit != 12 {
		t.Fatalf("unexpected bucket/limit %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsSubpaths
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RouteCount != 5 || body.UniqueSubpathCount != 9 {
		t.Fatalf("unexpected response %#v", body)
	}
}

func TestGetStatsChannels_InvalidBucket(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/channels", getStatsChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/channels?bucket=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsChannels_FilterContract(t *testing.T) {
	reader := &statsChannelsReader{}
	r := chi.NewRouter()
	r.Get("/stats/channels", getStatsChannels(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/channels?since=1000&until=5000&bucket=1h&iatas=yvr,YOW&limit=12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if reader.filter.Bucket != "1h" || reader.filter.Limit != 12 {
		t.Fatalf("unexpected bucket/limit %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsChannels
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TotalChannels != 4 || body.ObservationCount != 24 {
		t.Fatalf("unexpected response %#v", body)
	}
}

func TestGetStatsObserverHealth_InvalidStaleAfter(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observer-health", getStatsObserverHealth(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-health?staleAfterMinutes=nope", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsObserverCompare_InvalidObserverIDs(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observer-compare", getStatsObserverCompare(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-compare?observerIds=not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsObserverCompare_FilterContract(t *testing.T) {
	idA := uuid.MustParse("00000000-0000-0000-0000-000000000101")
	idB := uuid.MustParse("00000000-0000-0000-0000-000000000202")
	reader := &statsObserverCompareReader{}
	r := chi.NewRouter()
	r.Get("/stats/observer-compare", getStatsObserverCompare(reader))
	req := httptest.NewRequest(http.MethodGet, "/stats/observer-compare?observerIds="+idA.String()+","+idB.String()+"&since=1000&until=5000&bucket=1h&iatas=yvr,YOW", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(reader.filter.ObserverIDs) != 2 || reader.filter.ObserverIDs[0] != idA || reader.filter.ObserverIDs[1] != idB {
		t.Fatalf("unexpected observer ids %#v", reader.filter.ObserverIDs)
	}
	if !reader.filter.Since.Equal(time.UnixMilli(1000)) || !reader.filter.Until.Equal(time.UnixMilli(5000)) {
		t.Fatalf("unexpected window %#v", reader.filter)
	}
	if got := strings.Join(reader.filter.IATAs, ","); got != "YVR,YOW" {
		t.Fatalf("unexpected iatas %q", got)
	}
	var body api.StatsObserverCompare
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].PacketCount != 4 {
		t.Fatalf("unexpected response %#v", body.Items)
	}
}
