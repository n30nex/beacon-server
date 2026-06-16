// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type liveRegionLookupReader struct {
	stubReader
	called bool
}

type liveBackfillCaptureReader struct {
	stubReader
	filter api.LiveBackfillFilter
	called bool
}

func (r *liveRegionLookupReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	r.called = true
	return nil, nil
}

func (r *liveBackfillCaptureReader) ListLiveBackfill(ctx context.Context, filter api.LiveBackfillFilter) (api.Page[api.LivePacketObservation], error) {
	r.called = true
	r.filter = filter
	return api.Page[api.LivePacketObservation]{}, nil
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
