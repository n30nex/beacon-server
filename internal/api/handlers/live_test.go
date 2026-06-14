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

func (r *liveRegionLookupReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	r.called = true
	return nil, nil
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
