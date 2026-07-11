// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

type atlasReader struct {
	stubReader
	summarySlug  string
	summarySince time.Time
	summaryUntil time.Time
	briefRegion  string
	briefSince   time.Time
	briefUntil   time.Time
	replayRegion string
	replayCursor int64
	replayLimit  int32
	summaryErr   error
	briefErr     error
}

func (r *atlasReader) GetRegionAtlasSummary(ctx context.Context, slug string, since, until time.Time) (*api.RegionAtlasSummary, error) {
	r.summarySlug = slug
	r.summarySince = since
	r.summaryUntil = until
	return &api.RegionAtlasSummary{
		Region: api.Region{
			RegionSummary: api.RegionSummary{ID: 1, Slug: slug, Name: "Western Canada"},
			IATAs:         []string{"YVR", "YYJ"},
		},
		Window: api.AtlasWindow{Since: since.UnixMilli(), Until: until.UnixMilli()},
	}, r.summaryErr
}

func (r *atlasReader) GetAtlasBriefing(ctx context.Context, regionSlug string, since, until time.Time) (*api.AtlasBriefing, error) {
	r.briefRegion = regionSlug
	r.briefSince = since
	r.briefUntil = until
	return &api.AtlasBriefing{
		Region: api.Region{
			RegionSummary: api.RegionSummary{ID: 0, Slug: regionSlug, Name: "All Regions"},
			IATAs:         []string{"YVR", "YYZ"},
		},
		Window: api.AtlasWindow{Since: since.UnixMilli(), Until: until.UnixMilli()},
		Health: api.AtlasBriefingHealth{
			Status:      "ok",
			HealthScore: 92,
		},
	}, r.briefErr
}

func (r *atlasReader) ListAtlasReplay(ctx context.Context, regionSlug string, since, until time.Time, cursor int64, limit int32) (api.Page[api.AtlasReplayPacket], error) {
	r.replayRegion = regionSlug
	r.replayCursor = cursor
	r.replayLimit = limit
	next := int64(42)
	return api.Page[api.AtlasReplayPacket]{
		Items: []api.AtlasReplayPacket{{
			PacketHash:       "abc123",
			PayloadTypeName:  "Text",
			RouteTypeName:    "Flood",
			FirstHeardAt:     since.UnixMilli(),
			LastHeardAt:      until.UnixMilli(),
			ObservationCount: 2,
			IATAs:            []string{"YVR"},
		}},
		NextCursor: &next,
		HasMore:    true,
	}, nil
}

func TestGetAtlasRegion_ParsesWindow(t *testing.T) {
	reader := &atlasReader{}
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(reader))
	req := httptest.NewRequest(http.MethodGet, "/atlas/regions/western-canada?since=1710000000000&until=1710003600000", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.summarySlug != "western-canada" {
		t.Fatalf("expected slug western-canada, got %q", reader.summarySlug)
	}
	if reader.summarySince.UnixMilli() != 1710000000000 || reader.summaryUntil.UnixMilli() != 1710003600000 {
		t.Fatalf("unexpected parsed window: %d..%d", reader.summarySince.UnixMilli(), reader.summaryUntil.UnixMilli())
	}
	var body api.RegionAtlasSummary
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Region.Slug != "western-canada" || len(body.Region.IATAs) != 2 {
		t.Fatalf("unexpected summary body: %+v", body.Region)
	}
}

func TestGetAtlasBriefing_DefaultsAndParsesWindow(t *testing.T) {
	reader := &atlasReader{}
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(reader))
	req := httptest.NewRequest(http.MethodGet, "/atlas/briefing?since=1710000000000&until=1710003600000", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.briefRegion != "all" {
		t.Fatalf("expected default all region, got %q", reader.briefRegion)
	}
	if reader.briefSince.UnixMilli() != 1710000000000 || reader.briefUntil.UnixMilli() != 1710003600000 {
		t.Fatalf("unexpected parsed window: %d..%d", reader.briefSince.UnixMilli(), reader.briefUntil.UnixMilli())
	}
	var body api.AtlasBriefing
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Region.Slug != "all" || body.Health.Status != "ok" {
		t.Fatalf("unexpected briefing body: %+v", body)
	}
}

func TestGetAtlasBriefing_RejectsInvalidWindow(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/atlas/briefing?since=1710003600000&until=1710000000000", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestGetAtlasBriefing_ReportsDeadlineAndUnexpectedErrorsTruthfully(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
		{name: "unexpected", err: errors.New("database unavailable"), want: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chi.NewRouter()
			r.Mount("/atlas", AtlasRouter(&atlasReader{briefErr: tt.err}))
			w := httptest.NewRecorder()

			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/atlas/briefing", nil))

			if w.Code != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, w.Code)
			}
		})
	}
}

func TestGetAtlasRegion_OnlyReportsMissingRowsAsNotFound(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(&atlasReader{summaryErr: pgx.ErrNoRows}))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/atlas/regions/missing", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestGetAtlasRegion_RejectsInvalidWindow(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/atlas/regions/western-canada?since=1710003600000&until=1710000000000", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestListAtlasReplay_DefaultsAndClamps(t *testing.T) {
	reader := &atlasReader{}
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(reader))
	req := httptest.NewRequest(http.MethodGet, "/atlas/replay?cursor=12&limit=999", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.replayRegion != "all" {
		t.Fatalf("expected default all region, got %q", reader.replayRegion)
	}
	if reader.replayCursor != 12 || reader.replayLimit != 200 {
		t.Fatalf("expected cursor 12 and clamped limit 200, got cursor %d limit %d", reader.replayCursor, reader.replayLimit)
	}
	var page api.Page[api.AtlasReplayPacket]
	if err := json.NewDecoder(w.Body).Decode(&page); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !page.HasMore || page.NextCursor == nil || *page.NextCursor != 42 {
		t.Fatalf("unexpected page envelope: %+v", page)
	}
}

func TestListAtlasReplay_RejectsBadCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/atlas", AtlasRouter(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/atlas/replay?cursor=nope", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
