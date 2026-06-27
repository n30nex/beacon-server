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
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type searchReader struct {
	stubReader
	nodes     []api.NodeSummary
	observers []api.ObserverSummary
	channels  []api.ChannelSummary
	routes    []api.KnownRoute
	traces    []api.TraceTagSummary
}

func (r searchReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	return api.Page[api.NodeSummary]{Items: r.nodes}, nil
}

func (r searchReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	return api.Page[api.ObserverSummary]{Items: r.observers}, nil
}

func (r searchReader) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	return api.Page[api.ChannelSummary]{Items: r.channels}, nil
}

func (r searchReader) ListKnownRoutes(ctx context.Context, iatas []string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	return r.routes, nil
}

func (r searchReader) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	return r.traces, nil
}

func TestGlobalSearchAggregatesResults(t *testing.T) {
	nodeName := "YVR Repeater"
	observerName := "YVR Gateway"
	channelName := "Public"
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000101")
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000202")
	routeNodeName := "YVR Hop"

	reader := searchReader{
		nodes: []api.NodeSummary{{
			ID:           nodeID,
			PublicKey:    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			NodeTypeName: "repeater",
			Name:         &nodeName,
			IATAs:        []api.NodeIATA{{IATA: "YVR"}},
		}},
		observers: []api.ObserverSummary{{
			ID:           observerID,
			DisplayName:  &observerName,
			ObserverType: strPtr("meshcoretomqtt"),
			IATA:         "YVR",
			Status:       "online",
		}},
		channels: []api.ChannelSummary{{
			ID:          7,
			Name:        &channelName,
			ChannelHash: "11",
			KeyKnown:    true,
		}},
		routes: []api.KnownRoute{{
			ID:       42,
			IATA:     "YVR",
			HopCount: 2,
			Hops: []api.RouteHop{{
				HashBytes: "abcd",
				Node: &api.ResolvedNode{
					Name:      &routeNodeName,
					PublicKey: "abcd0011",
				},
			}},
		}},
		traces: []api.TraceTagSummary{{
			TraceTag:    "feedbeef",
			TraceType:   "TRACE",
			PacketCount: 3,
			IATACount:   1,
		}},
	}

	r := chi.NewRouter()
	r.Mount("/search", SearchRouter(reader))
	req := httptest.NewRequest(http.MethodGet, "/search?q=YVR&limit=20", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body api.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Query != "YVR" {
		t.Fatalf("expected query YVR, got %q", body.Query)
	}
	types := map[string]bool{}
	for _, item := range body.Items {
		types[item.Type] = true
		if item.URL == "" {
			t.Fatalf("result %s/%s missing URL", item.Type, item.ID)
		}
	}
	for _, typ := range []string{"node", "observer", "route"} {
		if !types[typ] {
			t.Fatalf("expected %s result in %#v", typ, body.Items)
		}
	}
}

func TestGlobalSearchTypesAndLimit(t *testing.T) {
	reader := searchReader{
		nodes: []api.NodeSummary{
			{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), PublicKey: "aa", NodeTypeName: "repeater", Name: strPtr("Alpha one")},
			{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), PublicKey: "bb", NodeTypeName: "sensor", Name: strPtr("Alpha two")},
		},
		observers: []api.ObserverSummary{{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), DisplayName: strPtr("Alpha observer"), IATA: "YVR", Status: "online"}},
	}
	r := chi.NewRouter()
	r.Mount("/search", SearchRouter(reader))
	req := httptest.NewRequest(http.MethodGet, "/search?q=alpha&types=node&limit=1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body api.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 1 {
		t.Fatalf("expected 1 result, got %d", len(body.Items))
	}
	if body.Items[0].Type != "node" {
		t.Fatalf("expected node result, got %s", body.Items[0].Type)
	}
}

func TestGlobalSearchPagesUseCurrentNavigation(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/search", SearchRouter(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/search?q=analytics&types=page&limit=10", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body api.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) == 0 {
		t.Fatal("expected analytics page result")
	}
	if got := body.Items[0].Label; got != "Analytics" {
		t.Fatalf("expected Analytics page result, got %q", got)
	}
	if got := body.Items[0].URL; got != "/?tab=Analytics" {
		t.Fatalf("expected Analytics tab URL, got %q", got)
	}

	for _, item := range body.Items {
		if item.Label == "Atlas" || item.Label == "Stats" {
			t.Fatalf("legacy page label returned: %#v", item)
		}
	}
}

func TestGlobalSearchInvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Mount("/search", SearchRouter(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/search?q=node&limit=bad", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func strPtr(v string) *string {
	return &v
}
