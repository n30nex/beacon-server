// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

type channelListReader struct {
	stubReader
	gotIATA string
}

func (r *channelListReader) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	r.gotIATA = iata
	return api.Page[api.ChannelSummary]{Items: []api.ChannelSummary{}}, nil
}

func TestListChannels_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_InvalidHash(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?hash=nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_HashNotSingleByte(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?hash=aabb", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_MultiIATAFilter(t *testing.T) {
	reader := &channelListReader{}
	r := chi.NewRouter()
	r.Get("/channels", listChannels(reader))
	req := httptest.NewRequest(http.MethodGet, "/channels?iatas=yvr,%20yyj&limit=10", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if reader.gotIATA != "YVR,YYJ" {
		t.Fatalf("expected multi-IATA filter YVR,YYJ, got %q", reader.gotIATA)
	}
}

func TestGetChannel_InvalidID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}", getChannel(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidChannelID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/notanint/messages", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
