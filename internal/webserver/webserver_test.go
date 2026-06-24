// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package webserver

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTestDist(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<main>Beacon</main>"), 0o644); err != nil {
		t.Fatal(err)
	}
	assets := filepath.Join(dir, "assets")
	if err := os.Mkdir(assets, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(assets, "index-abc123.js"), []byte("console.log('beacon')"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSPAHandlerServesIndexFallbackNoStore(t *testing.T) {
	handler, err := New(Config{DistDir: writeTestDist(t), APIOrigin: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/Map?tab=Map", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected no-store cache header, got %q", got)
	}
	if !strings.Contains(w.Body.String(), "Beacon") {
		t.Fatalf("expected index body, got %q", w.Body.String())
	}
}

func TestSPAHandlerServesHashedAssetsImmutable(t *testing.T) {
	handler, err := New(Config{DistDir: writeTestDist(t), APIOrigin: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/assets/index-abc123.js", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("expected immutable cache header, got %q", got)
	}
}

func TestAPIPathsProxyToOrigin(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/stats/home" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Host != "beacon.test" {
			t.Fatalf("expected forwarded host beacon.test, got %q", r.Host)
		}
		if got := r.Header.Get("X-Forwarded-Host"); got != "beacon.test" {
			t.Fatalf("expected X-Forwarded-Host beacon.test, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	handler, err := New(Config{DistDir: writeTestDist(t), APIOrigin: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats/home", nil)
	req.Host = "beacon.test"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Fatalf("expected upstream body, got %q", w.Body.String())
	}
}
