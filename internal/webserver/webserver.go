// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package webserver serves the built Beacon SPA and proxies API/WebSocket paths
// to a running beacon-server API process.
package webserver

import (
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type Config struct {
	DistDir   string
	APIOrigin string
}

func New(cfg Config) (http.Handler, error) {
	if cfg.DistDir == "" {
		return nil, errors.New("dist dir is required")
	}
	if cfg.APIOrigin == "" {
		cfg.APIOrigin = "http://127.0.0.1:8080"
	}
	indexPath := filepath.Join(cfg.DistDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		return nil, err
	}
	target, err := url.Parse(cfg.APIOrigin)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		originalHost := r.Host
		director(r)
		r.Host = originalHost
		r.Header.Set("X-Forwarded-Host", originalHost)
		r.Header.Set("X-Forwarded-Proto", forwardedProto(r))
	}

	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	})
	mux.Handle("/api/", proxyHandler)
	mux.Handle("/healthz", proxyHandler)
	mux.Handle("/readyz", proxyHandler)
	mux.Handle("/ws", proxyHandler)
	mux.Handle("/swagger/", proxyHandler)
	mux.Handle("/swagger", proxyHandler)
	mux.Handle("/", spaHandler{distDir: cfg.DistDir, indexPath: indexPath})
	return mux, nil
}

func forwardedProto(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

type spaHandler struct {
	distDir   string
	indexPath string
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.NotFound(w, r)
		return
	}

	requestPath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))
	filePath := filepath.Join(h.distDir, filepath.FromSlash(strings.TrimPrefix(requestPath, "/")))
	if info, err := os.Stat(filePath); err == nil && !info.IsDir() {
		h.serveFile(w, r, filePath, requestPath)
		return
	}

	if path.Ext(requestPath) != "" {
		http.NotFound(w, r)
		return
	}
	h.serveIndex(w, r)
}

func (h spaHandler) serveFile(w http.ResponseWriter, r *http.Request, filePath, requestPath string) {
	setContentType(w, filePath)
	if strings.HasPrefix(requestPath, "/assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else if filepath.Base(filePath) == "index.html" {
		w.Header().Set("Cache-Control", "no-store")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=3600")
	}
	http.ServeFile(w, r, filePath)
}

func (h spaHandler) serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	setContentType(w, h.indexPath)
	http.ServeFile(w, r, h.indexPath)
}

func setContentType(w http.ResponseWriter, filePath string) {
	ext := filepath.Ext(filePath)
	if ext == "" {
		return
	}
	if contentType := mime.TypeByExtension(ext); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
}

func HasDist(distDir string) bool {
	if distDir == "" {
		return false
	}
	_, err := fs.Stat(os.DirFS(distDir), "index.html")
	return err == nil
}
