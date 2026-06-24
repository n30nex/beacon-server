// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/webserver"
)

var version = "dev"

func main() {
	addr := env("BEACON_WEB_LISTEN_ADDR", ":5174")
	distDir := env("BEACON_WEB_DIST", "../beacon-web/dist")
	apiOrigin := env("BEACON_API_ORIGIN", "http://127.0.0.1:8080")

	handler, err := webserver.New(webserver.Config{DistDir: distDir, APIOrigin: apiOrigin})
	if err != nil {
		log.Fatalf("failed to configure Beacon web server: %v", err)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("beacon-web version %s serving %s on %s with API origin %s", version, distDir, addr, apiOrigin)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("beacon-web failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Print("beacon-web shutting down")
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
