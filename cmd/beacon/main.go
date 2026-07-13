// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	_ "github.com/MeshCore-Beacon/beacon-server/docs"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// Build provenance is injected by the atomic promotion workflow. Local source
// builds remain visibly dirty instead of masquerading as a promoted artifact.
var (
	version    = "dev"
	commitSHA  = "unknown"
	buildTime  = ""
	buildDirty = "true"
)

//	@title			MeshCore Beacon API
//	@version		1.4.1
//	@description	MeshCore network observation backend. Ingests LoRa packets from MQTT brokers, stores in PostgreSQL, and streams live events via WebSocket.
//	@termsOfService	https://github.com/MeshCore-Beacon/beacon-server

//	@contact.name	MeshCore Beacon
//	@contact.url	https://github.com/MeshCore-Beacon/beacon-server

//	@license.name	AGPL-3-or-later

//	@host		localhost:8080
//	@BasePath	/api/v1

//	@schemes	http https

//	@securityDefinitions.apikey	BearerAuth
//	@in					header
//	@name				Authorization
//	@description			Operator diagnostics token using the Bearer scheme.

// @tag.name			IATAs
// @tag.description	Airport/location codes that group observers and packets
// @tag.name			Regions
// @tag.description	Super-regions grouping multiple IATAs
// @tag.name			Observers
// @tag.description	MeshCore MQTT observers (gateways)
// @tag.name			Nodes
// @tag.description	MeshCore radio nodes
// @tag.name			Packets
// @tag.description	LoRa packets heard by observers
// @tag.name			Channels
// @tag.description	MeshCore group text channels
// @tag.name			Messages
// @tag.description	Decrypted channel messages
// @tag.name			Brokers
// @tag.description	MQTT broker connection status
// @tag.name			Stats
// @tag.description	Network statistics and time series
func main() {
	log.Printf("beacon version %s", version)
	_ = godotenv.Load()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	telemetryResolution := cfg.Telemetry.Resolution.Duration
	if telemetryResolution == 0 {
		telemetryResolution = time.Hour
	}
	telemetryRetention := cfg.Telemetry.Retention.Duration
	if telemetryRetention == 0 {
		telemetryRetention = 28 * 24 * time.Hour // 4 weeks
	}
	packetRetention := cfg.Packets.Retention.Duration
	if packetRetention == 0 {
		packetRetention = 30 * 24 * time.Hour // 30 days
	}

	maxConnsPerIP := cfg.WebSocket.MaxConnectionsPerIP
	if maxConnsPerIP == 0 {
		maxConnsPerIP = 5
	}
	cfg.WebSocket.MaxConnectionsPerIP = maxConnsPerIP

	// resolve background intervals with defaults
	viewRefreshInterval := cfg.Background.ViewRefresh.Duration
	if viewRefreshInterval == 0 {
		viewRefreshInterval = time.Hour
	}
	atlasAggregateInterval := cfg.Background.AtlasAggregate.Duration
	if atlasAggregateInterval == 0 {
		atlasAggregateInterval = 5 * time.Minute
	}
	reconfirmInterval := cfg.Background.Reconfirm.Duration
	if reconfirmInterval == 0 {
		reconfirmInterval = time.Hour
	}
	cleanupInterval := cfg.Background.Cleanup.Duration
	if cleanupInterval == 0 {
		cleanupInterval = time.Hour
	}

	log.Printf("config: loaded — telemetryResolution=%s telemetryRetention=%s packetRetention=%s maxConnsPerIP=%d atlasAggregate=%s viewRefresh=%s reconfirm=%s cleanup=%s",
		telemetryResolution, telemetryRetention, packetRetention, maxConnsPerIP, atlasAggregateInterval, viewRefreshInterval, reconfirmInterval, cleanupInterval)

	// ── Hub ──────────────────────────────────────────────────────────────────
	h := hub.New()
	go h.Run()

	// ── MQTT ingest workers ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := getEnv("POSTGRES_DSN")
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("failed to parse postgres DSN: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "beacon-interactive"
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = getEnvDefault("POSTGRES_INTERACTIVE_STATEMENT_TIMEOUT", "5s")
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = getEnvDefault("POSTGRES_INTERACTIVE_LOCK_TIMEOUT", "500ms")
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("failed to connect to postgres at %s: %v", os.Getenv("POSTGRES_DSN_HOST"), err)
	}
	defer pool.Close()
	if err := waitForPostgres(ctx, pool.Ping, 60*time.Second, time.Second); err != nil {
		log.Fatalf("postgres did not become ready: %v", err)
	}

	backgroundPoolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("failed to parse background postgres DSN: %v", err)
	}
	backgroundPoolConfig.ConnConfig.RuntimeParams["application_name"] = "beacon-background"
	backgroundPoolConfig.ConnConfig.RuntimeParams["statement_timeout"] = getEnvDefault("POSTGRES_BACKGROUND_STATEMENT_TIMEOUT", "10min")
	backgroundPoolConfig.ConnConfig.RuntimeParams["lock_timeout"] = getEnvDefault("POSTGRES_BACKGROUND_LOCK_TIMEOUT", "2s")
	backgroundPoolConfig.MaxConns = 2
	backgroundPool, err := pgxpool.NewWithConfig(ctx, backgroundPoolConfig)
	if err != nil {
		log.Fatalf("failed to connect background postgres pool: %v", err)
	}
	defer backgroundPool.Close()

	if err := db.RunMigrations(ctx, pool); err != nil {
		log.Fatalf("migrations failed: %v", err)
	}

	store := db.New(pool)
	backgroundStore := db.New(backgroundPool)

	// ── Redis cache layer ────────────────────────────────────────────────────
	var reader api.Reader = store
	cacheStatus := "disabled"
	cacheBackend := ""
	var cacheSnapshot func() map[string]cache.CategorySnapshot
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		redisClient := cache.NewClient(
			redisAddr,
			os.Getenv("REDIS_PASSWORD"),
			func() int {
				if db := os.Getenv("REDIS_DB"); db != "" {
					n, err := strconv.Atoi(db)
					if err == nil {
						return n
					}
				}
				return 0
			}(),
		)
		if err := redisClient.Ping(ctx); err != nil {
			log.Printf("warning: redis unavailable at %s, caching disabled: %v", redisAddr, err)
			cacheStatus = "degraded"
			cacheBackend = redisAddr
		} else {
			ttls := cache.ResolveTTLs(cfg.Cache)
			reader = cache.NewCachedReader(store, redisClient, ttls)
			defer redisClient.Close()
			cacheSnapshot = redisClient.MetricsSnapshot
			cacheStatus = "ok"
			cacheBackend = redisAddr
			log.Printf("cache: Redis connected at %s (atlas=%s live=%s stats=%s reference=%s nodes=%s observers=%s)",
				redisAddr, ttls.Atlas, ttls.Live, ttls.Stats, ttls.Reference, ttls.Nodes, ttls.Observers)
		}
	}

	// ── Seed config data ─────────────────────────────────────────────────────
	if err := config.Seed(ctx, cfg, store); err != nil {
		log.Fatalf("failed to seed config: %v", err)
	}

	// ── Build transport scope keystore ───────────────────────────────────────
	scopes := scopestore.New()
	scopeEntries, err := store.GetTransportScopes(ctx)
	if err != nil {
		log.Fatalf("failed to load transport scopes: %v", err)
	}
	scopes.Load(scopeEntries)
	log.Printf("loaded %d transport scopes", len(scopeEntries))

	// ── Build channel keystore ──────────────────────────────────────────────
	entries := make(map[string][]keystore.Entry)

	// Hashtag-derived channels: secret = SHA256("#tag")[:16], hash = SHA256(secret)[0]
	for _, tag := range cfg.ChannelKeys.Hashtags {
		secret, channelHash, fingerprint := keystore.DeriveHashtagKey(tag)
		hashHex := fmt.Sprintf("%02x", channelHash)
		entry := keystore.Entry{
			Key:         secret,
			Fingerprint: fingerprint,
			Hashtag:     tag,
			Name:        "#" + tag,
		}
		if !entryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			log.Printf("config: loaded hashtag channel #%s (hash=%s)", tag, hashHex)
		}
	}

	// Explicit keys: hash provided directly, key is hex-encoded
	for hashHex, keyCfg := range cfg.ChannelKeys.Keys {
		key, err := hex.DecodeString(keyCfg.Key)
		if err != nil {
			log.Printf("warning: invalid channel key for hash %s, skipping: %v", hashHex, err)
			continue
		}
		entry := keystore.Entry{
			Key:         key,
			Fingerprint: keystore.Fingerprint(key),
			Name:        keyCfg.Name,
		}
		if !entryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			log.Printf("config: loaded explicit channel key for hash %s name=%q", hashHex, keyCfg.Name)
		}
	}

	keys := keystore.NewMapKeyStore(entries)

	// ── Build geographic ingest filter ───────────────────────────────────────────────────────────
	allowedIATAs := iatadb.BuildAllowedSet(cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents)
	if allowedIATAs != nil {
		log.Printf("config: ingest filter active — %d allowed IATAs (countries=%v continents=%v)",
			len(allowedIATAs), cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents)
	} else {
		log.Printf("config: ingest filter inactive — accepting all IATAs")
	}

	broker1 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt1",
			ClientID:            getEnvDefault("MQTT_BROKER_1_CLIENT_ID", "beacon-mqtt1"),
			URL:                 getEnv("MQTT_BROKER_1_URL"),
			Username:            getEnv("MQTT_BROKER_1_USERNAME"),
			Password:            getEnv("MQTT_BROKER_1_PASSWORD"),
			TelemetryResolution: telemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		store,
		h,
		keys,
		scopes,
	)

	broker2 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt2",
			ClientID:            getEnvDefault("MQTT_BROKER_2_CLIENT_ID", "beacon-mqtt2"),
			URL:                 getEnv("MQTT_BROKER_2_URL"),
			Username:            getEnv("MQTT_BROKER_2_USERNAME"),
			Password:            getEnv("MQTT_BROKER_2_PASSWORD"),
			TelemetryResolution: telemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		store,
		h,
		keys,
		scopes,
	)

	if cr, ok := reader.(*cache.CachedReader); ok {
		broker1.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
		broker2.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
	}

	dirtyIATAs := background.NewDirtyIATAs(true)
	broker1.SetDirtyIATANotifier(dirtyIATAs.Mark)
	broker2.SetDirtyIATANotifier(dirtyIATAs.Mark)
	go broker1.Start(ctx)
	go broker2.Start(ctx)

	atlasAggregateTask := background.AtlasAggregateTask(backgroundStore, atlasAggregateInterval)
	atlasAggregateTask.Offset = durationOrDefault(cfg.Background.AtlasAggregateOffset.Duration, 30*time.Second)
	atlasAggregateTask.Timeout = durationOrDefault(cfg.Background.AtlasAggregateTimeout.Duration, 10*time.Minute)
	viewRefreshTask := background.ViewRefreshTask(backgroundStore, viewRefreshInterval)
	viewRefreshTask.Offset = durationOrDefault(cfg.Background.ViewRefreshOffset.Duration, 5*time.Minute)
	viewRefreshTask.Timeout = durationOrDefault(cfg.Background.ViewRefreshTimeout.Duration, 10*time.Minute)
	reconfirmTask := background.ReconfirmTask(backgroundStore, dirtyIATAs, reconfirmInterval)
	reconfirmTask.Offset = durationOrDefault(cfg.Background.ReconfirmOffset.Duration, 20*time.Minute)
	reconfirmTask.Timeout = durationOrDefault(cfg.Background.ReconfirmTimeout.Duration, 10*time.Minute)
	cleanupTask := background.CleanupTask(backgroundStore, telemetryRetention, packetRetention, cleanupInterval)
	cleanupTask.Offset = durationOrDefault(cfg.Background.CleanupOffset.Duration, 45*time.Minute)
	cleanupTask.Timeout = durationOrDefault(cfg.Background.CleanupTimeout.Duration, 15*time.Minute)
	scheduler := background.New([]background.Task{atlasAggregateTask, viewRefreshTask, reconfirmTask, cleanupTask})
	go scheduler.Start(ctx)

	// ── HTTP server ──────────────────────────────────────────────────────────
	r := router.New(h, reader, []*ingest.Worker{broker1, broker2}, cfg.WebSocket, cfg.CORS, cfg.RateLimits, handlers.HealthConfig{
		Version: version,
		Build: handlers.BuildProvenance{
			Version:   version,
			SHA:       commitSHA,
			BuildTime: buildTime,
			Dirty:     parseBuildDirty(buildDirty),
		},
		CacheStatus:        cacheStatus,
		CacheBackend:       cacheBackend,
		DiagnosticsToken:   os.Getenv("BEACON_OPS_DIAGNOSTICS_TOKEN"),
		CacheSnapshot:      cacheSnapshot,
		BackgroundSnapshot: scheduler.MetricsSnapshot,
		DatabasePoolSnapshot: func() handlers.DatabasePoolSnapshot {
			stats := pool.Stat()
			return handlers.DatabasePoolSnapshot{
				AcquiredConnections:     stats.AcquiredConns(),
				IdleConnections:         stats.IdleConns(),
				TotalConnections:        stats.TotalConns(),
				MaxConnections:          stats.MaxConns(),
				AcquireCount:            stats.AcquireCount(),
				EmptyAcquireCount:       stats.EmptyAcquireCount(),
				CanceledAcquireCount:    stats.CanceledAcquireCount(),
				CumulativeAcquireWaitMs: stats.AcquireDuration().Milliseconds(),
			}
		},
		BackupSnapshot: localBackupSnapshot,
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		fmt.Printf("Beacon listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel() // stops ingest workers
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}

// entryExists checks if an identical key entry already exists in the slice
// to prevent loading duplicate key/hash pairs from config.
func entryExists(entries []keystore.Entry, e keystore.Entry) bool {
	for _, existing := range entries {
		if bytes.Equal(existing.Key, e.Key) {
			return true
		}
	}
	return false
}

// getEnv returns the value of an env var and logs a warning if it is unset.
// Callers that require the value to be non-empty should fatal themselves;
// ingest workers tolerate missing broker config and will fail on connect instead.
func getEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Printf("warning: %s is not set", key)
	}
	return v
}

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func waitForPostgres(
	ctx context.Context,
	ping func(context.Context) error,
	timeout time.Duration,
	retryInterval time.Duration,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		if err := ping(waitCtx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf("timed out after %s: %w", timeout, lastErr)
		case <-timer.C:
		}
	}
}

func parseBuildDirty(value string) bool {
	dirty, err := strconv.ParseBool(value)
	if err != nil {
		return true
	}
	return dirty
}

func localBackupSnapshot() *handlers.BackupSnapshot {
	dir := getEnvDefault("BEACON_BACKUP_DIR", filepath.Join("..", ".local-backups"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return &handlers.BackupSnapshot{Status: "missing"}
	}
	var newest os.FileInfo
	var newestPath string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".manifest.json") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr == nil && (newest == nil || info.ModTime().After(newest.ModTime())) {
			newest = info
			newestPath = filepath.Join(dir, entry.Name())
		}
	}
	if newest == nil {
		return &handlers.BackupSnapshot{Status: "missing"}
	}
	var manifest struct {
		CreatedAt              string `json:"createdAt"`
		ListVerified           bool   `json:"listVerified"`
		ScratchRestoreVerified bool   `json:"scratchRestoreVerified"`
	}
	data, readErr := os.ReadFile(newestPath)
	if readErr != nil || json.Unmarshal(data, &manifest) != nil {
		return &handlers.BackupSnapshot{Status: "invalid"}
	}
	createdAt, parseErr := time.Parse(time.RFC3339, manifest.CreatedAt)
	if parseErr != nil {
		return &handlers.BackupSnapshot{Status: "invalid"}
	}
	age := time.Since(createdAt)
	status := "ok"
	if age > 36*time.Hour || !manifest.ListVerified {
		status = "stale"
	}
	return &handlers.BackupSnapshot{
		Status:          status,
		LastBackupAt:    createdAt.UnixMilli(),
		AgeMs:           age.Milliseconds(),
		ListVerified:    manifest.ListVerified,
		RestoreVerified: manifest.ScratchRestoreVerified,
	}
}
