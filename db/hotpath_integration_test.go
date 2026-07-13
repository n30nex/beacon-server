// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type hotPathFixture struct {
	Since             time.Time
	Until             time.Time
	IATAs             []string
	RegionSlug        string
	DatasetLabel      string
	NodeID            uuid.UUID
	RouteNodeID       uuid.UUID
	RouteAt           time.Time
	Scale             int
	NodeCount         int
	ObserverCount     int
	PacketCount       int
	ObservationCount  int
	KnownRouteCount   int
	NeighborEdgeCount int
}

const (
	maxHotPathFixtureScale        = 100
	defaultHotPathSnapshotRegion  = "western-canada"
	defaultHotPathSnapshotIATAs   = 4
	defaultHotPathSnapshotWindowH = 6
)

func TestHotPathIntegrationFixture(t *testing.T) {
	store, pool, fx := setupHotPathIntegration(t)
	exerciseHotPaths(t, store, fx)
	if output := strings.TrimSpace(os.Getenv("BEACON_EXPLAIN_OUTPUT")); output != "" {
		if err := captureHotPathExplainMarkdown(context.Background(), pool, fx, output); err != nil {
			t.Fatalf("capture EXPLAIN output: %v", err)
		}
		t.Logf("wrote EXPLAIN summary to %s", output)
	}
	if output := strings.TrimSpace(os.Getenv("BEACON_TIMINGS_OUTPUT")); output != "" {
		if err := captureHotPathTimingMarkdown(context.Background(), store, fx, output); err != nil {
			t.Fatalf("capture timing output: %v", err)
		}
		t.Logf("wrote timing summary to %s", output)
	}
}

func BenchmarkHotPathStoreMethods(b *testing.B) {
	store, _, fx := setupHotPathIntegration(b)
	benchmarks := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "AtlasBriefing",
			run: func(ctx context.Context) error {
				briefing, err := store.GetAtlasBriefing(ctx, "western-canada", fx.Since, fx.Until)
				if err == nil && briefing == nil {
					err = fmt.Errorf("nil briefing")
				}
				return err
			},
		},
		{
			name: "LiveSummary",
			run: func(ctx context.Context) error {
				_, err := store.GetLiveSummary(ctx, api.LiveSummaryFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until})
				return err
			},
		},
		{
			name: "LiveBackfill",
			run: func(ctx context.Context) error {
				_, err := store.ListLiveBackfill(ctx, api.LiveBackfillFilter{PayloadType: -1, RouteType: -1, IATAs: fx.IATAs, Limit: 50})
				return err
			},
		},
		{
			name: "NetgraphRoutes",
			run: func(ctx context.Context) error {
				_, err := store.ListKnownRoutes(ctx, fx.IATAs, 0, time.Time{}, 2500)
				return err
			},
		},
		{
			name: "KnownRoutesByNode",
			run: func(ctx context.Context) error {
				_, err := store.GetKnownRoutesByNode(ctx, fx.IATAs[0], fx.RouteNodeID, 0)
				return err
			},
		},
		{
			name: "KnownRoutesByNodeLimit300",
			run: func(ctx context.Context) error {
				_, err := store.GetKnownRoutesByNode(ctx, fx.IATAs[0], fx.RouteNodeID, 300)
				return err
			},
		},
		{
			name: "StatsSummary",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsSummary(ctx, api.StatsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until, Bucket: "1h", Limit: 100})
				return err
			},
		},
		{
			name: "ObserverHealth",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
					StatsFilter: api.StatsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until, Bucket: "1h", Limit: 100},
					StaleAfter:  15 * time.Minute,
				})
				return err
			},
		},
		{
			name: "NodeAnalytics",
			run: func(ctx context.Context) error {
				_, err := store.GetNodeAnalytics(ctx, fx.NodeID, api.NodeAnalyticsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until})
				return err
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := bm.run(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func setupHotPathIntegration(tb testing.TB) (*Store, *pgxpool.Pool, hotPathFixture) {
	tb.Helper()
	dsn := strings.TrimSpace(os.Getenv("BEACON_TEST_DATABASE_URL"))
	if dsn == "" {
		tb.Skip("set BEACON_TEST_DATABASE_URL to run DB-backed hot-path integration tests")
	}
	snapshotMode, err := hotPathSnapshotMode()
	if err != nil {
		tb.Fatalf("parse BEACON_HOTPATH_SNAPSHOT_MODE: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		tb.Fatalf("parse BEACON_TEST_DATABASE_URL: %v", err)
	}
	dbName := strings.ToLower(cfg.ConnConfig.Database)
	scale := 1
	if snapshotMode {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
		if timeout := strings.TrimSpace(os.Getenv("BEACON_HOTPATH_STATEMENT_TIMEOUT")); timeout != "" {
			cfg.ConnConfig.RuntimeParams["statement_timeout"] = timeout
		}
	} else {
		scale, err = hotPathFixtureScale()
		if err != nil {
			tb.Fatalf("parse BEACON_HOTPATH_FIXTURE_SCALE: %v", err)
		}
		if !strings.Contains(dbName, "test") {
			tb.Fatalf("refusing to reset database %q; use a disposable database name containing 'test'", cfg.ConnConfig.Database)
		}
	}

	timeout := 60*time.Second + time.Duration(scale)*2*time.Second
	if snapshotMode {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	tb.Cleanup(cancel)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		tb.Fatalf("connect integration database: %v", err)
	}
	tb.Cleanup(pool.Close)

	if snapshotMode {
		fx, err := loadHotPathSnapshotFixture(ctx, pool)
		if err != nil {
			tb.Fatalf("load hot-path snapshot context: %v", err)
		}
		return New(pool), pool, fx
	}

	if err := resetPublicSchema(ctx, pool); err != nil {
		tb.Fatalf("reset integration schema: %v", err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		tb.Fatalf("run migrations: %v", err)
	}
	fx, err := seedHotPathFixture(ctx, pool, scale)
	if err != nil {
		tb.Fatalf("seed hot-path fixture: %v", err)
	}
	return New(pool), pool, fx
}

func hotPathFixtureScale() (int, error) {
	raw := strings.TrimSpace(os.Getenv("BEACON_HOTPATH_FIXTURE_SCALE"))
	if raw == "" {
		return 1, nil
	}
	scale, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("must be a positive integer, got %q", raw)
	}
	if scale < 1 {
		return 0, fmt.Errorf("must be at least 1, got %d", scale)
	}
	if scale > maxHotPathFixtureScale {
		return 0, fmt.Errorf("must be %d or lower, got %d", maxHotPathFixtureScale, scale)
	}
	return scale, nil
}

func hotPathSnapshotMode() (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("BEACON_HOTPATH_SNAPSHOT_MODE")))
	if raw == "" {
		return false, nil
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("must be true or false, got %q", raw)
	}
}

func loadHotPathSnapshotFixture(ctx context.Context, pool *pgxpool.Pool) (hotPathFixture, error) {
	regionSlug := strings.TrimSpace(os.Getenv("BEACON_HOTPATH_REGION_SLUG"))
	if regionSlug == "" {
		regionSlug = defaultHotPathSnapshotRegion
	}
	since, until, err := hotPathSnapshotWindow(ctx, pool)
	if err != nil {
		return hotPathFixture{}, err
	}
	iatas, err := hotPathSnapshotIATAs(ctx, pool, regionSlug, since, until)
	if err != nil {
		return hotPathFixture{}, err
	}
	nodeID, err := hotPathSnapshotNodeID(ctx, pool, since, until, iatas)
	if err != nil {
		return hotPathFixture{}, err
	}
	routeNodeID, err := hotPathSnapshotRouteNodeID(ctx, pool, iatas[0])
	if err != nil {
		return hotPathFixture{}, err
	}

	var nodeCount, observerCount, packetCount, observationCount, knownRouteCount, neighborEdgeCount int64
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT COUNT(*) FROM nodes),
  (SELECT COUNT(*) FROM observers),
  (SELECT COUNT(*) FROM packets),
  (SELECT COUNT(*) FROM packet_observations),
  (SELECT COUNT(*) FROM known_routes),
  (SELECT COUNT(*) FROM node_neighbors)
`).Scan(&nodeCount, &observerCount, &packetCount, &observationCount, &knownRouteCount, &neighborEdgeCount); err != nil {
		return hotPathFixture{}, fmt.Errorf("count snapshot tables: %w", err)
	}

	return hotPathFixture{
		Since:             since,
		Until:             until,
		IATAs:             iatas,
		RegionSlug:        regionSlug,
		DatasetLabel:      "scrubbed snapshot",
		NodeID:            nodeID,
		RouteNodeID:       routeNodeID,
		RouteAt:           until,
		Scale:             0,
		NodeCount:         int(nodeCount),
		ObserverCount:     int(observerCount),
		PacketCount:       int(packetCount),
		ObservationCount:  int(observationCount),
		KnownRouteCount:   int(knownRouteCount),
		NeighborEdgeCount: int(neighborEdgeCount),
	}, nil
}

func hotPathSnapshotWindow(ctx context.Context, pool *pgxpool.Pool) (time.Time, time.Time, error) {
	until, ok, err := hotPathSnapshotTimeFromEnv("BEACON_HOTPATH_UNTIL")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !ok {
		if err := pool.QueryRow(ctx, `SELECT MAX(heard_at) FROM packet_observations`).Scan(&until); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("select snapshot window upper bound: %w", err)
		}
	}
	since, ok, err := hotPathSnapshotTimeFromEnv("BEACON_HOTPATH_SINCE")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !ok {
		hours, err := hotPathSnapshotWindowHours()
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		since = until.Add(-time.Duration(hours) * time.Hour)
	}
	if !since.Before(until) {
		return time.Time{}, time.Time{}, fmt.Errorf("snapshot window since %s must be before until %s", since.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	return since, until, nil
}

func hotPathSnapshotTimeFromEnv(name string) (time.Time, bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return time.Time{}, false, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse %s as RFC3339: %w", name, err)
	}
	return value, true, nil
}

func hotPathSnapshotWindowHours() (int, error) {
	raw := strings.TrimSpace(os.Getenv("BEACON_HOTPATH_WINDOW_HOURS"))
	if raw == "" {
		return defaultHotPathSnapshotWindowH, nil
	}
	hours, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse BEACON_HOTPATH_WINDOW_HOURS: %w", err)
	}
	if hours < 1 || hours > 168 {
		return 0, fmt.Errorf("BEACON_HOTPATH_WINDOW_HOURS must be between 1 and 168, got %d", hours)
	}
	return hours, nil
}

func hotPathSnapshotIATAs(ctx context.Context, pool *pgxpool.Pool, regionSlug string, since, until time.Time) ([]string, error) {
	if iatas := parseHotPathSnapshotIATAs(os.Getenv("BEACON_HOTPATH_IATAS")); len(iatas) > 0 {
		return iatas, nil
	}
	limit, err := hotPathSnapshotIATALimit()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT po.iata
FROM packet_observations po
JOIN region_iatas ri ON ri.iata = po.iata
JOIN regions r ON r.id = ri.region_id
WHERE r.slug = $1
  AND po.heard_at >= $2
  AND po.heard_at <= $3
  AND po.iata <> ''
GROUP BY po.iata
ORDER BY COUNT(*) DESC, po.iata
LIMIT $4`, regionSlug, since, until, limit)
	if err != nil {
		return nil, fmt.Errorf("select snapshot IATAs: %w", err)
	}
	defer rows.Close()

	iatas := []string{}
	for rows.Next() {
		var iata string
		if err := rows.Scan(&iata); err != nil {
			return nil, fmt.Errorf("scan snapshot IATA: %w", err)
		}
		iatas = append(iatas, strings.ToUpper(strings.TrimSpace(iata)))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot IATAs: %w", err)
	}
	if len(iatas) == 0 {
		return nil, fmt.Errorf("no snapshot IATAs found for region %q in window %s to %s; set BEACON_HOTPATH_IATAS or BEACON_HOTPATH_REGION_SLUG", regionSlug, since.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	return iatas, nil
}

func parseHotPathSnapshotIATAs(raw string) []string {
	parts := strings.Split(raw, ",")
	iatas := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		iata := strings.ToUpper(strings.TrimSpace(part))
		if iata == "" {
			continue
		}
		if _, ok := seen[iata]; ok {
			continue
		}
		seen[iata] = struct{}{}
		iatas = append(iatas, iata)
	}
	return iatas
}

func hotPathSnapshotIATALimit() (int, error) {
	raw := strings.TrimSpace(os.Getenv("BEACON_HOTPATH_IATA_LIMIT"))
	if raw == "" {
		return defaultHotPathSnapshotIATAs, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse BEACON_HOTPATH_IATA_LIMIT: %w", err)
	}
	if limit < 1 || limit > 20 {
		return 0, fmt.Errorf("BEACON_HOTPATH_IATA_LIMIT must be between 1 and 20, got %d", limit)
	}
	return limit, nil
}

func hotPathSnapshotNodeID(ctx context.Context, pool *pgxpool.Pool, since, until time.Time, iatas []string) (uuid.UUID, error) {
	var nodeID uuid.UUID
	if err := pool.QueryRow(ctx, `
SELECT n.id
FROM nodes n
JOIN packets p ON p.origin_pubkey = n.public_key
JOIN packet_observations po ON po.packet_hash = p.packet_hash
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY n.id
ORDER BY COUNT(*) DESC, MAX(po.heard_at) DESC
LIMIT 1`, since, until, strings.Join(iatas, ",")).Scan(&nodeID); err != nil {
		return uuid.Nil, fmt.Errorf("select snapshot node for analytics: %w", err)
	}
	return nodeID, nil
}

func hotPathSnapshotRouteNodeID(ctx context.Context, pool *pgxpool.Pool, iata string) (uuid.UUID, error) {
	var nodeID uuid.UUID
	if err := pool.QueryRow(ctx, `
SELECT u.node_id
FROM known_routes kr
CROSS JOIN LATERAL unnest(kr.node_ids) AS u(node_id)
WHERE ($1::text = '' OR kr.iata = $1)
GROUP BY u.node_id
ORDER BY COUNT(*) DESC, MAX(kr.last_seen) DESC
LIMIT 1`, iata).Scan(&nodeID); err != nil {
		return uuid.Nil, fmt.Errorf("select snapshot node for known-route containment: %w", err)
	}
	return nodeID, nil
}

func resetPublicSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO public;
`)
	return err
}

func seedHotPathFixture(ctx context.Context, pool *pgxpool.Pool, scale int) (hotPathFixture, error) {
	until := time.Now().UTC().Truncate(time.Second)
	since := until.Add(-6 * time.Hour)
	routeAt := until.Add(-20 * time.Minute)
	iatas := []string{"YVR", "YYJ"}
	nodeCount := 6 * scale
	observerCount := 3 * scale
	packetCount := 90 * scale
	knownRouteCount := 4 * scale
	neighborEdgeCount := 3 * scale

	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata, display_name, approx_lat, approx_lng)
VALUES
  ('YVR', 'Vancouver International', 49.1967, -123.1815),
  ('YYJ', 'Victoria International', 48.6469, -123.4260);
INSERT INTO regions (slug, name, display_order, center_lat, center_lng, zoom_level)
VALUES ('western-canada', 'Western Canada', 1, 49.9, -123.0, 6);
INSERT INTO region_iatas (region_id, iata)
SELECT r.id, i.iata
FROM regions r
JOIN iata_codes i ON i.iata IN ('YVR', 'YYJ')
WHERE r.slug = 'western-canada';
`); err != nil {
		return hotPathFixture{}, err
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO transport_scopes (name, display_name, transport_key, key_fingerprint)
VALUES ('#bc', 'BC Mesh', $1, $2);
`, hotPathHashBytes("transport-key", 16), hotPathHashBytes("transport-fingerprint", 8)); err != nil {
		return hotPathFixture{}, err
	}

	scopeID := int32(0)
	if err := pool.QueryRow(ctx, `SELECT id FROM transport_scopes WHERE name = '#bc'`).Scan(&scopeID); err != nil {
		return hotPathFixture{}, err
	}

	nodeIDs := make([]uuid.UUID, nodeCount)
	nodeKeys := make([][]byte, nodeCount)
	baseNodeNames := []string{"Origin Alpha", "Repeater Bravo", "Room Charlie", "Sensor Delta", "Peer Echo", "Peer Foxtrot"}
	nodeNames := make([]string, nodeCount)
	for i := range nodeIDs {
		nodeIDs[i] = uuid.New()
		nodeKeys[i] = hotPathHashBytes(fmt.Sprintf("node-key-%d", i), 32)
		if i < len(baseNodeNames) {
			nodeNames[i] = baseNodeNames[i]
		} else {
			nodeNames[i] = fmt.Sprintf("Fixture Node %03d", i+1)
		}
	}
	for i, id := range nodeIDs {
		nodeType := int16(1 + (i % 4))
		lat := 49.0 + float64(i%40)/20
		lng := -123.0 - float64((i/40)%40)/20
		if _, err := pool.Exec(ctx, `
INSERT INTO nodes (
  id, public_key, node_type, name, latitude, longitude, location_source, last_advert_at,
  default_scope_id, first_seen, last_seen, radio_freq_mhz, radio_sf, radio_bw_khz
) VALUES ($1, $2, $3, $4, $5, $6, 'fixture', $7, $8, $9, $10, 910.525, 7, 62.5)
`, id, nodeKeys[i], nodeType, nodeNames[i], lat, lng, routeAt, scopeID, since, routeAt); err != nil {
			return hotPathFixture{}, err
		}
		iata := iatas[i%len(iatas)]
		if _, err := pool.Exec(ctx, `
INSERT INTO node_iatas (node_id, iata, first_heard, last_heard, observation_count)
VALUES ($1, $2, $3, $4, $5)
`, id, iata, since, routeAt, int64(20+i)); err != nil {
			return hotPathFixture{}, err
		}
	}

	observerIDs := make([]uuid.UUID, observerCount)
	observerNames := make([]string, observerCount)
	for i := range observerIDs {
		observerIDs[i] = uuid.New()
		observerNames[i] = fmt.Sprintf("Fixture Observer %03d", i+1)
	}
	for i, id := range observerIDs {
		observerType := "station"
		if _, err := pool.Exec(ctx, `
INSERT INTO observers (
  id, public_key, display_name, observer_type, software_version, radio_freq_mhz, radio_sf,
  radio_bw_khz, last_status_at, first_seen, last_seen, observation_count
) VALUES ($1, $2, $3, $4, 'fixture', 910.525, 7, 62.5, $5, $6, $7, 0)
`, id, hotPathHashBytes(fmt.Sprintf("observer-key-%d", i), 32), observerNames[i], observerType, routeAt.Add(-time.Duration(i)*time.Minute), since, routeAt); err != nil {
			return hotPathFixture{}, err
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO observer_scopes (observer_id, scope_id, first_seen, last_seen)
VALUES ($1, $2, $3, $4)
`, id, scopeID, since, routeAt); err != nil {
			return hotPathFixture{}, err
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO observer_telemetry (
  observer_id, reported_at, battery_voltage_mv, airtime_tx_pct, airtime_rx_pct,
  noise_floor_db, uptime_seconds, queue_length, receive_errors
) VALUES ($1, $2, $3, $4, $5, $6, 3600, $7, $8)
`, id, routeAt.Add(-time.Duration(i)*time.Minute), int32(3900-100*(i%8)), float32(4+i%10), float32(3+i%10), float32(-118+i%8), int32(i%20), int32(i%10)); err != nil {
			return hotPathFixture{}, err
		}
	}

	packetStep := until.Add(-time.Minute).Sub(since)
	if packetCount > 1 {
		packetStep = packetStep / time.Duration(packetCount-1)
	}
	if packetStep <= 0 {
		packetStep = time.Second
	}
	observationCount := 0
	for i := 0; i < packetCount; i++ {
		heardAt := since.Add(time.Duration(i) * packetStep)
		packetHash := hotPathHashBytes(fmt.Sprintf("packet-%d", i), 32)
		payloadType := int16(1 + (i % 4))
		routeType := int16(i % 3)
		originKey := nodeKeys[i%len(nodeKeys)]
		if _, err := pool.Exec(ctx, `
INSERT INTO packets (
  packet_hash, payload_type, payload_version, route_type, scope_id, origin_pubkey,
  raw_payload, raw_header, parsed_payload, first_heard_at, last_heard_at
) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, '{}'::jsonb, $8, $9)
`, packetHash, payloadType, routeType, scopeID, originKey, hotPathHashBytes(fmt.Sprintf("payload-%d", i), 6), []byte{0x01, byte(i)}, heardAt, heardAt.Add(30*time.Second)); err != nil {
			return hotPathFixture{}, err
		}
		for j := 0; j < 2; j++ {
			observerID := observerIDs[(i+j)%len(observerIDs)]
			iata := iatas[(i+j)%len(iatas)]
			if _, err := pool.Exec(ctx, `
INSERT INTO packet_observations (
  packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count,
  path_bytes, rssi, snr, propagation_time_ms, radio_freq_mhz, spread_factor,
  bandwidth_khz, coding_rate, source_broker
) VALUES ($1, $2, $3, $4, 34, 2, 2, $5, $6, $7, $8, 910.525, 7, 62.5, 5, 'fixture')
`, packetHash, observerID, iata, heardAt.Add(time.Duration(j)*time.Second), hotPathHashBytes(fmt.Sprintf("path-%d-%d", i, j), 4), int16(-82+(i%12)), float32(5+(i%8)), int32(120+j)); err != nil {
				return hotPathFixture{}, err
			}
			observationCount++
		}
	}

	routeStep := routeAt.Sub(since)
	if knownRouteCount > 0 {
		routeStep = routeStep / time.Duration(knownRouteCount)
	}
	for i := 0; i < knownRouteCount; i++ {
		firstNode := i % len(nodeIDs)
		secondNode := (i*7 + 1) % len(nodeIDs)
		thirdNode := (i*13 + 2) % len(nodeIDs)
		if secondNode == firstNode {
			secondNode = (secondNode + 1) % len(nodeIDs)
		}
		if thirdNode == firstNode || thirdNode == secondNode {
			thirdNode = (thirdNode + 2) % len(nodeIDs)
		}
		nodes := []uuid.UUID{nodeIDs[firstNode], nodeIDs[secondNode], nodeIDs[thirdNode]}
		iata := iatas[i%len(iatas)]
		lastSeen := since.Add(time.Duration(i+1) * routeStep)
		if _, err := pool.Exec(ctx, `
INSERT INTO known_routes (
  node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
) VALUES (
  ARRAY[$1::uuid, $2::uuid, $3::uuid],
  ARRAY[$4::bytea, $5::bytea, $6::bytea],
  $7, 3, $8, $9, $10
)
`, nodes[0], nodes[1], nodes[2], hotPathHashBytes(fmt.Sprintf("route-%d-hop-1", i), 4), hotPathHashBytes(fmt.Sprintf("route-%d-hop-2", i), 4), hotPathHashBytes(fmt.Sprintf("route-%d-hop-3", i), 4), iata, since, lastSeen, int64(18+i%31)); err != nil {
			return hotPathFixture{}, err
		}
	}

	for i := 0; i < neighborEdgeCount; i++ {
		fromIndex := i % len(nodeIDs)
		toIndex := (i*5 + 1) % len(nodeIDs)
		if toIndex == fromIndex {
			toIndex = (toIndex + 1) % len(nodeIDs)
		}
		iata := iatas[i%len(iatas)]
		lastSeen := since.Add(time.Duration(i+1) * routeStep)
		if _, err := pool.Exec(ctx, `
INSERT INTO node_neighbors (node_id, neighbor_id, iata, first_seen, last_seen, observation_count)
VALUES ($1, $2, $3, $4, $5, 12)
`, nodeIDs[fromIndex], nodeIDs[toIndex], iata, since, lastSeen); err != nil {
			return hotPathFixture{}, err
		}
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO observer_iatas (observer_id, iata, first_heard, last_heard, observation_count)
SELECT observer_id, iata, MIN(heard_at), MAX(heard_at), COUNT(*)::bigint
FROM packet_observations
GROUP BY observer_id, iata;
UPDATE observers o
SET observation_count = counts.count, last_seen = counts.last_seen
FROM (
  SELECT observer_id, COUNT(*)::bigint AS count, MAX(heard_at) AS last_seen
  FROM packet_observations
  GROUP BY observer_id
) counts
WHERE counts.observer_id = o.id;
REFRESH MATERIALIZED VIEW mv_hourly_iata_stats;
INSERT INTO stats_hourly_iata (iata, hour, observation_count, unique_packets, active_observers, refreshed_at)
SELECT iata, hour, observation_count, unique_packets, active_observers, NOW()
FROM mv_hourly_iata_stats
ON CONFLICT (iata, hour) DO UPDATE SET
  observation_count = EXCLUDED.observation_count,
  unique_packets = EXCLUDED.unique_packets,
  active_observers = EXCLUDED.active_observers,
  refreshed_at = EXCLUDED.refreshed_at;
REFRESH MATERIALIZED VIEW mv_top_nodes_by_iata;
REFRESH MATERIALIZED VIEW mv_radio_presets;
`); err != nil {
		return hotPathFixture{}, err
	}

	return hotPathFixture{
		Since:             since,
		Until:             until,
		IATAs:             iatas,
		RegionSlug:        defaultHotPathSnapshotRegion,
		DatasetLabel:      "synthetic fixture",
		NodeID:            nodeIDs[0],
		RouteNodeID:       nodeIDs[0],
		RouteAt:           routeAt,
		Scale:             scale,
		NodeCount:         nodeCount,
		ObserverCount:     observerCount,
		PacketCount:       packetCount,
		ObservationCount:  observationCount,
		KnownRouteCount:   knownRouteCount,
		NeighborEdgeCount: neighborEdgeCount,
	}, nil
}

func exerciseHotPaths(tb testing.TB, store *Store, fx hotPathFixture) {
	tb.Helper()
	ctx := context.Background()
	briefing, err := store.GetAtlasBriefing(ctx, fx.RegionSlug, fx.Since, fx.Until)
	if err != nil {
		tb.Fatalf("GetAtlasBriefing: %v", err)
	}
	if briefing == nil || briefing.Health.HealthScore == 0 || len(briefing.Hotspots) == 0 {
		tb.Fatalf("GetAtlasBriefing returned incomplete payload: %#v", briefing)
	}
	live, err := store.GetLiveSummary(ctx, api.LiveSummaryFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until})
	if err != nil {
		tb.Fatalf("GetLiveSummary: %v", err)
	}
	if live.ObservationCount == 0 || len(live.PayloadMix) == 0 {
		tb.Fatalf("GetLiveSummary returned incomplete payload: %#v", live)
	}
	backfill, err := store.ListLiveBackfill(ctx, api.LiveBackfillFilter{PayloadType: -1, RouteType: -1, IATAs: fx.IATAs, Limit: 25})
	if err != nil {
		tb.Fatalf("ListLiveBackfill: %v", err)
	}
	if len(backfill.Items) == 0 {
		tb.Fatal("ListLiveBackfill returned no items")
	}
	routes, err := store.ListKnownRoutes(ctx, fx.IATAs, 0, time.Time{}, 25)
	if err != nil {
		tb.Fatalf("ListKnownRoutes: %v", err)
	}
	if len(routes) == 0 || len(routes[0].Hops) == 0 {
		tb.Fatalf("ListKnownRoutes returned incomplete routes: %#v", routes)
	}
	routesByNode, err := store.GetKnownRoutesByNode(ctx, fx.IATAs[0], fx.RouteNodeID, 0)
	if err != nil {
		tb.Fatalf("GetKnownRoutesByNode: %v", err)
	}
	if len(routesByNode) == 0 || len(routesByNode[0].Hops) == 0 {
		tb.Fatalf("GetKnownRoutesByNode returned incomplete routes: %#v", routesByNode)
	}
	stats, err := store.GetStatsSummary(ctx, api.StatsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until, Bucket: "1h", Limit: 100})
	if err != nil {
		tb.Fatalf("GetStatsSummary: %v", err)
	}
	if stats.Overview.TotalObservations == 0 || len(stats.TopNodes) == 0 {
		tb.Fatalf("GetStatsSummary returned incomplete payload: %#v", stats)
	}
	health, err := store.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
		StatsFilter: api.StatsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until, Bucket: "1h", Limit: 100},
		StaleAfter:  15 * time.Minute,
	})
	if err != nil {
		tb.Fatalf("GetStatsObserverHealth: %v", err)
	}
	if len(health.Items) == 0 || health.Summary.TotalObservers == 0 {
		tb.Fatalf("GetStatsObserverHealth returned incomplete payload: %#v", health)
	}
	analytics, err := store.GetNodeAnalytics(ctx, fx.NodeID, api.NodeAnalyticsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until})
	if err != nil {
		tb.Fatalf("GetNodeAnalytics: %v", err)
	}
	if analytics.KPIs.ObservationCount == 0 || len(analytics.PayloadMix) == 0 {
		tb.Fatalf("GetNodeAnalytics returned incomplete payload: %#v", analytics)
	}
}

type explainSpec struct {
	Name string
	SQL  string
	Args []any
}

type explainSummary struct {
	Name          string
	PlanningMS    float64
	ExecutionMS   float64
	ActualRows    float64
	SharedHits    int64
	SharedReads   int64
	Nodes         []string
	Indexes       []string
	Relations     []string
	ExpectedIndex string
}

type timingSummary struct {
	Group      string
	Name       string
	DurationMS float64
}

func captureHotPathExplainMarkdown(ctx context.Context, pool *pgxpool.Pool, fx hotPathFixture, output string) error {
	specs := []explainSpec{
		{
			Name: "atlas_iata_rollup",
			SQL: `
WITH counts AS MATERIALIZED (
  SELECT
    po.iata,
    COUNT(*)::bigint AS observation_count,
    COUNT(DISTINCT po.packet_hash)::bigint AS unique_packets,
    COUNT(DISTINCT po.observer_id)::bigint AS active_observers
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.iata
)
SELECT
  i.iata,
  i.display_name,
  i.approx_lat,
  i.approx_lng,
  COALESCE(c.observation_count, 0)::bigint AS observation_count,
  COALESCE(c.unique_packets, 0)::bigint AS unique_packets,
  COALESCE(c.active_observers, 0)::bigint AS active_observers
FROM iata_codes i
LEFT JOIN counts c ON c.iata = i.iata
WHERE ($3::text = '' OR i.iata = ANY(string_to_array($3::text, ',')))
ORDER BY observation_count DESC, i.iata`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ",")},
		},
		{
			Name: "atlas_payload_route_mix_combined",
			SQL: `
WITH base AS (
  SELECT p.payload_type, p.route_type
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
payload_mix AS (
  SELECT payload_type AS code, COUNT(*)::bigint AS count
  FROM base
  GROUP BY payload_type
),
route_mix AS (
  SELECT route_type AS code, COUNT(*)::bigint AS count
  FROM base
  GROUP BY route_type
  ORDER BY count DESC, route_type ASC
  LIMIT 8
)
SELECT 'payload' AS kind, code, count
FROM payload_mix
UNION ALL
SELECT 'route' AS kind, code, count
FROM route_mix
ORDER BY kind, count DESC, code ASC`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ",")},
		},
		{
			Name: "atlas_active_nodes_by_iata",
			SQL: `
WITH active_origins AS (
  SELECT po.iata, p.origin_pubkey
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND p.origin_pubkey IS NOT NULL
  GROUP BY po.iata, p.origin_pubkey
)
SELECT ao.iata, COUNT(*)::bigint
FROM active_origins ao
JOIN nodes n ON n.public_key = ao.origin_pubkey
GROUP BY ao.iata`,
			Args: []any{fx.Since, fx.Until},
		},
		{
			Name: "atlas_top_nodes",
			SQL: `
WITH node_counts AS (
  SELECT
    p.origin_pubkey,
    COALESCE((array_agg(DISTINCT po.iata ORDER BY po.iata))[1], '')::text AS iata,
    COUNT(*)::bigint AS observation_count,
    MAX(po.heard_at)::timestamptz AS last_heard
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND p.origin_pubkey IS NOT NULL
  GROUP BY p.origin_pubkey
)
SELECT
  n.id,
  n.name,
  n.node_type,
  nc.iata,
  nc.observation_count,
  nc.last_heard
FROM node_counts nc
JOIN nodes n ON n.public_key = nc.origin_pubkey
ORDER BY nc.observation_count DESC, nc.last_heard DESC
LIMIT $4`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ","), int32(8)},
		},
		{
			Name: "live_summary_combined",
			SQL: `
WITH base AS MATERIALIZED (
  SELECT
    po.id,
    po.packet_hash,
    po.observer_id,
    po.iata,
    p.payload_type,
    p.route_type
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
),
summary AS (
  SELECT
    COALESCE(MAX(id), 0)::bigint AS latest_observation_id,
    COUNT(DISTINCT packet_hash)::bigint AS packet_count,
    COUNT(*)::bigint AS observation_count,
    COUNT(DISTINCT observer_id)::bigint AS active_observers
  FROM base
),
payload_mix AS (
  SELECT payload_type, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY payload_type
  ORDER BY observation_count DESC, payload_type ASC
  LIMIT 8
),
route_mix AS (
  SELECT route_type, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY route_type
  ORDER BY observation_count DESC, route_type ASC
),
iata_mix AS (
  SELECT iata, COUNT(*)::bigint AS observation_count
  FROM base
  GROUP BY iata
  ORDER BY observation_count DESC, iata ASC
  LIMIT 8
),
observer_counts AS (
  SELECT
    b.observer_id,
    o.display_name,
    o.observer_type,
    (array_agg(b.iata ORDER BY b.id DESC))[1] AS latest_iata,
    COUNT(*)::bigint AS observation_count
  FROM base b
  LEFT JOIN observers o ON o.id = b.observer_id
  GROUP BY b.observer_id, o.display_name, o.observer_type
),
observer_mix AS (
  SELECT observer_id, display_name, observer_type, latest_iata, observation_count
  FROM observer_counts
  ORDER BY observation_count DESC, latest_iata ASC
  LIMIT 8
)
SELECT
  summary.latest_observation_id,
  summary.packet_count,
  summary.observation_count,
  summary.active_observers,
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'payloadType', payload_type,
      'count', observation_count
    ) ORDER BY observation_count DESC, payload_type ASC)
    FROM payload_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'routeType', route_type,
      'count', observation_count
    ) ORDER BY observation_count DESC, route_type ASC)
    FROM route_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'iata', iata,
      'count', observation_count
    ) ORDER BY observation_count DESC, iata ASC)
    FROM iata_mix
  ), '[]'::jsonb),
  COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'observerId', observer_id,
      'displayName', display_name,
      'observerType', observer_type,
      'iata', latest_iata,
      'observationCount', observation_count
    ) ORDER BY observation_count DESC, latest_iata ASC)
    FROM observer_mix
  ), '[]'::jsonb)
FROM summary`,
			Args: []any{fx.Until.Add(-15 * time.Minute), fx.Until, strings.Join(fx.IATAs, ",")},
		},
		{
			Name: "live_backfill_page",
			SQL: `
SELECT
  po.id,
  encode(p.packet_hash, 'hex') AS packet_hash,
  p.payload_type,
  p.route_type,
  po.iata,
  po.heard_at
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE ($2::smallint = -1 OR p.payload_type = $2::smallint)
  AND ($3::smallint = -1 OR p.route_type = $3::smallint)
  AND ($4::text = '' OR po.iata = ANY(string_to_array($4::text, ',')))
  AND ($5::text = '' OR ts.name = $5::text)
  AND po.id > $1
ORDER BY po.id ASC
LIMIT $6`,
			Args: []any{int64(0), int16(-1), int16(-1), strings.Join(fx.IATAs, ","), "", int32(51)},
		},
		{
			Name: "netgraph_known_routes",
			SQL: `
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE ($1::text = '' OR iata = ANY(string_to_array($1::text, ',')))
  AND ($2::int = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL OR last_seen < $3)
ORDER BY last_seen DESC
LIMIT $4`,
			Args: []any{strings.Join(fx.IATAs, ","), int32(0), nil, int32(100)},
		},
		{
			Name: "netgraph_known_routes_cap_2500",
			SQL: `
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE ($1::text = '' OR iata = ANY(string_to_array($1::text, ',')))
  AND ($2::int = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL OR last_seen < $3)
ORDER BY last_seen DESC
LIMIT $4`,
			Args: []any{strings.Join(fx.IATAs, ","), int32(0), nil, int32(2500)},
		},
		{
			Name: "known_routes_node_containment",
			SQL: `
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE ($1::text = '' OR iata = $1)
  AND node_ids @> ARRAY[$2::uuid]
ORDER BY hop_count ASC, last_seen DESC, observation_count DESC, id ASC`,
			Args: []any{fx.IATAs[0], fx.RouteNodeID},
		},
		{
			Name: "known_routes_node_containment_limit300",
			SQL: `
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE ($1::text = '' OR iata = $1)
  AND node_ids @> ARRAY[$2::uuid]
ORDER BY hop_count ASC, last_seen DESC, observation_count DESC, id ASC
LIMIT $3`,
			Args: []any{fx.IATAs[0], fx.RouteNodeID, int32(300)},
		},
		{
			Name: "stats_overview_window",
			SQL: `
SELECT
  COUNT(DISTINCT po.packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT po.observer_id)::bigint,
  COUNT(DISTINCT po.iata)::bigint
FROM packet_observations po
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ",")},
		},
		{
			Name: "stats_payloads_combined",
			SQL: `
SELECT
  CASE
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.route_type) = 1 THEN 'payload_total'
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.payload_type) = 1 THEN 'route_total'
    WHEN GROUPING(p.route_type) = 1 THEN 'payload_timeline'
    ELSE 'route_timeline'
  END AS kind,
  b.bucket,
  CASE WHEN GROUPING(p.route_type) = 1 THEN p.payload_type ELSE p.route_type END AS code,
  COUNT(*)::bigint AS count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
CROSS JOIN LATERAL (
  SELECT to_timestamp(floor(extract(epoch from po.heard_at) / ($4::double precision * 3600)) * ($4::double precision * 3600)) AS bucket
) b
WHERE po.heard_at >= $1
  AND po.heard_at <= $2
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
GROUP BY GROUPING SETS ((p.payload_type), (p.route_type), (b.bucket, p.payload_type), (b.bucket, p.route_type))
ORDER BY
  CASE
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.route_type) = 1 THEN 1
    WHEN GROUPING(b.bucket) = 1 AND GROUPING(p.payload_type) = 1 THEN 2
    WHEN GROUPING(p.route_type) = 1 THEN 3
    ELSE 4
  END,
  b.bucket ASC NULLS FIRST,
  count DESC,
  code ASC`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ","), float64(1)},
		},
		{
			Name: "stats_top_observers_window",
			SQL: `
WITH window_counts AS MATERIALIZED (
  SELECT po.observer_id, COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id
),
latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata
  FROM observer_iatas oi
  JOIN window_counts wc ON wc.observer_id = oi.observer_id
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
)
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE(li.iata, '') AS latest_iata,
  wc.observation_count
FROM window_counts wc
JOIN observers o ON o.id = wc.observer_id
LEFT JOIN latest_iata li ON li.observer_id = wc.observer_id
ORDER BY wc.observation_count DESC, latest_iata ASC
LIMIT $4`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ","), int32(10)},
		},
		{
			Name: "stats_top_nodes_window",
			SQL: `
WITH node_counts AS (
  SELECT
    p.origin_pubkey,
    po.iata,
    COUNT(*)::bigint AS observation_count,
    MAX(po.heard_at)::timestamptz AS last_heard
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
    AND p.origin_pubkey IS NOT NULL
  GROUP BY p.origin_pubkey, po.iata
)
SELECT
  n.id,
  n.name,
  n.node_type,
  nc.iata,
  nc.observation_count,
  nc.last_heard
FROM node_counts nc
JOIN nodes n ON n.public_key = nc.origin_pubkey
ORDER BY nc.observation_count DESC, nc.last_heard DESC
LIMIT $4`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ","), int32(10)},
		},
		{
			Name: "observer_health",
			SQL: `
WITH latest_iata AS (
  SELECT DISTINCT ON (oi.observer_id)
    oi.observer_id,
    oi.iata,
    oi.last_heard AS heard_at
  FROM observer_iatas oi
  WHERE ($3::text = '' OR oi.iata = ANY(string_to_array($3::text, ',')))
  ORDER BY oi.observer_id, oi.last_heard DESC
),
window_counts AS (
  SELECT po.observer_id, COUNT(*)::bigint AS observation_count
  FROM packet_observations po
  WHERE po.heard_at >= $1
    AND po.heard_at <= $2
    AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  GROUP BY po.observer_id
),
latest_tel AS (
  SELECT DISTINCT ON (ot.observer_id)
    ot.observer_id,
    ot.reported_at,
    ot.battery_voltage_mv,
    ot.airtime_tx_pct,
    ot.airtime_rx_pct,
    ot.noise_floor_db,
    ot.queue_length,
    ot.receive_errors
  FROM observer_telemetry ot
  ORDER BY ot.observer_id, ot.reported_at DESC
)
SELECT
  o.id,
  COALESCE(li.iata, '') AS iata,
  CASE WHEN COALESCE(o.last_status_at, o.last_seen) >= $4 THEN 'online' ELSE 'offline' END AS status,
  COALESCE(wc.observation_count, 0)::bigint,
  lt.reported_at
FROM observers o
LEFT JOIN latest_iata li ON li.observer_id = o.id
LEFT JOIN window_counts wc ON wc.observer_id = o.id
LEFT JOIN latest_tel lt ON lt.observer_id = o.id
WHERE ($3::text = '' OR li.iata IS NOT NULL)
ORDER BY COALESCE(wc.observation_count, 0) DESC, COALESCE(li.heard_at, o.last_status_at, o.last_seen) DESC
LIMIT $5`,
			Args: []any{fx.Since, fx.Until, strings.Join(fx.IATAs, ","), fx.Until.Add(-15 * time.Minute), int32(100)},
		},
		{
			Name: "scope_stats",
			SQL: `
SELECT
    ts.name,
    COALESCE(packet_counts.packet_count, 0)::bigint AS packet_count,
    COALESCE(observer_counts.observer_count, 0)::bigint AS observer_count,
    COALESCE(node_counts.node_count, 0)::bigint AS node_count
FROM transport_scopes ts
LEFT JOIN (
  SELECT scope_id, COUNT(*)::bigint AS packet_count
  FROM packets
  WHERE scope_id IS NOT NULL
  GROUP BY scope_id
) packet_counts ON packet_counts.scope_id = ts.id
LEFT JOIN (
  SELECT scope_id, COUNT(*)::bigint AS observer_count
  FROM observer_scopes
  GROUP BY scope_id
) observer_counts ON observer_counts.scope_id = ts.id
LEFT JOIN (
  SELECT default_scope_id AS scope_id, COUNT(*)::bigint AS node_count
  FROM nodes
  WHERE default_scope_id IS NOT NULL
  GROUP BY default_scope_id
) node_counts ON node_counts.scope_id = ts.id
ORDER BY ts.name`,
		},
		{
			Name: "node_analytics_kpis",
			SQL: `
WITH target AS (SELECT public_key FROM nodes WHERE id = $1),
filtered AS (
  SELECT po.packet_hash, po.observer_id, po.iata, po.heard_at, po.rssi, po.snr, po.hop_count
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  JOIN target t ON t.public_key = p.origin_pubkey
  WHERE po.heard_at >= $2
    AND po.heard_at <= $3
    AND ($4::text = '' OR po.iata = ANY(string_to_array(upper($4::text), ',')))
)
SELECT
  COUNT(DISTINCT packet_hash)::bigint,
  COUNT(*)::bigint,
  COUNT(DISTINCT observer_id)::bigint,
  COUNT(DISTINCT iata)::bigint,
  MIN(heard_at),
  MAX(heard_at),
  AVG(snr)::double precision,
  AVG(rssi)::double precision,
  AVG(hop_count)::double precision
FROM filtered`,
			Args: []any{fx.NodeID, fx.Since, fx.Until, strings.Join(fx.IATAs, ",")},
		},
	}

	summaries := make([]explainSummary, 0, len(specs))
	for _, spec := range specs {
		raw, err := explainJSON(ctx, pool, spec)
		if err != nil {
			return fmt.Errorf("%s: %w", spec.Name, err)
		}
		summary, err := summarizeExplain(spec.Name, raw)
		if err != nil {
			return fmt.Errorf("%s: %w", spec.Name, err)
		}
		summaries = append(summaries, summary)
	}

	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}
	return os.WriteFile(output, []byte(renderExplainMarkdown(fx, summaries)), 0o644)
}

func captureHotPathTimingMarkdown(ctx context.Context, store *Store, fx hotPathFixture, output string) error {
	summaries, err := measureHotPathTimings(ctx, store, fx)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}
	return os.WriteFile(output, []byte(renderTimingMarkdown(fx, summaries)), 0o644)
}

func measureHotPathTimings(ctx context.Context, store *Store, fx hotPathFixture) ([]timingSummary, error) {
	iataFilter := strings.Join(fx.IATAs, ",")
	statsFilter := api.StatsFilter{IATAs: fx.IATAs, Since: fx.Since, Until: fx.Until, Bucket: "1h", Limit: 100}
	liveSince := fx.Until.Add(-15 * time.Minute)
	if liveSince.Before(fx.Since) {
		liveSince = fx.Since
	}

	summaries := []timingSummary{}
	measure := func(group, name string, run func(context.Context) error) error {
		start := time.Now()
		err := run(ctx)
		summaries = append(summaries, timingSummary{
			Group:      group,
			Name:       name,
			DurationMS: float64(time.Since(start).Microseconds()) / 1000,
		})
		if err != nil {
			return fmt.Errorf("%s/%s: %w", group, name, err)
		}
		return nil
	}

	var currentIATAs []api.AtlasIATA
	var previousIATAs []api.AtlasIATA
	atlasSteps := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "getAtlasIATAs_current_previous",
			run: func(ctx context.Context) error {
				var err error
				currentIATAs, previousIATAs, err = store.getAtlasCurrentAndPreviousIATAs(ctx, fx.Since, fx.Until, "")
				return err
			},
		},
		{
			name: "getAtlasPayloadAndRouteMix_region",
			run: func(ctx context.Context) error {
				_, _, err := store.getAtlasPayloadAndRouteMix(ctx, fx.Since, fx.Until, iataFilter)
				return err
			},
		},
		{
			name: "getAtlasTopNodes_region",
			run: func(ctx context.Context) error {
				_, err := store.getAtlasTopNodes(ctx, fx.Since, fx.Until, iataFilter, 8)
				return err
			},
		},
		{
			name: "getAtlasTopObservers_region",
			run: func(ctx context.Context) error {
				_, err := store.getAtlasTopObservers(ctx, fx.Since, fx.Until, iataFilter, 8)
				return err
			},
		},
		{
			name: "getAtlasScopeSummaries",
			run: func(ctx context.Context) error {
				_, err := store.getAtlasScopeSummaries(ctx, len(fx.IATAs))
				return err
			},
		},
		{
			name: "GetStatsObserverHealth_limit500",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
					StatsFilter: api.StatsFilter{
						IATAs: fx.IATAs,
						Since: fx.Since,
						Until: fx.Until,
						Limit: 500,
					},
					StaleAfter: statsDefaultStaleAfter,
				})
				return err
			},
		},
		{
			name: "getAtlasNotableRoutes",
			run: func(ctx context.Context) error {
				_, err := store.getAtlasNotableRoutes(ctx, fx.IATAs)
				return err
			},
		},
		{
			name: "getAtlasBriefingRegions_preloaded",
			run: func(ctx context.Context) error {
				_, err := store.getAtlasBriefingRegions(ctx, fx.Since, fx.Until, currentIATAs, previousIATAs, false)
				return err
			},
		},
	}
	for _, step := range atlasSteps {
		if err := measure("AtlasBriefing", step.name, step.run); err != nil {
			return nil, err
		}
	}

	netgraphSteps := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "ListKnownRoutes_cap2500",
			run: func(ctx context.Context) error {
				_, err := store.ListKnownRoutes(ctx, fx.IATAs, 0, time.Time{}, 2500)
				return err
			},
		},
		{
			name: "GetKnownRoutesByNode",
			run: func(ctx context.Context) error {
				_, err := store.GetKnownRoutesByNode(ctx, fx.IATAs[0], fx.RouteNodeID, 0)
				return err
			},
		},
		{
			name: "GetKnownRoutesByNode_limit300",
			run: func(ctx context.Context) error {
				_, err := store.GetKnownRoutesByNode(ctx, fx.IATAs[0], fx.RouteNodeID, 300)
				return err
			},
		},
	}
	for _, step := range netgraphSteps {
		if err := measure("Netgraph", step.name, step.run); err != nil {
			return nil, err
		}
	}

	statsSteps := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "getStatsOverviewWindow",
			run: func(ctx context.Context) error {
				_, err := store.getStatsOverviewWindow(ctx, statsFilter, iataFilter)
				return err
			},
		},
		{
			name: "GetLiveSummary_15m",
			run: func(ctx context.Context) error {
				_, err := store.GetLiveSummary(ctx, api.LiveSummaryFilter{IATAs: fx.IATAs, Since: liveSince, Until: fx.Until})
				return err
			},
		},
		{
			name: "GetStatsNodeTypes",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsNodeTypes(ctx, fx.IATAs)
				return err
			},
		},
		{
			name: "GetStatsPayloads",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsPayloads(ctx, statsFilter)
				return err
			},
		},
		{
			name: "getStatsTopIATAs",
			run: func(ctx context.Context) error {
				_, err := store.getStatsTopIATAs(ctx, statsFilter, iataFilter)
				return err
			},
		},
		{
			name: "getStatsTopObserversWindow",
			run: func(ctx context.Context) error {
				_, err := store.getStatsTopObserversWindow(ctx, statsFilter, iataFilter, 10)
				return err
			},
		},
		{
			name: "getStatsTopNodesWindow",
			run: func(ctx context.Context) error {
				_, err := store.getStatsTopNodesWindow(ctx, statsFilter, iataFilter, 10)
				return err
			},
		},
		{
			name: "GetRadioPresets",
			run: func(ctx context.Context) error {
				_, err := store.GetRadioPresets(ctx, "", fx.IATAs)
				return err
			},
		},
		{
			name: "GetScopeStats",
			run: func(ctx context.Context) error {
				_, err := store.GetScopeStats(ctx)
				return err
			},
		},
		{
			name: "GetStatsObserverHealth_limit500",
			run: func(ctx context.Context) error {
				_, err := store.GetStatsObserverHealth(ctx, api.StatsObserverHealthFilter{
					StatsFilter: api.StatsFilter{
						IATAs:  fx.IATAs,
						Since:  fx.Since,
						Until:  fx.Until,
						Bucket: "1h",
						Limit:  500,
					},
					StaleAfter: statsDefaultStaleAfter,
				})
				return err
			},
		},
	}
	for _, step := range statsSteps {
		if err := measure("StatsSummary", step.name, step.run); err != nil {
			return nil, err
		}
	}

	return summaries, nil
}

func explainJSON(ctx context.Context, pool *pgxpool.Pool, spec explainSpec) ([]byte, error) {
	var raw []byte
	err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) "+spec.SQL, spec.Args...).Scan(&raw)
	return raw, err
}

func summarizeExplain(name string, raw []byte) (explainSummary, error) {
	var docs []map[string]any
	if err := json.Unmarshal(raw, &docs); err != nil {
		return explainSummary{}, err
	}
	if len(docs) == 0 {
		return explainSummary{}, fmt.Errorf("empty explain document")
	}
	root, ok := docs[0]["Plan"].(map[string]any)
	if !ok {
		return explainSummary{}, fmt.Errorf("missing root plan")
	}
	nodes := map[string]struct{}{}
	indexes := map[string]struct{}{}
	relations := map[string]struct{}{}
	var sharedHits, sharedReads int64
	collectPlanFields(root, nodes, indexes, relations, &sharedHits, &sharedReads)
	return explainSummary{
		Name:        name,
		PlanningMS:  floatField(docs[0], "Planning Time"),
		ExecutionMS: floatField(docs[0], "Execution Time"),
		ActualRows:  floatField(root, "Actual Rows"),
		SharedHits:  sharedHits,
		SharedReads: sharedReads,
		Nodes:       sortedKeys(nodes),
		Indexes:     sortedKeys(indexes),
		Relations:   sortedKeys(relations),
	}, nil
}

func collectPlanFields(plan map[string]any, nodes, indexes, relations map[string]struct{}, sharedHits, sharedReads *int64) {
	addStringField(plan, "Node Type", nodes)
	addStringField(plan, "Index Name", indexes)
	addStringField(plan, "Relation Name", relations)
	*sharedHits += int64Field(plan, "Shared Hit Blocks")
	*sharedReads += int64Field(plan, "Shared Read Blocks")
	if children, ok := plan["Plans"].([]any); ok {
		for _, child := range children {
			if childPlan, ok := child.(map[string]any); ok {
				collectPlanFields(childPlan, nodes, indexes, relations, sharedHits, sharedReads)
			}
		}
	}
}

func addStringField(plan map[string]any, key string, out map[string]struct{}) {
	value, ok := plan[key].(string)
	if ok && value != "" {
		out[value] = struct{}{}
	}
}

func floatField(m map[string]any, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func int64Field(m map[string]any, key string) int64 {
	return int64(math.Round(floatField(m, key)))
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func renderExplainMarkdown(fx hotPathFixture, summaries []explainSummary) string {
	var b strings.Builder
	b.WriteString("# Beacon Hot-Path EXPLAIN Capture\n\n")
	b.WriteString("Generated by `go test ./db -run TestHotPathIntegrationFixture -count=1 -v` with `BEACON_TEST_DATABASE_URL` and `BEACON_EXPLAIN_OUTPUT` set.\n\n")
	b.WriteString("## Dataset Context\n\n")
	fmt.Fprintf(&b, "- Dataset mode: `%s`\n", fx.DatasetLabel)
	fmt.Fprintf(&b, "- Region: `%s`\n", fx.RegionSlug)
	fmt.Fprintf(&b, "- Window: `%s` to `%s`\n", fx.Since.Format(time.RFC3339), fx.Until.Format(time.RFC3339))
	fmt.Fprintf(&b, "- IATAs: `%s`\n", strings.Join(fx.IATAs, ","))
	fmt.Fprintf(
		&b,
		"- Dataset size: %d IATAs in scope, %d nodes, %d observers, %d packets, %d observations, %d known routes, %d neighbor edges.\n",
		len(fx.IATAs),
		fx.NodeCount,
		fx.ObserverCount,
		fx.PacketCount,
		fx.ObservationCount,
		fx.KnownRouteCount,
		fx.NeighborEdgeCount,
	)
	if fx.Scale > 0 {
		fmt.Fprintf(&b, "- Synthetic scale factor: `%dx` via `BEACON_HOTPATH_FIXTURE_SCALE`; materialized views refreshed during fixture seed.\n\n", fx.Scale)
	} else {
		b.WriteString("- Snapshot context is derived from existing rows; schema, seed data, and materialized views are not modified.\n\n")
	}
	b.WriteString("## Plan Summary\n\n")
	b.WriteString("| Plan | Planning ms | Execution ms | Rows | Shared hits | Shared reads | Nodes | Indexes | Relations |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |\n")
	for _, s := range summaries {
		fmt.Fprintf(
			&b,
			"| `%s` | %.3f | %.3f | %.0f | %d | %d | %s | %s | %s |\n",
			s.Name,
			s.PlanningMS,
			s.ExecutionMS,
			s.ActualRows,
			s.SharedHits,
			s.SharedReads,
			escapeTable(strings.Join(s.Nodes, ", ")),
			escapeTable(strings.Join(s.Indexes, ", ")),
			escapeTable(strings.Join(s.Relations, ", ")),
		)
	}
	return b.String()
}

func renderTimingMarkdown(fx hotPathFixture, summaries []timingSummary) string {
	var b strings.Builder
	b.WriteString("# Beacon Hot-Path Timing Breakdown\n\n")
	b.WriteString("Generated by `go test ./db -run TestHotPathIntegrationFixture -count=1 -v` with `BEACON_TEST_DATABASE_URL` and `BEACON_TIMINGS_OUTPUT` set.\n\n")
	b.WriteString("## Dataset Context\n\n")
	fmt.Fprintf(&b, "- Dataset mode: `%s`\n", fx.DatasetLabel)
	fmt.Fprintf(&b, "- Region: `%s`\n", fx.RegionSlug)
	fmt.Fprintf(&b, "- Window: `%s` to `%s`\n", fx.Since.Format(time.RFC3339), fx.Until.Format(time.RFC3339))
	fmt.Fprintf(&b, "- IATAs: `%s`\n", strings.Join(fx.IATAs, ","))
	fmt.Fprintf(
		&b,
		"- Dataset size: %d IATAs in scope, %d nodes, %d observers, %d packets, %d observations, %d known routes, %d neighbor edges.\n",
		len(fx.IATAs),
		fx.NodeCount,
		fx.ObserverCount,
		fx.PacketCount,
		fx.ObservationCount,
		fx.KnownRouteCount,
		fx.NeighborEdgeCount,
	)
	if fx.Scale > 0 {
		fmt.Fprintf(&b, "- Synthetic scale factor: `%dx` via `BEACON_HOTPATH_FIXTURE_SCALE`; materialized views refreshed during fixture seed.\n\n", fx.Scale)
	} else {
		b.WriteString("- Snapshot context is derived from existing rows; schema, seed data, and materialized views are not modified.\n\n")
	}
	b.WriteString("## Timing Summary\n\n")
	b.WriteString("| Group | Step | Duration ms |\n")
	b.WriteString("| --- | --- | ---: |\n")
	for _, s := range summaries {
		fmt.Fprintf(&b, "| `%s` | `%s` | %.3f |\n", s.Group, s.Name, s.DurationMS)
	}
	return b.String()
}

func escapeTable(value string) string {
	if value == "" {
		return "-"
	}
	return strings.ReplaceAll(value, "|", "\\|")
}

func hotPathHashBytes(label string, n int) []byte {
	if n > sha256.Size {
		n = sha256.Size
	}
	sum := sha256.Sum256([]byte(label))
	out := make([]byte, n)
	copy(out, sum[:])
	return out
}
