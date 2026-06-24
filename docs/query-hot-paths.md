# Beacon Query Hot Paths

Date: 2026-06-24

This document names the current high-value backend read paths before deeper refactors. Use it when adding indexes, changing cache TTLs, or splitting large DB/API files. The goal is to keep endpoint behavior, expected cardinality, cache policy, and query-plan evidence in one place.

## Operating Rules

- Capture `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` before and after query changes against a seeded local database or a scrubbed production-like snapshot.
- Record row counts for `packets`, `packet_observations`, `known_routes`, `observers`, `observer_telemetry`, `nodes`, and materialized views with each plan.
- Keep query windows explicit. Default windows hide real scan cost when traffic or retention grows.
- Treat cache hit rate as part of the acceptance criteria. Hot endpoints should move repeated requests into Redis unless they are intentionally cursor/live streams.
- Do not cache cursor-driven endpoints that must reflect strict append order, such as Live backfill.

## Cache and Background Metrics

Health responses expose:

- `cacheMetrics`: Redis-backed counters grouped by `atlas`, `live`, `stats`, `reference`, `nodes`, `observers`, or `unknown`. Each category reports hits, misses, invalidations, TTL seconds, and error counts.
- `backgroundTasks`: periodic task counters for `view_refresh`, `cleanup`, and `reconfirm`, including runs, successes, failures, last status, last error, and last duration.

Use these fields from `/healthz` and `/readyz` to verify whether a hot-path fix is working at runtime. A successful cache change should show miss-on-first-read, hits on repeated reads inside the TTL, and stable or intentional invalidation counts. A successful materialized-view or cleanup change should keep background task failures at zero and durations within the expected maintenance window.

## Hot Endpoint Inventory

| Surface | Endpoint | Handler | Store/query path | Cache category and TTL | Expected cardinality | Primary indexes/views |
| --- | --- | --- | --- | --- | --- | --- |
| Atlas briefing | `GET /api/v1/atlas/briefing` | `internal/api/handlers/atlas.go:getAtlasBriefing` | `db/atlas.go:GetAtlasBriefing`, plus regional summaries, notable routes, active observers, packet observations | `atlas`, default `30s`, window bucketed by TTL | One briefing document per region/window. Internally scans 24h default packet/observation windows and top route summaries. | `idx_observations_heard_packet_cover`, `idx_packets_hash_origin_cover`, `packets_pkey`, `idx_known_routes_last_seen`, `idx_known_routes_iata`, `idx_known_routes_hop_count` |
| Atlas replay | `GET /api/v1/atlas/replay` | `internal/api/handlers/atlas.go:listAtlasReplay` | `db/atlas.go:ListAtlasReplay` | Uncached cursor stream | Up to 200 packet observations per page, ordered by observation cursor/window. | `idx_packets_last_heard_hash`, `idx_observations_packet_heard_id`, `idx_observations_iata_heard` |
| Live summary | `GET /api/v1/live/summary` | `internal/api/handlers/live.go:getLiveSummary` | `db/live.go:GetLiveSummary` | `live`, default `5s`, 15m default window | One compact summary, multiple aggregates over the live window. | `idx_observations_heard_packet_cover`, `packets_pkey`, `observers_pkey` |
| Live backfill | `GET /api/v1/live/backfill` | `internal/api/handlers/live.go:listLiveBackfill` | `db/live.go:ListLiveBackfill` | Uncached append/cursor stream | 1-250 observations per page. Initial page reads newest observations then restores ascending order. | `packet_observations_pkey`, `packets_pkey`, `idx_observations_iata_heard` |
| Netgraph snapshot | `GET /api/v1/netgraph` | `internal/api/handlers/netgraph.go:getNetgraph` | `db/routes.go:ListKnownRoutes`, then in-memory graph shaping | Uncached today; consider short stats/topology cache only after route freshness requirements are explicit | Up to 2500 routes scanned, capped to 2600 nodes and 4200 edges by handler defaults. | `idx_known_routes_iata`, `idx_known_routes_hop_count`, `idx_known_routes_last_seen`, `idx_known_routes_node_ids_gin` |
| Stats home | `GET /api/v1/stats/home` | `internal/api/handlers/stats.go:getStatsHome` | Stats summary, regions, payloads, topology, observer health, top nodes/observers depending on home composition | `stats`, default `1h`, window bucketed by TTL | Dashboard-sized aggregate response, typically 24h/7d/30d windows. | `mv_hourly_iata_stats`, `mv_top_nodes_by_iata`, `mv_radio_presets`, `idx_observations_heard_packet_cover`, `idx_observer_iatas_iata`, `idx_observer_iatas_observer_last` |
| Stats summary | `GET /api/v1/stats/summary` | `internal/api/handlers/stats.go:getStatsSummary` | `db/stats_ops.go:GetStatsSummary`, calls live summary, node types, payloads, top IATAs, observers, nodes, presets, scope stats, observer health | `stats`, default `1h`, window bucketed by TTL | One aggregate response, fan-out across multiple query families. | Same as Stats home, plus `idx_packets_hash_origin_cover`, `idx_packets_scope_id`, `packets_pkey`, `idx_nodes_type_last_seen`, `idx_mv_radio_presets`, `idx_mv_top_nodes` |
| Observer health | `GET /api/v1/stats/observer-health` | `internal/api/handlers/stats.go:getStatsObserverHealth` | `db/stats_ops.go:GetStatsObserverHealth` with latest observer IATA from `observer_iatas`, window counts, latest telemetry | `stats`, default `1h`, includes `staleAfter` in cache key | Up to `limit` observers, default bounded by handler filter. | `idx_observations_heard_packet_cover`, `idx_observer_iatas_iata`, `idx_observer_iatas_observer_last`, `idx_telemetry_observer_recent`, `observers_pkey` |
| Node analytics | `GET /api/v1/nodes/{nodeId}/analytics` | `internal/api/handlers/nodes.go:getNodeAnalytics` | `db/nodes.go:GetNodeAnalytics`, fan-out over packet/observation aggregates for one origin node | Uncached today because it is detail-panel scoped and node/window specific | One node, bounded window, multiple top-N aggregates and timelines. | `idx_packets_origin`, `idx_observations_packet_heard_id`, `nodes_pkey` |
| Node reach and route neighborhood | `GET /api/v1/nodes/{nodeId}/reach`, `GET /api/v1/nodes/{nodeId}/route-neighborhood` | `internal/api/handlers/nodes.go:getNodeReach`, `getNodeRouteNeighborhood` | `db/routes.go:GetKnownRoutesByNode` over route frontier expansion | Uncached today because it is detail-panel scoped and node/IATA specific | Default `routeLimit=300` per node/IATA query, max `600`, max 96 route queries, max 2500 source routes, max 48 frontier nodes per depth, response reports `queryCount`, `sourceRouteCount`, and `truncated`. | `idx_known_routes_node_ids_gin`, `idx_known_routes_iata`, `idx_known_routes_hop_count` |

## EXPLAIN Capture Template

Replace bind values with a realistic region/IATA/window from the dataset being profiled.

```sql
-- Dataset context
SELECT 'packets' AS table_name, COUNT(*) FROM packets
UNION ALL SELECT 'packet_observations', COUNT(*) FROM packet_observations
UNION ALL SELECT 'known_routes', COUNT(*) FROM known_routes
UNION ALL SELECT 'observers', COUNT(*) FROM observers
UNION ALL SELECT 'observer_telemetry', COUNT(*) FROM observer_telemetry
UNION ALL SELECT 'nodes', COUNT(*) FROM nodes;

-- Query under review
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT ...
;
```

Record these notes with every plan:

- Endpoint, exact URL, and query parameters.
- Store method and source file.
- Dataset row counts and retention window.
- Cache state before/after request: category, hit/miss count, TTL, and invalidation count.
- Total execution time, planning time, shared buffer hits/reads/dirties, temp reads/writes, and whether any sequential scan is expected.
- Proposed action: keep, index, rewrite, materialize, cache, or split into a fixture benchmark.

## Latest Seeded Fixture Capture

The DB-backed hot-path fixture lives in `db/hotpath_integration_test.go` and is intentionally opt-in so normal `go test ./...` does not require a running PostgreSQL instance. The wrapper script creates or reuses a disposable database whose name contains `test`, resets its public schema, runs migrations, seeds representative data, refreshes materialized views, exercises the store methods, and writes a compact EXPLAIN summary.

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Run-HotPathIntegration.ps1 -Scale 25 -Benchmark -TimingOutput docs/query-hot-paths-timings.latest.md
```

Omit `-Scale` for the fast default smoke fixture. `-Scale` accepts `1` through `100` and maps to `BEACON_HOTPATH_FIXTURE_SCALE`.

Latest local capture, 2026-06-24:

- Disposable database: `beacon_hotpath_test`
- Window: 2026-06-24T13:45:46Z to 2026-06-24T19:45:46Z
- Dataset size: 2 IATAs in scope, 150 nodes, 75 observers, 2250 packets, 4500 observations, 100 known routes, 75 neighbor edges
- Synthetic scale factor: `25x`
- Full generated output: `docs/query-hot-paths-explain.latest.md`
- Timing breakdown output: `docs/query-hot-paths-timings.latest.md`

| Plan | Planning ms | Execution ms | Rows | Shared hits | Shared reads | Nodes | Indexes | Relations |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| `atlas_iata_rollup` | 0.174 | 5.740 | 2 | 1146 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, CTE Scan, Nested Loop, Seq Scan, Sort | idx_observations_heard_packet_cover | iata_codes, packet_observations |
| `atlas_payload_route_mix_combined` | 0.326 | 15.275 | 7 | 95818 | 0 | Aggregate, Append, Bitmap Heap Scan, Bitmap Index Scan, CTE Scan, Index Scan, Limit, Nested Loop, Sort, Subquery Scan | idx_observations_heard_packet_cover, packets_pkey | packet_observations, packets |
| `atlas_active_nodes_by_iata` | 0.273 | 6.152 | 2 | 2036 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Group, Hash, Hash Join, Seq Scan, Sort | idx_observations_heard_packet_cover | nodes, packet_observations, packets |
| `atlas_top_nodes` | 0.320 | 10.636 | 8 | 97018 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Index Only Scan, Index Scan, Limit, Nested Loop, Sort | idx_nodes_pubkey, idx_observations_heard_packet_cover, idx_packets_hash_origin_cover | nodes, packet_observations, packets |
| `live_summary_combined` | 0.741 | 1.312 | 1 | 6036 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, CTE Scan, Index Scan, Limit, Nested Loop, Sort, Subquery Scan | idx_observations_heard_packet_cover, observers_pkey, packets_pkey | observers, packet_observations, packets |
| `live_backfill_page` | 0.445 | 4.051 | 51 | 670 | 0 | Bitmap Heap Scan, Bitmap Index Scan, Hash, Hash Join, Limit, Seq Scan, Sort | packet_observations_pkey | packet_observations, packets |
| `netgraph_known_routes` | 0.298 | 0.076 | 100 | 9 | 0 | Limit, Seq Scan, Sort | - | known_routes |
| `netgraph_known_routes_cap_2500` | 0.071 | 0.070 | 100 | 9 | 0 | Limit, Seq Scan, Sort | - | known_routes |
| `known_routes_node_containment` | 0.107 | 0.037 | 2 | 6 | 0 | Seq Scan, Sort | - | known_routes |
| `known_routes_node_containment_limit300` | 0.053 | 0.054 | 2 | 9 | 0 | Limit, Seq Scan, Sort | - | known_routes |
| `stats_overview_window` | 0.090 | 4.032 | 1 | 618 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Sort | idx_observations_heard_packet_cover | packet_observations |
| `stats_payloads_combined` | 0.315 | 12.132 | 56 | 68468 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, CTE Scan, Index Scan, Nested Loop, Sort | idx_observations_heard_packet_cover, packets_pkey | packet_observations, packets |
| `stats_top_observers_window` | 0.283 | 3.305 | 10 | 3529 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, CTE Scan, Index Scan, Limit, Nested Loop, Sort, Unique | idx_observations_heard_packet_cover, idx_observer_iatas_observer_last, observers_pkey | observer_iatas, observers, packet_observations |
| `stats_top_nodes_window` | 0.323 | 10.108 | 10 | 98218 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Index Only Scan, Index Scan, Limit, Nested Loop, Sort | idx_nodes_pubkey, idx_observations_heard_packet_cover, idx_packets_hash_origin_cover | nodes, packet_observations, packets |
| `observer_health` | 0.315 | 2.630 | 75 | 1378 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Hash, Hash Join, Index Only Scan, Limit, Merge Join, Seq Scan, Sort, Subquery Scan, Unique | idx_observations_heard_packet_cover, idx_telemetry_observer_recent | observer_iatas, observer_telemetry, observers, packet_observations |
| `scope_stats` | 0.347 | 0.611 | 1 | 388 | 0 | Aggregate, Hash, Hash Join, Seq Scan, Sort, Subquery Scan | - | nodes, observer_scopes, packets, transport_scopes |
| `node_analytics_kpis` | 0.377 | 0.181 | 1 | 277 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Index Scan, Nested Loop, Sort | idx_observations_packet_heard_id, idx_packets_origin, nodes_pkey | nodes, packet_observations, packets |

Benchmark output from the same disposable fixture on this workstation:

| Store method | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `GetAtlasBriefing` | 38,046,723 | 182,415 | 3,402 |
| `GetLiveSummary` | 24,867,323 | 5,828 | 95 |
| `ListLiveBackfill` | 41,190,142 | 101,970 | 1,756 |
| `ListKnownRoutes` | 1,610,356 | 377,255 | 7,568 |
| `KnownRoutesByNode` | 828,759 | 12,466 | 283 |
| `KnownRoutesByNodeLimit300` | 894,292 | 12,463 | 284 |
| `GetStatsSummary` | 22,066,370 | 135,554 | 2,455 |
| `GetStatsObserverHealth` | 3,721,581 | 92,870 | 1,611 |
| `GetNodeAnalytics` | 113,073,693 | 17,884 | 421 |

Timing breakdown from the same run:

| Group | Slowest named child path | Duration ms |
| --- | --- | ---: |
| `AtlasBriefing` | `getAtlasTopNodes_region` | 10.988 |
| `AtlasBriefing` | `getAtlasPayloadAndRouteMix_region` | 10.539 |
| `AtlasBriefing` | `getAtlasTopObservers_region` | 7.376 |
| `StatsSummary` | `GetStatsPayloads` | 13.042 |
| `StatsSummary` | `getStatsTopNodesWindow` | 9.769 |
| `StatsSummary` | `getStatsTopObserversWindow` | 6.349 |
| `StatsSummary` | `GetScopeStats` | 0.900 |

Interpretation:

- This fixture proves the hot-path store methods run against a migrated PostgreSQL schema with materialized views, not only handler stubs.
- `Scale 25` reaches 100 seeded known routes; the harness now also probes the current 2500-route Netgraph cap, uncapped route-node containment, and default bounded route-node containment lookup explicitly.
- Sequential scans on `packet_observations` and `known_routes` still appear in the compact synthetic EXPLAIN summaries because the fixture is small. Snapshot capture is the authority for production-like plan shape.
- `node_analytics_kpis` still leans on `nodes_pkey`, `idx_packets_origin`, and `idx_observations_packet_heard_id` for the node-origin fan-out.
- The `Scale 25` timing pass exposed `GetScopeStats` as the shared Atlas/Stats outlier before this capture. Rewriting it to pre-aggregate packet, observer, and node counts per scope removed a raw join product of packets x observer scopes x nodes; the full scaled harness dropped from about 275 seconds to about 17-23 seconds locally.
- The top-node and active-node EXPLAIN specs now prove the aggregate-before-joining-nodes shape on every fixture run.
- `observer_iatas` removes the latest-IATA lookup from the observer-health and Stats top-observer observation windows, and the Stats payload query now keeps payload/route totals plus timelines on one lateral `GROUPING SETS` aggregate without a materialized base scan.
- `GetLiveSummary` now builds overview counts, payload mix, route mix, top IATAs, and top observers from one materialized base query. On the scaled fixture, the benchmark is about `24.9 ms/op` and `95 allocs/op`.
- `idx_observations_heard_packet_cover` gives recent-window aggregate reads a covering observation-side path; the scaled fixture now uses it for Atlas, Stats, observer-health, and Live summary probes.
- `idx_packets_hash_origin_cover` gives top-node and active-node paths a covering packet-origin lookup after the observation-side window scan.
- Atlas IATA rows now pre-aggregate observations before joining airport metadata, Atlas briefing combines payload plus route mix from one materialized base query, and the Atlas briefing store method parallelizes independent child reads. On the latest scaled fixture, `GetAtlasBriefing` is about `38.0 ms/op`.
- Stats summary now parallelizes independent dashboard child reads, reuses `observer_iatas` for top-observer latest-IATA labels, counts scoped packets through `idx_packets_scope_id`, and uses a single lateral grouping-sets aggregate for payload/route totals plus timelines. On the latest scaled fixture, `GetStatsPayloads` is about `13.0 ms` in timing capture and `GetStatsSummary` is about `22.1 ms/op`; remaining Stats pressure is inside wider-window SQL children, not summary fan-out.
- Node route-neighborhood expansion now has a bounded API contract: default `routeLimit=300` per node/IATA query, max `600`, max 96 route queries, max 2500 source routes, max 48 frontier nodes per depth, and response metadata for `routeLimit`, `queryCount`, `sourceRouteCount`, and `truncated`.

## Scrubbed Snapshot Capture

The same harness can run in read-only snapshot mode against an already-scrubbed PostgreSQL database. Snapshot mode does not reset schemas, run migrations, seed rows, or refresh materialized views. It sets the PostgreSQL session to read-only, derives the default window from `MAX(packet_observations.heard_at)`, selects the busiest IATAs in `BEACON_HOTPATH_REGION_SLUG`, and writes separate snapshot evidence files.

When `-Snapshot` is used without an explicit `-Output`, the wrapper writes `docs/query-hot-paths-explain.snapshot.md` instead of replacing the synthetic `latest` capture.

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Run-HotPathIntegration.ps1 -Snapshot -Dsn "postgres://beacon:beacon@localhost:5432/beacon?sslmode=disable" -Output docs/query-hot-paths-explain.snapshot.md -TimingOutput docs/query-hot-paths-timings.snapshot.md -StatementTimeout 120s
```

Useful snapshot knobs:

- `-RegionSlug western-canada`, mapped to `BEACON_HOTPATH_REGION_SLUG`.
- `-IATAs YYC,YYJ,YVR,YEG`, mapped to `BEACON_HOTPATH_IATAS`; omit it to select the busiest region IATAs.
- `-WindowHours 6`, mapped to `BEACON_HOTPATH_WINDOW_HOURS`.
- `-Since` and `-Until` with RFC3339 timestamps for exact replay windows.

Latest local snapshot capture, 2026-06-24:

- Source database: local `beacon` database, read-only test session
- Region: `western-canada`
- Window: 2026-06-24T10:00:45-04:00 to 2026-06-24T16:00:45-04:00
- IATAs: `YYC,YYJ,YVR,YEG`
- Dataset size: 4138 nodes, 112 observers, 340338 packets, 2198677 observations, 152575 known routes, 8270 neighbor edges
- Full generated output: `docs/query-hot-paths-explain.snapshot.md`
- Timing breakdown output: `docs/query-hot-paths-timings.snapshot.md`

| Plan | Planning ms | Execution ms | Rows | Shared hits | Shared reads | Nodes | Indexes | Relations |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| `atlas_iata_rollup` | 0.242 | 48.150 | 4 | 91954 | 0 | Aggregate, CTE Scan, Hash, Hash Join, Index Only Scan, Seq Scan, Sort | idx_observations_heard_packet_cover | iata_codes, packet_observations |
| `atlas_payload_route_mix_combined` | 0.355 | 74.516 | 18 | 1031715 | 0 | Aggregate, Append, CTE Scan, Gather, Index Only Scan, Index Scan, Limit, Nested Loop, Sort, Subquery Scan | idx_observations_heard_packet_cover, packets_pkey | packet_observations, packets |
| `atlas_active_nodes_by_iata` | 0.355 | 83.104 | 20 | 1469988 | 0 | Aggregate, Gather Merge, Group, Hash, Hash Join, Index Only Scan, Nested Loop, Seq Scan, Sort | idx_observations_heard_packet_cover, idx_packets_hash_origin_cover | nodes, packet_observations, packets |
| `atlas_top_nodes` | 0.576 | 47.410 | 8 | 1061980 | 0 | Aggregate, Gather Merge, Hash, Hash Join, Index Only Scan, Limit, Nested Loop, Seq Scan, Sort, Subquery Scan | idx_observations_heard_packet_cover, idx_packets_hash_origin_cover | nodes, packet_observations, packets |
| `live_summary_combined` | 0.695 | 6.869 | 1 | 33204 | 0 | Aggregate, CTE Scan, Hash, Hash Join, Index Only Scan, Index Scan, Limit, Nested Loop, Seq Scan, Sort, Subquery Scan | idx_observations_heard_packet_cover, packets_pkey | observers, packet_observations, packets |
| `live_backfill_page` | 0.464 | 0.565 | 51 | 254 | 21 | Index Scan, Limit, Memoize, Nested Loop | packet_observations_pkey, packets_pkey | packet_observations, packets |
| `netgraph_known_routes` | 0.304 | 0.730 | 100 | 1150 | 4 | Index Scan, Limit | idx_known_routes_last_seen | known_routes |
| `netgraph_known_routes_cap_2500` | 0.058 | 37.860 | 2500 | 13882 | 7132 | Gather Merge, Limit, Seq Scan, Sort | - | known_routes |
| `known_routes_node_containment` | 0.213 | 12.450 | 5722 | 4432 | 0 | Bitmap Heap Scan, Bitmap Index Scan, Sort | idx_known_routes_node_ids_gin | known_routes |
| `known_routes_node_containment_limit300` | 0.111 | 5.688 | 300 | 6629 | 0 | Bitmap Heap Scan, Bitmap Index Scan, Limit, Sort | idx_known_routes_node_ids_gin | known_routes |
| `stats_overview_window` | 0.140 | 41.533 | 1 | 45975 | 0 | Aggregate, Index Only Scan, Sort | idx_observations_heard_packet_cover | packet_observations |
| `stats_payloads_combined` | 0.353 | 61.980 | 109 | 774055 | 0 | Aggregate, CTE Scan, Gather, Index Only Scan, Index Scan, Nested Loop, Sort | idx_observations_heard_packet_cover, packets_pkey | packet_observations, packets |
| `stats_top_observers_window` | 0.567 | 26.825 | 10 | 123178 | 0 | Aggregate, CTE Scan, Hash, Hash Join, Index Only Scan, Limit, Seq Scan, Sort, Subquery Scan, Unique | idx_observations_heard_packet_cover | observer_iatas, observers, packet_observations |
| `stats_top_nodes_window` | 0.405 | 46.252 | 10 | 956378 | 0 | Aggregate, Gather Merge, Hash, Hash Join, Index Only Scan, Limit, Nested Loop, Seq Scan, Sort | idx_observations_heard_packet_cover, idx_packets_hash_origin_cover | nodes, packet_observations, packets |
| `observer_health` | 0.370 | 35.609 | 22 | 145897 | 5 | Aggregate, Index Only Scan, Index Scan, Limit, Merge Join, Nested Loop, Seq Scan, Sort, Subquery Scan, Unique | idx_observations_heard_packet_cover, idx_telemetry_observer_recent, observers_pkey | observer_iatas, observer_telemetry, observers, packet_observations |
| `scope_stats` | 0.667 | 0.605 | 2 | 630 | 0 | Aggregate, Hash, Hash Join, Index Only Scan, Merge Join, Seq Scan, Sort, Subquery Scan | idx_packets_scope_id | nodes, observer_scopes, packets, transport_scopes |
| `node_analytics_kpis` | 0.564 | 2.011 | 1 | 5545 | 0 | Aggregate, Bitmap Heap Scan, Bitmap Index Scan, Index Scan, Nested Loop, Sort | idx_observations_packet_heard_id, idx_packets_origin, nodes_pkey | nodes, packet_observations, packets |

Snapshot timing hotspots:

| Group | Slowest named child path | Duration ms |
| --- | --- | ---: |
| `AtlasBriefing` | `getAtlasIATAs_current_previous` | 130.472 |
| `AtlasBriefing` | `getAtlasPayloadAndRouteMix_region` | 77.783 |
| `StatsSummary` | `GetStatsPayloads` | 42.263 |
| `AtlasBriefing` | `getAtlasTopObservers_region` | 44.252 |
| `Netgraph` | `ListKnownRoutes_cap2500` | 38.763 |
| `Netgraph` | `GetKnownRoutesByNode_limit300` | 10.896 |
| `StatsSummary` | `getStatsTopObserversWindow` | 26.594 |
| `StatsSummary` | `GetScopeStats` | 1.223 |
| `StatsSummary` | `GetLiveSummary_15m` | 7.524 |

Snapshot interpretation:

- The production-like data confirms `GetScopeStats` and the prior top-node/active-node join shape are no longer the shared outliers.
- Rewriting top-node and active-node helpers to aggregate by packet origin before joining `nodes`, then adding the observation and packet-origin covering indexes, dropped `getAtlasTopNodes_region` and `getStatsTopNodesWindow` from multi-second outliers into tens to low hundreds of milliseconds on moving local snapshots.
- Maintaining `observer_iatas` at ingest time removed the full observation-window latest-IATA scan from observer health; snapshot observer-health child timings fell from about `748.9-1385.5 ms` to roughly `30-90 ms` depending on the moving window.
- Pre-aggregating Atlas IATA rows before joining airport metadata reduced the single-window IATA rollup from about `200.8 ms` into a lower but moving range; the Atlas briefing current/previous helper now runs both windows concurrently, with the latest moving local window reporting about `130.5 ms` wall-clock.
- Combining Atlas briefing payload and route mix into one materialized base query replaces the prior separate `getAtlasPayloadMix_region` and `getAtlasRouteMix_region` paths; on moving snapshots this child is now a secondary watchpoint, ranging from about `37 ms` on cleaner runs to about `77.8 ms` in the latest timing capture.
- Parallelizing independent Atlas briefing child reads reduced the scaled-fixture `GetAtlasBriefing` benchmark from about `59.7 ms/op` to about `38.0 ms/op` on the latest run; the local snapshot now shows `getAtlasBriefingRegions_preloaded` at about `45.6 ms`.
- Parallelizing independent Stats summary child reads plus reusing `observer_iatas` for top-observer latest-IATA labels reduced the scaled-fixture `GetStatsSummary` benchmark from about `51.3 ms/op` to about `22.1 ms/op` on the latest run; the remaining Stats costs are SQL children, not summary composition fan-out.
- Combining the payload total, route total, payload timeline, and route timeline reads behind one lateral grouping-sets aggregate plus `idx_observations_heard_packet_cover` reduced `GetStatsPayloads` from about `421-449 ms` to about `42.3 ms` on the latest moving local snapshot timing run. The current query has no materialized `CTE Scan`; on the latest scaled fixture, `GetStatsPayloads` is about `13.0 ms` in timing capture and `stats_payloads_combined` is about `10.2 ms` EXPLAIN.
- Reusing `observer_iatas` for Stats top-observer latest-IATA labels removed the prior per-observer ordered `array_agg` spill; the snapshot `getStatsTopObserversWindow` path moved from about `96.7 ms` to roughly `23-54 ms` depending on the moving window.
- Consolidating Live summary's overview, payload mix, route mix, top IATAs, and top observers into one materialized base query plus `idx_observations_heard_packet_cover` reduced the snapshot `GetLiveSummary_15m` child path from about `305.6 ms` to roughly `6-10 ms`.
- Adding `idx_packets_scope_id` and removing unnecessary distinct counts moved `GetScopeStats` from about `38.9 ms` to about `0.5-1.0 ms` wall-clock; the explicit `scope_stats` EXPLAIN probe uses the partial index and runs in about `0.6 ms`.
- `idx_observations_heard_packet_cover` plus `idx_packets_hash_origin_cover` moved `stats_top_nodes_window` and `atlas_top_nodes` away from their previous multi-hundred-millisecond to multi-second plans; latest moving snapshots remain indexed but should still be watched as row counts grow.
- `idx_known_routes_last_seen` gives the 100-route recency page an index plan under 1 ms on the snapshot. The explicit current-cap Netgraph probe returns 2500 routes in about `91.6 ms` EXPLAIN / `38.8 ms` store timing, and `idx_known_routes_node_ids_gin` gives uncapped node-containment lookups an indexed path at about `20.1 ms` EXPLAIN / `31.6 ms` store timing for a busiest-node case returning 5723 routes. The bounded `routeLimit=300` path is about `12.3 ms` EXPLAIN / `10.9 ms` store timing for 300 routes.
- Remaining backend pressure is concentrated in Atlas IATA/active-node rollup growth and future wider-window materialization decisions rather than Atlas or Stats composition fan-out, Stats payload fan-out, Live summary fan-out, scope scans, top-observer spills, top-node joins, route-neighborhood expansion, or observation-window heap reads.

## Fixture and Benchmark Targets

- Handler-level response-shape fixtures cover routing, parameter normalization, response envelopes, and JSON shape for the hot endpoint families.
- `TestHotPathIntegrationFixture` now covers the same hot paths against PostgreSQL with migrations, scalable synthetic seed data, refreshed materialized views, store-method assertions, and optional EXPLAIN capture.
- Optional timing capture now writes `docs/query-hot-paths-timings.latest.md` when `BEACON_TIMINGS_OUTPUT` or `-TimingOutput` is set.
- `BenchmarkHotPathStoreMethods` now times Atlas briefing, Live summary/backfill, Netgraph route reads, known-routes-by-node containment, bounded known-routes-by-node containment, Stats summary, Observer health, and Node analytics against the same fixture.
- The synthetic scale-up target is covered by `-Scale 25`; read-only snapshot mode is now available and has a local production-like baseline for cap/index decisions including `observer_iatas`, combined payloads, combined Live summary, scoped-packet counts, observation-window covering reads, route-recency indexing, route-node containment indexing, and bounded route-neighborhood expansion.

## Current Open Questions

- Netgraph now has route-recency and route-node-containment indexes plus bounded route-neighborhood expansion. Keep the current caps unless a short cache or materialized neighborhood summary is added for wider public caps.
- Stats home and summary share expensive subpaths. If cache hit rates stay low because parameters are too fragmented, consider coarser canonical windows or a small materialized dashboard table.
- Atlas IATA/active-node rollup growth and wider-window materialization are now the main snapshot-proven backend pressure points to inspect before widening windows or public traffic.
- Route-mix aggregates and payload breakdowns remain secondary child paths to watch as row counts grow, even after the payload consolidation.
- Observer health still uses `DISTINCT ON` for latest telemetry and bounded window counts, but latest-IATA selection now comes from `observer_iatas`; verify ingest/backfill behavior before relying on it for historical reprocessing workflows.
- Node analytics is intentionally uncached. If detail-panel traffic grows, consider a short `nodes` category cache keyed by node/window/IATA after validating freshness expectations.
