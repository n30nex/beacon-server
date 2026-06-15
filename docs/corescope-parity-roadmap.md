# Beacon CoreScope Parity Roadmap

Snapshot date: 2026-06-15  
Roadmap style: Beacon-native parity, not a CoreScope clone

## Executive Summary

Beacon is already past the "blank replacement" stage: it has a React/Vite frontend, Go/Postgres backend, Redis cache support, regional Atlas, Live map console, packets, channels, map, nodes, observers, routes, traces, global search, and Stats ops surfaces. The parity gap is now concentrated in mature specialty surfaces that CoreScope has accumulated over many releases: deeper node analytics/reach, observer compare, hash/path/topology analytics, customization workflows, and formal API/contract parity.

CoreScope remains the reference catalog for proven MeshCore analysis ideas. Beacon should port capabilities only when they make sense in its architecture: Go/Postgres as source of truth, Redis for hot aggregates, MapLibre/ECharts/React Query on the frontend, regional IATA/region filters as first-class state, and local-operator health as a core product concern.

Recommended strategy:

- Prioritize data/API foundations before adding UI chrome.
- Treat `Stats` as Beacon's analytics home, not a one-to-one copy of CoreScope's 11 analytics tabs.
- Keep `Atlas`, `Live`, and `Map` as Beacon-native flagship experiences.
- Preserve CoreScope ideas that improve operator decision-making: reach, topology, observer compare, hash collision risk, RF/clock health, channel key affordances, and shareable deep links.
- Defer CoreScope features that conflict with Beacon direction, duplicate existing Beacon UX, or depend on SQLite/in-memory-store assumptions that do not map cleanly to Postgres.

## Source Snapshot

| Source | Snapshot |
| --- | --- |
| CoreScope public repo | `https://github.com/Kpa-clawbot/CoreScope` |
| CoreScope branch | `master` |
| CoreScope public SHA | `92e001c093d877ef3c4cb79b33c3c264edde4493` |
| CoreScope README source | `https://raw.githubusercontent.com/Kpa-clawbot/CoreScope/master/README.md` |
| CoreScope changelog source | `https://raw.githubusercontent.com/Kpa-clawbot/CoreScope/master/CHANGELOG.md` |
| CoreScope API docs advertised | `https://analyzer.00id.net/api/docs` and `/api/spec` |
| CoreScope API spec availability during capture | `/api/spec` request timed out from this host; use public docs/spec as an M0 follow-up check |
| Local CoreScope clone | `F:\Beacon\CoreScope`, local SHA `31bbdcf1fad4dc9421184411c5caf8dd05ff5787`, not authoritative |
| Beacon web | `F:\Beacon\beacon-web`, `main`, SHA `0b516b65bdf90ed9fe0ca3576ca29334aefc5337` |
| Beacon server | `F:\Beacon\beacon-server`, `dev`, SHA `f6db2e2d914e17817b584c0d52761196d2dbcbd8` |

CoreScope README parity targets captured:

- In-memory Go read path with sub-millisecond packet queries and under-100 ms API goal.
- Live trace map with packet route visualization; Beacon will keep cinematic Live animation and route drilldowns, but dedicated VCR scrub/speed controls are no longer an active parity target.
- Packet feed with byte-level breakdown, resizable columns, detail pane, and "My Nodes".
- Network overview with node, packet, and observer coverage.
- Per-node analytics: activity timeline, packet type breakdown, SNR, hop count, peer graph, hourly heatmap.
- Channel chat with decoded messages, sender names, mentions, and timestamps.
- Mobile-ready layout with compact Live controls and analytics-first drilldowns.
- 11 analytics tabs covering RF, topology, channels, hash stats, distance, route patterns, and more.
- Node directory with role tabs, detail panel, QR codes, and advert timeline.
- Packet tracing across observers with SNR/RSSI timeline.
- Observer status and per-observer analytics.
- Hash collision matrix.
- Channel key auto-derivation for hashtag channels.
- Multi-broker MQTT with per-source IATA filtering.
- Theme customizer, global search, shareable URLs, protobuf contract, accessibility.

CoreScope changelog highlights to include in parity scope:

- WCAG AA contrast pass.
- Observer Compare.
- Phosphor/icon migration and lint gate.
- Per-node Reach page/API.
- Hashtag channels catalogue integration.
- Operator-customizable hidden name prefixes.
- Configurable `liveMap.maxNodes`, runtime memory limit, observer-health thresholds, branding home URL, and customizer disabled tabs.
- Relay timelines rebuilt after ingestor restart.
- Matrix/digital-rain Live visuals as an optional advanced visual reference.

## Parity Matrix

Status legend: `Done`, `Partial`, `Missing`, `Not Applicable`, `Beacon Superset`.

### Live / Map Motion

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Live animated route map | Live MapLibre console with trails, heat, packet feed, adaptive caps, node activity ripples, and directional packet markers | Partial | `src/features/live/LiveView.tsx`; `/api/v1/live/*` | Keep improving clarity/perf, but do not add standalone VCR controls | Live health/backfill reliability | P0 |
| VCR playback for last 24h | Route replay link and live backfill | Deferred | `src/features/map/MapView.tsx`; `internal/api/handlers/live.go` | User direction: skip VCR controls; preserve route replay only as a drilldown, not a scrubber product | Analytics priorities first | Deferred |
| Matrix/digital-rain mode | CRT/Bytes/Rain/Audio advanced controls | Partial | `LiveView.tsx` advanced settings | Keep optional; improve visual accuracy only after core Live perf | Canvas effect caps | P3 |
| Compact mobile VCR controls | Mobile-first Live dock/rail work | Deferred | `LiveView.tsx` responsive controls | User direction: no VCR controls; focus mobile Live controls and analytics readability | Browser smoke | Deferred |
| Live route-type filtering | WebSocket route filters and live backfill filters | Done | `internal/ws/handler.go`; `internal/hub/hub.go`; `internal/api/handlers/live.go` | Maintain as contract in tests | Existing API | P0 |

### Packets

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Filterable packet feed | Packets tab with URL-backed filters and live merge | Done | `src/features/packets/*`; `/api/v1/packets` | Preserve infinite pagination and live dedupe | Existing API | P0 |
| Byte-level breakdown | Packet analyzer drawer/overlay | Partial | `PacketAnalyzerDrawer.tsx`; `packet-structure.tsx` | Need CoreScope-level raw byte inspection, labels, and copy/export affordances | Packet raw fields and decoder metadata | P1 |
| Excel-like resizable columns | Beacon virtual list/card feed | Not Applicable | `PacketVirtualList.tsx` | Beacon should keep dense responsive rows/cards, not clone spreadsheet columns | None | Deferred |
| "My Nodes" focus | Node/name filters and route scopes | Missing | `usePacketFilters.ts` | Add local saved node set and packet filter | Saved preference model | P2 |
| Shareable packet detail URLs | `?tab=Packets&hash=` analyzer | Done | `App.tsx`; `PacketAnalyzerDrawer.tsx` | Extend to all relevant packet-derived drilldowns | Existing URL state | P0 |

### Map / Routes

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Interactive route visualization | MapLibre map, verified route overlays, route replay | Beacon Superset | `src/features/map/*`; `useRouteOverlays.ts` | Beacon has topography/profile work and route-to-map replay | Maintain tests | P0 |
| Route patterns analytics | Routes tab and known-route APIs | Partial | `RouteTable.tsx`; `/api/v1/routes` | Add aggregate route pattern charts and route reliability metrics | Stats/route aggregate endpoints | P2 |
| Path/subpath analysis | Traces and route-neighborhood | Partial | `src/features/traces/*`; route-neighborhood API | Add CoreScope subpath detail analytics in Stats | Path aggregate API | P1 |
| Route replay deep links | `?tab=Map&routeId=&routeReplay=1` | Done | `RouteTable.tsx`; `MapView.tsx` | Keep browser acceptance | Existing | P0 |
| Region/area filters | Region/IATA selector | Beacon Superset | `useRegion.tsx`; server region expansion | Beacon uses region-first Canadian atlas model | Existing | P0 |

### Nodes

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Node directory with role tabs | Dense node grid/cards and filters | Partial | `NodeTable.tsx`; `/api/v1/nodes` | Add role tabs if they improve scanning; current grid is Beacon-native | Existing nodes API | P2 |
| Node detail panel | Shared NodeDetailPanel across Atlas/Live/Map/Nodes | Done | `NodeDetailPanel.tsx`; `App.tsx` | Continue sharing one detail implementation | Existing | P0 |
| QR codes | None | Missing | No QR implementation found | Add QR card for node identity/share links if useful locally | QR generation utility | P3 |
| Advert timeline | Node detail includes recent advert snapshots with packet drilldown | Partial | `/api/v1/nodes/{id}/adverts`; `NodeDetailPanel.tsx` | Dedicated advert history page and richer diffing between adverts still remain | Store advert events/history | P2 |
| Per-node analytics | Compact node analytics API and NodeDetailPanel section | Partial | `internal/api/handlers/nodes.go`; `db/nodes.go`; `NodeDetailPanel.tsx` | Panel covers KPIs, mix, signal buckets, hourly activity heatmap, peers, and reach graph; dedicated chart page still needed | Node analytics UI route | P0 |
| Per-node reach page/API | Verified route-neighborhood overlay plus compact reach analytics in the shared node detail panel | Partial | `/api/v1/nodes/{id}/route-neighborhood`; `/api/v1/nodes/{id}/reach`; `NodeDetailPanel.tsx` | Dedicated reach page, cache invalidation hooks, and richer peer graph still remain | Graph aggregate API | P0 |
| Peer graph/hourly heatmap | Shared NodeDetailPanel has compact hourly heatmap plus peer/reach graph | Partial | `NodeDetailPanel.tsx`; `useRouteOverlays.ts` | Dedicated ECharts page and richer graph interactions still remain | Node analytics API | P1 |

### Observers

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Observer status table | Observers tab and broker/status filters | Done | `ObserverTable.tsx`; `/api/v1/observers` | Preserve health/status semantics | Existing | P0 |
| Observer telemetry charts | Stats Observers tab charts | Done | `ObserverTab.tsx`; `/api/v1/observers/{id}/telemetry` | Expand flags/threshold config | Existing telemetry API | P1 |
| Per-observer analytics | Stats observers and detail panel | Partial | `ObserverDetailPanel.tsx`; `ObserverTab.tsx` | Add observer-specific packet/payload/route breakdowns | Observer analytics endpoint | P1 |
| Observer Compare | Stats/Observers compare mode with prepared compare API | Partial | `/api/v1/stats/observer-compare`; `ObserverTab.tsx` | First pass supports 2-6 selected observers, shared IATAs, health/activity/RF comparison; URL state and richer charts still needed | URL serialization + chart polish | P0 |
| Clock health/skew | RF health has telemetry flags | Missing | `RFHealthTab.tsx` | Add node/observer clock skew endpoints/charts if data supports it | Clock-skew query logic | P2 |
| Configurable observer thresholds | Static health classification | Partial | `stats/transforms.ts`; server stats health | Move thresholds to config and health endpoint metadata | Server config + frontend labels | P2 |

### Channels

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Channel chat | Channels tab with channel list/message panel | Done | `src/features/channels/*`; `/api/v1/channels` | Continue Public default and region filtering | Existing | P0 |
| Decoded group messages | Known channel keys and messages | Partial | server keystore/channels handlers | Need parity validation against hashtag/explicit key catalog behavior | Key catalog tests | P1 |
| Sender names and mentions | Message panel | Partial | `MessagePanel.tsx` | Verify mention rendering, sender labels, and empty encrypted states | UI polish | P2 |
| Hashtag key auto-derivation | Server config/keystore supports hashtag keys | Partial | `internal/keystore`; config README | Add visible catalogue/import affordance | Key catalogue API | P1 |
| Known hashtag catalogue integration | None obvious in Beacon UI | Missing | No public catalogue UI found | Add generated/default public hashtag channel catalogue | Config seed + docs | P1 |
| Channel share/deep links | Channel selection state | Partial | `ChannelList.tsx` | Add durable URL for selected channel/message | URL state | P2 |

### Analytics / Stats

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Network overview | Stats Overview + Atlas summary | Beacon Superset | `StatsOverview.tsx`; `AtlasView.tsx` | Beacon regional Atlas exceeds CoreScope overview direction | Existing | P0 |
| RF analytics | Stats RF Health | Partial | `RFHealthTab.tsx`; `/api/v1/stats/rf-health` | Add richer SNR/RSSI scatter/distribution if raw metrics support it | RF aggregate endpoint | P1 |
| Topology analytics | Stats Topology tab with verified-route repeaters, adjacent pairs, hop distribution, and best paths | Partial | `/api/v1/stats/topology`; `src/features/stats/TopologyTab.tsx` | First pass is verified-route-only; hops-vs-signal and reach rings still needed | RF join and chart polish | P0 |
| Channel analytics | Stats Channels tab with key-state mix, activity timeline, top channels, top senders, and IATA distribution | Partial | `/api/v1/stats/channels`; `src/features/stats/ChannelsTab.tsx` | First pass covers timelines and top senders; catalogue import and mention-specific analysis still remain | Key catalogue API + channel polish | P1 |
| Hash stats | Stats Hash tab with size mix, multibyte share, timeline, active short-ID collision matrix, inconsistent samples, and path-prefix lookup | Partial | `/api/v1/stats/hash`; `/api/v1/stats/hash-prefix`; `src/features/stats/HashTab.tsx` | Add deeper node-level collision explainers and command-palette shortcut | Hash explorer polish | P1 |
| Hash collision matrix | Active short-ID collision matrix grouped by IATA/hash-size with risky-cell prefix lookup | Done | `/api/v1/stats/hash`; `HashTab.tsx` | Maintain browser acceptance and add richer drilldowns if operators need them | Existing hash analytics API | P0 |
| Distance analytics | None | Missing | No distance analytics found | Add optional distance tab if coordinates are reliable | Geo distance query | P3 |
| Subpath analytics | Stats Paths tab with verified-route subpath and endpoint-pair analytics | Partial | `/api/v1/stats/subpaths`; `src/features/stats/PathsTab.tsx` | First pass ranks repeated verified subpaths, endpoint pressure, length buckets, and timelines; route-specific drilldowns remain to add | Route detail drill links + chart polish | P1 |
| Scopes analytics | Stats Scopes tab | Done | `ScopesTab.tsx`; `/api/v1/stats/scopes` | Expand with channel/radio context | Existing | P1 |
| RF/clock health | RF health present; clock missing | Partial | `RFHealthTab.tsx` | Add clock skew and threshold config | Clock data model | P2 |

### Search

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Global Ctrl+K search | Search endpoint plus terminal command palette | Done | `/api/v1/search`; `GlobalSearchPalette.tsx`; `AppShell.tsx` | Keep expanding result types as new parity endpoints land | Existing | P0 |
| Node search | Nodes server-side filters | Done | `NodeTable.tsx`; `getNodesPage` | Include in global search index | Existing | P1 |
| Packet hash search | Packets URL/filter and analyzer | Done | `usePacketFilters.ts`; `PacketAnalyzerDrawer.tsx` | Include in global search overlay | Existing | P1 |
| Channel search | Client-side channel filtering | Partial | `channel-filters.ts` | Add backend search or cached all-channel index if scale demands | Optional API | P2 |
| Prefix tool | Stats Hash prefix lookup with packet drilldowns | Partial | `/api/v1/stats/hash-prefix`; `src/features/stats/HashTab.tsx` | Searches path-hash prefixes across observed hops; add command-palette shortcut later | Search integration | P1 |

### Theme / Customizer

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Dark/light mode | Global CRT themes plus map profiles | Beacon Superset | `useTheme.tsx`; `map/appearance.ts` | Keep global theme separate from map visual profile | Existing | P0 |
| Theme customizer/export | Theme picker/toggles only | Missing | No export customizer found | Add operator theme customizer only after parity-critical analytics | Theme schema and preview | P3 |
| Phosphor/icon migration | Beacon custom icons/inline glyphs | Not Applicable | Existing Beacon icon style | Do not add new icon dependency unless needed | None | Deferred |
| Hidden name prefixes config | Label sanitization only | Missing | `display-label.ts` | Add server config and UI/documentation if local data needs it | Config + filtering policy | P2 |
| Branding home URL | Beacon wordmark/version controls | Partial | `BeaconWordmark.tsx`; shell | Add config-driven home URL/branding metadata | Server/client config endpoint | P3 |
| Customizer disabled tabs | None | Missing | No customizer found | Defer until customizer exists | Theme/customizer milestone | P4 |

### Mobile / Accessibility

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Full mobile experience | Mobile shell/bottom controls across pages | Partial | `BottomNav.tsx`; Live responsive controls | Need acceptance pass for all parity pages | Browser smoke | P0 |
| Compact Live controls | Live dock/rail | Partial | `LiveView.tsx` | Preserve map prominence and no overlap | Browser smoke | P0 |
| WCAG AA contrast pass | Recent map profile contrast, global theme tokens | Partial | `index.css`; `map/appearance.ts` | Add measured contrast gate across themes | A11y test tooling | P1 |
| Keyboard navigation | React components partially accessible | Partial | shared components | Add command palette, tab flows, focus traps tests | A11y test pass | P2 |
| Screen reader patterns | Status/loading components have ARIA in parts | Partial | `TerminalLoader.tsx`, shared components | Add page-level a11y audit | A11y smoke | P2 |

### Ops / Deploy

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| One-container self-host image | Split Beacon web/server/Postgres/Redis | Not Applicable | Beacon deployment model | Keep split stack; document local/public runner clearly | Docs | P2 |
| Built-in Mosquitto/Caddy | External brokers, local Vite/public runner | Not Applicable | Beacon local ops | Keep local-operator first; no need to clone container bundle | Docs | Deferred |
| `manage.sh` operations | `Start-BeaconLocal.ps1`, Podman containers | Partial | `F:\Beacon\Start-BeaconLocal.ps1` | Add restart/status/health/smoke parity script | Ops script | P1 |
| Health endpoints | `/healthz`, brokers/cache/DB | Done | `internal/api/router/router.go`; health handlers | Keep public health banner in UI | Existing | P0 |
| Backup/restore docs | Server deployment docs limited | Missing | README only | Add Postgres/Redis/local ops guide | Docs | P2 |
| CDN/cache warning | No obvious Beacon equivalent | Missing | No CDN docs found | Add no-store/static cache guidance for public tunnel/CDN | Middleware/docs check | P3 |

### API / Contracts

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| 40+ REST endpoints | `/api/v1` route groups | Partial | `internal/api/router/router.go` | Many feature endpoints exist; parity gaps remain in analytics/search/customizer | Endpoint roadmap | P0 |
| OpenAPI docs | Swagger mounted at `/swagger/*` | Done | router Swagger setup | Ensure generated docs include latest handlers | Swagger generation in CI | P1 |
| Protobuf contract | None | Missing | No proto files in Beacon repos | Prefer OpenAPI/TypeScript contracts for Beacon unless binary clients need proto | Contract decision | P3 |
| WebSocket broadcast | Hub + WS manager | Done | `internal/hub`; `src/api/ws-manager.ts` | Continue durable cursors/backfill | Existing | P0 |
| API no-store/cache behavior | Not confirmed | Missing | No explicit parity evidence captured | Add middleware/tests if public CDN is used | Middleware | P2 |
| Region/IATA filters | First-class in Beacon APIs | Beacon Superset | handlers use `iatas` and `region` | Maintain consistent filter semantics | Existing | P0 |

### Performance

| CoreScope capability | Beacon equivalent | Status | Evidence | Gap notes | Dependency | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| In-memory hot read path | Postgres + Redis cached aggregates | Partial | `internal/cache/reader.go`; stats/live/atlas TTLs | Do not clone SQLite in-memory store; add targeted hot paths | Query profiling | P0 |
| Packet query under 1 ms | Postgres paginated queries | Partial | `/api/v1/packets`; db queries | Define Beacon-specific SLOs by dataset size | Bench harness | P1 |
| API under 100 ms | Some cached stats/live/atlas | Partial | Redis TTLs | Add performance budget and smoke timings | Benchmark script | P1 |
| Runtime memory limit config | Go server config supports Redis/Postgres, not CoreScope memory model | Partial | config files | Add Go memory/env docs if needed for local host | Ops docs | P3 |
| Live animation caps | Adaptive caps in Live | Done | `LiveView.tsx` quality caps | Keep mobile/frame-pressure checks | Browser perf smoke | P0 |
| Cold-load resilience | Postgres source of truth; cache fallback | Partial | cache reader fallback | Add explicit startup readiness timings | Health metrics | P1 |

## Milestones

### M0: Source Snapshot And Evidence Capture

Goal: make the roadmap mechanically auditable.

Deliverables:

- Refresh CoreScope public `master` SHA and collect README, changelog, release notes, public API docs/spec, and key screenshots when available.
- Generate a Beacon endpoint inventory from `/swagger/doc.json` or server router tests.
- Generate a Beacon screen inventory from `TABS`, Stats sub-tabs, and URL states.
- Convert the parity matrix above into a living checklist with evidence links to repo paths, API endpoints, and browser screenshots.

Acceptance:

- Every major README feature and 3.9.x changelog highlight appears in the matrix.
- Every matrix row has a status, dependency, and priority.
- `/api/spec` timeout/unavailability is either resolved or recorded with a local fallback.
- No implementation work is mixed into this milestone.

### M1: API And Data Parity Foundations

Goal: add the server contracts required for high-value parity features.

Deliverables:

- Node analytics API: activity timeline, payload mix, route mix, SNR/RSSI/hop distributions, hourly heatmap, peer graph inputs.
- Node reach API: CoreScope-style reach summary using Beacon verified routes/neighbors, with cache invalidation on route/blacklist changes.
- Observer compare API: multi-observer summary, telemetry timelines, packet/payload/route deltas, shared IATAs, stale/health flags.
- Hash analytics API: hash-size distribution, inconsistent sizes, collision matrix data, prefix lookup tool.
- Topology/subpath API: top repeaters, top pairs, best paths, hops-vs-signal, subpath ranking/detail.
- Global search API: packets, nodes, observers, channels, routes, traces, with type, label, URL target, and relevance score.

Acceptance:

- Go tests cover filters, pagination, empty data, invalid params, cache keys, Redis fallback, and region/IATA expansion.
- New endpoints are documented in Swagger.
- Existing Atlas, Live, Stats, Packets, Nodes, Observers, Channels, Routes, and Traces behavior does not regress.

### M2: High-Value UX Parity

Goal: expose the most operator-useful parity features in Beacon-native screens.

Deliverables:

- Node analytics page/panel reachable from Nodes, Map, Atlas, Live, Packets, Routes, and global search.
- Observer Compare under Stats/Observers with multi-select, state-preserving URL, and side-by-side health/traffic charts.
- Global command/search overlay, keyboard shortcut, and URL targets for all result types.
- Channel key/catalogue affordances: public hashtag catalogue, explicit key status, encrypted/unknown empty states, shareable channel URLs.
- Packet analyzer parity pass for byte-level structure, raw hex copy/export, route/observer drilldowns.

Acceptance:

- Vitest coverage for transforms, URL serialization, empty/degraded states, and keyboard interaction.
- Desktop/mobile browser checks for no overlap, usable search, and working drilldowns.
- New UX uses existing Beacon theme, terminal loaders, ECharts, React Query, and URL-state patterns.

### M3: Advanced Analysis Parity

Goal: bring CoreScope's deeper network science into Beacon Stats.

Deliverables:

- Stats Topology section: top repeaters, top node pairs, best paths, hops-vs-signal, reach rings.
- Stats Hash section: hash size timeline, multibyte adopters, inconsistent hash sizes, collision matrix, prefix tool.
- Stats Channels section: channel timelines, top senders, encrypted/unknown buckets, transport scope relation where data supports it.
- Stats Subpaths/Routes section: route pattern ranking, subpath detail, cross-region path comparisons.
- Clock/RF health pass: observer/node clock skew where measurable, threshold configuration, RF offender explanations.

Acceptance:

- Charts are nonblank when data exists and show clear empty states otherwise.
- Tables remain usable on mobile with horizontal containment where needed.
- Queries meet Beacon SLOs on the local dataset with Redis enabled and still serve safely without Redis.

### M4: Customization, Mobile, And Accessibility Parity

Goal: match CoreScope's polish where it matters while preserving Beacon's retro operator identity.

Deliverables:

- Mobile acceptance sweep across every top-level tab and Stats sub-tab.
- WCAG contrast audit for global CRT themes, retro/modern font toggle, scanlines on/off, and Dark/Liberty/Light map profiles.
- Keyboard/focus audit for command search, detail panels, drawers, filters, maps, and modal/bottom-sheet controls.
- Optional theme customizer v1: preview, import/export `theme.json`, reset defaults, and operator-safe disabled customization if configured.
- Config-driven hidden name prefixes and branding home URL if confirmed useful for Beacon operators.

Acceptance:

- No overlapping panels or unreadable controls at common mobile widths.
- A11y smoke includes keyboard navigation, focus trapping, status announcements, and contrast checks.
- Customization remains optional and cannot break the default Beacon theme.

### M5: Ops, Performance, And Release Polish

Goal: make parity durable in the live local/public Beacon environment.

Deliverables:

- Beacon SLOs: endpoint timing budgets, Live frame-pressure budget, map first-paint budget, Stats chart budget.
- Benchmark script for local dataset and public smoke: health, API summaries, Live backfill, Stats aggregates, search, node analytics.
- Ops docs for Podman Postgres/Redis, Vite public runner, cloudflared tunnel expectations, backups, restore, and CDN/no-store guidance.
- Startup/readiness health UI: DB, Redis, brokers, runtime, stale observers, degraded cache.
- CI/local validation checklist: Go tests, Vitest, build, swagger generation, browser smoke.

Acceptance:

- Local operator can recover Beacon after reboot using documented scripts and verify `/healthz`, Redis, Postgres, Vite, and WebSocket state.
- Browser smoke covers `Atlas`, `Live`, `Map`, `Stats`, `Nodes`, `Observers`, `Channels`, `Routes`, `Traces`, and `Packets`.
- Performance results are recorded before release and regressions are actionable.

## Deferred Or Rejected Items

| Item | Decision | Rationale |
| --- | --- | --- |
| Exact CoreScope UI clone | Rejected | Beacon has its own regional/operator design and React component system. |
| SQLite in-memory read architecture | Rejected | Beacon's source of truth is Postgres with Redis hot aggregates. |
| One-container Caddy/Mosquitto bundle | Rejected for v1 parity | Beacon runs as local operator stack with server, web, Postgres, Redis, and existing broker sources. |
| Excel-like packet table cloning | Deferred | Beacon should keep responsive packet rows unless operators specifically need spreadsheet affordances. |
| Protobuf contract | Deferred | Beacon already uses OpenAPI/TypeScript shapes; add proto only for external client demand. |
| Sonification-first experiences | Deferred | Keep audio as optional Live advanced mode, not a parity blocker. |
| Dedicated VCR scrub/speed controls | Deferred | User direction is to skip VCR controls and prioritize analytics. Keep lightweight route replay/drilldowns only where they support analysis. |
| Full theme customizer before analytics parity | Deferred | Operator analytics/search/reach have higher value. |

## Ready For Implementation Checklist

- [ ] Refresh CoreScope `master` SHA before starting M0.
- [ ] Save CoreScope README/changelog/API evidence in the roadmap or companion issue.
- [ ] Confirm Beacon local web/server SHAs before each milestone starts.
- [ ] Implement API/data foundations before frontend parity screens.
- [ ] Keep all new features region/IATA-aware.
- [ ] Cache aggregate-heavy endpoints through Redis with Postgres fallback.
- [ ] Preserve URL-deep-link behavior for every drilldown.
- [ ] Validate desktop and mobile browser surfaces before release.
- [ ] Keep CoreScope read-only; never mutate the reference clone for parity work.
- [ ] Commit roadmap and future work in Beacon repos, not CoreScope.
