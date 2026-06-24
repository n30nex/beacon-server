-- Netgraph reads route snapshots ordered by route recency. This index keeps
-- smaller first-page and mobile route-limit reads from sorting known_routes.
CREATE INDEX IF NOT EXISTS idx_known_routes_last_seen
  ON known_routes(last_seen DESC);
