-- Node reach and cross-IATA route searches ask whether a route contains a
-- specific node anywhere in its hop array. GIN keeps those containment reads
-- from degrading into full known_routes scans as route history grows.
CREATE INDEX IF NOT EXISTS idx_known_routes_node_ids_gin
  ON known_routes USING GIN (node_ids);
