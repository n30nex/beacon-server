-- Local query observability. shared_preload_libraries is configured by the
-- tracked local Compose definition; creating the extension remains harmless
-- on deployments that preload it by another mechanism.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- High-churn tables need analyze/vacuum decisions based on materially less
-- than the cluster-wide default of 20 percent growth.
ALTER TABLE packets SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);

ALTER TABLE packet_observations SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.005
);

ALTER TABLE known_routes SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);

ALTER TABLE node_short_ids SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE node_neighbors SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);
