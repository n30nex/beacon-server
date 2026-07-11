-- Incremental Atlas aggregates for canonical 24-hour briefing windows.
-- Each hour is replaced atomically by the background worker. Arrays preserve
-- exact distinct packet/observer counts across hour boundaries while keeping
-- the hot read path off the multi-million-row observation table.

CREATE TABLE IF NOT EXISTS atlas_aggregate_hours (
  hour         TIMESTAMPTZ PRIMARY KEY,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS atlas_hourly_iata_aggregates (
  hour              TIMESTAMPTZ NOT NULL,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  observation_count BIGINT NOT NULL,
  packet_hashes     BYTEA[] NOT NULL,
  observer_ids      UUID[] NOT NULL,
  PRIMARY KEY (hour, iata)
);

CREATE INDEX IF NOT EXISTS idx_atlas_hourly_iata_window
  ON atlas_hourly_iata_aggregates(iata, hour);

CREATE TABLE IF NOT EXISTS atlas_hourly_mix_aggregates (
  hour              TIMESTAMPTZ NOT NULL,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  payload_type      SMALLINT NOT NULL,
  route_type        SMALLINT NOT NULL,
  observation_count BIGINT NOT NULL,
  PRIMARY KEY (hour, iata, payload_type, route_type)
);

CREATE INDEX IF NOT EXISTS idx_atlas_hourly_mix_window
  ON atlas_hourly_mix_aggregates(iata, hour);

CREATE TABLE IF NOT EXISTS atlas_hourly_node_aggregates (
  hour              TIMESTAMPTZ NOT NULL,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  origin_pubkey     BYTEA NOT NULL,
  observation_count BIGINT NOT NULL,
  last_heard        TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (hour, iata, origin_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_atlas_hourly_node_window
  ON atlas_hourly_node_aggregates(iata, hour);

CREATE TABLE IF NOT EXISTS atlas_hourly_observer_aggregates (
  hour              TIMESTAMPTZ NOT NULL,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  observer_id       UUID NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
  observation_count BIGINT NOT NULL,
  last_heard        TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (hour, iata, observer_id)
);

CREATE INDEX IF NOT EXISTS idx_atlas_hourly_observer_window
  ON atlas_hourly_observer_aggregates(observer_id, hour);
CREATE INDEX IF NOT EXISTS idx_atlas_hourly_observer_iata_window
  ON atlas_hourly_observer_aggregates(iata, hour);

ALTER TABLE atlas_aggregate_hours SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
ALTER TABLE atlas_hourly_iata_aggregates SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
ALTER TABLE atlas_hourly_mix_aggregates SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
ALTER TABLE atlas_hourly_node_aggregates SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
ALTER TABLE atlas_hourly_observer_aggregates SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
