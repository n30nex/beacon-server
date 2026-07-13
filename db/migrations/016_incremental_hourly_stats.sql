-- Incrementally maintained replacement for mv_hourly_iata_stats. Keep the
-- materialized view intact so an older application image can still roll back.

CREATE TABLE IF NOT EXISTS stats_hourly_iata (
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  hour              TIMESTAMPTZ NOT NULL,
  observation_count BIGINT NOT NULL,
  unique_packets    BIGINT NOT NULL,
  active_observers  BIGINT NOT NULL,
  refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (iata, hour)
);

CREATE INDEX IF NOT EXISTS idx_stats_hourly_iata_hour
  ON stats_hourly_iata(hour);

INSERT INTO stats_hourly_iata (
  iata, hour, observation_count, unique_packets, active_observers, refreshed_at
)
SELECT iata, hour, observation_count, unique_packets, active_observers, NOW()
FROM mv_hourly_iata_stats
ON CONFLICT (iata, hour) DO UPDATE SET
  observation_count = EXCLUDED.observation_count,
  unique_packets = EXCLUDED.unique_packets,
  active_observers = EXCLUDED.active_observers,
  refreshed_at = EXCLUDED.refreshed_at;

ALTER TABLE stats_hourly_iata SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01
);
