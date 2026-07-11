-- Canonical Atlas briefings use hourly bucket semantics. Preserve the exact
-- per-hour distinct values so hot reads can add 24-hour buckets without
-- unnesting arrays or returning to the raw observation table.

ALTER TABLE atlas_hourly_iata_aggregates
  ADD COLUMN IF NOT EXISTS unique_packet_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS active_observer_count BIGINT NOT NULL DEFAULT 0;

UPDATE atlas_hourly_iata_aggregates
SET unique_packet_count = cardinality(packet_hashes),
    active_observer_count = cardinality(observer_ids)
WHERE unique_packet_count = 0 OR active_observer_count = 0;
