-- Windowed dashboard and Atlas aggregates repeatedly scan recent observations
-- then join packets by hash. This covering index lets those reads stay index-only
-- for the observation side while preserving the existing BRIN for broad pruning.
CREATE INDEX IF NOT EXISTS idx_observations_heard_packet_cover
  ON packet_observations(heard_at DESC, packet_hash)
  INCLUDE (iata, observer_id, id);
