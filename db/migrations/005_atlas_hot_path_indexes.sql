-- Speed up Atlas replay pagination and per-packet observation enrichment.
CREATE INDEX IF NOT EXISTS idx_packets_last_heard_hash
  ON packets(last_heard_at DESC, packet_hash DESC);

CREATE INDEX IF NOT EXISTS idx_observations_packet_heard_id
  ON packet_observations(packet_hash, heard_at DESC, id DESC);
