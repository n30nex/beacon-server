-- Atlas and Stats top-node paths join recent observations to packets by hash,
-- then read origin_pubkey. Covering that lookup avoids a full packets scan on
-- production-sized windows while preserving exact historical-window semantics.
CREATE INDEX IF NOT EXISTS idx_packets_hash_origin_cover
  ON packets(packet_hash)
  INCLUDE (origin_pubkey)
  WHERE origin_pubkey IS NOT NULL;

ANALYZE packets;
ANALYZE packet_observations;
