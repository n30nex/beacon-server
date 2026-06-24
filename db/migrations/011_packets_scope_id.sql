-- Scope dashboards count packets by transport scope. Most packets have no
-- scope, so a partial index keeps those counts from scanning the full packets
-- table as retention grows.
CREATE INDEX IF NOT EXISTS idx_packets_scope_id
  ON packets(scope_id)
  WHERE scope_id IS NOT NULL;
