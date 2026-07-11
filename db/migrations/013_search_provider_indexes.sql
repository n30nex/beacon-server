CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_nodes_name_trgm
  ON nodes USING gin (name gin_trgm_ops)
  WHERE name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_observers_display_name_trgm
  ON observers USING gin (display_name gin_trgm_ops)
  WHERE display_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_channels_name_trgm
  ON channels USING gin (name gin_trgm_ops)
  WHERE name IS NOT NULL;
