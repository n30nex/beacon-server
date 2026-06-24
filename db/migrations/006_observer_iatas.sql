-- Track observer activity per IATA so health dashboards do not need to scan
-- packet_observations to find each observer's latest region.
CREATE TABLE IF NOT EXISTS observer_iatas (
  observer_id       UUID NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  first_heard       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_heard        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  observation_count BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (observer_id, iata)
);

CREATE INDEX IF NOT EXISTS idx_observer_iatas_iata
  ON observer_iatas(iata, last_heard DESC);

CREATE INDEX IF NOT EXISTS idx_observer_iatas_observer_last
  ON observer_iatas(observer_id, last_heard DESC);

INSERT INTO observer_iatas (observer_id, iata, first_heard, last_heard, observation_count)
SELECT
  observer_id,
  iata,
  MIN(heard_at),
  MAX(heard_at),
  COUNT(*)::bigint
FROM packet_observations
GROUP BY observer_id, iata
ON CONFLICT (observer_id, iata) DO UPDATE SET
  first_heard = LEAST(observer_iatas.first_heard, EXCLUDED.first_heard),
  last_heard = GREATEST(observer_iatas.last_heard, EXCLUDED.last_heard),
  observation_count = EXCLUDED.observation_count;
