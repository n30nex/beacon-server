// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestResolveTTLsAtlasDefaultsToShortHotPath(t *testing.T) {
	ttls := ResolveTTLs(config.CacheConfig{})
	if ttls.Atlas != 30*time.Second {
		t.Fatalf("expected atlas ttl to default to 30s, got %s", ttls.Atlas)
	}
	if ttls.Stats != time.Hour {
		t.Fatalf("expected stats ttl to keep 1h default, got %s", ttls.Stats)
	}
}
