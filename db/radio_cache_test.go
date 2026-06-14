// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
)

func TestRadioCache_StoreThenHit(t *testing.T) {
	rc := newRadioCache()
	id := uuid.New()
	want := ingest.RadioSettings{FreqMHz: 910.5, SF: 7, BWKHz: 62.5, CR: 5}

	if _, ok, _ := rc.lookup(id); ok {
		t.Fatal("expected miss on empty cache")
	}
	_, _, gen := rc.lookup(id)
	rc.store(id, want, gen)

	got, ok, _ := rc.lookup(id)
	if !ok {
		t.Fatal("expected hit after store")
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestRadioCache_InvalidateDropsEntryAndBumpsGen(t *testing.T) {
	rc := newRadioCache()
	id := uuid.New()
	_, _, gen := rc.lookup(id)
	rc.store(id, ingest.RadioSettings{SF: 7}, gen)

	_, _, genBefore := rc.lookup(id)
	rc.invalidate(id)

	if _, ok, genAfter := rc.lookup(id); ok {
		t.Error("expected miss after invalidate")
	} else if genAfter == genBefore {
		t.Error("expected generation to advance after invalidate")
	}
}

// TestRadioCache_StaleStoreSkipped models a read-through racing an
// invalidation: a value read against an old generation must not be cached over
// the newer state.
func TestRadioCache_StaleStoreSkipped(t *testing.T) {
	rc := newRadioCache()
	id := uuid.New()

	// Reader samples the generation on a miss...
	_, ok, gen := rc.lookup(id)
	if ok {
		t.Fatal("expected initial miss")
	}
	// ...then an invalidation (e.g. a /status update) lands before the store.
	rc.invalidate(id)

	// The reader's store carries the now-stale generation and must be a no-op.
	rc.store(id, ingest.RadioSettings{SF: 12}, gen)

	if _, ok, _ := rc.lookup(id); ok {
		t.Error("stale store should have been skipped, but entry was cached")
	}
}

func TestRadioCache_FreshStoreSucceeds(t *testing.T) {
	rc := newRadioCache()
	id := uuid.New()
	rc.invalidate(id) // advance the generation first

	_, _, gen := rc.lookup(id)
	rc.store(id, ingest.RadioSettings{SF: 9}, gen)

	if _, ok, _ := rc.lookup(id); !ok {
		t.Error("store with current generation should have cached the value")
	}
}
