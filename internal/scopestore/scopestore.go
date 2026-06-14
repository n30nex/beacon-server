// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package scopestore provides an in-memory lookup of transport scope keys
// loaded from the database at startup.
package scopestore

import "sync"

// Entry holds a single transport scope key and its metadata.
type Entry struct {
	Name           string
	TransportKey   []byte // 16 bytes
	KeyFingerprint []byte // 8 bytes
}

// ScopeStore holds all known transport scope keys in memory.
type ScopeStore struct {
	mu      sync.RWMutex
	entries []Entry
}

// New creates an empty ScopeStore.
func New() *ScopeStore {
	return &ScopeStore{}
}

// Load replaces all entries — call on startup after DB seeding.
func (s *ScopeStore) Load(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
}

// Entries returns the loaded entries for read-only iteration. The slice is
// swapped wholesale by Load and never mutated in place, so callers may range
// over the result safely but must not modify it. Avoiding the defensive copy
// matters because this is called for every transport packet during ingest.
func (s *ScopeStore) Entries() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries
}
