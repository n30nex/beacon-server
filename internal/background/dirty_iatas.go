// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"sort"
	"strings"
	"sync"
)

// DirtyIATAs coalesces short-ID changes between reconfirmation passes. The
// startup all-dirty marker guarantees one complete validation after restart.
type DirtyIATAs struct {
	mu  sync.Mutex
	all bool
	set map[string]struct{}
}

func NewDirtyIATAs(markAll bool) *DirtyIATAs {
	return &DirtyIATAs{all: markAll, set: map[string]struct{}{}}
}

func (d *DirtyIATAs) Mark(iata string) {
	if d == nil {
		return
	}
	iata = strings.ToUpper(strings.TrimSpace(iata))
	if iata == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.all {
		d.set[iata] = struct{}{}
	}
}

func (d *DirtyIATAs) Take() (bool, []string) {
	if d == nil {
		return true, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	all := d.all
	iatas := make([]string, 0, len(d.set))
	for iata := range d.set {
		iatas = append(iatas, iata)
	}
	sort.Strings(iatas)
	d.all = false
	d.set = map[string]struct{}{}
	return all, iatas
}

func (d *DirtyIATAs) Restore(all bool, iatas []string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if all {
		d.all = true
	}
	if !d.all {
		for _, iata := range iatas {
			d.set[iata] = struct{}{}
		}
	}
}
