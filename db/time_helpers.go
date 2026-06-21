// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "time"

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
