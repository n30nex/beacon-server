// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"errors"
	"testing"
	"time"
)

func TestRecorderSnapshotRecordsSuccessAndFailure(t *testing.T) {
	recorder := NewRecorder()
	startedAt := time.Unix(100, 0)

	recorder.register("cleanup")
	recorder.start("cleanup", startedAt)
	recorder.finish("cleanup", startedAt, startedAt.Add(25*time.Millisecond), nil)
	recorder.start("cleanup", startedAt.Add(time.Second))
	recorder.finish("cleanup", startedAt.Add(time.Second), startedAt.Add(time.Second+10*time.Millisecond), errors.New("delete failed"))

	snapshot := recorder.Snapshot()
	cleanup := snapshot["cleanup"]
	if cleanup.Runs != 2 || cleanup.Successes != 1 || cleanup.Failures != 1 {
		t.Fatalf("unexpected cleanup counters: %#v", cleanup)
	}
	if cleanup.LastStatus != "failed" || cleanup.LastError != "delete failed" {
		t.Fatalf("unexpected cleanup status: %#v", cleanup)
	}
	if cleanup.LastDurationMs != 10 {
		t.Fatalf("expected last duration 10ms, got %d", cleanup.LastDurationMs)
	}
}

func TestNewWithRecorderRegistersTasks(t *testing.T) {
	recorder := NewRecorder()
	scheduler := NewWithRecorder([]Task{{Name: "view_refresh", Interval: time.Hour}}, recorder)

	snapshot := scheduler.MetricsSnapshot()
	if _, ok := snapshot["view_refresh"]; !ok {
		t.Fatalf("expected registered task in snapshot, got %#v", snapshot)
	}
}
