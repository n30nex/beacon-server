// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerNeverOverlapsTasks(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	var runs atomic.Int32
	run := func(context.Context) (TaskResult, error) {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(12 * time.Millisecond)
		active.Add(-1)
		runs.Add(1)
		return TaskResult{}, nil
	}
	scheduler := New([]Task{
		{Name: "first", Interval: 25 * time.Millisecond, Offset: time.Millisecond, Timeout: time.Second, Run: run},
		{Name: "second", Interval: 25 * time.Millisecond, Offset: time.Millisecond, Timeout: time.Second, Run: run},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()
	scheduler.Start(ctx)

	if runs.Load() < 2 {
		t.Fatalf("runs = %d, want at least two", runs.Load())
	}
	if maxActive.Load() != 1 {
		t.Fatalf("maximum concurrent tasks = %d, want 1", maxActive.Load())
	}
}

func TestDirtyIATAsTakeAndRestore(t *testing.T) {
	dirty := NewDirtyIATAs(true)
	dirty.Mark("yvr")
	all, iatas := dirty.Take()
	if !all || len(iatas) != 0 {
		t.Fatalf("startup marker = %v/%v, want all", all, iatas)
	}
	dirty.Mark("yow")
	all, iatas = dirty.Take()
	if all || len(iatas) != 1 || iatas[0] != "YOW" {
		t.Fatalf("dirty set = %v/%v", all, iatas)
	}
	dirty.Restore(false, iatas)
	_, iatas = dirty.Take()
	if len(iatas) != 1 || iatas[0] != "YOW" {
		t.Fatalf("restored dirty set = %v", iatas)
	}
}
