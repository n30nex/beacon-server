// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package background runs periodic maintenance tasks on one non-overlapping
// worker while preserving independent schedules.
package background

import (
	"context"
	"log"
	"time"
)

// Task is a named unit of work that runs on a fixed interval.
type Task struct {
	Name     string
	Interval time.Duration
	Offset   time.Duration
	Timeout  time.Duration
	Run      func(ctx context.Context) (TaskResult, error)
}

type TaskResult struct {
	AffectedRows int64
	Skipped      bool
}

// Scheduler runs a set of tasks on independent tickers.
type Scheduler struct {
	tasks    []Task
	recorder *Recorder
}

// New creates a Scheduler with the given tasks.
func New(tasks []Task) *Scheduler {
	return NewWithRecorder(tasks, NewRecorder())
}

// NewWithRecorder creates a Scheduler using a caller-owned metrics recorder.
func NewWithRecorder(tasks []Task, recorder *Recorder) *Scheduler {
	if recorder == nil {
		recorder = NewRecorder()
	}
	for _, task := range tasks {
		recorder.register(task.Name)
	}
	return &Scheduler{tasks: tasks, recorder: recorder}
}

// MetricsSnapshot returns the latest background task counters.
func (s *Scheduler) MetricsSnapshot() map[string]TaskSnapshot {
	if s == nil || s.recorder == nil {
		return nil
	}
	return s.recorder.Snapshot()
}

// Start runs all due tasks serially. A slow maintenance operation can delay a
// later one, but it can never overlap it and compound database contention.
func (s *Scheduler) Start(ctx context.Context) {
	if len(s.tasks) == 0 {
		<-ctx.Done()
		return
	}
	nextRuns := make([]time.Time, len(s.tasks))
	now := time.Now()
	for i, task := range s.tasks {
		delay := task.Offset
		if delay <= 0 {
			delay = task.Interval
		}
		nextRuns[i] = now.Add(delay)
		if s.recorder != nil {
			s.recorder.schedule(task.Name, nextRuns[i], task.Timeout)
		}
	}

	for {
		next := nextRuns[0]
		for _, candidate := range nextRuns[1:] {
			if candidate.Before(next) {
				next = candidate
			}
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}

		for i, task := range s.tasks {
			if time.Now().Before(nextRuns[i]) {
				continue
			}
			startedAt := time.Now()
			if s.recorder != nil {
				s.recorder.start(task.Name, startedAt)
			}
			log.Printf("background[%s]: running", task.Name)
			runCtx := ctx
			cancel := func() {}
			if task.Timeout > 0 {
				runCtx, cancel = context.WithTimeout(ctx, task.Timeout)
			}
			result, err := task.Run(runCtx)
			cancel()
			finishedAt := time.Now()
			if s.recorder != nil {
				s.recorder.finish(task.Name, startedAt, finishedAt, result, err)
			}
			if err != nil {
				log.Printf("background[%s]: %v", task.Name, err)
			}
			log.Printf("background[%s]: complete affected_rows=%d skipped=%t", task.Name, result.AffectedRows, result.Skipped)

			interval := task.Interval
			if interval <= 0 {
				interval = time.Hour
			}
			for !nextRuns[i].After(finishedAt) {
				nextRuns[i] = nextRuns[i].Add(interval)
			}
			if s.recorder != nil {
				s.recorder.schedule(task.Name, nextRuns[i], task.Timeout)
			}
		}
	}
}
