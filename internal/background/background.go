// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package background runs periodic maintenance tasks on independent schedules.
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
	Run      func(ctx context.Context) error
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

// Start launches each task in its own goroutine. Blocks until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	for _, t := range s.tasks {
		task := t
		go func() {
			ticker := time.NewTicker(task.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					startedAt := time.Now()
					if s.recorder != nil {
						s.recorder.start(task.Name, startedAt)
					}
					log.Printf("background[%s]: running", task.Name)
					if err := task.Run(ctx); err != nil {
						if s.recorder != nil {
							s.recorder.finish(task.Name, startedAt, time.Now(), err)
						}
						log.Printf("background[%s]: %v", task.Name, err)
					} else {
						if s.recorder != nil {
							s.recorder.finish(task.Name, startedAt, time.Now(), nil)
						}
					}
					log.Printf("background[%s]: complete", task.Name)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}
