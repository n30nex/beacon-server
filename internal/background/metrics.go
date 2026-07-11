// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"sort"
	"sync"
	"time"
)

// TaskSnapshot is the health-visible state for one scheduled task.
type TaskSnapshot struct {
	Runs             uint64 `json:"runs"`
	Successes        uint64 `json:"successes"`
	Failures         uint64 `json:"failures"`
	LastStatus       string `json:"lastStatus,omitempty"`
	LastError        string `json:"lastError,omitempty"`
	LastStartedAt    int64  `json:"lastStartedAt,omitempty"`
	LastFinishedAt   int64  `json:"lastFinishedAt,omitempty"`
	LastDurationMs   int64  `json:"lastDurationMs,omitempty"`
	NextRunAt        int64  `json:"nextRunAt,omitempty"`
	TimeoutMs        int64  `json:"timeoutMs,omitempty"`
	SkippedRuns      uint64 `json:"skippedRuns"`
	LastAffectedRows int64  `json:"lastAffectedRows"`
}

type taskMetrics struct {
	runs             uint64
	successes        uint64
	failures         uint64
	lastStatus       string
	lastError        string
	lastStartedAt    time.Time
	lastFinishedAt   time.Time
	lastDuration     time.Duration
	nextRunAt        time.Time
	timeout          time.Duration
	skippedRuns      uint64
	lastAffectedRows int64
}

// Recorder tracks periodic background task outcomes for status reporting.
type Recorder struct {
	mu    sync.Mutex
	tasks map[string]*taskMetrics
}

// NewRecorder returns an empty background metrics recorder.
func NewRecorder() *Recorder {
	return &Recorder{tasks: map[string]*taskMetrics{}}
}

func (r *Recorder) register(name string) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.taskLocked(name)
}

func (r *Recorder) start(name string, at time.Time) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.taskLocked(name)
	stats.runs++
	stats.lastStatus = "running"
	stats.lastError = ""
	stats.lastStartedAt = at
}

func (r *Recorder) schedule(name string, nextRun time.Time, timeout time.Duration) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.taskLocked(name)
	stats.nextRunAt = nextRun
	stats.timeout = timeout
}

func (r *Recorder) finish(name string, startedAt, finishedAt time.Time, result TaskResult, err error) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.taskLocked(name)
	stats.lastFinishedAt = finishedAt
	stats.lastDuration = finishedAt.Sub(startedAt)
	stats.lastAffectedRows = result.AffectedRows
	if result.Skipped {
		stats.skippedRuns++
	}
	if err != nil {
		stats.failures++
		stats.lastStatus = "failed"
		stats.lastError = err.Error()
		return
	}
	stats.successes++
	stats.lastStatus = "success"
	stats.lastError = ""
}

// Snapshot returns a copy of task metrics keyed by task name.
func (r *Recorder) Snapshot() map[string]TaskSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.tasks) == 0 {
		return nil
	}

	keys := make([]string, 0, len(r.tasks))
	for key := range r.tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make(map[string]TaskSnapshot, len(keys))
	for _, key := range keys {
		stats := r.tasks[key]
		snap := TaskSnapshot{
			Runs:             stats.runs,
			Successes:        stats.successes,
			Failures:         stats.failures,
			LastStatus:       stats.lastStatus,
			LastError:        stats.lastError,
			LastDurationMs:   stats.lastDuration.Milliseconds(),
			SkippedRuns:      stats.skippedRuns,
			LastAffectedRows: stats.lastAffectedRows,
		}
		if !stats.nextRunAt.IsZero() {
			snap.NextRunAt = stats.nextRunAt.UnixMilli()
		}
		if stats.timeout > 0 {
			snap.TimeoutMs = stats.timeout.Milliseconds()
		}
		if !stats.lastStartedAt.IsZero() {
			snap.LastStartedAt = stats.lastStartedAt.UnixMilli()
		}
		if !stats.lastFinishedAt.IsZero() {
			snap.LastFinishedAt = stats.lastFinishedAt.UnixMilli()
		}
		out[key] = snap
	}
	return out
}

func (r *Recorder) taskLocked(name string) *taskMetrics {
	stats := r.tasks[name]
	if stats == nil {
		stats = &taskMetrics{}
		r.tasks[name] = stats
	}
	return stats
}
