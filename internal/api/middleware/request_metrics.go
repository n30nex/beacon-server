// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

const (
	defaultRequestWindow = 5 * time.Minute
	maxRequestRoutes     = 64
	maxSamplesPerRoute   = 512
)

type requestSample struct {
	at       time.Time
	duration time.Duration
	status   int
}

// RouteRequestSnapshot is a bounded rolling view of one route pattern.
type RouteRequestSnapshot struct {
	Count                uint64            `json:"count"`
	Errors               uint64            `json:"errors"`
	SlowRequests         uint64            `json:"slowRequests"`
	HardDeadlineBreaches uint64            `json:"hardDeadlineBreaches"`
	P95DurationMs        int64             `json:"p95DurationMs"`
	LastDurationMs       int64             `json:"lastDurationMs"`
	LastStatus           int               `json:"lastStatus"`
	TargetMs             int64             `json:"targetMs"`
	HardDeadlineMs       int64             `json:"hardDeadlineMs"`
	LatencyBuckets       map[string]uint64 `json:"latencyBuckets"`
}

// ServiceLevelSnapshot deliberately remains separate from readiness. A
// performance breach can degrade the operator experience without authorizing
// a process restart.
type ServiceLevelSnapshot struct {
	Status      string `json:"status"`
	WorstRoute  string `json:"worstRoute,omitempty"`
	WorstP95Ms  int64  `json:"worstP95Ms,omitempty"`
	TargetMs    int64  `json:"targetMs,omitempty"`
	WindowSecs  int64  `json:"windowSeconds"`
	LastUpdated int64  `json:"lastUpdated"`
}

// RequestMetricsSnapshot is returned through /healthz and /readyz.
type RequestMetricsSnapshot struct {
	Routes       map[string]RouteRequestSnapshot `json:"routes,omitempty"`
	ServiceLevel ServiceLevelSnapshot            `json:"serviceLevel"`
}

// RequestMetrics records only method and Chi route patterns. It never stores
// raw URLs, query strings, request bodies, or message content.
type RequestMetrics struct {
	mu     sync.Mutex
	window time.Duration
	routes map[string][]requestSample
	now    func() time.Time
}

func NewRequestMetrics() *RequestMetrics {
	return &RequestMetrics{
		window: defaultRequestWindow,
		routes: make(map[string][]requestSample),
		now:    time.Now,
	}
}

// Handler times requests and records them after Chi has resolved the route
// pattern. Unknown routes are grouped under a fixed label.
func (m *RequestMetrics) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := m.now()
		wrapped := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(wrapped, r)

		status := wrapped.Status()
		if status == 0 {
			status = http.StatusOK
		}
		pattern := chi.RouteContext(r.Context()).RoutePattern()
		if pattern == "" {
			pattern = "unmatched"
		}
		label := r.Method + " " + pattern
		duration := m.now().Sub(started)
		m.record(label, requestSample{at: m.now(), duration: duration, status: status})

		target, _ := routeSLO(pattern)
		if duration > target {
			log.Printf("slow request method=%s route=%s status=%d duration_ms=%d request_id=%s",
				r.Method, pattern, status, duration.Milliseconds(), chimiddleware.GetReqID(r.Context()))
		}
	})
}

func (m *RequestMetrics) record(label string, sample requestSample) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.routes[label]; !exists && len(m.routes) >= maxRequestRoutes {
		label = "OTHER other"
	}
	cutoff := sample.at.Add(-m.window)
	samples := m.routes[label]
	first := 0
	for first < len(samples) && samples[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		samples = append([]requestSample(nil), samples[first:]...)
	}
	samples = append(samples, sample)
	if len(samples) > maxSamplesPerRoute {
		samples = append([]requestSample(nil), samples[len(samples)-maxSamplesPerRoute:]...)
	}
	m.routes[label] = samples
}

// Snapshot returns a defensive copy of metrics still inside the rolling
// window and calculates the worst current SLO independently of readiness.
func (m *RequestMetrics) Snapshot() RequestMetricsSnapshot {
	now := time.Now()
	if m == nil {
		return RequestMetricsSnapshot{ServiceLevel: ServiceLevelSnapshot{Status: "unknown", WindowSecs: int64(defaultRequestWindow.Seconds()), LastUpdated: now.UnixMilli()}}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now = m.now()
	cutoff := now.Add(-m.window)
	out := RequestMetricsSnapshot{
		Routes:       make(map[string]RouteRequestSnapshot),
		ServiceLevel: ServiceLevelSnapshot{Status: "ok", WindowSecs: int64(m.window.Seconds()), LastUpdated: now.UnixMilli()},
	}
	worstRatio := 0.0
	for label, stored := range m.routes {
		first := 0
		for first < len(stored) && stored[first].at.Before(cutoff) {
			first++
		}
		if first > 0 {
			stored = append([]requestSample(nil), stored[first:]...)
			m.routes[label] = stored
		}
		if len(stored) == 0 {
			continue
		}
		pattern := label
		if split := strings.IndexByte(label, ' '); split >= 0 && split+1 < len(label) {
			pattern = label[split+1:]
		}
		target, hardDeadline := routeSLO(pattern)
		snap := summarizeSamples(stored, target, hardDeadline)
		out.Routes[label] = snap
		ratio := float64(snap.P95DurationMs) / math.Max(1, float64(snap.TargetMs))
		if snap.HardDeadlineBreaches > 0 {
			ratio = math.Max(ratio, float64(snap.LastDurationMs)/math.Max(1, float64(snap.HardDeadlineMs)))
		}
		if ratio > worstRatio {
			worstRatio = ratio
			out.ServiceLevel.WorstRoute = label
			out.ServiceLevel.WorstP95Ms = snap.P95DurationMs
			out.ServiceLevel.TargetMs = snap.TargetMs
		}
		if snap.P95DurationMs > snap.TargetMs || snap.HardDeadlineBreaches > 0 {
			out.ServiceLevel.Status = "degraded"
		}
	}
	if len(out.Routes) == 0 {
		out.Routes = nil
		out.ServiceLevel.Status = "unknown"
	}
	return out
}

func summarizeSamples(samples []requestSample, target, hardDeadline time.Duration) RouteRequestSnapshot {
	durations := make([]int64, 0, len(samples))
	snap := RouteRequestSnapshot{
		TargetMs:       target.Milliseconds(),
		HardDeadlineMs: hardDeadline.Milliseconds(),
		LatencyBuckets: map[string]uint64{"le100ms": 0, "le250ms": 0, "le500ms": 0, "le1s": 0, "le2s": 0, "le5s": 0, "gt5s": 0},
	}
	for _, sample := range samples {
		ms := sample.duration.Milliseconds()
		durations = append(durations, ms)
		snap.Count++
		snap.LastDurationMs = ms
		snap.LastStatus = sample.status
		if sample.status >= http.StatusInternalServerError {
			snap.Errors++
		}
		if sample.duration > target {
			snap.SlowRequests++
		}
		if sample.duration > hardDeadline {
			snap.HardDeadlineBreaches++
		}
		switch {
		case sample.duration <= 100*time.Millisecond:
			snap.LatencyBuckets["le100ms"]++
		case sample.duration <= 250*time.Millisecond:
			snap.LatencyBuckets["le250ms"]++
		case sample.duration <= 500*time.Millisecond:
			snap.LatencyBuckets["le500ms"]++
		case sample.duration <= time.Second:
			snap.LatencyBuckets["le1s"]++
		case sample.duration <= 2*time.Second:
			snap.LatencyBuckets["le2s"]++
		case sample.duration <= 5*time.Second:
			snap.LatencyBuckets["le5s"]++
		default:
			snap.LatencyBuckets["gt5s"]++
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	index := int(math.Ceil(float64(len(durations))*0.95)) - 1
	if index < 0 {
		index = 0
	}
	snap.P95DurationMs = durations[index]
	return snap
}

func routeSLO(pattern string) (time.Duration, time.Duration) {
	switch {
	case pattern == "/healthz" || pattern == "/readyz":
		return 100 * time.Millisecond, time.Second
	case strings.Contains(pattern, "/search"):
		return 500 * time.Millisecond, 2 * time.Second
	case strings.Contains(pattern, "/live"):
		return 250 * time.Millisecond, 2 * time.Second
	case strings.Contains(pattern, "/atlas") || strings.Contains(pattern, "/stats"):
		return 750 * time.Millisecond, 5 * time.Second
	default:
		return 500 * time.Millisecond, 5 * time.Second
	}
}
