// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// StatsRouter mounts all /stats routes onto a subrouter.
//
// GET  /stats/overview          → getStatsOverview
// GET  /stats/observations      → getStatsObservations
// GET  /stats/payload-breakdown → getStatsPayloadBreakdown
// GET  /stats/top-nodes         → getStatsTopNodes
// GET  /stats/top-observers     → getStatsTopObservers
// GET  /stats/radio-presets     → getStatsRadioPresets
// GET  /stats/scopes            → GetStatsScopes
//
// All endpoints accept an optional iata= filter (case-insensitive).
func StatsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/summary", getStatsSummary(reader))
	r.Get("/regions", getStatsRegions(reader))
	r.Get("/payloads", getStatsPayloads(reader))
	r.Get("/hash", getStatsHashAnalytics(reader))
	r.Get("/topology", getStatsTopology(reader))
	r.Get("/channels", getStatsChannels(reader))
	r.Get("/rf-health", getStatsRFHealth(reader))
	r.Get("/observer-health", getStatsObserverHealth(reader))
	r.Get("/observer-compare", getStatsObserverCompare(reader))
	r.Get("/overview", getStatsOverview(reader))
	r.Get("/observations", getStatsObservations(reader))
	r.Get("/payload-breakdown", getStatsPayloadBreakdown(reader))
	r.Get("/top-nodes", getStatsTopNodes(reader))
	r.Get("/top-observers", getStatsTopObservers(reader))
	r.Get("/radio-presets", getStatsRadioPresets(reader))
	r.Get("/scopes", getStatsScopes(reader))
	r.Get("/node-types", getStatsNodeTypes(reader))
	return r
}

func statsIATAs(r *http.Request, reader api.Reader) ([]string, error) {
	iatas := parseIATAs(r)
	if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
		regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
		if err != nil {
			return nil, err
		}
		iatas = append(iatas, regionIATAs...)
	}
	return iatas, nil
}

type errStatsSinceAfterUntil struct{}

func (errStatsSinceAfterUntil) Error() string { return "since must be before until" }

type errStatsInvalidBucket struct{}

func (errStatsInvalidBucket) Error() string { return "bucket must be one of 1h, 6h, 24h" }

func parseStatsWindow(r *http.Request) (time.Time, time.Time, string, error) {
	until, err := parseEpochMillisParam(r, "until")
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	since, err := parseEpochMillisParam(r, "since")
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	rangeName := r.URL.Query().Get("range")
	if until.IsZero() {
		until = time.Now()
	}
	if since.IsZero() {
		switch rangeName {
		case "7d":
			since = until.Add(-7 * 24 * time.Hour)
		case "30d":
			since = until.Add(-30 * 24 * time.Hour)
		default:
			since = until.Add(-24 * time.Hour)
		}
	}
	if since.After(until) {
		return time.Time{}, time.Time{}, "", errStatsSinceAfterUntil{}
	}
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		switch rangeName {
		case "7d":
			bucket = "6h"
		case "30d":
			bucket = "24h"
		default:
			if until.Sub(since) > 30*time.Hour {
				bucket = "6h"
			} else {
				bucket = "1h"
			}
		}
	}
	switch bucket {
	case "1h", "6h", "24h":
		return since, until, bucket, nil
	default:
		return time.Time{}, time.Time{}, "", errStatsInvalidBucket{}
	}
}

func parseStatsLimit(r *http.Request, fallback, max int32) (int32, error) {
	limit := fallback
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, err
		}
		limit = int32(value)
	}
	if limit < 1 {
		limit = 1
	}
	if limit > max {
		limit = max
	}
	return limit, nil
}

func parseStatsFilter(r *http.Request, reader api.Reader, fallbackLimit int32) (api.StatsFilter, error) {
	since, until, bucket, err := parseStatsWindow(r)
	if err != nil {
		return api.StatsFilter{}, err
	}
	limit, err := parseStatsLimit(r, fallbackLimit, 500)
	if err != nil {
		return api.StatsFilter{}, err
	}
	iatas, err := statsIATAs(r, reader)
	if err != nil {
		return api.StatsFilter{}, err
	}
	return api.StatsFilter{IATAs: iatas, Since: since, Until: until, Bucket: bucket, Limit: limit}, nil
}

func parseStatsObserverHealthFilter(r *http.Request, reader api.Reader, fallbackLimit int32) (api.StatsObserverHealthFilter, error) {
	filter, err := parseStatsFilter(r, reader, fallbackLimit)
	if err != nil {
		return api.StatsObserverHealthFilter{}, err
	}
	staleAfter := 30 * time.Minute
	if raw := r.URL.Query().Get("staleAfterMinutes"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return api.StatsObserverHealthFilter{}, err
		}
		if value < 1 {
			value = 1
		}
		if value > 7*24*60 {
			value = 7 * 24 * 60
		}
		staleAfter = time.Duration(value) * time.Minute
	}
	return api.StatsObserverHealthFilter{StatsFilter: filter, StaleAfter: staleAfter}, nil
}

type errStatsObserverCompareIDs struct{}

func (errStatsObserverCompareIDs) Error() string {
	return "observerIds must include 2 to 6 comma-separated UUIDs"
}

func parseStatsObserverCompareFilter(r *http.Request, reader api.Reader) (api.StatsObserverCompareFilter, error) {
	healthFilter, err := parseStatsObserverHealthFilter(r, reader, 6)
	if err != nil {
		return api.StatsObserverCompareFilter{}, err
	}
	raw := strings.TrimSpace(r.URL.Query().Get("observerIds"))
	if raw == "" {
		return api.StatsObserverCompareFilter{}, errStatsObserverCompareIDs{}
	}
	seen := map[uuid.UUID]struct{}{}
	ids := make([]uuid.UUID, 0, 6)
	for _, part := range strings.Split(raw, ",") {
		id, err := uuid.Parse(strings.TrimSpace(part))
		if err != nil {
			return api.StatsObserverCompareFilter{}, errStatsObserverCompareIDs{}
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) < 2 || len(ids) > 6 {
		return api.StatsObserverCompareFilter{}, errStatsObserverCompareIDs{}
	}
	return api.StatsObserverCompareFilter{StatsObserverHealthFilter: healthFilter, ObserverIDs: ids}, nil
}

func respondStatsParamError(w http.ResponseWriter, err error) {
	switch err.(type) {
	case errStatsSinceAfterUntil, errStatsInvalidBucket, errStatsObserverCompareIDs:
		respondError(w, http.StatusBadRequest, err.Error())
	default:
		respondError(w, http.StatusBadRequest, "invalid stats query parameter")
	}
}

// getStatsSummary godoc
//
//	@Summary	Stats operator console summary
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Success	200	{object}	api.StatsSummary
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/summary [get]
func getStatsSummary(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		summary, err := reader.GetStatsSummary(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsSummary failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, summary)
	}
}

// getStatsRegions godoc
//
//	@Summary	Stats per-IATA regional comparison
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Success	200	{object}	api.StatsRegions
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/regions [get]
func getStatsRegions(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		regions, err := reader.GetStatsRegions(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsRegions failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, regions)
	}
}

// getStatsPayloads godoc
//
//	@Summary	Stats payload and route timelines
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Success	200	{object}	api.StatsPayloads
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/payloads [get]
func getStatsPayloads(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		payloads, err := reader.GetStatsPayloads(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsPayloads failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, payloads)
	}
}

// getStatsHashAnalytics godoc
//
//	@Summary	Stats path-hash analytics and collision risk
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Param		limit	query	int	false	"Max risky prefixes and inconsistent samples, clamped to 1-500"
//	@Success	200	{object}	api.StatsHashAnalytics
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/hash [get]
func getStatsHashAnalytics(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		hashes, err := reader.GetStatsHashAnalytics(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsHashAnalytics failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, hashes)
	}
}

// getStatsTopology godoc
//
//	@Summary	Stats verified-route topology analytics
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Param		limit	query	int	false	"Max rows per list, clamped to 1-500"
//	@Success	200	{object}	api.StatsTopology
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/topology [get]
func getStatsTopology(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		topology, err := reader.GetStatsTopology(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsTopology failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, topology)
	}
}

// getStatsChannels godoc
//
//	@Summary	Stats channel analytics
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Param		limit	query	int	false	"Max rows per list, clamped to 1-500"
//	@Success	200	{object}	api.StatsChannels
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/channels [get]
func getStatsChannels(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsFilter(r, reader, 25)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		channels, err := reader.GetStatsChannels(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsChannels failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, channels)
	}
}

// getStatsRFHealth godoc
//
//	@Summary	Stats RF health aggregates
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Param		staleAfterMinutes	query	int	false	"Observer stale threshold in minutes"
//	@Success	200	{object}	api.StatsRFHealth
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/rf-health [get]
func getStatsRFHealth(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsObserverHealthFilter(r, reader, 50)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		health, err := reader.GetStatsRFHealth(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsRFHealth failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, health)
	}
}

// getStatsObserverHealth godoc
//
//	@Summary	Stats observer health rows
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		limit	query	int	false	"Max rows, clamped to 1-500"
//	@Param		staleAfterMinutes	query	int	false	"Observer stale threshold in minutes"
//	@Success	200	{object}	api.StatsObserverHealthResponse
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/observer-health [get]
func getStatsObserverHealth(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsObserverHealthFilter(r, reader, 50)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		health, err := reader.GetStatsObserverHealth(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsObserverHealth failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, health)
	}
}

// getStatsObserverCompare godoc
//
//	@Summary	Stats observer compare
//	@Tags		Stats
//	@Produce	json
//	@Param		observerIds	query	string	true	"Comma-separated observer UUIDs, 2 to 6"
//	@Param		iatas	query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int	false	"Filter by region ID, expands to member IATAs"
//	@Param		region	query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		range	query	string	false	"Window preset: 24h, 7d, or 30d"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Param		bucket	query	string	false	"Bucket size: 1h, 6h, or 24h"
//	@Param		staleAfterMinutes	query	int	false	"Observer stale threshold in minutes"
//	@Success	200	{object}	api.StatsObserverCompare
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/observer-compare [get]
func getStatsObserverCompare(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseStatsObserverCompareFilter(r, reader)
		if err != nil {
			respondStatsParamError(w, err)
			return
		}
		compare, err := reader.GetStatsObserverCompare(r.Context(), filter)
		if err != nil {
			log.Printf("api: GetStatsObserverCompare failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, compare)
	}
}

// getStatsOverview godoc
//
//	@Summary	Network overview stats (last 24h)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200		{object}	api.StatsOverview
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/overview [get]
func getStatsOverview(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		overview, err := reader.GetStatsOverview(r.Context(), iatas)
		if err != nil {
			log.Printf("api: GetStatsOverview failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, overview)
	}
}

// getStatsObservations godoc
//
//	@Summary	Hourly observation time series
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default 7 days ago)"
//	@Success	200		{array}		api.ObservationPoint
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/observations [get]
func getStatsObservations(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		points, err := reader.GetStatsObservations(r.Context(), iatas, since)
		if err != nil {
			log.Printf("api: GetStatsObservations failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, points)
	}
}

// getStatsPayloadBreakdown godoc
//
//	@Summary	Observation counts by payload type (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Success	200		{array}		api.PayloadBreakdownItem
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/payload-breakdown [get]
func getStatsPayloadBreakdown(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		breakdown, err := reader.GetStatsPayloadBreakdown(r.Context(), iatas, since)
		if err != nil {
			log.Printf("api: GetStatsPayloadBreakdown failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, breakdown)
	}
}

// getStatsTopNodes godoc
//
//	@Summary	Top N nodes by observation count (from materialized view)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		limit	query		int		false	"Max results (default 10)"
//	@Success	200		{array}		api.TopNode
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-nodes [get]
func getStatsTopNodes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var limit int32 = 10
		if p := r.URL.Query().Get("limit"); p != "" {
			l, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		nodes, err := reader.GetStatsTopNodes(r.Context(), iatas, limit)
		if err != nil {
			log.Printf("api: GetStatsTopNodes failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, nodes)
	}
}

// getStatsTopObservers godoc
//
//	@Summary	Top N observers by observation count (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Param		limit	query		int		false	"Max results (default 10)"
//	@Success	200		{array}		api.TopObserver
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-observers [get]
func getStatsTopObservers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		var limit int32 = 10
		if p := r.URL.Query().Get("limit"); p != "" {
			l, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		observers, err := reader.GetStatsTopObservers(r.Context(), iatas, since, limit)
		if err != nil {
			log.Printf("api: GetStatsTopObservers failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observers)
	}
}

// getStatsRadioPresets godoc
//
//	@Summary	Radio preset usage by IATA
//	@Tags		Stats
//	@Produce	json
//	@Param		preset	query		string	false	"Filter by preset string e.g. 910.525,62.5,7"
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200		{object}	[]api.RadioPreset
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/radio-presets [get]
func getStatsRadioPresets(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		preset := r.URL.Query().Get("preset")
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		presets, err := reader.GetRadioPresets(r.Context(), preset, iatas)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, presets)
	}
}

// getStatsScopes godoc
//
//	@Summary	Scope statistics
//	@Tags		Stats
//	@Produce	json
//	@Success	200	{object}	[]api.ScopeStats
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/scopes [get]
func getStatsScopes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetScopeStats(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, stats)
	}
}

// getStatsNodeTypes godoc
//
//	@Summary	Node type breakdown
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query		string	false	"Comma-separated IATA codes"
//	@Param		regionId	query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query		string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200			{array}		api.NodeTypeCount
//	@Failure	500			{object}	handlers.APIError
//	@Router		/stats/node-types [get]
func getStatsNodeTypes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		result, err := reader.GetStatsNodeTypes(r.Context(), iatas)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to get node type stats")
			return
		}
		respond(w, http.StatusOK, result)
	}
}
