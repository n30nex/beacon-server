// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// LiveRouter mounts Live console endpoints.
func LiveRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/backfill", listLiveBackfill(reader))
	r.Get("/summary", getLiveSummary(reader))
	return r
}

func liveIATAs(r *http.Request, reader api.Reader) ([]string, error) {
	iatas := parseIATAs(r)
	if r.URL.Query().Get("regionId") != "" || r.URL.Query().Get("region") != "" {
		regionIATAs, err := resolveRegionIATAs(r.Context(), r.URL.Query().Get("regionId"), r.URL.Query().Get("region"), reader)
		if err != nil {
			return nil, err
		}
		iatas = append(iatas, regionIATAs...)
	}
	return iatas, nil
}

func parseOptionalSmallInt(r *http.Request, name string) (int16, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return -1, nil
	}
	value, err := strconv.ParseInt(raw, 10, 16)
	if err != nil {
		return -1, err
	}
	return int16(value), nil
}

func parseLiveLimit(r *http.Request, fallback, max int32) (int32, error) {
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

// listLiveBackfill godoc
//
//	@Summary	Backfill Live packet observations after an observation ID
//	@Tags		Live
//	@Produce	json
//	@Param		afterObservationId	query		int		true	"Return observations after this ID"
//	@Param		iatas				query		string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region				query		string	false	"Filter by region slug"
//	@Param		regionId			query		int		false	"Filter by region ID"
//	@Param		payloadType			query		int		false	"Filter by payload type"
//	@Param		routeType			query		int		false	"Filter by route type"
//	@Param		scope				query		string	false	"Filter by transport scope name"
//	@Param		limit				query		int		false	"Max results, clamped to 1-250"
//	@Success	200					{object}	api.LiveBackfillPage
//	@Failure	400					{object}	handlers.APIError
//	@Failure	500					{object}	handlers.APIError
//	@Router		/live/backfill [get]
func listLiveBackfill(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawAfter := r.URL.Query().Get("afterObservationId")
		if rawAfter == "" {
			respondError(w, http.StatusBadRequest, "afterObservationId is required")
			return
		}
		afterID, err := strconv.ParseInt(rawAfter, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "afterObservationId must be an integer")
			return
		}
		payloadType, err := parseOptionalSmallInt(r, "payloadType")
		if err != nil {
			respondError(w, http.StatusBadRequest, "payloadType must be an integer")
			return
		}
		routeType, err := parseOptionalSmallInt(r, "routeType")
		if err != nil {
			respondError(w, http.StatusBadRequest, "routeType must be an integer")
			return
		}
		limit, err := parseLiveLimit(r, 100, 250)
		if err != nil {
			respondError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		iatas, err := liveIATAs(r, reader)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := reader.ListLiveBackfill(r.Context(), api.LiveBackfillFilter{
			AfterObservationID: afterID,
			PayloadType:        payloadType,
			RouteType:          routeType,
			IATAs:              iatas,
			Scope:              r.URL.Query().Get("scope"),
			Limit:              limit,
		})
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, page)
	}
}

// getLiveSummary godoc
//
//	@Summary	Get Live console summary
//	@Tags		Live
//	@Produce	json
//	@Param		iatas	query	string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region	query	string	false	"Filter by region slug"
//	@Param		regionId	query	int	false	"Filter by region ID"
//	@Param		since	query	int	false	"Window start as epoch milliseconds"
//	@Param		until	query	int	false	"Window end as epoch milliseconds"
//	@Success	200		{object}	api.LiveSummary
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/live/summary [get]
func getLiveSummary(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		since, err := parseEpochMillisParam(r, "since")
		if err != nil {
			respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
			return
		}
		until, err := parseEpochMillisParam(r, "until")
		if err != nil {
			respondError(w, http.StatusBadRequest, "until must be epoch milliseconds")
			return
		}
		if until.IsZero() {
			until = time.Now()
		}
		if since.IsZero() {
			since = until.Add(-15 * time.Minute)
		}
		if since.After(until) {
			respondError(w, http.StatusBadRequest, "since must be before until")
			return
		}
		iatas, err := liveIATAs(r, reader)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		summary, err := reader.GetLiveSummary(r.Context(), api.LiveSummaryFilter{IATAs: iatas, Since: since, Until: until})
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, summary)
	}
}
