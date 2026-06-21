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

// AtlasRouter mounts the regional Atlas endpoints.
func AtlasRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/briefing", getAtlasBriefing(reader))
	r.Get("/regions/{slug}", getAtlasRegion(reader))
	r.Get("/replay", listAtlasReplay(reader))
	return r
}

func parseEpochMillisParam(r *http.Request, name string) (time.Time, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return time.Time{}, nil
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms), nil
}

// getAtlasBriefing godoc
//
//	@Summary	Get Atlas operator briefing
//	@Tags		Atlas
//	@Produce	json
//	@Param		region	query		string	false	"Region slug, defaults to all"
//	@Param		since	query		int		false	"Window start as epoch milliseconds"
//	@Param		until	query		int		false	"Window end as epoch milliseconds"
//	@Success	200		{object}	api.AtlasBriefing
//	@Failure	400		{object}	handlers.APIError
//	@Failure	404		{object}	handlers.APIError
//	@Router		/atlas/briefing [get]
func getAtlasBriefing(reader api.Reader) http.HandlerFunc {
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
		if !since.IsZero() && !until.IsZero() && since.After(until) {
			respondError(w, http.StatusBadRequest, "since must be before until")
			return
		}
		region := r.URL.Query().Get("region")
		if region == "" {
			region = "all"
		}
		briefing, err := reader.GetAtlasBriefing(r.Context(), region, since, until)
		if err != nil {
			respondError(w, http.StatusNotFound, "atlas briefing not found")
			return
		}
		respond(w, http.StatusOK, briefing)
	}
}

// getAtlasRegion godoc
//
//	@Summary	Get regional Atlas story summary
//	@Tags		Atlas
//	@Produce	json
//	@Param		slug	path		string	true	"Region slug, or all"
//	@Param		since	query		int		false	"Window start as epoch milliseconds"
//	@Param		until	query		int		false	"Window end as epoch milliseconds"
//	@Success	200		{object}	api.RegionAtlasSummary
//	@Failure	400		{object}	handlers.APIError
//	@Failure	404		{object}	handlers.APIError
//	@Router		/atlas/regions/{slug} [get]
func getAtlasRegion(reader api.Reader) http.HandlerFunc {
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
		if !since.IsZero() && !until.IsZero() && since.After(until) {
			respondError(w, http.StatusBadRequest, "since must be before until")
			return
		}
		summary, err := reader.GetRegionAtlasSummary(r.Context(), chi.URLParam(r, "slug"), since, until)
		if err != nil {
			respondError(w, http.StatusNotFound, "atlas region not found")
			return
		}
		respond(w, http.StatusOK, summary)
	}
}

// listAtlasReplay godoc
//
//	@Summary	List regional Atlas replay packets
//	@Tags		Atlas
//	@Produce	json
//	@Param		region	query		string	false	"Region slug, defaults to all"
//	@Param		since	query		int		false	"Window start as epoch milliseconds"
//	@Param		until	query		int		false	"Window end as epoch milliseconds"
//	@Param		cursor	query		int		false	"Observation ID cursor for pagination"
//	@Param		limit	query		int		false	"Max results, clamped to 1-200"
//	@Success	200		{object}	api.AtlasReplayPage
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/atlas/replay [get]
func listAtlasReplay(reader api.Reader) http.HandlerFunc {
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
		if !since.IsZero() && !until.IsZero() && since.After(until) {
			respondError(w, http.StatusBadRequest, "since must be before until")
			return
		}
		var cursor int64
		if raw := r.URL.Query().Get("cursor"); raw != "" {
			cursor, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
		}
		limit := int32(80)
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(n)
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 200 {
			limit = 200
		}
		region := r.URL.Query().Get("region")
		if region == "" {
			region = "all"
		}
		page, err := reader.ListAtlasReplay(r.Context(), region, since, until, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, page)
	}
}
