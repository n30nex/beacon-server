// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"math"

	"github.com/meshcore-go/meshcore-go"
)

const advertCoordinateScale = 1e6

func validGeoCoordinate(lat, lng float64) bool {
	return !math.IsNaN(lat) &&
		!math.IsInf(lat, 0) &&
		!math.IsNaN(lng) &&
		!math.IsInf(lng, 0) &&
		lat >= -90 &&
		lat <= 90 &&
		lng >= -180 &&
		lng <= 180
}

func advertLocationCoordinates(appData meshcore.AdvertAppData, flags byte) (*float64, *float64) {
	if flags&meshcore.AdvertLatLonMask == 0 {
		return nil, nil
	}

	lat := float64(appData.Lat) / advertCoordinateScale
	lng := float64(appData.Lon) / advertCoordinateScale
	if !validGeoCoordinate(lat, lng) {
		return nil, nil
	}
	return &lat, &lng
}
