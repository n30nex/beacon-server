// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type connectionStateClient struct {
	mqtt.Client
	open bool
}

func (c connectionStateClient) IsConnectionOpen() bool { return c.open }

func TestWorkerReadinessRequiresOpenSubscribedSession(t *testing.T) {
	worker := &Worker{client: connectionStateClient{open: true}}
	if worker.IsConnected() {
		t.Fatal("open transport without SUBACK reported ready")
	}

	worker.subscribed.Store(true)
	if !worker.IsConnected() {
		t.Fatal("open subscribed session did not report ready")
	}

	worker.client = connectionStateClient{open: false}
	if worker.IsConnected() {
		t.Fatal("retrying/closed transport reported ready")
	}
}

func TestResolveClientID(t *testing.T) {
	if got := resolveClientID(Config{BrokerName: "mqtt1"}); got != "beacon-mqtt1" {
		t.Fatalf("legacy client ID = %q", got)
	}
	if got := resolveClientID(Config{BrokerName: "mqtt1", ClientID: " beacon-ca-prod-mqtt1 "}); got != "beacon-ca-prod-mqtt1" {
		t.Fatalf("configured client ID = %q", got)
	}
}
