// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/ratelimit"
)

func TestHandlerAcceptsSameOriginByDefault(t *testing.T) {
	server := httptest.NewServer(Handler(hub.New(), nil, Options{}))
	defer server.Close()

	conn := dialWS(t, server.URL, server.URL)
	defer conn.CloseNow()
	assertHello(t, conn)
}

func TestHandlerAllowsConfiguredCrossOrigin(t *testing.T) {
	server := httptest.NewServer(Handler(hub.New(), nil, Options{
		AllowedOrigins: []string{"https://beacon.canadaverse.org"},
	}))
	defer server.Close()

	conn := dialWS(t, server.URL, "https://beacon.canadaverse.org")
	defer conn.CloseNow()
	assertHello(t, conn)
}

func TestHandlerRejectsUnlistedCrossOrigin(t *testing.T) {
	server := httptest.NewServer(Handler(hub.New(), nil, Options{
		AllowedOrigins: []string{"https://beacon.canadaverse.org"},
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, res, err := websocket.Dial(ctx, wsURL(server.URL), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
	})
	if err == nil {
		conn.CloseNow()
		t.Fatal("expected cross-origin websocket dial to be rejected")
	}
	if res == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("rejected response status = %v, want 403", statusCode(res))
	}
}

func TestHandlerRateLimitsSubscriptionMessages(t *testing.T) {
	server := httptest.NewServer(Handler(hub.New(), nil, Options{
		MessageLimiter: ratelimit.New(ratelimit.Config{RequestsPerMinute: 1, Burst: 1}),
	}))
	defer server.Close()

	conn := dialWS(t, server.URL, server.URL)
	defer conn.CloseNow()
	assertHello(t, conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	first := []byte(`{"v":1,"type":"subscribe","id":"sub-1","scope":{"iatas":["YOW"]}}`)
	if err := conn.Write(ctx, websocket.MessageText, first); err != nil {
		t.Fatalf("write first subscribe: %v", err)
	}
	if got := readMessageType(t, conn); got != "subscribed" {
		t.Fatalf("first subscribe response = %q, want subscribed", got)
	}

	second := []byte(`{"v":1,"type":"subscribe","id":"sub-2","scope":{"iatas":["YOW"]}}`)
	if err := conn.Write(ctx, websocket.MessageText, second); err != nil {
		t.Fatalf("write second subscribe: %v", err)
	}
	if got := readMessageType(t, conn); got != "error" {
		t.Fatalf("second subscribe response = %q, want error", got)
	}
}

func dialWS(t *testing.T, serverURL, origin string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, res, err := websocket.Dial(ctx, wsURL(serverURL), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{origin}},
	})
	if err != nil {
		t.Fatalf("dial websocket: %v (status=%v)", err, statusCode(res))
	}
	return conn
}

func assertHello(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if !strings.Contains(string(data), `"type":"hello"`) {
		t.Fatalf("first websocket message = %s, want hello", data)
	}
}

func readMessageType(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	var msg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode websocket message %s: %v", data, err)
	}
	return msg.Type
}

func wsURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

func statusCode(res *http.Response) any {
	if res == nil {
		return nil
	}
	return res.StatusCode
}
