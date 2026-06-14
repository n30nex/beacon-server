// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package hub provides the central fan-out broker between the MQTT ingest
// goroutines and connected WebSocket clients.
//
// Design:
//   - A single Hub.Run() goroutine owns the client map; no mutexes needed.
//   - Ingest goroutines call hub.Broadcast() from any goroutine.
//   - Each WebSocket connection gets a *Client with a buffered send channel.
//   - If a client's send buffer is full it receives a lagged notification and
//     its buffer is drained so it doesn't stall the broadcast loop.
//
// Subscription filtering (by IATA, payload type, channel hash, etc.) is
// enforced here before events are placed on a client's send channel.
package hub

import (
	"encoding/json"
	"log"
)

// EventType identifies the kind of server-push event. These match the
// discriminator values in the WebSocket protocol ("packetObservation", etc.).
type EventType string

const (
	EventPacketObservation EventType = "packetObservation"
	EventObserverStatus    EventType = "observerStatus"
	EventNodeUpdate        EventType = "nodeUpdate"
	EventChannelMessage    EventType = "channelMessage"
)

// Event is a single fan-out unit. Payload is pre-serialised JSON so the
// broadcast loop never touches encoding — it's done once by the ingest path.
type Event struct {
	Type    EventType
	Payload json.RawMessage

	// Framed is the complete WebSocket frame ({"v":1,"type":"event",...})
	// built once by the hub at broadcast time and shared (read-only) across
	// every matching client. The WS write pump writes it verbatim, so the
	// envelope is serialised once per event rather than once per client.
	Framed json.RawMessage

	// Routing metadata used by the hub to match subscriptions.
	// Populated by the ingest layer before calling Broadcast.
	IATA        string
	PayloadType uint8
	ChannelHash string // hex string, non-empty only for channelMessage events
}

// frameEvent builds the wire frame for a server→client event exactly once.
// The envelope is {"v":1,"type":"event","event":"<type>","data":<payload>}.
// EventType values are fixed ASCII identifiers, so no JSON escaping is needed.
func frameEvent(eventType EventType, payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	const prefix = `{"v":1,"type":"event","event":"`
	const mid = `","data":`
	buf := make([]byte, 0, len(prefix)+len(eventType)+len(mid)+len(payload)+1)
	buf = append(buf, prefix...)
	buf = append(buf, eventType...)
	buf = append(buf, mid...)
	buf = append(buf, payload...)
	buf = append(buf, '}')
	return buf
}

// Scope mirrors the client-side subscribe message. All fields are optional:
// nil/empty means "no filter on this dimension" (match everything).
// An empty non-nil slice means "match nothing on this dimension".
type Scope struct {
	IATAs         []string
	PayloadTypes  []uint8
	ChannelHashes []string
	Events        []EventType
}

type LaggedNotification struct {
	DroppedCount int
}

// Client represents a connected WebSocket consumer.
type Client struct {
	Send          chan Event
	laggedCH      chan LaggedNotification
	subscriptions map[string]compiledScope // OR semantics: event matches if it matches any scope entry
}

// matches returns true if the event satisfies at least one of the client's
// active subscriptions.
func (c *Client) matches(e Event) bool {
	for _, s := range c.subscriptions {
		if s.matches(e) {
			return true
		}
	}
	return false
}

// LaggedCH returns the channel on which lagged notifications are delivered.
// The WS write pump selects on this alongside Send.
func (c *Client) LaggedCH() <-chan LaggedNotification {
	return c.laggedCH
}

// compiledScope is the matching-optimised form of a Scope. Each dimension is a
// set (nil = "no filter, match everything") so matching an event is O(1) per
// dimension instead of a linear scan — important for clients that subscribe to
// whole regions, which can expand to hundreds of IATAs. Compiled once per
// subscription, then reused for every broadcast.
type compiledScope struct {
	events        map[EventType]struct{}
	iatas         map[string]struct{}
	payloadTypes  map[uint8]struct{}
	channelHashes map[string]struct{}
}

func toSet[T comparable](xs []T) map[T]struct{} {
	if len(xs) == 0 {
		return nil
	}
	m := make(map[T]struct{}, len(xs))
	for _, x := range xs {
		m[x] = struct{}{}
	}
	return m
}

func compileScope(s Scope) compiledScope {
	return compiledScope{
		events:        toSet(s.Events),
		iatas:         toSet(s.IATAs),
		payloadTypes:  toSet(s.PayloadTypes),
		channelHashes: toSet(s.ChannelHashes),
	}
}

func (cs compiledScope) matches(e Event) bool {
	if cs.events != nil {
		if _, ok := cs.events[e.Type]; !ok {
			return false
		}
	}
	if cs.iatas != nil {
		if _, ok := cs.iatas[e.IATA]; !ok {
			return false
		}
	}
	if cs.payloadTypes != nil {
		if _, ok := cs.payloadTypes[e.PayloadType]; !ok {
			return false
		}
	}
	if cs.channelHashes != nil {
		if _, ok := cs.channelHashes[e.ChannelHash]; !ok {
			return false
		}
	}
	return true
}

// scopeMatches reports whether an event satisfies a (non-compiled) scope.
// Retained as the reference implementation used by tests; production code uses
// the compiled form cached on each client.
func scopeMatches(s Scope, e Event) bool {
	return compileScope(s).matches(e)
}

// Hub is the central event broker.
type Hub struct {
	subscribe   chan subscribeMsg
	unsubscribe chan unsubscribeMsg
	remove      chan *Client
	broadcast   chan Event
}

type subscribeMsg struct {
	client         *Client
	scope          Scope
	subscriptionID string
}

type unsubscribeMsg struct {
	client         *Client
	subscriptionID string
}

// New creates a Hub. Call Run() in a goroutine before using it.
func New() *Hub {
	return &Hub{
		subscribe:   make(chan subscribeMsg, 64),
		unsubscribe: make(chan unsubscribeMsg, 64),
		remove:      make(chan *Client, 64),
		broadcast:   make(chan Event, 512),
	}
}

// NewClient creates a Client and registers it with the hub.
// The caller is responsible for calling Remove when the connection closes.
func (h *Hub) NewClient() *Client {
	c := &Client{
		Send:          make(chan Event, 256),
		laggedCH:      make(chan LaggedNotification, 8),
		subscriptions: make(map[string]compiledScope),
	}
	// We don't add it to the map here; we send it through the channel so
	// Run() is the only goroutine that touches the client map.
	h.subscribe <- subscribeMsg{client: c}
	return c
}

// AddScope appends a subscription scope to a client. Called by the WS handler
// when it receives a "subscribe" message from the client.
func (h *Hub) AddScope(c *Client, id string, s Scope) {
	h.subscribe <- subscribeMsg{client: c, scope: s, subscriptionID: id}
}

// RemoveScope removes a single subscription by ID. Called by the WS handler
// when it receives an "unsubscribe" message from the client. Silently ignored
// if the ID is not found.
func (h *Hub) RemoveScope(c *Client, id string) {
	h.unsubscribe <- unsubscribeMsg{client: c, subscriptionID: id}
}

// Remove deregisters a client and closes its Send channel.
// Safe to call from any goroutine (e.g. the WS handler's defer).
func (h *Hub) Remove(c *Client) {
	h.remove <- c
}

// Broadcast enqueues an event for fan-out. Safe to call from any goroutine.
func (h *Hub) Broadcast(e Event) {
	select {
	case h.broadcast <- e:
	default:
		log.Println("hub: broadcast channel full, dropping event")
	}
}

// Run is the hub's single-goroutine event loop. Call it in a dedicated
// goroutine: go hub.Run().
//
// It processes registrations, removals, and broadcasts sequentially so the
// clients map needs no locking.
func (h *Hub) Run() {
	// clients maps a *Client to the set of subscription IDs it holds.
	// We use a map[*Client]struct{} for O(1) presence checks and O(n)
	// broadcast — fine at the scale Beacon targets.
	clients := make(map[*Client]struct{})

	for {
		select {

		case msg := <-h.subscribe:
			if msg.subscriptionID == "" {
				// Registration with no scope yet (NewClient path).
				clients[msg.client] = struct{}{}
			} else {
				// AddScope path — client must already be registered.
				if _, ok := clients[msg.client]; ok {
					msg.client.subscriptions[msg.subscriptionID] = compileScope(msg.scope)
				}
			}

		case msg := <-h.unsubscribe:
			if _, ok := clients[msg.client]; ok {
				delete(msg.client.subscriptions, msg.subscriptionID)
			}

		case c := <-h.remove:
			if _, ok := clients[c]; ok {
				delete(clients, c)
				close(c.Send)
				close(c.laggedCH)
			}

		case evt := <-h.broadcast:
			// Serialise the wire frame at most once per event (lazily, only if
			// a client actually matches) and share it across every recipient.
			for c := range clients {
				if !c.matches(evt) {
					continue
				}
				if evt.Framed == nil {
					evt.Framed = frameEvent(evt.Type, evt.Payload)
				}
				select {
				case c.Send <- evt:
				default:
					dropped := 1
					select {
					case <-c.Send:
					default:
					}
					select {
					case c.laggedCH <- LaggedNotification{DroppedCount: dropped}:
					default:
						// laggedCh itself full; write pump will catch up on next drain
					}
					log.Printf("hub: client send buffer full, dropped event type=%s", evt.Type)
				}
			}
		}
	}
}
