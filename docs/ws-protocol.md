# Beacon WebSocket Protocol

Beacon exposes one operator WebSocket endpoint:

```text
GET /ws
```

The protocol is JSON over text frames. Every versioned protocol message uses `v: 1`.

## Client Messages

### `subscribe`

Adds one subscription scope. The server replies with `subscribed` and a generated `subscriptionId`.

```json
{
  "v": 1,
  "type": "subscribe",
  "id": "client-message-id",
  "scope": {
    "iatas": ["YVR"],
    "regionIds": ["1"],
    "regionSlugs": ["pacific-northwest"],
    "payloadTypes": [4],
    "routeTypes": [1],
    "channelHashes": ["aabbccdd"],
    "observerIds": ["observer-1"],
    "events": ["packetObservation", "channelMessage", "observerStatus", "nodeUpdate"]
  }
}
```

Scope fields are optional. Empty or omitted scope dimensions mean no filter on that dimension. `regionIds` and `regionSlugs` expand to their member IATAs before the hub subscription is stored.

Current server-side fan-out filters are:

| Field | Applies To |
| --- | --- |
| `iatas` | All event types with IATA routing metadata |
| `payloadTypes` | Events with payload-type metadata |
| `routeTypes` | `packetObservation` events only |
| `channelHashes` | `channelMessage` events |
| `events` | All event types |

`observerIds` is accepted for forward compatibility but is not currently enforced by the server hub filter.

### `unsubscribe`

Removes one subscription scope by server-generated ID. The server replies with `unsubscribed`.

```json
{
  "v": 1,
  "type": "unsubscribe",
  "id": "client-message-id",
  "subscriptionId": "server-subscription-id"
}
```

### `ping`

Keeps the connection alive. The server replies with `pong` carrying the same `id`.

```json
{
  "v": 1,
  "type": "ping",
  "id": "client-message-id"
}
```

Clients should send `ping` about every 30 seconds. The server closes idle connections after about 90 seconds without receiving any message.

## Server Messages

### `hello`

Sent immediately after the WebSocket handshake succeeds.

```json
{
  "v": 1,
  "type": "hello",
  "serverTime": 1782043200000,
  "connectionId": "uuid"
}
```

### `subscribed`

Acknowledges a `subscribe` request.

```json
{
  "v": 1,
  "type": "subscribed",
  "id": "client-message-id",
  "subscriptionId": "server-subscription-id"
}
```

### `unsubscribed`

Acknowledges an `unsubscribe` request.

```json
{
  "v": 1,
  "type": "unsubscribed",
  "id": "client-message-id",
  "subscriptionId": "server-subscription-id"
}
```

### `pong`

Acknowledges a `ping` request.

```json
{
  "v": 1,
  "type": "pong",
  "id": "client-message-id"
}
```

### `event`

Carries one live Beacon event.

```json
{
  "v": 1,
  "type": "event",
  "event": "packetObservation",
  "data": {}
}
```

Current event discriminators are:

| Event | Purpose |
| --- | --- |
| `packetObservation` | Live packet observation with packet metadata, observer metadata, RF data, and optional resolved path. |
| `channelMessage` | Decoded channel/message event. |
| `observerStatus` | Observer status or telemetry update. |
| `nodeUpdate` | Node advert/update summary. |

The `data` shape is event-specific and should stay aligned with the frontend `WsServerMessage` union and REST backfill payloads where a recovery path exists.

### `lagged`

Reports that the server dropped queued frames for this client because its connection could not keep up.

```json
{
  "v": 1,
  "type": "lagged",
  "droppedCount": 47,
  "since": 1782043200000
}
```

Clients should treat `lagged` as a data-gap notice and heal from REST backfill endpoints when possible. Packet workflows should use `/api/v1/live/backfill` with the last seen observation cursor.

### `error`

Reserved protocol error shape.

```json
{
  "v": 1,
  "type": "error",
  "code": "bad_request",
  "message": "human-readable error"
}
```

## Compatibility Rules

- `/ws` stays stable for version `v: 1`.
- Additive fields are allowed.
- Existing message `type` values and event discriminator strings require a compatibility path before changing.
- Breaking event `data` shape changes should be paired with frontend type updates, tests, and a documented migration path.
