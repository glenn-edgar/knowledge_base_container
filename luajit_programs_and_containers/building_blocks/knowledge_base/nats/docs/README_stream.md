# StreamBuffer — Persistent Circular-Buffer Stream Subscription

Pure-Lua module built on top of PubSub + KeyStore. Subscribes to a NATS subject and stores incoming messages in a fixed-size circular buffer backed by JetStream KV. When the buffer is full, the oldest entry is overwritten. Data survives process restarts.

## Usage

```lua
local nats = require("lib.nats")

-- KeyStore provides persistence (JetStream KV)
local ks = nats.KeyStore.new({ server = "nats://127.0.0.1:4222", bucket = "streams" })
ks:connect()

-- PubSub provides the subscription
local ps = nats.PubSub.new({ server = "nats://127.0.0.1:4222" })
ps:connect()

-- Subscribe to "sensor.temp" with a 10-entry persistent circular buffer
local buf = nats.StreamBuffer.new(ps, ks, "sensor.temp", 10)

-- Messages arrive asynchronously and persist to KV...

local msg  = buf:latest()          -- most recent entry or nil
local all  = buf:entries()         -- all entries, oldest → newest
local n    = buf:count()           -- entries currently stored
local cap  = buf:capacity()        -- configured buffer size (10)
local tot  = buf:total_received()  -- lifetime message counter
local full = buf:is_full()         -- true when count == capacity

buf:clear()                        -- reset buffer + KV, keep subscription
buf:stop()                         -- unsubscribe (KV data preserved for reload)
buf:purge()                        -- delete all KV data
```

### Reloading after restart

```lua
-- On restart, same bucket + subject reloads previous state
local buf = nats.StreamBuffer.new(ps, ks, "sensor.temp", 10)
print(buf:count())     -- entries from previous session
print(buf:latest())    -- last message before shutdown
```

## API

| Method | Returns | Description |
|--------|---------|-------------|
| `StreamBuffer.new(pubsub, keystore, subject, size [, queue_group])` | StreamBuffer | Load persisted state, then subscribe |
| `buf:latest()` | msg table or nil | Most recent message |
| `buf:entries()` | array of msg tables | All entries oldest → newest |
| `buf:count()` | number | Current entries in buffer |
| `buf:capacity()` | number | Configured buffer size |
| `buf:total_received()` | number | Total messages received (including overwritten) |
| `buf:is_full()` | boolean | True when buffer is at capacity |
| `buf:clear()` | — | Empty buffer and KV; subscription stays active |
| `buf:stop()` | — | Unsubscribe; KV data preserved for reload |
| `buf:purge()` | — | Delete all KV data; buffer emptied |

Each message table has the same fields as a PubSub callback message:

| Field | Type | Description |
|-------|------|-------------|
| `subject` | string | Namespace-prefixed subject |
| `original_subject` | string | Raw NATS subject |
| `data` | string | Message payload |
| `data_len` | number | Payload byte length |
| `reply_to` | string or nil | Reply subject if present |

## Prerequisites

- **LuaJIT** on PATH
- **libnats_pubsub.so** and **libnats_key_store.so** on `LD_LIBRARY_PATH`
- **NATS server with JetStream** running:
  ```bash
  docker run -d -p 4222:4222 nats:latest -js
  ```

## Tests

9 integration tests in `test/test_stream.lua`:

| Test | What it verifies |
|------|-----------------|
| create and stop | Lifecycle, initial state (count=0, latest=nil) |
| fills buffer in order | Partial fill, `entries()` ordering, `latest()` |
| wraps around when full | Overflow evicts oldest, `total_received()` tracks all |
| clear resets buffer and KV | `clear()` empties buffer + KV, new messages still arrive |
| stop prevents new messages | `stop()` unsubscribes, existing data preserved |
| size of 1 | Edge case: single-slot buffer keeps only latest |
| persistence survives restart | Stop + new instance reloads entries from KV |
| persistence with wrap-around | Reload after overflow preserves correct oldest→newest order |
| purge deletes KV data | `purge()` wipes KV; reload starts empty |

```bash
# Start NATS with JetStream
docker run -d -p 4222:4222 nats:latest -js

# Run tests
export LD_LIBRARY_PATH=/path/to/libs
luajit test/test_stream.lua
```

## Design Notes

- Pure Lua — no C code, wraps `PubSub:subscribe()` + `KeyStore:put()`/`get()`
- Persistence via JetStream KV: each buffer slot stored at `{prefix}.__{slot}`, metadata at `{prefix}.__meta`
- Write-through: every incoming message is immediately persisted to KV
- On construction, existing state is loaded from KV before subscribing — no messages lost
- If the buffer size changes between runs, persisted state is discarded and the buffer starts fresh
- Messages are delivered asynchronously by nats.c's internal thread; read methods return immediately
- `stop()` preserves KV data for reload; `purge()` deletes it permanently
- `total_received()` counts every message including overwritten ones — useful for detecting if the buffer is too small
- LuaJIT quirk: method calls through `__index` must avoid nesting (e.g. `self._ks:get(self:_meta_key())` fails; assign to a local first)
