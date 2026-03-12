# NATS LuaJIT FFI Bindings

LuaJIT FFI bindings for the five NATS C libraries: KeyStore, KbStore, JobQueue, RPC, and PubSub.

## Structure

```
nats_luajit/
├── lib/
│   ├── nats.lua              # Unified entry point
│   ├── nats_key_store.lua    # KeyStore (JetStream KV operations)
│   ├── nats_kb_store.lua     # KbStore  (hierarchical knowledge base)
│   ├── nats_job_queue.lua    # JobQueue (priority job queue)
│   ├── nats_rpc.lua          # RPC      (client + server)
│   └── nats_pubsub.lua       # PubSub   (publish/subscribe)
└── test/
    ├── test_key_store.lua    # KeyStore integration tests
    ├── test_pubsub.lua       # PubSub integration tests
    └── test_rpc.lua          # RPC integration tests
```

## Prerequisites

1. **LuaJIT** installed and on PATH
2. **NATS C libraries** built as shared libraries (`.so`):
   - `libnats_key_store.so` — from `key_store/key_store_c/build/`
   - `libnats_kb_store.so`  — from `key_store/key_store_c/build/`
   - `libnats_job_queue.so` — from `key_store/key_store_c/build/`
   - `libnats_rpc.so`       — from `rpc/rpc_c/build/`
   - `libnats_pubsub.so`    — from `pub_sub/pub_sub_c/build/`
3. **NATS server** running:
   ```bash
   # Plain NATS (pub_sub, rpc):
   docker run -d -p 4222:4222 nats:latest

   # With JetStream (key_store, kb_store, job_queue):
   docker run -d -p 4222:4222 nats:latest -js
   ```

## Setup

Set `LD_LIBRARY_PATH` to include the build directories:

```bash
export LD_LIBRARY_PATH=\
/path/to/key_store/key_store_c/build:\
/path/to/rpc/rpc_c/build:\
/path/to/pub_sub/pub_sub_c/build:\
$LD_LIBRARY_PATH
```

Or install the `.so` files to `/usr/local/lib` and run `sudo ldconfig`.

## Usage

### Unified entry point

```lua
local nats = require("lib.nats")

local ks  = nats.KeyStore.new({ bucket = "test" })
local kb  = nats.KbStore.new("nats://127.0.0.1:4222", "my_kb", "desc")
local jq  = nats.JobQueue.new(ks:handle(), "worker-1")
local cli = nats.RpcClient.new({})
local ps  = nats.PubSub.new({})
```

### KeyStore

```lua
local ks_lib = require("lib.nats_key_store")
local ks = ks_lib.KeyStore.new({
    server = "nats://127.0.0.1:4222",
    bucket = "my_bucket",
})
ks:connect()

-- Put / Get
local rev = ks:put("user.1", '{"name":"Alice","age":30}')
local val = ks:get("user.1")   -- returns JSON string or nil

-- Delete / Exists
ks:delete("user.1")
local found = ks:exists("user.1")  -- false

-- Key listing
local keys = ks:keys("user.*")     -- glob pattern
local keys2 = ks:keys_prefix("user.")  -- prefix match

-- Counters
local v = ks:increment("hits", 1)   -- returns new value
local v = ks:decrement("hits", 1)

-- Auto connect/disconnect variants
ks:put_sync("key", '"value"')
local v = ks:get_sync("key")

ks:disconnect()
ks:destroy()
```

### KbStore (Knowledge Base)

```lua
local kb_lib = require("lib.nats_kb_store")
local kb = kb_lib.KbStore.new(
    "nats://127.0.0.1:4222",
    "my_kb",
    "My Knowledge Base"
)
kb:connect()

-- Store entry
local key = kb:store("sensors", "temperature", "outdoor",
    '{"type":"sensor","description":"outdoor temp"}',
    '{"id":"T1","data":{"unit":"celsius"}}',
    true)  -- composite key: "sensors.temperature.outdoor"

-- Retrieve entry
local entry = kb:get_entry(key)
print(entry.label_json)
print(entry.node_json)

-- List / Stats
local keys = kb:list_keys("sensors")
local stats = kb:get_stats()

-- Pop key: "sensors.temperature.outdoor" -> "sensors"
local base = kb_lib.KbStore.pop_key("sensors.temperature.outdoor")

kb:disconnect()
kb:destroy()
```

### JobQueue

```lua
local ks_lib = require("lib.nats_key_store")
local jq_lib = require("lib.nats_job_queue")

local ks = ks_lib.KeyStore.new({ bucket = "jobs" })
ks:connect()

local jq = jq_lib.JobQueue.new(ks:handle(), "my-worker")

-- Submit (payload, queue, priority, max_retries, timeout_secs)
local job_id = jq:submit('{"task":"process"}', "work", 5, 3, 300)

-- Claim next job (highest priority first)
local job = jq:claim_job({"work"})
if job then
    print(job.id, job.payload_json, job.priority)

    -- Complete or fail
    jq:complete_job(job.id, '{"result":"done"}')
    -- jq:fail_job(job.id, "something broke")
end

-- Stats
local stats = jq:get_stats("work")
print(stats.pending, stats.completed)

-- Workers
local workers = jq:get_active_workers(30)

-- Cleanup stale
local cleaned = jq:cleanup_stale_jobs(60)

jq:destroy()
ks:destroy()
```

### RPC

```lua
local rpc = require("lib.nats_rpc")

-- Server
local srv = rpc.RpcServer.new({
    server = "nats://127.0.0.1:4222",
    namespace_ = "myapp",
})
srv:register("math.add", function(req_json)
    -- Parse JSON, compute, return JSON
    return '{"result":8}'
end)
srv:start("rpc")  -- subscribes to method subjects

-- Client
local cli = rpc.RpcClient.new({
    server = "nats://127.0.0.1:4222",
    namespace_ = "myapp",
})
cli:connect()
local result = cli:call("rpc.math.add", '{"a":5,"b":3}', 5.0)
print(result)  -- '{"result":8}'

-- Batch calls
local results = cli:batch({
    { method = "rpc.math.add", request_json = '{"a":1,"b":2}' },
    { method = "rpc.math.add", request_json = '{"a":3,"b":4}' },
}, 5.0)

cli:disconnect()
cli:destroy()
srv:stop()
srv:destroy()
```

### PubSub

```lua
local ps_lib = require("lib.nats_pubsub")
local ps = ps_lib.PubSub.new({
    server = "nats://127.0.0.1:4222",
    namespace_ = "myapp",
})
ps:connect()

-- Subscribe (namespace auto-prepended)
local sub = ps:subscribe("sensor.temp", function(msg)
    print(msg.subject, msg.data, msg.original_subject)
end)

-- Queue group subscription (load balancing)
local sub2 = ps:subscribe("jobs.process", function(msg)
    print("Worker got:", msg.data)
end, "workers")

-- Publish
ps:publish_str("sensor.temp", '{"value":23.5}')
ps:publish("sensor.temp", "\x00\x01\x02")  -- binary

-- Request/Reply
local reply = ps:request("service.echo", "hello", 5.0)

-- Raw subject (no namespace prefix)
local sub3 = ps:subscribe_raw("raw.topic", function(msg)
    print("raw:", msg.data)
end)

-- Stats
local stats = ps:get_stats()
print(stats.messages_published, stats.active_subscriptions)

-- Cleanup
ps:unsubscribe(sub)
ps:unsubscribe_all()
ps:disconnect()
ps:destroy()
```

## API Summary

| C Library | LuaJIT Module | Class | Requires JetStream |
|-----------|---------------|-------|--------------------|
| `libnats_key_store` | `lib.nats_key_store` | `KeyStore` | Yes |
| `libnats_kb_store`  | `lib.nats_kb_store`  | `KbStore`  | Yes |
| `libnats_job_queue` | `lib.nats_job_queue` | `JobQueue` | Yes |
| `libnats_rpc`       | `lib.nats_rpc`       | `RpcServer`, `RpcClient` | No |
| `libnats_pubsub`    | `lib.nats_pubsub`    | `PubSub`   | No |

## Design Notes

- All bindings use LuaJIT's FFI `ffi.load()` to dynamically load the `.so` files
- Opaque C handles (`KeyStore*`, `PubSub*`, etc.) are managed by Lua wrapper objects
- Caller-frees-strings pattern: Lua copies C strings immediately, then `free()`s the C allocation
- Callbacks (PubSub, RPC server) are wrapped via `ffi.cast` with GC anchoring to prevent premature collection
- Status codes map to Lua errors via `error()` — catch with `pcall()`
- `nil` return from `get()` operations indicates "not found" (no error thrown)
- Each module exposes `_C` for direct access to the raw FFI library handle

## Running Tests

```bash
cd nats_luajit

# Start NATS with JetStream
docker run -d -p 4222:4222 nats:latest -js

# Set library path
export LD_LIBRARY_PATH=/path/to/builds:$LD_LIBRARY_PATH

# Run
luajit test/test_key_store.lua
luajit test/test_pubsub.lua
luajit test/test_rpc.lua
```
