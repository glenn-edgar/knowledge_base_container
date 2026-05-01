# MQTT LuaJIT FFI Bindings

LuaJIT FFI bindings for two MQTT C libraries: KV Store (writer + reader) and Queue (publisher + reader).

A thin C adapter (`libmqtt_luajit_adapter.so`) wraps the original stack-allocated C structs with opaque create/destroy handles that LuaJIT FFI can work with.

## Structure

```
.
├── Makefile                      # Builds libmqtt_luajit_adapter.so
├── mqtt_luajit_adapter.c         # C adapter implementation
├── include/
│   ├── mqtt_luajit_adapter.h     # Adapter header (opaque handle API)
│   ├── kv_store_writer.h         # Original C headers (needed at compile time)
│   ├── kv_store_reader.h
│   └── mqtt_queue.h
├── lib/
│   ├── mqtt.lua                  # Unified entry point
│   ├── mqtt_kv_store.lua         # KV Writer + Reader binding
│   └── mqtt_queue.lua            # Queue Publisher + Reader binding
├── test/
│   ├── test_kv_store.lua         # 8 tests
│   └── test_queue.lua            # 6 tests
├── libmqtt_kv_store.so           # Original C shared libraries
├── libmqtt_queue.so
├── libmqtt_luajit_adapter.so     # Built by make
└── test.sh                       # Build + run all tests
```

## Prerequisites

- LuaJIT on PATH
- Mosquitto broker on localhost:1883
- `libmqtt_kv_store.so` and `libmqtt_queue.so` in the project directory

## Building and Testing

```bash
make
./test.sh
```

## Architecture

```
LuaJIT FFI
    │
    ▼
libmqtt_luajit_adapter.so    ← opaque handle wrappers
    │
    ├──► libmqtt_kv_store.so  ← KV store (writer + reader)
    ├──► libmqtt_queue.so     ← Queue (publisher + reader)
    └──► libmosquitto.so      ← Mosquitto client
```

## Usage

### Unified Entry Point

```lua
local mqtt = require("lib.mqtt")
mqtt.lib_init()

local w   = mqtt.KvWriter.new()
local r   = mqtt.KvReader.new()
local pub = mqtt.Publisher.new()
local qr  = mqtt.QueueReader.new()

mqtt.lib_cleanup()
```

### KV Store (timeouts in seconds)

```lua
local kv = require("lib.mqtt_kv_store")
kv.lib_init()

-- Writer
local w = kv.Writer.new("localhost", 1883, "my-writer")
w:connect(5.0)
w:write("kv/config/host", "192.168.1.1")     -- retained, qos=1
w:update("kv/config/host", "10.0.0.1")       -- convenience: retain+qos1
w:delete("kv/config/host")
w:disconnect(); w:destroy()

-- Reader
local r = kv.Reader.new("localhost", 1883, "my-reader")
r:connect(5.0)
local val = r:read_single("kv/config/host", 5.0)   -- string or nil
local entries = r:read_pattern("kv/config/#", 1, 5.0, 64)
for _, e in ipairs(entries) do print(e.topic, e.value) end
local all = r:read_all("kv/#", 5.0, 256)
r:disconnect(); r:destroy()

kv.lib_cleanup()
```

### Queue (timeouts in milliseconds)

```lua
local mq = require("lib.mqtt_queue")

-- Publisher
local pub = mq.Publisher.new("localhost", 1883, "my-pub")
pub:connect(5000)
pub:publish("queue/tasks", '{"task":"process"}', 1, false)
pub:disconnect(); pub:destroy()

-- Reader
-- clean_session=false for persistent sessions (offline queueing)
local reader = mq.Reader.new("localhost", 1883, "my-reader", false)
reader:connect(5000)
local msgs = reader:read("queue/tasks", 1, 3000)
for _, m in ipairs(msgs) do print(m.topic, m.payload) end
reader:disconnect(); reader:destroy()
```

## API Summary

| LuaJIT Class | C Library | Methods |
|-------------|-----------|---------|
| `kv.Writer` | kv_read_store | `new`, `connect`, `write`, `update`, `delete`, `disconnect`, `destroy`, `is_connected` |
| `kv.Reader` | kv_read_store | `new`, `connect`, `read_single`, `read_pattern`, `read_all`, `disconnect`, `destroy`, `is_connected` |
| `mq.Publisher` | job_queue | `new`, `connect`, `publish`, `disconnect`, `destroy`, `is_connected` |
| `mq.Reader` | job_queue | `new`, `connect`, `subscribe`, `read`, `disconnect`, `destroy`, `is_connected` |

## Timeout Units

| Library | Unit |
|---------|------|
| KV Store (writer + reader) | seconds (double) |
| Queue (publisher + reader) | milliseconds (int) |

## Why No PubSub or RPC?

Both libraries use callback-based message dispatch. Mosquitto fires these callbacks on its internal thread, but LuaJIT FFI callbacks require the calling thread to own the Lua state. For publishing, use the Queue Publisher. For subscribing/receiving, use the Queue Reader (polling-based, works from the Lua thread). For RPC, use C or Zig servers with the NATS LuaJIT RPC client.