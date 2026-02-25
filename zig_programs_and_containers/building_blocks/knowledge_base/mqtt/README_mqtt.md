# MQTT Building Blocks for Zig

A suite of four MQTT libraries translated from C to Zig 0.13, designed for
distributed control systems spanning embedded microcontrollers to full servers.
All libraries wrap **libmosquitto** and replace cJSON with Zig's `std.json`.

## Libraries

### mqtt_queue

Reliable queued messaging over MQTT v3.1.1 with persistent sessions.

A **Publisher** sends individual or batched messages at QoS 1 or 2. A
**Reader** uses MQTT persistent sessions (clean_session=false) so the broker
queues messages while the reader is offline. On reconnect the reader drains
everything that accumulated. This is the foundation for reliable job
distribution across distributed nodes.

### mqtt_kv_store

MQTT-based key/value store using retained messages.

A **Writer** publishes key/value pairs as retained messages — the broker holds
the latest value for each topic indefinitely. A **Reader** subscribes with
wildcard patterns and collects the retained snapshot, using sentinel topics to
detect when all retained messages have been delivered. Supports exact-topic
reads, single-level wildcards (`+`), and multi-level wildcards (`#`). Useful
for configuration distribution, status dashboards, and sensor registries.

### mqtt_rpc

JSON-RPC 2.0 request/response over MQTT.

A **Server** listens on `rpc/{service}/request/+`, dispatches each request to
a worker thread, looks up the method in a registered handler table, and
publishes the JSON-RPC response back to the caller's dedicated response topic.
A **Client** sends synchronous calls with configurable timeouts and tracks
pending requests by ID. Designed for command-and-control patterns where a
node needs to invoke a specific operation on another node and wait for the
result.

### mqtt_pubsub

JSON-pubsub 2.0 request/response over MQTT with async support.

Structurally similar to mqtt_rpc but adds **asynchronous calls** — the client
can fire a request and receive the result via a callback on a background
thread, avoiding blocking the caller. The server side is identical in pattern:
topic-based routing, threaded dispatch, method registration. Suited for
scenarios where the caller has other work to do while waiting, or needs to
issue multiple concurrent requests.

## Architecture

All four libraries share the same layered design:

```
┌─────────────────────────────────────────────────┐
│              Application Code                    │
├─────────────┬──────────┬──────────┬─────────────┤
│  mqtt_queue │ kv_store │ mqtt_rpc │ mqtt_pubsub │
├─────────────┴──────────┴──────────┴─────────────┤
│              libmosquitto (C)                    │
├─────────────────────────────────────────────────┤
│              MQTT Broker (Mosquitto)             │
└─────────────────────────────────────────────────┘
```

Each library follows the same internal structure:

- **Mosquitto C callbacks** with `callconv(.C)` handle network events
- **Zig std.Thread.Mutex/Condition** replace pthreads for synchronization
- **Error unions** replace C return codes
- **Allocator pattern** — every struct takes a `std.mem.Allocator`, no hidden mallocs
- **Arena allocators** for temporary JSON parsing work

## Project Layout

Each library is a standalone Zig project with the same structure:

```
library_zig/
├── build.zig            # Static lib + shared lib + tests
├── build.zig.zon        # Package metadata
├── src/
│   ├── root.zig         # Public API re-exports
│   └── *.zig            # Implementation
└── test/
    └── *_test.zig       # Integration test (needs broker)
```

## Build Commands

All libraries use the same commands:

```bash
zig build                          # Build .a and .so
zig build -Doptimize=ReleaseFast   # Optimized build
zig build test                     # Unit tests (no broker)
zig build run-test                 # Integration test (needs broker)
```

## Prerequisites

```bash
sudo apt install libmosquitto-dev   # libmosquitto C library
# Zig 0.13 on PATH
# Mosquitto broker on localhost:1883 for integration tests
```

## Zig 0.13 Translation Notes

These libraries were translated with a specific set of lessons for Zig 0.13
compatibility. The key gotchas that differ from Zig 0.14+ documentation:

| Pattern | Zig 0.13 | Zig 0.14+ |
|---------|----------|-----------|
| Calling convention | `callconv(.C)` | `callconv(.c)` |
| LazyPath in build.zig | `.cwd_relative = "src/root.zig"` | `.path = "src/root.zig"` |
| Package name in .zon | `.name = "mqtt_queue"` | `.name = .mqtt_queue` |
| Module import in build | `&static_lib.root_module` | `static_lib.root_module` |
| Random numbers | `rng.next()` | `rng.random.int(u32)` |
| nanoTimestamp cast | `@truncate(@as(u128, @bitCast(...)))` | `@bitCast(...)` |

**Userdata pointer rule:** `init()` returns by value in Zig, so the struct
address changes after return. Pass `null` to `mosquitto_new`, then call
`mosquitto_user_data_set(self.mosq, @ptrCast(self))` in `start()`/`connect()`
where the caller holds a stable `*Self` pointer.

**Buffer copying in callbacks:** Mosquitto frees its internal buffers when a
message callback returns. Any data passed to a spawned thread must be copied
with `allocator.dupe()` before the callback exits.

## License

MIT
