# mqtt_pubsub_zig — JSON-pubsub 2.0 over MQTT (Zig)

Zig 0.13 translation of the C `mqtt_pubsub` library. Wraps libmosquitto,
replaces cJSON with `std.json`.

## Directory Layout

```
C original                          Zig translation
──────────                          ───────────────
include/mqtt_pubsub.h               (merged into .zig)
src/mqtt_pubsub.c                   src/mqtt_pubsub.zig
                                    src/root.zig          ← public re-exports
test/mqtt_pubsub_test.c             test/pubsub_test.zig
Makefile                            build.zig + build.zig.zon
```

## Build

```bash
# Prerequisites
sudo apt install libmosquitto-dev
# Zig 0.13 on PATH

# Build libraries (.a + .so)
zig build
zig build -Doptimize=ReleaseFast

# Unit tests (no broker needed)
zig build test

# Integration test (needs mosquitto on localhost:1883)
zig build run-test
zig build run-test -Doptimize=ReleaseFast
```

## Output

```
zig-out/
├── lib/
│   ├── libmqtt_pubsub.a
│   └── libmqtt_pubsub.so
└── bin/
    └── pubsub_test
```

## API Quick Reference

```zig
const pubsub = @import("mqtt_pubsub");

// Library lifecycle
pubsub.libInit();
defer pubsub.libCleanup();

// Server
var server = try pubsub.Server.init(cfg, allocator);
defer server.deinit();
server.register("method_name", handlerFn, userdata);
try server.start(true, 5000);
server.stop();

// Client — synchronous
var client = try pubsub.Client.init(cfg, allocator, 30000);
defer client.deinit();
try client.connect(5000);
var result = try client.call("method", params_json, 5000);
defer result.deinit(allocator);

// Client — asynchronous
client.callAsync("method", params_json, 5000, callbackFn, userdata);
```

## Key Translation Differences from C

### 1. cJSON → std.json
The biggest change. Method handlers receive/return JSON **strings** instead
of `cJSON*` trees:

```c
// C handler
cJSON *my_handler(const cJSON *params, void *userdata) {
    return cJSON_CreateString("hello");
}

// Zig handler
fn myHandler(alloc: Allocator, params_json: ?[]const u8, ud: ?*anyopaque) ?[]const u8 {
    return alloc.dupe(u8, "\"hello\"") catch null;
}
```

### 2. Userdata Pointer
`init()` returns by value in Zig → address changes. We pass `null` to
`mosquitto_new` and call `mosquitto_user_data_set` in `start()`/`connect()`
where the `*Self` pointer is stable.

### 3. Buffer Copying in srvOnMessage
The C version calls `cJSON_Parse` + `strdup` inside the callback (creating
heap copies). In Zig we explicitly `allocator.dupe()` the topic and payload
before spawning the worker thread, since mosquitto frees its buffers when
the callback returns.

### 4. Async Calls
C version: `pubsub_client_call_async()` returns a `char*` request ID.
Zig version: `client.callAsync()` is `void` — it spawns a detached thread
that calls `client.call()` synchronously and invokes the callback.

### 5. Error Handling
C return codes (-1, -2) become Zig error unions. The `CallResult` struct
holds optional `.result` and `.err` JSON strings.

### 6. Thread Synchronization
`pthread_mutex/cond` → `std.Thread.Mutex/Condition`.
`pthread_cond_timedwait` → `cond.timedWait(&mutex, ns)`.

## Test Scenarios

| # | Test                | What it checks                         |
|---|---------------------|----------------------------------------|
| 1 | echo                | Round-trip JSON params                 |
| 2 | add integers        | Numeric parsing, integer result        |
| 3 | add floats          | Float arithmetic                       |
| 4 | greet               | String extraction from params          |
| 5 | get_status          | No-params call, JSON object result     |
| 6 | slow_task           | 500 ms delay, timeout handling         |
| 7 | method not found    | Error response path                    |
| 8 | rapid sequential    | 5 calls in quick succession            |
| 9 | async call          | callAsync with callback verification   |