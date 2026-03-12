# rpc_zig

Idiomatic Zig bindings for the NATS RPC C library (`rpc_c`).

Wraps the C RPC server and client with Zig-native types: tagged unions for handler results, error unions instead of status codes, slices instead of raw pointers, and a handler trampoline that bridges C callbacks to Zig functions.

## Directory layout

```
rpc/
├── rpc_c/                  # C library (unchanged)
│   ├── include/
│   │   └── nats_rpc.h
│   ├── build/
│   │   ├── libnats_rpc.a
│   │   └── libnats_rpc.so
│   ├── src/
│   │   └── nats_rpc.c
│   └── test/
│       └── test_nats_rpc.c
└── rpc_zig/                # Zig wrapper
    ├── build.zig
    ├── build.zig.zon
    ├── src/
    │   ├── root.zig          # Public API re-exports
    │   ├── c_api.zig         # Manual extern declarations
    │   ├── status.zig        # rpc_status_t → Zig error mapping
    │   ├── server.zig        # Server, HandlerFn, HandlerResult, StatsArray
    │   └── client.zig        # Client, CallResult, BatchEntry, BatchResult
    └── test/
        ├── integration_test.zig
        └── example.zig
```

## Prerequisites

- Zig 0.13+
- `rpc_c` built (libraries in `rpc_c/build/`)
- `libnats` installed (the nats.c library)
- `libcjson` installed (the C library uses cJSON for JSON encoding)
- NATS server for integration tests and examples

## Building

From `rpc_zig/`:

```bash
zig build                # Build static and shared libraries
zig build test           # Unit tests (no NATS server needed)
zig build integration    # Integration tests (needs NATS server)
zig build example        # Build and run example program
```

The build expects `rpc_c/build/` at `../rpc_c/build/` by default. Override with:

```bash
zig build -Dc-lib=/path/to/rpc_c/build
```

### Starting a NATS server

```bash
docker run -p 4222:4222 nats:latest
```

## Quick start — Server

```zig
const rpc = @import("rpc_zig");

fn addHandler(params_json: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    // Parse params, compute result
    return .{ .ok = "{\"sum\":8}" };
}

var srv = try rpc.Server.init(.{
    .namespace = "myapp",
    .enable_health = true,
});
defer srv.deinit();

try srv.register("math.add", addHandler, null, false);
try srv.start("rpc");     // returns immediately
srv.wait();               // blocks until stop()
```

## Quick start — Client

```zig
const rpc = @import("rpc_zig");

var cli = try rpc.Client.init(.{ .namespace = "myapp" });
defer cli.deinit();
try cli.connect();

// Simple call
var result = try cli.call("rpc.math.add", "{\"a\":5,\"b\":3}", 5.0);
defer result.deinit();
std.debug.print("result: {s}\n", .{result.str()});

// Instance-specific call
var health = try cli.callInstance("rpc._health", "{}", 5.0, "my-instance-id");
defer health.deinit();

// Batch calls
const entries = [_]rpc.BatchEntry{
    .{ .method = "rpc.math.add", .params_json = "{\"a\":1,\"b\":2}" },
    .{ .method = "rpc.math.add", .params_json = "{\"a\":10,\"b\":20}" },
};
const results = try cli.callBatch(&entries, 5.0, allocator);
defer {
    for (results) |*r| r.deinit();
    allocator.free(results);
}
```

## API reference

### Config (shared by Server and Client)

| Field           | Type              | Default                        |
|-----------------|-------------------|--------------------------------|
| `server`        | `[:0]const u8`    | `"nats://127.0.0.1:4222"`     |
| `namespace`     | `[:0]const u8`    | `"default"`                    |
| `instance_id`   | `?[:0]const u8`   | `null` (auto-generated)        |
| `enable_health` | `bool`            | `true`                         |

### Server

| Method                                     | Description                                      |
|--------------------------------------------|--------------------------------------------------|
| `init(Config)`                             | Create server (does not connect)                 |
| `deinit()`                                 | Destroy server and free all resources            |
| `register(method, handler, data, inst)`    | Register a method handler                        |
| `start(prefix)`                            | Connect and begin handling (returns immediately) |
| `wait()`                                   | Block until `stop()` is called                   |
| `stop()`                                   | Stop server and unsubscribe all handlers         |
| `instanceId()`                             | Server's unique instance ID                      |
| `isRunning()`                              | Running state                                    |
| `getStats()`                               | Per-handler call/error counts                    |

### HandlerFn

```zig
pub const HandlerFn = *const fn (
    params_json: []const u8,
    user_data: ?*anyopaque,
) HandlerResult;
```

Handlers return a tagged union:

```zig
pub const HandlerResult = union(enum) {
    ok: ?[]const u8,   // JSON result string, or null
    err: []const u8,   // error message sent back to caller
};
```

### Client

| Method                                          | Description                                  |
|-------------------------------------------------|----------------------------------------------|
| `init(Config)`                                  | Create client (does not connect)             |
| `deinit()`                                      | Destroy client                               |
| `connect()`                                     | Connect to NATS server                       |
| `disconnect()`                                  | Disconnect from server                       |
| `isConnected()`                                 | Connection state                             |
| `instanceId()`                                  | Client's unique instance ID                  |
| `call(method, params, timeout)`                 | Synchronous RPC call                         |
| `callInstance(method, params, timeout, target)`  | Call targeting a specific server instance     |
| `callBatch(entries, timeout, allocator)`         | Execute multiple calls sequentially          |

### CallResult

Returned by `call()` and `callInstance()`. Owns malloc'd JSON data.

| Method    | Description                            |
|-----------|----------------------------------------|
| `str()`   | Get the result JSON as a `[]const u8`  |
| `deinit()`| Free the underlying C memory           |

### BatchResult

Returned per-entry by `callBatch()`.

| Method    | Description                                |
|-----------|--------------------------------------------|
| `isOk()`  | Whether this call succeeded                |
| `str()`   | Result JSON, or `null`                     |
| `deinit()`| Free the underlying C memory (if any)      |

### Errors

All methods return `Error` which maps from C status codes:

| Zig error         | C status             | Description                     |
|-------------------|----------------------|---------------------------------|
| `InvalidArg`      | `RPC_ERR_INVALID_ARG`| Bad argument                    |
| `ConnectionFailed` | `RPC_ERR_CONNECTION` | Cannot connect to NATS          |
| `Timeout`         | `RPC_ERR_TIMEOUT`    | No reply within timeout         |
| `EncodeError`     | `RPC_ERR_ENCODE`     | JSON encoding failed            |
| `DecodeError`     | `RPC_ERR_DECODE`     | JSON decoding failed            |
| `OutOfMemory`     | `RPC_ERR_MEMORY`     | Allocation failed               |
| `HandlerError`    | `RPC_ERR_HANDLER`    | Server handler returned error   |
| `NotFound`        | `RPC_ERR_NOT_FOUND`  | No handler for method           |
| `NatsError`       | `RPC_ERR_NATS`       | Generic nats.c error            |

## Design notes

**Manual extern declarations** — `c_api.zig` declares all C types and functions explicitly rather than using `@cImport`. This avoids the include-path propagation issues in Zig 0.13's module system.

**Handler trampoline** — The C library expects a handler with signature `rpc_status_t (*)(const char*, void*, char**)` — a status code return plus a malloc'd string out-param. The Zig wrapper lets you return a `HandlerResult` tagged union (`.ok` or `.err`) which the trampoline converts to C's calling convention. The trampoline malloc's the result string so C can `free()` it.

**Server/Client split** — Separate source files since they have independent lifecycles and different dependency shapes. `Config` is defined in `server.zig` with `pub` visibility and re-exported through `client.zig` and `root.zig`.

**Namespace handling** — The C library prepends namespaces automatically. Methods starting with `_` (like `_health`) bypass namespace prefixing. The Zig wrapper passes through without modification.

**Batch calls** — Sequential implementation matching the C library. Takes a Zig allocator for temporary C arrays. Each `BatchResult` owns its own malloc'd JSON string and must be individually freed.

**Health check** — When `enable_health` is `true` (default), the server registers a built-in `_health` handler that returns instance status, uptime, request counts, and registered methods as JSON. Access it via `callInstance("rpc._health", "{}", timeout, instance_id)`.

**Memory ownership** — `CallResult` and `BatchResult` hold malloc'd C strings. Always call `.deinit()` to free. `StatsArray` from `getStats()` holds both a Zig slice and C array — its `.deinit()` frees both.

## License

MIT