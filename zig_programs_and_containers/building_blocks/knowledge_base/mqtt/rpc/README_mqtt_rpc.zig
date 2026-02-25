# mqtt_rpc — Zig JSON-RPC 2.0 over MQTT

A Zig port of the C `mqtt_rpc` library. Provides an RPC server and client that
exchange JSON-RPC 2.0 messages over MQTT topics.

This README is written for C programmers who are new to Zig.

---

## Prerequisites

```bash
sudo apt install libmosquitto-dev
```

Tested with Zig 0.13. **Note:** cJSON is NOT required — this version uses
Zig's `std.json` instead.

---

## Project Structure

```
C original:                          Zig translation:

rpc_c/                               mqtt_rpc_zig/
├── Makefile                          ├── build.zig
├── include/                          ├── build.zig.zon
│   └── mqtt_rpc.h                    ├── src/
├── src/                              │   ├── root.zig
│   └── mqtt_rpc.c                    │   └── mqtt_rpc.zig    ← .h + .c combined
├── test/                             └── test/
│   └── rpc_main.c                        └── rpc_main.zig
└── build/
    ├── libmqtt_rpc.a                 zig-out/lib/
    ├── libmqtt_rpc.so                    ├── libmqtt_rpc.a
    └── rpc_main                          └── libmqtt_rpc.so
```

---

## Build Commands

```bash
zig build                          # build .a and .so
zig build -Doptimize=ReleaseFast   # optimized
zig build test                     # unit tests (no broker needed)
zig build run-test                 # integration test (broker on :1883)
```

---

## Biggest Change: cJSON → std.json

The C version depends on `libcjson`. The Zig version replaces it entirely with
`std.json` from the standard library. This eliminates a C dependency and
changes how method handlers work.

### C method handler

```c
// Takes and returns cJSON objects
typedef cJSON *(*rpc_method_fn)(const cJSON *params, void *userdata);

cJSON *method_add(const cJSON *params, void *userdata) {
    double a = cJSON_GetObjectItem(params, "a")->valuedouble;
    double b = cJSON_GetObjectItem(params, "b")->valuedouble;
    return cJSON_CreateNumber(a + b);
}
```

### Zig method handler

```zig
// Takes and returns JSON strings. The allocator is provided by the library.
pub const MethodFn = *const fn (
    allocator: std.mem.Allocator,
    params_json: ?[]const u8,     // raw JSON string, e.g. {"a":1,"b":2}
    userdata: ?*anyopaque,
) ?[]const u8;                     // return JSON string, e.g. "42"

fn methodAdd(allocator: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const json_bytes = params_json orelse return null;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const parsed = std.json.parseFromSlice(std.json.Value, arena.allocator(), json_bytes, .{}) catch return null;
    // ... extract a, b, compute sum ...
    return std.fmt.allocPrint(allocator, "{d}", .{a + b}) catch null;
}
```

The trade-off: handlers do their own JSON parsing, but there's no C dependency
to manage. The library handles all JSON-RPC envelope construction and parsing.

---

## API Quick Reference

### Server

```zig
const rpc = @import("mqtt_rpc");

rpc.libInit();
defer rpc.libCleanup();

var server = try rpc.Server.init(.{
    .service_name = "my_service",
}, allocator);
defer server.deinit();

server.register("add", methodAdd, null);
server.register("echo", methodEcho, null);

try server.start(true, 2000);

server.wait();    // blocks until stop() is called
server.stop();
```

### Client

```zig
var client = try rpc.Client.init(.{
    .service_name = "my_service",
}, allocator, 10000);   // 10s default timeout
defer client.deinit();

try client.connect(5000);
defer client.disconnect();

// Synchronous call
var result = try client.call("add", "{\"a\":1,\"b\":2}", 5000);
defer result.deinit(allocator);

if (result.result) |r| {
    // r is a JSON string like "3"
} else if (result.err) |e| {
    // e is a JSON string like {"code":-32601,"message":"Method not found"}
}
```

---

## C-to-Zig Translation Notes

### Topic Construction: aprintf → std.fmt.allocPrintZ

The C version uses a custom `aprintf` (printf into malloc'd string). Zig has
this built in:

```c
// C
char *topic = aprintf("rpc/%s/request/+", service_name);
free(topic);
```

```zig
// Zig
const topic = try std.fmt.allocPrintZ(allocator, "rpc/{s}/request/+", .{service_name});
defer allocator.free(topic);
```

### Auto-Generated Client IDs

The C version uses `rand_r` + `getpid`. The Zig version uses
`std.rand.DefaultPrng` seeded from `nanoTimestamp`:

```zig
fn autoClientId(allocator: std.mem.Allocator, prefix: []const u8) ![:0]const u8 {
    var rng = std.rand.DefaultPrng.init(@as(u64, @bitCast(std.time.nanoTimestamp())));
    const suffix = rng.random();
    return try std.fmt.allocPrintZ(allocator, "{s}_{x:0>8}", .{ prefix, @truncate(suffix) });
}
```

### Detached Worker Threads

The C server dispatches each request to a detached pthread. The Zig version
uses `std.Thread.spawn` + `thread.detach()`:

```c
// C
pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
pthread_create(&tid, &attr, srv_process_thread, ctx);
```

```zig
// Zig
const thread = std.Thread.spawn(.{}, processThread, .{ self, topic, payload }) catch return;
thread.detach();
```

### Pending Request Tracking

The C client uses a linked list of `rpc_pending_t` nodes with per-node
condition variables. The Zig version uses the same pattern with
`PendingRequest` structs, but with `std.Thread.Condition` and optionals:

```c
// C - walk list with strcmp
for (rpc_pending_t *p = cli->pending_head; p; p = p->next) {
    if (strcmp(p->id, id_str) == 0) { ... }
}
```

```zig
// Zig - walk list with std.mem.eql
var pend = self.pending_head;
while (pend) |p| {
    if (std.mem.eql(u8, p.id, id_str)) { ... }
    pend = p.next;
}
```

### CallResult Instead of Output Parameters

The C client uses output parameters (`cJSON **out_result, cJSON **out_error`)
and return codes (0, -1, -2). The Zig version returns a `CallResult` struct:

```c
// C
cJSON *result = NULL, *error = NULL;
int rc = rpc_client_call(cli, "add", params, 5.0, &result, &error);
if (rc == 0) { /* use result */ }
else if (rc == -2) { /* use error */ }
cJSON_Delete(result);
cJSON_Delete(error);
```

```zig
// Zig
var result = try client.call("add", params_json, 5000);
defer result.deinit(allocator);
if (result.result) |r| { /* use r */ }
else if (result.err) |e| { /* use e */ }
```

### Timeouts: float Seconds → u64 Milliseconds

Same as the other libraries: `float timeout_s` becomes `u64 timeout_ms`.

---

## Lessons Applied from Previous Ports

All issues discovered during the mqtt_queue and kv_store ports are pre-applied:

1. **`callconv(.C)`** — uppercase C for Zig 0.13 callbacks
2. **Userdata pointer** — `null` in `init`, `mosquitto_user_data_set` in `start`/`connect`
3. **`&static_lib.root_module`** — pointer in build.zig addImport
4. **`@import("mqtt_rpc")`** — named module in test (no `.zig`)
5. **`"mqtt_rpc"`** — string literal in build.zig.zon (not `.mqtt_rpc`)
6. **No `pub` as variable name** — reserved keyword
7. **No `fingerprint`** — Zig 0.14+ only