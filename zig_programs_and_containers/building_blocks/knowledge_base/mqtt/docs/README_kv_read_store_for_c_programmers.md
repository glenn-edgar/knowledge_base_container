# mqtt_kv_store — Zig MQTT Key/Value Store Library

A Zig port of the C `kv_read_store` library. Uses MQTT retained messages as a
distributed key/value store, wrapping the Mosquitto C client library.

This README is written for C programmers who are new to Zig.

---

## Prerequisites

```bash
sudo apt install libmosquitto-dev
```

Requires Zig 0.13. Download from https://ziglang.org/download/

---

## Project Structure

```
C original:                          Zig translation:

kv_read_store_c/                     mqtt_kv_store_zig/
├── Makefile                         ├── build.zig            ← replaces Makefile
├── README_kv_read_store.md          ├── build.zig.zon        ← package manifest
├── include/                         ├── src/
│   ├── kv_store_reader.h            │   ├── root.zig         ← public API exports
│   └── kv_store_writer.h            │   ├── kv_store_reader.zig  ← .h + .c combined
├── src/                             │   └── kv_store_writer.zig  ← .h + .c combined
│   ├── kv_store_reader.c            └── test/
│   └── kv_store_writer.c                └── kv_store_test.zig
├── test/
│   └── kv_store_test.c
└── build/
    ├── libmqtt_kv_store.a           zig-out/lib/
    ├── kv_store_reader.o                ├── libmqtt_kv_store.a
    ├── kv_store_writer.o                └── libmqtt_kv_store.so
    └── kv_store_test
```

Key observations: there is no `include/` directory because Zig has no header
files. Each `.zig` file contains both the type definitions and the
implementation. The `build/` directory becomes `zig-out/` and is auto-managed.

---

## File-by-File Explanation

### `build.zig` — The Build Script

Replaces your Makefile. Written in Zig, it declares four build targets:

- Static library (`.a`)
- Shared library (`.so`)
- Unit tests (no broker needed)
- Integration test executable (needs a running broker)

The test executable gets the library as a named module so it can
`@import("mqtt_kv_store")` even though the files are in different directories.
In C you'd use `-I../include`; in Zig this is:

```zig
test_exe.root_module.addImport("mqtt_kv_store", &static_lib.root_module);
```

### `build.zig.zon` — Package Manifest

Declares the package name and version. Equivalent to the metadata you'd put
in a `CMakeLists.txt` or `configure.ac`.

### `src/root.zig` — Public API Surface

In C, your two `.h` files define the public API. In Zig, `root.zig` re-exports
the types and functions that external consumers should see:

```zig
pub const Writer = kv_writer.Writer;
pub const Reader = kv_reader.Reader;
pub const Entry  = kv_reader.Entry;
pub const Qos    = kv_writer.Qos;
pub fn libInit() void { ... }
pub fn libCleanup() void { ... }
```

Anything not re-exported here is internal to the library.

### `src/kv_store_writer.zig` — The Writer

Replaces both `kv_store_writer.h` and `kv_store_writer.c`. Contains the
`Writer` struct with all its methods: `init`, `connect`, `disconnect`,
`deinit`, `writeSingle`, `writeBatch`, `deleteSingle`, `deleteBatch`,
`updateSingle`, `isConnected`.

Also contains the pending-publish tracking system (the `PendingPublish` array
and the `pendingAdd`/`pendingFind`/`pendingRemove`/`pendingWait` helpers),
and the mosquitto C callbacks.

### `src/kv_store_reader.zig` — The Reader

Replaces both `kv_store_reader.h` and `kv_store_reader.c`. Contains the
`Reader` struct with: `init`, `connect`, `disconnect`, `deinit`, `readPattern`,
`readSingle`, `readAll`, `isConnected`.

Also contains the sentinel coordination system, the entry storage, and the
mosquitto C callbacks.

### `test/kv_store_test.zig` — Integration Test

Equivalent to `test/kv_store_test.c`. Writes 26 retained key/value pairs to
the broker using the Writer, then exercises all Reader modes: wildcard
patterns, single-level wildcards, exact topic reads, sentinel-based early
return, and full broker scan.

---

## Build Commands

```bash
zig build                          # build .a and .so → zig-out/lib/
zig build -Doptimize=ReleaseFast   # optimized (like gcc -O2)
zig build test                     # unit tests (no broker needed)
zig build run-test                 # integration test (broker on localhost:1883)
```

---

## C-to-Zig Translation Notes

This section documents the specific patterns used in this translation.
For general C-to-Zig concepts (error unions, defer, optionals, @cImport),
see the README in the `mqtt_queue` sister project.

### Fixed-Size Buffers Stay Fixed-Size

The C version uses fixed-size char arrays for entries:

```c
typedef struct {
    char topic[KVR_MAX_TOPIC_LEN];   // 256 bytes
    char value[KVR_MAX_VALUE_LEN];   // 4096 bytes
    bool active;
} kvr_entry_t;
```

The Zig version keeps the same layout. This is deliberate — the entry
storage happens inside mosquitto callbacks on the network thread, where
heap allocation could fail or introduce latency:

```zig
pub const Entry = struct {
    topic: [MAX_TOPIC_LEN]u8 = [_]u8{0} ** MAX_TOPIC_LEN,
    value: [MAX_VALUE_LEN]u8 = [_]u8{0} ** MAX_VALUE_LEN,
    active: bool = false,
};
```

To get a usable string from these buffers, use the helper methods:

```zig
const topic = entry.topicSlice();  // returns []const u8
const value = entry.valueSlice();  // returns []const u8
```

These use `std.mem.sliceTo` to find the null terminator and return a
proper slice. In C you'd just pass the array directly to `printf("%s")`.

### strncpy → @memcpy + Manual Null

C's `strncpy` copies up to N bytes and may or may not null-terminate.
In Zig, you compute the length, `@memcpy` the bytes, then set the
null terminator explicitly:

```c
// C
strncpy(e->topic, topic, KVR_MAX_TOPIC_LEN - 1);
e->topic[KVR_MAX_TOPIC_LEN - 1] = '\0';
```

```zig
// Zig
const tlen = @min(topic.len, MAX_TOPIC_LEN - 1);
@memcpy(e.topic[0..tlen], topic[0..tlen]);
e.topic[tlen] = 0;
```

### strcmp → std.mem.eql

String comparison in the sentinel check and entry lookup:

```c
// C
if (strcmp(kv->sentinels[i], topic) == 0) return true;
```

```zig
// Zig
const sentinel = std.mem.sliceTo(&self.sentinels[i], 0);
if (std.mem.eql(u8, sentinel, topic)) return true;
```

`std.mem.eql` compares two slices byte-by-byte. Unlike `strcmp`, it works
on slices with known lengths rather than scanning for null terminators.

### NULL-Terminated Arrays → Slices

The C reader takes sentinel topics as a NULL-terminated array of pointers:

```c
// C
const char *sents[] = {"kv/example/.sentinel", NULL};
kvr_read_pattern(&reader, "kv/example/#", 1, 2.0, sents, true, entries, 256);
```

The Zig version takes a slice of null-terminated strings:

```zig
// Zig
const sents = [_][:0]const u8{"kv/example/.sentinel"};
const n = reader.readPattern("kv/example/#", 1, 2000, &sents, true, &entries);
```

No NULL terminator needed — the slice carries its length.

### Timeouts: double Seconds → u64 Milliseconds

The C API uses `double timeout_sec`. The Zig version uses `u64 timeout_ms`
to avoid floating-point and stay consistent with the standard library:

```c
// C
kvr_connect(&reader, 5.0);                    // 5 seconds
kvr_read_pattern(&reader, ..., 2.0, ...);     // 2 seconds
```

```zig
// Zig
try reader.connect(5000);                      // 5000 ms
_ = reader.readPattern(..., 2000, ...);        // 2000 ms
```

### Pending Publish Tracking

The C writer tracks in-flight publishes with a fixed array and linear scan:

```c
static int kvw_pending_add(kvw_store_writer_t *kv, int mid) { ... }
static int kvw_pending_find(kvw_store_writer_t *kv, int mid) { ... }
static void kvw_pending_remove(kvw_store_writer_t *kv, int idx) { ... }
```

The Zig version uses the same algorithm but with optionals instead of `-1`:

```zig
fn pendingAdd(self: *Self, mid: c_int) ?usize { ... }   // null instead of -1
fn pendingFind(self: *Self, mid: c_int) ?usize { ... }
fn pendingRemove(self: *Self, idx: usize) void { ... }
```

This means the compiler forces you to handle the "not found" case:

```zig
if (self.pendingFind(mid)) |idx| {
    // idx is valid — use it
    self.pendingRemove(idx);
}
// vs C: if (idx >= 0) { kvw_pending_remove(kv, idx); }
```

### The Userdata Pointer Pattern

Same as the mqtt_queue library: `mosquitto_new` gets `null` for userdata
in `init()`, and `mosquitto_user_data_set` is called in `connect()` where
`self` is a stable pointer. This is the most important Zig-vs-C difference
when porting callback-heavy code:

```zig
pub fn init(cfg: Config) Error!Self {
    // ...
    const mosq = c.mosquitto_new(cfg.client_id.ptr, cfg.clean_session, null);
    // ...
    return self;  // returned by value — address changes!
}

pub fn connect(self: *Self, timeout_ms: u64) Error!void {
    c.mosquitto_user_data_set(self.mosq, @ptrCast(self));  // stable address
    // ...
}
```

### Batch Operations

The C batch functions use VLA-style local arrays. Zig uses fixed-size
arrays with comptime-known sizes:

```c
// C
int mids[KVW_MAX_BATCH];
bool queued[KVW_MAX_BATCH];
```

```zig
// Zig
var mids: [MAX_BATCH]c_int = [_]c_int{0} ** MAX_BATCH;
var queued: [MAX_BATCH]bool = [_]bool{false} ** MAX_BATCH;
```

The `[_]c_int{0} ** MAX_BATCH` syntax creates an array of MAX_BATCH zeros.
This is Zig's equivalent of `= {0}` aggregate initialization in C.

### Callback Function Signatures

Mosquitto callbacks need the C calling convention. In Zig 0.13 this is
`callconv(.C)` (uppercase). Unused parameters use `_`:

```c
// C
static void on_connect_cb(struct mosquitto *mosq, void *obj, int reason_code)
{
    (void)mosq;
    kvw_store_writer_t *kv = (kvw_store_writer_t *)obj;
    ...
}
```

```zig
// Zig
fn onConnectCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
    const self: *Self = @ptrCast(@alignCast(userdata));
    ...
}
```

The `_` parameter name tells Zig the value is intentionally unused.
`@ptrCast(@alignCast(userdata))` replaces `(kvw_store_writer_t *)obj`.

---

## API Summary

### Writer Methods

| C function          | Zig method             | Notes                          |
|---------------------|------------------------|--------------------------------|
| `kvw_config_init`   | `WriterConfig{}`       | Struct defaults replace init   |
| `kvw_init`          | `Writer.init(cfg)`     | Returns struct, not error code |
| `kvw_connect`       | `writer.connect(ms)`   | Error union, not bool          |
| `kvw_disconnect`    | `writer.disconnect()`  |                                |
| `kvw_destroy`       | `writer.deinit()`      |                                |
| `kvw_write_single`  | `writer.writeSingle()` | Returns bool (same as C)       |
| `kvw_write_batch`   | `writer.writeBatch()`  | Returns success count          |
| `kvw_delete_single` | `writer.deleteSingle()`|                                |
| `kvw_delete_batch`  | `writer.deleteBatch()` |                                |
| `kvw_update_single` | `writer.updateSingle()`|                                |
| `kvw_is_connected`  | `writer.isConnected()` |                                |

### Reader Methods

| C function          | Zig method              | Notes                          |
|---------------------|-------------------------|--------------------------------|
| `kvr_config_init`   | `ReaderConfig{}`        | Struct defaults replace init   |
| `kvr_init`          | `Reader.init(cfg)`      | Returns struct, not error code |
| `kvr_connect`       | `reader.connect(ms)`    | Error union, not bool          |
| `kvr_disconnect`    | `reader.disconnect()`   |                                |
| `kvr_destroy`       | `reader.deinit()`       |                                |
| `kvr_read_pattern`  | `reader.readPattern()`  | Returns count (same as C)      |
| `kvr_read_single`   | `reader.readSingle()`   | Returns bool (same as C)       |
| `kvr_read_all`      | `reader.readAll()`      | Returns count                  |
| `kvr_is_connected`  | `reader.isConnected()`  |                                |