# mqtt_kv_store — Zig MQTT Key/Value Store Library

A Zig port of the C `kv_store_reader` / `kv_store_writer` library.
Uses MQTT retained messages as a distributed key/value store.

For C-to-Zig concept explanations, see the README in the `mqtt_queue` sister project.

---

## Prerequisites

```bash
sudo apt install libmosquitto-dev
```

Tested with Zig 0.13.

---

## Project Structure

```
mqtt_kv_store_zig/
├── build.zig                    ← Build script (replaces Makefile)
├── build.zig.zon                ← Package manifest
├── src/
│   ├── root.zig                 ← Public API re-exports
│   ├── kv_store_writer.zig      ← Writer: write/delete/update retained KV pairs
│   └── kv_store_reader.zig      ← Reader: subscribe + collect retained messages
└── test/
    └── kv_store_test.zig        ← Integration test (populates broker, reads back)
```

After building:

```
zig-out/
└── lib/
    ├── libmqtt_kv_store.a       ← Static library
    └── libmqtt_kv_store.so      ← Shared library
```

---

## File Mapping (C → Zig)

| C file               | Zig file               | Notes                                    |
|-----------------------|------------------------|------------------------------------------|
| `kv_store_writer.h`  | `kv_store_writer.zig`  | Config, Writer struct, all methods        |
| `kv_store_writer.c`  | `kv_store_writer.zig`  | Same file (Zig has no .h/.c split)        |
| `kv_store_reader.h`  | `kv_store_reader.zig`  | Config, Entry, Reader struct, all methods |
| `kv_store_reader.c`  | `kv_store_reader.zig`  | Same file                                 |
| `kv_store_test.c`    | `kv_store_test.zig`    | Integration test driver                   |
| `Makefile`           | `build.zig`            | Build system                              |

---

## Build Commands

```bash
zig build                          # build .a and .so
zig build -Doptimize=ReleaseFast   # optimized build
zig build test                     # run unit tests (no broker needed)
zig build run-test                 # run integration test (broker on :1883)
```

---

## API Quick Reference

### Writer

```zig
const kv = @import("mqtt_kv_store");

kv.libInit();
defer kv.libCleanup();

var writer = try kv.Writer.init(.{ .client_id = "my-writer" });
defer writer.deinit();

try writer.connect(5000);
defer writer.disconnect();

// Write a retained key
_ = writer.writeSingle("kv/config/host", "192.168.1.1", .at_least_once, true, 5000);

// Delete a key
_ = writer.deleteSingle("kv/config/host", 5000);
```

### Reader

```zig
var reader = try kv.Reader.init(.{ .client_id = "my-reader" });
defer reader.deinit();

try reader.connect(5000);
defer reader.disconnect();

// Read with wildcard + sentinel
var entries: [kv.MAX_ENTRIES]kv.Entry = undefined;
const sents = [_][:0]const u8{"kv/config/.sentinel"};
const n = reader.readPattern("kv/config/+", 1, 2000, &sents, true, &entries);

for (0..n) |i| {
    // entries[i].topicSlice() and entries[i].valueSlice() return []const u8
}

// Read single exact topic
var buf: [kv.MAX_VALUE_LEN]u8 = undefined;
if (reader.readSingle("kv/system/version", 1000, &buf)) {
    const version = std.mem.sliceTo(&buf, 0);
}
```

---

## Key Design Decisions

**Fixed-size buffers for entries** — Like the C version, Entry uses fixed-size
arrays (`[256]u8` for topic, `[4096]u8` for value) instead of heap-allocated
slices. This keeps the reader zero-allocation in the message callback path and
matches the C library's behavior. Use `entry.topicSlice()` and
`entry.valueSlice()` to get proper `[]const u8` slices.

**Userdata set in connect, not init** — Same pattern as the mqtt_queue library.
`init()` returns by value, so the struct address changes after return.
`mosquitto_user_data_set` is called in `connect()` where `self` is a stable pointer.

**Timeouts in milliseconds** — The C version uses `double timeout_sec`. The Zig
version uses `u64 timeout_ms` to stay consistent with the mqtt_queue library
and avoid floating-point.

**Sentinel topics** — Passed as `[]const [:0]const u8` (a slice of
null-terminated strings) instead of a NULL-terminated C array.