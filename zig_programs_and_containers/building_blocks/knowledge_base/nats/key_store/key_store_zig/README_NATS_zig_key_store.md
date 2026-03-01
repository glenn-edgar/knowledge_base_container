# key_store_zig — Zig Bindings for NATS JetStream KeyStore / KbStore / JobQueue

Idiomatic Zig wrappers around the C libraries in `key_store_c/`.  Links against
the existing `.so` / `.a` files via Zig's native C interop—no protocol
reimplementation, no FFI boilerplate.

## Prerequisites

| Dependency | Purpose |
|---|---|
| Zig ≥ 0.13 | Build toolchain |
| `key_store_c/build/` | Compiled C libraries (`libnats_key_store`, `libnats_kb_store`, `libnats_job_queue`) |
| `libnats` (C client) | Transitive dependency of the C libs |
| `libcjson` | JSON handling in C libs |
| NATS server w/ JetStream | `docker run -p 4222:4222 nats:latest -js` |

## Directory layout

```
key_store/                         # top-level project directory
├── key_store_c/                   # existing C project
│   ├── Makefile
│   ├── include/
│   │   ├── nats_key_store.h
│   │   ├── nats_kb_store.h
│   │   └── nats_job_queue.h
│   ├── src/
│   │   ├── nats_key_store.c
│   │   ├── nats_kb_store.c
│   │   └── nats_job_queue.c
│   ├── test/
│   │   ├── test_nats_kv.c
│   │   └── test_job_queue.c
│   └── build/
│       ├── libnats_key_store.so / .a
│       ├── libnats_kb_store.so / .a
│       └── libnats_job_queue.so / .a
│
└── key_store_zig/                 # this project
    ├── build.zig
    ├── build.zig.zon
    ├── README.md
    ├── src/
    │   ├── root.zig               # public module — import this
    │   ├── c_api.zig              # @cImport bridge
    │   ├── status.zig             # error mapping (C → Zig)
    │   ├── key_store.zig          # KeyStore wrapper
    │   ├── kb_store.zig           # KbStore (Knowledge Base) wrapper
    │   └── job_queue.zig          # JobQueue wrapper
    └── test/
        ├── integration.zig        # full test suite (mirrors C tests)
        └── example.zig            # demo program
```

## Building

```bash
cd key_store/key_store_zig
zig build
```

Override paths if your layout differs:

```bash
zig build -Dc-include=../key_store_c/include -Dc-lib=../key_store_c/build
```

### Build targets

```bash
zig build                 # shared + static lib
zig build test            # compile-time unit tests (no NATS needed)
zig build integration     # integration tests (needs NATS)
zig build example         # build + run the demo
```

Set `LD_LIBRARY_PATH` if linking against shared libs:

```bash
export LD_LIBRARY_PATH=../key_store_c/build:$LD_LIBRARY_PATH
```

## Quick start

```zig
const nats = @import("nats_zig");

pub fn main() !void {
    // -- KeyStore --
    var ks = try nats.KeyStore.init(.{
        .server = "nats://127.0.0.1:4222",
        .bucket = "my_bucket",
    });
    defer ks.deinit();
    try ks.connect();

    _ = try ks.put("greeting", "\"Hello from Zig!\"");

    const val = try ks.getRaw("greeting");
    defer nats.KeyStore.freeRaw(val);
    std.debug.print("got: {s}\n", .{val});

    // -- KbStore --
    var kb = try nats.KbStore.init(
        "nats://127.0.0.1:4222",
        "knowledge",
        "My KB",
    );
    defer kb.deinit();
    {
        var kb_ks = kb.getKeyStore();
        try kb_ks.connect();
    }

    var result = try kb.store(
        "project.team",
        "person",
        "alice",
        "{\"type\":\"member\"}",
        "{\"id\":\"1\",\"data\":{\"name\":\"Alice\"}}",
        true,
    );
    defer result.deinit();

    // -- JobQueue --
    var jq = try nats.JobQueue.init(&ks, "my-worker");
    defer jq.deinit();

    const job_id = try jq.submit("{\"task\":\"process\"}", .{
        .queue   = "work",
        .priority = 5,
    });
    defer nats.JobQueue.freeJobId(job_id);

    const queues = [_][:0]const u8{"work"};
    var job = try jq.claimJob(&queues);
    defer job.deinit();
    // ... process ...
}
```

## API overview

### `nats.KeyStore`

| Method | Description |
|---|---|
| `init(Config)` | Create (does not connect) |
| `connect()` / `disconnect()` | Session management |
| `put(key, value)` → `u64` | Store string, returns revision |
| `getRaw(key)` → `[:0]u8` | Retrieve (caller frees with `freeRaw`) |
| `getAlloc(alloc, key)` → `[]u8` | Retrieve into Zig allocator |
| `delete(key)` | Delete key |
| `exists(key)` → `bool` | Check existence |
| `keys(pattern)` → `KeyList` | Glob-match listing |
| `increment(key, delta)` → `i64` | Atomic counter |
| `decrement(key, delta)` → `i64` | Atomic counter |
| `*Sync(...)` variants | Connect → op → disconnect |

### `nats.KbStore`

| Method | Description |
|---|---|
| `init(server, bucket, desc)` | Create with internal KeyStore |
| `store(topic, label, node, ...)` | Store KB entry |
| `get(key)` → `KbEntry` | Retrieve label + node JSON |
| `delete(key)` | Delete entry |
| `listKeys(topic?)` → `KeyList` | List by base topic |
| `getStats()` → `KbStats` | Topic counts |
| `validateTopic/LabelName/NodeName` | Input validation |
| `validateKeyFormat(key)` → `bool` | Full key validation |
| `popKey(key)` → `[:0]u8` | Remove last 2 segments |

### `nats.JobQueue`

| Method | Description |
|---|---|
| `init(ks, worker_id?)` | Create (borrows KeyStore) |
| `submit(payload, opts)` → job ID | Enqueue a job |
| `claimJob(queues)` → `Job` | Claim next by priority |
| `completeJob(id, result?)` → `bool` | Mark completed |
| `failJob(id, error?)` → `bool` | Fail (auto-retries) |
| `cancelJob(id)` → `bool` | Cancel pending job |
| `getJob(id)` → `Job` | Retrieve by ID |
| `getStats(queue)` → `JqStats` | Queue counters |
| `getActiveWorkers(staleness)` | Worker monitoring |
| `cleanupStaleJobs(timeout)` | Reset stuck jobs |

## Error handling

All fallible operations return `nats.Error`, which maps 1:1 from the C
`ks_status_t` enum:

```zig
const val = ks.getRaw("missing") catch |err| switch (err) {
    error.NotFound   => { /* handle */ },
    error.ConnectionFailed => { /* reconnect? */ },
    else => return err,
};
```

## Memory ownership

| Source | Free with |
|---|---|
| `getRaw()` | `KeyStore.freeRaw(slice)` |
| `getAlloc()` | `allocator.free(slice)` |
| `submit()` job ID | `JobQueue.freeJobId(id)` |
| `KbEntry` | `entry.deinit()` |
| `KeyList` | `list.deinit()` |
| `StoreResult` | `result.deinit()` |
| `Job` | `job.deinit()` |

## License

Same as the underlying C libraries (MIT).