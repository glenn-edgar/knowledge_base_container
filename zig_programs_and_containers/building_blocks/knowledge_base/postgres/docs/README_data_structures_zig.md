# Knowledge Base PostgreSQL — Zig Library

Idiomatic Zig wrapper for the Knowledge Base C library (`data_structures_c`, PostgreSQL backend). This library provides runtime access to knowledge base data structures stored in PostgreSQL, designed for embedded and distributed control systems where multiple processes may concurrently access the same knowledge base.

## Overview

The knowledge base uses PostgreSQL with the `ltree` extension to organize hierarchical configuration data. The construct modules (Python or LuaJIT) build the database schema and populate it with node definitions, pre-allocated slots, and structural metadata. This Zig library provides the runtime API to read, write, and manage the data at execution time.

The Zig wrapper encapsulates the underlying C library (`data_structures_c`) and provides idiomatic Zig access with error unions, RAII-style resource management via `defer`, sentinel-terminated strings, and optional pointer safety. All C library functionality is available through the wrapper — there is no need to interact with the C API directly, though raw bindings remain accessible via `kb.c` for advanced use.

## Architecture

```
construct_kb (build time — Python or LuaJIT)
    │
    ▼
PostgreSQL (knowledge_base DB with ltree extension)
    │
    ▼
data_structures_c  (C library: libkb_data_structures.a / .so, links libpq)
    │
    ▼
data_structures_zig (this library: Zig wrapper via @cImport)
    │
    ▼
ChainTree / application code (Zig)
```

### PostgreSQL and ltree

The knowledge base runs on PostgreSQL with the **ltree** extension, which provides native hierarchical label tree path columns and operators. Every node has a dot-separated path (e.g. `kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status`) stored as an ltree column. The ltree extension enables path-aware queries — ancestor/descendant matching, subtree selection, and pattern-based path filtering — directly in SQL without recursive CTEs or string manipulation.

### How It Works: Discovery Then Data

The knowledge base follows a two-step pattern: **discover** node paths using the Search module, then **operate** on those paths using the data modules (Status, JobQueue, Stream, etc.). You never hardcode paths — the Search module finds them by label, name, or property.

```zig
// Step 1: Discover
var ks = try kb.Search.create(&conn, "knowledge_base");
var paths = try ks.findStatusPaths();   // returns all KB_STATUS_FIELD nodes
defer paths.deinit();

// Step 2: Operate
const status = kb.Status.init(&conn, "knowledge_base");
try status.setDefault(paths.get(0).?, "{\"temp\":72.5}");
```

## Modules

### Connection — PostgreSQL Connection Management

Foundation module providing PostgreSQL connection management via libpq. Handles automatic `BEGIN` on first query, commit/rollback, and result set management.

```zig
var conn = try kb.Connection.connectParams(
    "localhost", "5432", "knowledge_base", "gedgar", password,
);
defer conn.disconnect();

try conn.begin();
// ... operations ...
try conn.commit();
```

### Search — Knowledge Base Node Discovery

Queries the main knowledge base table to find nodes by label, name, property values, and ltree path patterns. This is the entry point for all other modules — you discover paths here, then pass them to the data modules.

**Specialized finders** for each node type:

| Finder Method              | Discovers             |
|----------------------------|-----------------------|
| `findStatusPaths()`        | `KB_STATUS_FIELD`     |
| `findStreamPaths()`        | `KB_STREAM_FIELD`     |
| `findJobPaths()`           | `KB_JOB_QUEUE`        |
| `findRpcServerPaths()`     | `KB_RPC_SERVER_FIELD` |
| `findRpcClientPaths()`     | `KB_RPC_CLIENT_FIELD` |
| `findBitStructurePaths()`  | `KB_BIT_MASK`         |
| `findDocumentPaths()`      | `KB_JSONB_FIELD`      |
| `findLinkPaths()`          | Link table entries    |
| `findLinkMountPaths()`     | Mount table entries   |
| `findNodePaths(label)`     | Any label             |

**CTE filter chain** for custom queries — filters are additive (AND chain):

| Filter Method         | Mechanism     | SQL Effect |
|-----------------------|---------------|------------|
| `label(label)`        | exact match   | `WHERE label = ?` |
| `name(name)`          | exact match   | `WHERE name = ?` |
| `propertyKey(key)`    | json_extract  | `WHERE properties ? 'key'` |
| `propertyValue(k,v)`  | json_extract  | `WHERE properties->>'key' = ?` |
| `path(pattern)`       | ltree match   | `WHERE path ~ 'pattern'` |

```zig
var ks = try kb.Search.create(&conn, "knowledge_base");
defer ks.destroy();

// Specialized finder
var status_paths = try ks.findStatusPaths();
defer status_paths.deinit();

// Custom CTE filter chain
ks.clear();
try ks.label("KB_STATUS_FIELD");
try ks.name("info2_status");
try ks.execute();
const rs = ks.results();   // nullable *kb_resultset_t
```

### Status — Key-Value Store

Table: `{database}_status` — One row per declared status node. Stores arbitrary JSONB data keyed by ltree path.

- `get(path)` — Read with `FOR SHARE` lock. Returns owned `?[:0]const u8`.
- `set(path, data, max_retries, base_delay_ms)` — Atomic UPSERT with retry.
- `setDefault(path, data)` — UPSERT with default retry (3 retries, 100ms base delay).

```zig
const status = kb.Status.init(&conn, "knowledge_base");

try status.setDefault(path, "{\"temp\":72.5,\"unit\":\"F\"}");

if (try status.get(path)) |data| {
    // data is a C-allocated string
    defer kb.freeCStr(@ptrCast(@constCast(data.ptr)));
    // use data...
}
```

### Stream — Circular Buffer Time-Series

Table: `{database}_stream` — Pre-allocated rows forming a fixed-length circular buffer. New data overwrites the oldest entry.

- `push / pushDefault` — Write to oldest slot (`FOR UPDATE SKIP LOCKED`)
- `list(path, start_time, end_time)` — List valid entries with optional time-range filter
- `latest(path)` — Most recent valid entry
- `range(path, start, end)` — Entries within a time window
- `getById(path, id)` — Fetch specific row
- `count / countTotal` — Valid / total slot counts
- `statistics(path)` — Aggregate stats (total/valid/invalid counts, min/max timestamps)
- `clear / clearDefault` — Reset all slots

```zig
const stream = kb.Stream.init(&conn, "knowledge_base");

try stream.pushDefault(path, "{\"temp\":72.5}");

var rs = try stream.latest(path);
defer rs.deinit();
if (rs.get(0, "data")) |data| {
    // use data...
}

const stats = try stream.statistics(path);
defer stats.deinit();
```

### JobQueue — Fixed-Length Job Queue

Table: `{database}_job` — Pre-allocated job slots with a push/peek/complete lifecycle.

State machine: `free → queued → active → completed (free)`

- `push / pushDefault` — Fill a free slot (`FOR UPDATE SKIP LOCKED`)
- `peek / peekDefault` — Claim oldest pending job. Returns `PeekResult` with `.found`, `.id`, `.data`.
- `complete / completeDefault` — Recycle slot back to free
- `clear / clearDefault` — Reset all slots
- `freeCount / queuedCount / activeCount` — Slot counts by state
- `listPending / listActive` — List jobs by state

```zig
const jq = kb.JobQueue.init(&conn, "knowledge_base");

try jq.pushDefault(path, "{\"task\":\"backup\"}");

var info = try jq.peekDefault(path);
defer info.deinit();
if (info.found) {
    // process info.data...
    try jq.completeDefault(info.id);
}
```

### RpcServer — RPC Server Request Queue

Table: `{database}_rpc_server` — Pre-allocated request slots with priority ordering.

State machine: `empty → new_job → processing → empty`

- `push(path, uuid, action, payload, tx_tag, priority, client_path, ...)` — Fill slot with `SERIALIZABLE` isolation + advisory lock
- `peek / peekDefault` — Claim highest-priority job. Returns `PeekResult` with `.found`, `.id`, `.priority`, `.rpc_action`, `.request_payload`, etc.
- `complete / completeDefault` — Reset slot
- `clear / clearDefault` — Reset all slots
- `countNew / countProcessing` — Slot counts by state

```zig
const rpc = kb.RpcServer.init(&conn, "knowledge_base");

try rpc.push(path, &uuid, "process_data", "{\"input\":\"test\"}", "tx_001", 1,
    client_path, kb.default_max_retries, kb.default_base_delay_ms);

var job = try rpc.peekDefault(path);
defer job.deinit();
if (job.found) {
    // process job.request_payload, job.rpc_action...
    try rpc.completeDefault(path, job.id);
}
```

### RpcClient — RPC Client Reply Queue

Table: `{database}_rpc_client` — Pre-allocated reply slots for receiving RPC responses.

- `pushReply(path, uuid, server_path, action, tx_tag, payload, ...)` — Fill a free slot
- `peekReply / peekReplyDefault` — Consume oldest result. Returns `PeekReplyResult` with `.found`, `.id`, `.response_payload`, etc.
- `clear / clearDefault` — Reset all slots
- `freeSlots / queuedSlots` — Slot counts

```zig
const rpc_client = kb.RpcClient.init(&conn, "knowledge_base");

try rpc_client.pushReply(path, &uuid, "server.path", "process_data",
    "tx_001", "{\"result\":\"ok\"}", kb.default_max_retries, kb.default_base_delay_ms);

var reply = try rpc_client.peekReplyDefault(path);
defer reply.deinit();
if (reply.found) {
    // use reply.response_payload...
}
```

### BitStructures — Bit Mask Operations + S-Expression Evaluator

Table: `bit_mask_table` (global) — 64-bit integer masks keyed by `node_id`.

- `getMask / setMask / setMaskDefault` — Read/write entire 64-bit mask
- `getBit / setBit / setBitDefault` — Read/write individual bits (0–63)
- `evalSexpr(node_id, expr, bit_defs, prev_mask)` — Evaluate boolean S-expressions over bit masks

`setBit` uses atomic read-modify-write under `FOR UPDATE`.

**S-expression format** (JSON arrays):

```json
["and", ["bit", 0], ["not", ["bit", 1]]]
["or", ["bit", "sensor_a"], ["bit_changed", 4]]
```

Operators: `and`, `or`, `not`, `bit` (test single bit), `bit_changed` (compare with previous mask).

```zig
const bits = kb.BitStructures.init(&conn, "knowledge_base");

try bits.setMaskDefault(node_id, 0);
try bits.setBitDefault(node_id, 0, true);
try bits.setBitDefault(node_id, 4, true);

const mask = try bits.getMask(node_id);    // 17
const b0 = try bits.getBit(node_id, 0);   // true
const b1 = try bits.getBit(node_id, 1);   // false

// S-expression evaluation
const result = try bits.evalSexpr(
    node_id,
    "[\"and\", [\"bit\", 0], [\"bit\", 4]]",
    null,
    0,
);
// result == true
```

### Document — JSONB Document Operations

Table: `{database}_document` — JSONB documents keyed by ltree path and optional type tag.

**Core operations:** get, set, delete key, delete path. All writes use `FOR UPDATE`.

**Key existence:** `hasKey`. Multi-key checks (`has_any_keys`, `has_all_keys`) available via raw C API.

**Containment:** `contains`, `containedBy`.

**JSONPath:** `pathExists`, `pathQuery`.

**Array operations:** `arrayAppend`, `arrayPrepend`, `arrayContains`.

**Queue (FIFO):** `enqueue`, `dequeue`, `queuePeek`, `queueSize`, `queueIsEmpty`, `queueClear`, `queueGetAll`. The queue is stored as a JSONB array inside the document — `enqueue` appends, `dequeue` removes from index 0.

**Stack (LIFO):** `stackPush` (prepend to index 0), `stackPop` (remove from index 0).

**Metadata:** `getMetadata`, `setMetadata` — access a `_metadata` sub-document.

```zig
const doc = kb.Document.init(&conn, "knowledge_base");

// Set entire document
try doc.set(path, "", "{\"name\":\"Test\",\"tags\":[\"zig\",\"postgres\"]}", true, null);

// Get nested field as text
if (try doc.get(path, "name", true, null)) |val| {
    defer kb.freeCStr(@ptrCast(@constCast(val.ptr)));
    // val == "Test"
}

// Containment and JSONPath
const has_admin = try doc.contains(path, "{\"role\":\"admin\"}", null);
const role_ok = try doc.pathExists(path, "$.role ? (@ == \"admin\")", null);

// Queue (FIFO)
try doc.enqueue(path, "{\"task\":\"A\"}", null, null);
try doc.enqueue(path, "{\"task\":\"B\"}", null, null);
if (try doc.dequeue(path, null, null)) |val| {
    defer kb.freeCStr(@ptrCast(@constCast(val.ptr)));
    // val contains task A (FIFO)
}

// Stack (LIFO)
try doc.stackPush(path, "{\"msg\":\"first\"}", null, null);
try doc.stackPush(path, "{\"msg\":\"second\"}", null, null);
if (try doc.stackPop(path, null, null)) |val| {
    defer kb.freeCStr(@ptrCast(@constCast(val.ptr)));
    // val contains "second" (LIFO)
}
```

### LinkTable / LinkMountTable — Cross-KB Link Resolution

Tables: `{database}_link` and `{database}_link_mount` — Read-only at runtime. Populated by construct modules to define links between knowledge bases.

- `queryByPath(path)` — Find links or mounts by parent path
- `queryByName(name)` — Find links or mounts by name
- `decodeNodes(path)` — Resolve link names to mount paths (LinkTable only)

```zig
const lt = kb.LinkTable.init(&conn, "knowledge_base");

var rs = try lt.queryByPath("kb1.header2_link.header2_name");
defer rs.deinit();

var decoded = try lt.decodeNodes("kb1.header2_link.header2_name");
defer decoded.deinit();
for (0..decoded.len()) |i| {
    if (decoded.get(i)) |p| {
        // p is a resolved mount path
    }
}
```

### ResultSet — Query Results

Row-oriented query result wrapper for `kb_resultset_t`. Returned by Search, Stream, and LinkTable operations.

- `rowCount()` — Number of rows
- `colCount()` — Number of columns
- `get(row, col_name)` — Get string value by column name
- `getInt(row, col_name)` — Parse as `c_int`
- `getInt64(row, col_name)` — Parse as `i64`
- `getBool(row, col_name)` — Parse as `bool`
- `deinit()` — Free the underlying C result set

## Module Summary

| Zig Struct       | PostgreSQL Table              | Purpose |
|------------------|-------------------------------|---------|
| `Connection`     | —                             | libpq connection wrapper, transaction control |
| `Search`         | `{db}` (main KB table)        | CTE filter chain, node discovery |
| `Status`         | `{db}_status`                 | Key-value store (path → JSON) |
| `JobQueue`       | `{db}_job`                    | Pre-allocated job slots with state machine |
| `Stream`         | `{db}_stream`                 | Circular buffer time-series |
| `BitStructures`  | `bit_mask_table`              | 64-bit masks + S-expression evaluator |
| `RpcServer`      | `{db}_rpc_server`             | 3-state job queue (empty→new→processing) |
| `RpcClient`      | `{db}_rpc_client_table`       | 2-state reply toggle |
| `LinkTable`      | `{db}_link`                   | Cross-KB link queries |
| `LinkMountTable` | `{db}_link_mount`             | Mount point resolution |
| `Document`       | `{db}_document`               | JSONB document ops, queue/stack abstractions |
| `ResultSet`      | —                             | Row-oriented query results with named columns |

## Concurrent Write Safety

All write operations use PostgreSQL row-level locking to prevent lost updates when multiple processes access the same knowledge base:

| Pattern | Used By | Behavior |
|---------|---------|----------|
| `FOR UPDATE SKIP LOCKED` | push/peek ops | Non-blocking: skips locked rows, retries on next attempt |
| `FOR UPDATE` | complete/clear/set ops | Blocking: waits for lock, wrapped in retry with backoff |
| `SERIALIZABLE` + advisory lock | RPC server push | Strongest isolation for priority slot allocation |
| Atomic UPSERT | Status set | `INSERT ... ON CONFLICT DO UPDATE` is inherently atomic |
| `FOR SHARE` | Status get | Prevents reading mid-update |

All write functions accept `max_retries` and `base_delay_ms` parameters for retry with exponential backoff. The wrapper provides `*Default` convenience methods using `max_retries=3, base_delay_ms=100`. These defaults are available as `kb.default_max_retries` and `kb.default_base_delay_ms`.

## Error Handling

All functions return `KbError!T` — Zig error unions wrapping the C error codes:

| Zig Error        | C Error           | Meaning |
|------------------|-------------------|---------|
| `error.NullArg`  | `KB_ERR_NULL_ARG` | NULL argument passed |
| `error.Pg`       | `KB_ERR_PG`       | PostgreSQL error |
| `error.NotFound` | `KB_ERR_NOT_FOUND`| Row/path not found |
| `error.Json`     | `KB_ERR_JSON`     | JSON parse/encode error |
| `error.NoMem`    | `KB_ERR_NOMEM`    | Allocation failed |
| `error.Busy`     | `KB_ERR_BUSY`     | Lock contention / retry exhausted |
| `error.Invalid`  | `KB_ERR_INVALID`  | Invalid parameter |
| `error.Overflow` | `KB_ERR_OVERFLOW` | Buffer overflow |
| `error.State`    | `KB_ERR_STATE`    | Invalid state transition |

Use `kb.errorString(err)` to get a human-readable description.

## Resource Ownership

- **Connection** owns the `PGconn` handle; call `disconnect()` when done (or use `defer`).
- **Search** owns CTE state and last result set; call `destroy()` when done.
- **Subsystem structs** (`Status`, `JobQueue`, `Stream`, etc.) are non-owning views — they borrow from `Connection` and are initialized with `init(&conn, database)`.
- **PathList** owns C-allocated path arrays; call `deinit()`.
- **PeekResult / PeekReplyResult** own C-allocated strings; call `deinit()`.
- **ResultSet** owns C-allocated result data; call `deinit()`.
- **Heap strings** returned as `?[:0]const u8` from `get`/`dequeue`/`pop` are C-allocated; free with `kb.freeCStr(@ptrCast(@constCast(ptr)))`.
- Raw C bindings are always available via `kb.c` for any functionality not directly wrapped.

## Prerequisites

- **Zig** ≥ 0.13
- **Built `data_structures_c`** producing `libkb_data_structures.a` (or `.so`)
- **PostgreSQL** with the `ltree` extension installed and enabled
- **libpq** development headers/libraries (`libpq-dev` on Debian/Ubuntu)
- **cJSON** library (`libcjson-dev` on Debian/Ubuntu)
- A knowledge base **constructed by the `construct_*` modules** (Python or LuaJIT)

```bash
# Install dependencies on Debian/Ubuntu
sudo apt-get install libpq-dev libcjson-dev
```

## Build

```bash
cd data_structures_zig
zig build                                    # static + shared libs + example
zig build -Dc_root=../data_structures_c      # explicit C library path
zig build -Dpg_include=/usr/include/postgresql/16   # custom PostgreSQL headers
```

Outputs in `zig-out/`:

- `lib/libkb_pg_zig.a` — Static library
- `lib/libkb_pg_zig.so` — Shared library
- `bin/kb_pg_example` — Example executable
- `bin/test_driver` — Test executable (via `zig build build-test-driver`)

## Testing

Tests require a live PostgreSQL database with the knowledge base already constructed.

```bash
# Construct knowledge base (from construct_kb project)
POSTGRES_PASSWORD=secret luajit test_construct_data_tables.lua False True

# Build test driver to zig-out/bin/
zig build build-test-driver

# Run test driver directly
POSTGRES_PASSWORD=secret ./zig-out/bin/test_driver

# Or build + run via zig build
POSTGRES_PASSWORD=secret zig build test-driver

# Run wrapper compile-time tests (no database needed)
zig build test

# Run all tests
POSTGRES_PASSWORD=secret zig build test-all
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `knowledge_base` | Database name |
| `POSTGRES_USER` | `gedgar` | Database user |
| `POSTGRES_PASSWORD` | (none, required) | Database password |
| `KB_DATABASE` | `knowledge_base` | Knowledge base prefix (table prefix) |

**Test coverage (280+ assertions):**

- **Search/Discovery** — Find nodes by label for all types (status, stream, job, RPC, bit mask, document, links)
- **Status** — Get, set, update, verify JSONB values
- **Job Queue** — Clear, push, count free/queued, peek (claim), complete (recycle), verify counts
- **Stream** — Clear, count total slots, push 3 entries, count valid, list, latest, statistics, clear and verify
- **RPC Server** — Clear, push with UUID and priority, count new, peek by priority order, complete, verify empty
- **RPC Client** — Clear, push reply, count queued, peek (consume), verify payload, clear
- **Bit Structures** — KB search for bit mask nodes, extract `record_id` from properties, get/set mask, set individual bits, verify mask arithmetic, S-expression evaluation (`and`, `or`, `not`, `bit`, `bit_changed`)
- **Document** (×3 labels) — Set/get entire document, key access, nested path access, text vs JSON retrieval, key existence checks (`has_key`, `has_any_keys`, `has_all_keys`), containment, array containment, JSONPath predicates and queries, set/delete keys and paths, array element expansion, queue (FIFO) operations, stack (LIFO) operations, empty queue/stack edge cases
- **Link Table** — Query links by path, decode nodes, verify mount resolution

## License

MIT