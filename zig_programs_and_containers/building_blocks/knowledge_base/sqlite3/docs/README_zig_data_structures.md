# data_structures_zig

Idiomatic Zig wrapper for the Knowledge Base C library (`data_structures_c`).

## Overview

The knowledge base system is a two-phase architecture:

**Phase 1 — Construction.** The `construct_kb` programs build a SQLite database containing a main knowledge base table and a set of auxiliary runtime tables (status, job queue, stream, bit mask, RPC server queue, RPC client queue, link table, link mount table). The constructor defines the structure of each table — number of slots, initial properties, path hierarchy, and metadata — so that the runtime only needs to read and operate on pre-allocated resources.

**Phase 2 — Runtime (this library).** The Zig wrapper opens the constructed database via the C library and provides:

1. **Table discovery.** The knowledge base describes every auxiliary table — its path, label type, metadata, and relationships. The library uses CTE (Common Table Expression) progressive filtering to search the knowledge base and locate tables by label, name, path pattern, property values, or link relationships.

2. **Runtime operations.** Once a table is discovered, the library provides CRUD operations for each table type: status read/write, job queue push/peek/complete, stream circular buffer, bit mask get/set with change tracking, RPC server and client queue management, and link/mount traversal.

3. **S-expression evaluation.** Bit mask conditions can be expressed as S-expressions and evaluated against live bit data — enabling declarative guard conditions in ChainTree behavior trees and state machines.

4. **Driver construction.** Higher-level code (ChainTree behavior trees, state machines, sequential controllers) dynamically locates backing tables at startup and constructs drivers without hardcoded table references.

## Architecture

The Zig library wraps the C port (`data_structures_c`) which itself mirrors the original LuaJIT and Python implementations. The C library is the shared core; the Zig wrapper provides idiomatic Zig access with error unions, RAII-style resource management, and optional-pointer safety.

```
construct_kb (build time)
    │
    ▼
knowledge_base.db (SQLite + ltree extension)
    │
    ▼
data_structures_c  (C library: libkb.a / libkb.so)
    │
    ▼
data_structures_zig (this library: Zig wrapper via @cImport)
    │
    ▼
ChainTree / application code (Zig)
```

### The ltree Extension

The knowledge base uses **ltree.so**, a SQLite extension that brings PostgreSQL-style ltree (label tree) path querying to SQLite. Every node in the knowledge base has a dot-separated hierarchical path (e.g. `kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status`). The ltree extension enables path-aware queries — ancestor/descendant matching, subtree selection, and pattern-based path filtering — that would otherwise require complex string manipulation or recursive CTEs.

The `Search` module builds its CTE filter chain using both ltree path operations and `json_extract` on the properties column, combining hierarchical path traversal with JSON property matching in a single query pipeline. This gives the knowledge base Cypher-like graph query capability using standard SQLite with the ltree extension.

When opening a database, the `ltree_path` parameter specifies where to find the extension:

```zig
// Load ltree extension
var ds = try kb.DataStructures.create("knowledge_base.db", "knowledge_base", "./ltree");
```

## Runtime Modules

| Zig Type          | C Module                    | Purpose |
|-------------------|-----------------------------|---------|
| `DataStructures`  | `kb_data_structures`        | Aggregator facade — creates all subsystems, delegates operations |
| `Search`          | `kb_query_support`          | CTE progressive filter chain. Filters by KB name, label, node name, property key/value, path pattern, starting path, has_link, has_link_mount |
| `StatusTable`     | `kb_status_table`           | Status data CRUD — get/set JSON data by path with UPSERT |
| `JobQueue`        | `kb_job_queue`              | Job queue — push (find free slot), peek (highest priority, oldest first), complete (return to free), clear |
| `Stream`          | `kb_stream`                 | Circular buffer stream — push data at write_index, list with optional time range, clear |
| `BitMaskOps`      | `bit_mask_rt_operations`    | Bit-level operations — get/set individual bits, full mask read/write, change mask tracking |
| `BitStructures`   | `kb_bit_structures`         | Orchestrator combining Search + bit mask operations + S-expression evaluation |
| `RpcServer`       | `kb_rpc_server`             | RPC server queue — 4-state machine (empty → new_job → processing → empty) |
| `RpcClient`       | `kb_rpc_client`             | RPC client reply queue — 2-state toggle (free ↔ queued) |
| `LinkTable`       | `kb_link_table`             | Link table queries — find records by link_name or node_path |
| `LinkMountTable`  | `kb_link_mount_table`       | Link mount table queries — find records by link_name or mount_path |

Standalone functions: `sexprEval` (S-expression evaluator), `uuid4`, `timestampNow`, `openDatabase`/`closeDatabase`, and the `Sql` namespace for raw query execution.

## Table Naming Convention

The constructor creates tables using a base name (typically `knowledge_base`) with suffixes:

| Table              | Name                                  |
|--------------------|---------------------------------------|
| Main KB            | `knowledge_base`                      |
| Status             | `knowledge_base_status_table`         |
| Job queue          | `knowledge_base_job_queue`            |
| Stream             | `knowledge_base_stream_table`         |
| Bit mask store     | `knowledge_base_bit_mask_store`       |
| RPC server queue   | `knowledge_base_rpc_server_queue`     |
| RPC client queue   | `knowledge_base_rpc_client_queue`     |
| Link table         | `knowledge_base_link_table`           |
| Link mount table   | `knowledge_base_link_mount_table`     |

## CTE Search Filters

The `Search` module (wrapping `kb_query_support`) builds SQL using a chain of CTEs that progressively narrow results. The filter chain combines two query mechanisms: **ltree path operations** for hierarchical path matching and **`json_extract`** for querying JSON-encoded properties stored in each node's `properties` column.

| Filter Method         | Mechanism     | SQL Effect |
|-----------------------|---------------|------------|
| `kb(name)`            | exact match   | `WHERE knowledge_base = ?` |
| `label(label)`        | exact match   | `WHERE label = ?` |
| `name(name)`          | exact match   | `WHERE name = ?` |
| `propertyKey(key)`    | json_extract  | `WHERE json_extract(properties, '$.key') IS NOT NULL` |
| `propertyValue(k,v)`  | json_extract  | `WHERE json_extract(properties, '$.key') = ?` |
| `path(expr)`          | ltree pattern | `WHERE path LIKE ?` (with `*` → `%` conversion for ltree-style wildcards) |
| `startingPath(prefix)`| ltree prefix  | `WHERE path LIKE 'prefix%'` (subtree selection) |
| `hasLink()`           | exact match   | `WHERE has_link = 1` |
| `hasLinkMount()`      | exact match   | `WHERE has_link_mount = 1` |

Filters are additive — each CTE reads from the previous, creating an AND chain. The combination of ltree hierarchical paths with JSON property extraction provides Cypher-like graph query capability over the knowledge base using standard SQLite with the ltree extension.

## Project Layout

```
data_structures_zig/
├── build.zig                          # Build config — links libkb.a + sqlite3
├── README.md
├── src/
│   ├── kb.zig                         # Main wrapper module (all subsystems)
│   └── example.zig                    # Usage example (in-memory DB)
└── tests/
    ├── knowledge_base.kb              # SQLite test database
    ├── test_bit_s_expression.zig      # 17 tests — S-expression evaluator
    ├── test_query_support.zig         # 7 tests  — CTE filter chain
    └── test_data_structures.zig       # 10 tests — full integration
```

## Prerequisites

- Zig ≥ 0.13
- Built `data_structures_c` producing `lib/libkb.a` (or `.so`)
- `sqlite3` and `cjson` development headers/libraries installed
- `ltree.so` — SQLite ltree extension for path hierarchy queries

## Building

The build expects `data_structures_c` as a sibling directory by default:

```
project_root/
├── data_structures_c/    # C library with lib/libkb.a
└── data_structures_zig/  # This directory
```

```bash
cd data_structures_zig
zig build                          # build static + shared libs + example
zig build run                      # run the example (in-memory DB)
zig build -Dc_root=/path/to/c_lib  # override C library location
```

Outputs:

```
zig-out/
├── bin/kb_example
└── lib/
    ├── libkb_zig.a
    └── libkb_zig.so
```

## Testing

```bash
zig build test           # kb.zig internal tests
zig build test-sexpr     # S-expression evaluator (no DB)
zig build test-query     # KB_Search filter chain (in-memory DB)
zig build test-ds        # Full integration tests (in-memory DB)
zig build test-all       # Run all four suites
zig build test-all --summary all   # With summary output
```

## Usage from Zig

### Opening a Constructed Database

```zig
const kb = @import("kb");

// Open from file (constructed by construct_kb)
var ds = try kb.DataStructures.create("knowledge_base.db", "knowledge_base", "./ltree");
defer ds.destroy();

// Or from an existing sqlite3 handle
var ds2 = try kb.DataStructures.createFromDb(db, "knowledge_base");
defer ds2.destroy();
```

### Discovering Tables by Label

```zig
var search = try ds.search();
search.clearFilters();

// Chainable filters
try search.kb("kb1");
try search.label("KB_STATUS_FIELD");
try search.name("info2_status");
try search.propertyValue("prop3", "val3");
try search.startingPath("kb1.header1_link");
try search.execute();

const results = search.results();
// results.count has the number of matching rows
```

### Operating on Discovered Tables

```zig
// Status table — get/set JSON data
var status = try ds.status();
try status.setData("kb1.sensors.temp", "{\"value\":42}");
const data = try status.getData("kb1.sensors.temp");
defer if (data) |d| kb.freeCStr(@ptrCast(d.ptr));

// Job queue — push/peek/complete cycle
var jq = try ds.jobQueue();
try jq.push("kb1.jobs.worker", "{\"task\":\"process\"}", 1);
var peek = try jq.peek("kb1.jobs.worker");
defer peek.deinit();
try jq.complete("kb1.jobs.worker", peek.record_id);

// Bit mask — get/set individual bits
var bits = try ds.bitStructures();
var ops = try bits.getOps();
try ops.setBit("kb1.flags.main", 0, 1);
const val = try ops.getBit("kb1.flags.main", 0);
const mask = try ops.getMask("kb1.flags.main");

// S-expression evaluation against bit data
const result = try bits.evalSexpr("kb1.flags.main", "(and (bit 0) (not (bit 1)))");

// Standalone S-expression eval (no DB needed)
const r = try kb.sexprEval("(or 1 0)", .{ .bit_mask = 0x05, .change_mask = 0 });

// RPC server — push/peek/claim/complete
var srv = try ds.rpcServer();
const uuid = try srv.push("kb1.rpc.server", "action", "{\"arg\":1}", 1, "kb1.rpc.client");
var srv_peek = try srv.peek("kb1.rpc.server");
defer srv_peek.deinit();
try srv.claim("kb1.rpc.server", srv_peek.record_id);
try srv.complete_job("kb1.rpc.server", srv_peek.record_id);

// RPC client — push reply / peek / clear
var client = try ds.rpcClient();
try client.pushAndClaim("kb1.rpc.client", "uuid", "server.path", "action", "tag", "{\"reply\":\"ok\"}");
var reply = try client.peekReply("kb1.rpc.client");
defer reply.deinit();
try client.clearReply("kb1.rpc.client", reply.record_id);

// Link tables
var lt = try ds.linkTable();
var links = try lt.getByLinkName("link1");
defer links.deinit();

// Stream — circular buffer
var stream = try ds.stream();
try stream.pushData("kb1.stream.data", "{\"msg\":\"hello\"}");
var entries = try stream.listData("kb1.stream.data", null, null);
defer entries.deinit();
```

### Error Handling

All C `kb_error_t` codes map to `kb.KbError`:

```zig
status.getData("missing.path") catch |err| switch (err) {
    error.NotFound => { /* handle */ },
    error.Sqlite   => { /* handle */ },
    error.NullArg  => { /* handle */ },
    error.Busy     => { /* handle */ },
    else           => return err,
};
```

### Resource Ownership

- **`DataStructures`**: Owns the sqlite3 handle and all subsystems. Call `destroy()` once.
- **Subsystem accessors** (`.status()`, `.stream()`, etc.): Return **non-owning** wrappers. Do **not** call `.destroy()` on these — the parent `DataStructures` manages their lifetime.
- **Standalone creation** (`StatusTable.create(...)`, etc.): Returns an **owning** handle. You must call `.destroy()`.
- **C-allocated strings** (from `getData`, `peek`, etc.): Must be freed with `kb.freeCStr()`.
- **`PeekResult` / `PeekReplyResult`**: Contain owned strings. Call `.deinit()` when done.

### Low-Level Access

The raw C bindings are always available via `kb.c`:

```zig
const raw_db = try ds.getDb();         // *c.sqlite3
const raw_ks = (try ds.search()).handle; // *c.kb_search_t
kb.c.kb_sql_exec(raw_db, "...", null); // direct C call
```

### As a Module Dependency

In your own `build.zig`, add `kb` as a module:

```zig
const kb_mod = b.dependency("kb", .{}).module("kb");
exe.root_module.addImport("kb", kb_mod);
```

## Related Projects

- **construct_kb/** — Knowledge base constructor. Builds the SQLite database that this library operates on.
- **data_structures_c/** — C port of the runtime library. The shared core that this Zig wrapper binds to.
- **data_structures_luajit/** — Original LuaJIT implementation of the runtime library.
- **ChainTree** — Unified control framework (behavior trees, state machines, sequential control) that uses this library for runtime table access.

## License

MIT