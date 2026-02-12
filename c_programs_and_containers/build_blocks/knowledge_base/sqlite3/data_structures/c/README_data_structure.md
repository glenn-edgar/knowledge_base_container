# Knowledge Base C Runtime Library

## Mission

This library provides C runtime access to knowledge base databases constructed
by the LuaJIT toolchain in the `construct_kb` directory.

The knowledge base system is a two-phase architecture:

**Phase 1 — Construction (LuaJIT).** The `construct_kb` programs build a
SQLite database containing a main knowledge base table and a set of auxiliary
runtime tables (status, job queue, stream, bit mask, RPC server queue, RPC
client queue, link table, link mount table). The constructor defines the
structure of each table — number of slots, initial properties, path hierarchy,
and metadata — so that the runtime only needs to read and operate on
pre-allocated resources. This is critical for embedded and deterministic
control systems where dynamic allocation at runtime is avoided.

**Phase 2 — Runtime (this library).** The C library opens the constructed
database and provides:

1. **Table discovery.** The knowledge base describes the properties of every
   auxiliary table — its path in the hierarchy, its label type, its metadata,
   and its relationships to other tables. The library uses Cypher-style CTE
   (Common Table Expression) progressive filtering to search the knowledge
   base and locate tables by label, name, path pattern, property values, or
   link relationships. This replaces hardcoded table references with a
   searchable, self-describing schema.

2. **Runtime operations.** Once a table is discovered, the library provides
   the CRUD operations for each table type: status read/write, job queue
   push/peek/complete, stream circular buffer, bit mask get/set with change
   tracking, RPC server and client queue management, and link/mount traversal.

3. **Driver construction.** The discovery + operations pattern allows higher-level
   code (ChainTree behavior trees, state machines, sequential controllers) to
   dynamically locate their backing tables at startup and construct drivers
   without compile-time knowledge of the database layout.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                  kb_data_structures.h                     │
│              Aggregator facade (kb_ds_t)                  │
│  Creates all subsystems, provides accessor functions      │
├──────────┬───────────┬───────────┬───────────┬───────────┤
│ kb_query │ kb_status │ kb_job    │ kb_stream │ kb_bit    │
│ _support │ _table    │ _queue    │           │ _structs  │
├──────────┼───────────┼───────────┼───────────┼───────────┤
│ kb_rpc   │ kb_rpc    │ kb_link   │ kb_link   │ bit_mask  │
│ _server  │ _client   │ _table    │ _mount    │ _rt_ops   │
├──────────┴───────────┴───────────┴───────────┴───────────┤
│  kb_common.h — SQL exec, bind params, results, errors    │
│  kb_uuid.h   — UUID4 generation                          │
│  kb_json.h   — cJSON wrapper                             │
│  bit_s_expression.h — S-expression evaluator             │
└──────────────────────────────────────────────────────────┘
              │                          │
         SQLite3 C API              cJSON library
```

### Module Summary

| Module | Purpose |
|--------|---------|
| `kb_common` | Error codes, SQL execution with bind parameters, result set abstraction, transaction helpers, timestamp formatting, database open/close |
| `kb_uuid` | UUID4 generation for request IDs and record tracking |
| `kb_json` | cJSON wrapper — encode, decode, key/value queries |
| `kb_query_support` | CTE progressive filter chain (KB_Search). Filters by knowledge base name, label, node name, property key/value, path pattern, starting path, has_link, has_link_mount. Supports find_path_values, find_description, decode_link_nodes |
| `bit_mask_rt_operations` | Atomic bit-level operations on the bit_mask_store table — get/set individual bits, read/write full mask, change mask tracking |
| `bit_s_expression` | Recursive descent S-expression evaluator for bit mask expressions — `(and (bit 0) (not (bit_changed 1)))` |
| `kb_bit_structures` | Orchestrator combining KB_Search + bit mask operations + S-expression evaluation |
| `kb_status_table` | Status data CRUD — get/set JSON data by path with UPSERT |
| `kb_stream` | Circular buffer stream — push data at write_index, list with optional time range filters, clear |
| `kb_job_queue` | Job queue — push (find free slot, set queued), peek (highest priority, oldest first), complete (return to free), clear |
| `kb_rpc_server` | RPC server queue — 4-state machine (empty → new_job → processing → empty). Push with UUID, peek, claim, complete |
| `kb_rpc_client` | RPC client reply queue — 2-state toggle (free ↔ queued). Push-and-claim reply data, peek oldest reply, clear |
| `kb_link_table` | Link table queries — find by link_name or node_path |
| `kb_link_mount_table` | Link mount table queries — find by link_name or mount_path |
| `kb_data_structures` | Aggregator facade — opens database, creates all subsystems, provides typed accessor functions. Entry point for application code |

### Table Naming Convention

The constructor creates tables using a base name (typically `knowledge_base`)
with suffixes:

| Table | Suffix |
|-------|--------|
| Main KB | `knowledge_base` |
| Status | `knowledge_base_status_table` |
| Job queue | `knowledge_base_job_queue` |
| Stream | `knowledge_base_stream_table` |
| Bit mask store | `knowledge_base_bit_mask_store` |
| RPC server queue | `knowledge_base_rpc_server_queue` |
| RPC client queue | `knowledge_base_rpc_client_queue` |
| Link table | `knowledge_base_link_table` |
| Link mount table | `knowledge_base_link_mount_table` |

The table name is passed as the `database` parameter when creating the
aggregator, and all modules append the appropriate suffix.

### CTE Search Filters

The `kb_query_support` module builds SQL queries using a chain of CTEs
that progressively narrow results. Available filters:

| Filter | SQL Effect |
|--------|------------|
| `kb(name)` | `WHERE knowledge_base = ?` |
| `label(label)` | `WHERE label = ?` |
| `name(name)` | `WHERE name = ?` |
| `property_key(key)` | `WHERE json_extract(properties, '$.key') IS NOT NULL` |
| `property_value(key, value)` | `WHERE json_extract(properties, '$.key') = ?` |
| `path(expr)` | `WHERE path LIKE ?` (with `*` → `%` conversion) |
| `starting_path(prefix)` | `WHERE path LIKE 'prefix%'` |
| `has_link` | `WHERE has_link = 1` |
| `has_link_mount` | `WHERE has_link_mount = 1` |

Filters are additive — each CTE reads from the previous, creating an AND chain.
Up to 16 filters can be stacked per query.

## Dependencies

### Required

- **GCC or Clang** — C11 or later
- **SQLite3** — version 3.30+ with development headers
  ```bash
  sudo apt-get install libsqlite3-dev    # Debian/Ubuntu
  sudo dnf install sqlite-devel          # Fedora/RHEL
  ```
- **cJSON** — JSON parser library with development headers
  ```bash
  sudo apt-get install libcjson-dev      # Debian/Ubuntu
  sudo dnf install cjson-devel           # Fedora/RHEL
  ```

### Optional

- **SQLite ltree extension** — enables PostgreSQL-style ltree path queries.
  Pass the extension path to `kb_ds_create()` or `kb_open_database()` to load
  it at runtime. Not required for basic CTE-based filtering.

### Header Detection

`kb_json.h` auto-detects the cJSON header location:
1. System install: `<cjson/cJSON.h>` (from libcjson-dev)
2. Vendored fallback: `"cJSON.h"` (place in include path)

## Build

### GNU Make

```bash
make                # builds lib/libkb.a (static) and lib/libkb.so (shared)
make tests          # builds test executables in tests/
make run-tests      # builds and runs all tests
make clean          # removes build artifacts
```

### CMake

```bash
mkdir build && cd build
cmake ..
make
ctest
```

### Cross-compilation

For ARM targets (e.g. Snapdragon), set the cross-compiler:

```bash
make CC=aarch64-linux-gnu-gcc AR=aarch64-linux-gnu-ar
```

Ensure SQLite3 and cJSON are available for the target architecture.

## Usage

### Opening a Constructed Database

```c
#include "kb_data_structures.h"

/* Open database file with default table name "knowledge_base" */
kb_ds_t *ds = kb_ds_create("knowledge_base.db", "knowledge_base", NULL);

/* Or with ltree extension */
kb_ds_t *ds = kb_ds_create("knowledge_base.db", "knowledge_base",
                            "/usr/lib/sqlite3/ltree");
```

### Discovering Tables by Label

```c
kb_search_t *ks = kb_ds_search(ds);

/* Find all status fields */
kb_search_clear_filters(ks);
kb_search_label(ks, "KB_STATUS_FIELD");
kb_search_execute(ks);
const kb_result_t *r = kb_search_results(ks);

for (int i = 0; i < r->count; i++) {
    const char *path = kb_row_get(r, i, "path");
    const char *name = kb_row_get(r, i, "name");
    printf("  %s (%s)\n", path, name);
}
```

### Operating on Discovered Tables

```c
/* Status table: read and write */
char *data = NULL;
kb_status_get_data(kb_ds_status(ds), path, &data);
kb_status_set_data(kb_ds_status(ds), path, "{\"value\":42}");
free(data);

/* Job queue: push, peek, complete */
kb_job_push(kb_ds_job_queue(ds), path, "{\"task\":\"run\"}", 1);
char *jdata = NULL; int jid = 0;
kb_job_peek(kb_ds_job_queue(ds), path, &jdata, &jid);
kb_job_complete(kb_ds_job_queue(ds), path, jid);
free(jdata);

/* Bit mask: set, get, evaluate S-expression */
kb_bit_mask_ops_t *ops = kb_bit_structures_get_ops(kb_ds_bit_structures(ds));
kb_bit_set(ops, path, 0, 1);
int val = 0;
kb_bit_get(ops, path, 0, &val);
```

### Cleanup

```c
kb_ds_destroy(ds);  /* closes database and frees all subsystems */
```

## Project Structure

```
knowledge_base_c/
├── include/           15 public headers
│   ├── kb_common.h
│   ├── kb_uuid.h
│   ├── kb_json.h
│   ├── kb_query_support.h
│   ├── bit_mask_rt_operations.h
│   ├── bit_s_expression.h
│   ├── kb_bit_structures.h
│   ├── kb_status_table.h
│   ├── kb_stream.h
│   ├── kb_job_queue.h
│   ├── kb_link_table.h
│   ├── kb_link_mount_table.h
│   ├── kb_rpc_server.h
│   ├── kb_rpc_client.h
│   └── kb_data_structures.h
├── src/               15 implementation files
├── tests/             3 test drivers + test macros + README
├── Makefile           GNU Make build
├── CMakeLists.txt     CMake build
└── README.md          This file
```

## Related Projects

- **construct_kb/** — LuaJIT knowledge base constructor. Builds the SQLite
  database that this library operates on.
- **ChainTree** — Unified control framework (behavior trees, state machines,
  sequential control) that uses this library for runtime table access.

## License

MIT

