# Knowledge Base PostgreSQL C Library

A C library providing runtime access to knowledge base data structures stored in PostgreSQL. This library is the C equivalent of the LuaJIT runtime modules and operates on databases constructed by the Python/LuaJIT `construct_*` modules.

## Overview

The knowledge base uses PostgreSQL with the `ltree` extension to organize hierarchical configuration data. The construct modules (Python or LuaJIT) build the database schema and populate it with node definitions, pre-allocated slots, and structural metadata. This C library provides the runtime API to read, write, and manage the data at execution time.

The library is designed for embedded and distributed control systems where multiple processes may concurrently access the same knowledge base. All write operations use PostgreSQL row-level locking (`FOR UPDATE` / `FOR UPDATE SKIP LOCKED`) with configurable retry and backoff to prevent lost updates.

## Prerequisites

**Operating System:** Linux (tested on Ubuntu 24.04, targets ARM64 and x86-64)

**Required Libraries:**

| Library | Package | Purpose |
|---------|---------|---------|
| libpq | `libpq-dev` | PostgreSQL C client |
| cJSON | `libcjson-dev` | JSON parsing for bit definitions and document queue ops |

**Install on Debian/Ubuntu:**

```bash
sudo apt-get install libpq-dev libcjson-dev
```

**PostgreSQL Server:**

A running PostgreSQL instance with the `ltree` extension enabled. The database must have been constructed by the `construct_*` modules (Python `construct_knowledge_base.py` / `construct_status_table.py` / etc., or the equivalent LuaJIT constructors) before this library can operate on it.

## Building

```bash
make            # Build static lib, shared lib, and test driver
make static     # Build libkb_data_structures.a only
make shared     # Build libkb_data_structures.so only
make test       # Build test driver (statically linked)
make clean      # Remove all build artifacts
```

Build artifacts:

- `obj/` — Object files (`.o` for static, `.pic.o` for shared)
- `libkb_data_structures.a` — Static library
- `libkb_data_structures.so` — Shared library
- `tests/test_driver` — Test executable

## Project Structure

```
knowledge_base_pg_c/
├── Makefile
├── README.md
├── include/
│   ├── kb_all.h              — Convenience header (includes everything)
│   ├── kb_common.h           — Connection, query, retry, result set helpers
│   ├── kb_search.h           — Knowledge base node discovery
│   ├── kb_status.h           — Status key-value store
│   ├── kb_stream.h           — Circular buffer time-series
│   ├── kb_job_queue.h        — Fixed-length job queue
│   ├── kb_rpc_server.h       — RPC server request queue
│   ├── kb_rpc_client.h       — RPC client reply queue
│   ├── kb_bit_structures.h   — Bit mask operations + S-expression evaluator
│   ├── kb_link_table.h       — Link table queries
│   └── kb_document.h         — JSONB document operations
├── src/
│   ├── kb_common.c           — libpq wrapper, auto-begin, retry with backoff
│   ├── kb_search.c           — Query knowledge_base table by label/name/path
│   ├── kb_status.c           — UPSERT status data with FOR SHARE reads
│   ├── kb_stream.c           — Circular buffer push/list/clear/statistics
│   ├── kb_job_queue.c        — Push/peek/complete/clear job lifecycle
│   ├── kb_rpc_server.c       — SERIALIZABLE push, priority-ordered peek
│   ├── kb_rpc_client.c       — Reply push/consume with slot management
│   ├── kb_bit_structures.c   — 64-bit masks, S-expression boolean evaluator
│   ├── kb_link_table.c       — Cross-KB link resolution
│   └── kb_document.c         — JSONB get/set/delete, arrays, queue/stack
├── tests/
│   └── test_driver.c         — Comprehensive test suite
└── obj/                      — Build artifacts (generated)
```

## Modules

### kb_common — Connection and Query Infrastructure

Foundation module providing PostgreSQL connection management via libpq. Handles automatic `BEGIN` on first query, commit/rollback, parameterized queries, and result set management. Includes a generic retry wrapper with exponential backoff used by all write operations.

Key types: `kb_conn_t` (connection), `kb_resultset_t` (query results), `kb_error_t` (error codes).

### kb_search — Knowledge Base Node Discovery

Queries the `{database}_knowledge_base` table to find nodes by label, name, property values, and ltree path patterns. This is the entry point for all other modules — you discover paths here, then pass them to the data modules.

Finder functions for each node type:

- `kb_find_status_paths()` — finds `KB_STATUS_FIELD` nodes
- `kb_find_stream_paths()` — finds `KB_STREAM_FIELD` nodes
- `kb_find_job_queue_paths()` — finds `KB_JOB_QUEUE` nodes
- `kb_find_rpc_server_paths()` — finds `KB_RPC_SERVER_FIELD` nodes
- `kb_find_rpc_client_paths()` — finds `KB_RPC_CLIENT_FIELD` nodes
- `kb_find_bit_structure_paths()` — finds `KB_BIT_MASK` nodes
- `kb_find_document_paths()` — finds `KB_JSONB_FIELD` nodes
- `kb_find_node_paths()` — generic finder for any label

### kb_status — Status Key-Value Store

Table: `{database}_status` — One row per declared status node. Stores arbitrary JSONB data keyed by ltree path.

- `kb_status_get()` — Read with `FOR SHARE` lock
- `kb_status_set()` — Atomic UPSERT with retry
- `kb_status_set_multiple()` — Batch UPSERT in a single transaction

### kb_stream — Circular Buffer Time-Series

Table: `{database}_stream` — Pre-allocated rows forming a fixed-length circular buffer. New data overwrites the oldest entry.

- `kb_stream_push()` — Write to oldest slot (`FOR UPDATE SKIP LOCKED`)
- `kb_stream_list()` — List valid entries with optional time-range filter
- `kb_stream_clear()` — Reset all slots (`FOR UPDATE`)
- `kb_stream_count()` / `kb_stream_count_total()` — Valid / total slot counts
- `kb_stream_latest()` — Most recent valid entry
- `kb_stream_range()` — Entries within a time window
- `kb_stream_get_by_id()` — Fetch specific row
- `kb_stream_statistics()` — Aggregate stats (total/valid/invalid counts, min/max timestamps)

### kb_job_queue — Fixed-Length Job Queue

Table: `{database}_job` — Pre-allocated job slots with a push/peek/complete lifecycle.

State machine: `free → queued → active → completed (free)`

- `kb_job_push()` — Fill a free slot (`FOR UPDATE SKIP LOCKED`)
- `kb_job_peek()` — Claim oldest pending job (`FOR UPDATE SKIP LOCKED`)
- `kb_job_complete()` — Recycle slot back to free (`FOR UPDATE`)
- `kb_job_clear()` — Reset all slots (`FOR UPDATE`)
- `kb_job_list_pending()` / `kb_job_list_active()` — List by state
- `kb_job_free_count()` / `kb_job_queued_count()` / `kb_job_active_count()`

### kb_rpc_server — RPC Server Request Queue

Table: `{database}_rpc_server` — Pre-allocated request slots with priority ordering.

State machine: `empty → new_job → processing → empty`

- `kb_rpc_server_push()` — Fill slot with `SERIALIZABLE` isolation + advisory lock
- `kb_rpc_server_peek()` — Claim highest-priority job (`FOR UPDATE SKIP LOCKED`)
- `kb_rpc_server_complete()` — Reset slot (`FOR UPDATE`)
- `kb_rpc_server_clear()` — Reset all slots (`FOR UPDATE`)

### kb_rpc_client — RPC Client Reply Queue

Table: `{database}_rpc_client` — Pre-allocated reply slots for receiving RPC responses.

- `kb_rpc_client_push_reply()` — Fill a free slot (`FOR UPDATE SKIP LOCKED`)
- `kb_rpc_client_peek_reply()` — Consume oldest result (`FOR UPDATE SKIP LOCKED`)
- `kb_rpc_client_clear()` — Reset all slots (`FOR UPDATE`)

### kb_bit_structures — Bit Mask Operations + S-Expression Evaluator

Table: `bit_mask_table` (global) — 64-bit integer masks keyed by `node_id`.

- `kb_bit_get_mask()` / `kb_bit_set_mask()` — Read/write entire mask
- `kb_bit_get()` / `kb_bit_set()` — Read/write individual bits (0-63)
- `kb_bit_set()` uses atomic read-modify-write under `FOR UPDATE`
- `kb_bit_assemble_data()` — Load named bit definitions from KB properties
- `kb_bit_eval_sexpr()` — Evaluate boolean S-expressions over bit masks

S-expression format (JSON arrays):

```json
["and", ["bit", "sensor_a"], ["not", ["bit", "sensor_b"]]]
["or", ["bit", 0], ["bit_changed", 4]]
```

### kb_document — JSONB Document Operations

Table: `{database}_document` — JSONB documents keyed by ltree path and type.

Core operations: get, set, delete key, delete path (all writes use `FOR UPDATE`).

Key existence: `has_key`, `has_any_keys`, `has_all_keys`.

Containment: `contains`, `contained_by`.

JSONPath: `path_exists`, `path_query`.

Array operations: append, prepend, remove by index, contains check, expand elements.

Queue (FIFO): enqueue, dequeue, peek, size, is_empty, clear, get_all.

Stack (LIFO): push (prepend to index 0), pop (remove from index 0).

Metadata: get/set metadata sub-document.

### kb_link_table — Cross-KB Link Resolution

Tables: `{database}_link` and `{database}_link_mount` — Read-only at runtime. Populated by construct modules to define links between knowledge bases.

- `kb_link_query()` — Find links by parent path
- `kb_link_mount_query()` — Find mounts by mount path
- `kb_link_decode_nodes()` — Resolve link names to mount paths

## Concurrent Write Safety

All write operations across every module use PostgreSQL row-level locking to prevent lost updates when multiple processes access the same knowledge base:

| Pattern | Used By | Behavior |
|---------|---------|----------|
| `FOR UPDATE SKIP LOCKED` | push/peek ops | Non-blocking: skips locked rows, retries on next attempt |
| `FOR UPDATE` | complete/clear/set ops | Blocking: waits for lock, wrapped in retry with backoff |
| `SERIALIZABLE` + advisory lock | RPC server push | Strongest isolation for priority slot allocation |
| Atomic UPSERT | Status set | `INSERT ... ON CONFLICT DO UPDATE` is inherently atomic |
| `FOR SHARE` | Status get | Prevents reading mid-update |

All write functions accept `max_retries` and `base_delay_ms` parameters. The retry wrapper uses exponential backoff with jitter.

## Test Program

The test driver (`tests/test_driver.c`) exercises every module against a live PostgreSQL database. It expects a knowledge base that has been constructed by the construct modules.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `knowledge_base` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | (none) | Database password |
| `KB_DATABASE` | `kb1` | Knowledge base prefix (table prefix) |

**Running:**

```bash
export POSTGRES_PASSWORD=yourpassword
make run-test
```

**Test coverage (280+ assertions):**

- **Search/Discovery** — Find nodes by label for all types (status, stream, job, RPC, bit mask, document)
- **Status** — Get, set, update, verify JSONB values
- **Job Queue** — Clear, push 3 jobs, count free/queued, peek (claim), complete (recycle), verify counts
- **Stream** — Clear, count total slots, push 3 entries, count valid, list, latest, statistics, clear and verify
- **RPC Server** — Clear, push with UUID, count new, peek (claim), verify fields, complete, verify empty
- **RPC Client** — Clear, push reply, count queued, peek (consume), verify payload, clear
- **Bit Structures** — KB search for bit mask nodes, extract `record_id` from properties, get/set mask, set individual bits, verify mask arithmetic, S-expression evaluation (`and`, `or`, `not`, `bit`, `bit_changed`)
- **Document** (×3 labels) — Set/get entire document, key access, nested path access, text vs JSON retrieval, key existence checks, containment, array containment, JSONPath predicates and queries, set/delete keys and paths, array element expansion, queue (FIFO) operations, stack (LIFO) operations, empty queue/stack edge cases
- **Link Table** — Query links by path, verify mount resolution

## Linking Your Application

**Static linking:**

```bash
gcc -o myapp myapp.c libkb_data_structures.a -lpq -lcjson -lm
```

**Dynamic linking:**

```bash
gcc -o myapp myapp.c -L. -lkb_data_structures -lpq -lcjson -lm
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH
./myapp
```

**Include path:**

```c
#include "kb_all.h"   /* includes everything */
/* or include individual headers as needed */
#include "kb_common.h"
#include "kb_search.h"
#include "kb_status.h"
```

## License

MIT
