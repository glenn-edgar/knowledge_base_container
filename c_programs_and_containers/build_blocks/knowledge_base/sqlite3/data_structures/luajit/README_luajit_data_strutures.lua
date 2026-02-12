# Knowledge Base LuaJIT Runtime Library

## Mission

This library provides LuaJIT runtime access to knowledge base databases
constructed by the `construct_kb` toolchain.

The knowledge base system is a two-phase architecture:

**Phase 1 — Construction.** The `construct_kb` programs build a SQLite
database containing a main knowledge base table and a set of auxiliary runtime
tables (status, job queue, stream, bit mask, RPC server queue, RPC client
queue, link table, link mount table). The constructor defines the structure of
each table — number of slots, initial properties, path hierarchy, and
metadata — so that the runtime only needs to read and operate on pre-allocated
resources.

**Phase 2 — Runtime (this library).** The LuaJIT library opens the constructed
database and provides:

1. **Table discovery.** The knowledge base describes every auxiliary table —
   its path, label type, metadata, and relationships. The library uses
   CTE (Common Table Expression) progressive filtering to search the
   knowledge base and locate tables by label, name, path pattern, property
   values, or link relationships.

2. **Runtime operations.** Once a table is discovered, the library provides
   CRUD operations for each table type: status read/write, job queue
   push/peek/complete, stream circular buffer, bit mask get/set with change
   tracking, RPC server and client queue management, and link/mount traversal.

3. **S-expression evaluation.** Bit mask conditions can be expressed as
   S-expressions and evaluated against live bit data — enabling declarative
   guard conditions in ChainTree behavior trees and state machines.

4. **Driver construction.** Higher-level code (ChainTree behavior trees, state
   machines, sequential controllers) dynamically locates backing tables at
   startup and constructs drivers without hardcoded table references.

## Dependencies

- **LuaJIT** — 2.1 or later
- **SQLite3** — shared library (`libsqlite3.so`) accessible via LuaJIT FFI
- **ltree.so** — SQLite ltree extension (included in this directory)
- **cjson** or **dkjson** — JSON library (cjson preferred for performance)

## Directory Contents

### Runtime Modules

| Module | Purpose |
|--------|---------|
| `kb_data_structures.lua` | Aggregator facade. Entry point — creates all subsystems, delegates operations. Mirrors the C `kb_ds_t` interface |
| `kb_query_support.lua` | CTE progressive filter chain (KB_Search). Filters by knowledge base name, label, node name, property key/value, path pattern, starting path, has_link, has_link_mount |
| `kb_status_table.lua` | Status data CRUD — get/set JSON data by path with UPSERT |
| `kb_job_queue.lua` | Job queue — push (find free slot, set queued), peek (highest priority, oldest first), complete (return to free), clear, list pending/active |
| `kb_stream.lua` | Circular buffer stream — push data at write_index, list with optional time range filters, clear |
| `bit_mask_rt_operations.lua` | Bit-level operations on the bit_mask_store table — get/set individual bits, read/write full mask, set all ones/zeros, change mask tracking |
| `bit_s_expression.lua` | S-expression tokenizer and evaluator for bit mask expressions — `(or (and user_1:F user_1:J) (bit_changed user_1:G))` |
| `kb_bit_structures.lua` | Orchestrator combining KB_Search + bit mask operations + S-expression evaluation. Assembles bit data classes with named flags per user |
| `kb_rpc_server.lua` | RPC server queue — 4-state machine (empty → new_job → processing → empty). Push with UUID, peek by priority, claim, complete, list by state |
| `kb_rpc_client.lua` | RPC client reply queue — 2-state toggle (free ↔ queued). Push-and-claim reply data, peek oldest reply, clear, list waiting jobs |
| `kb_link_table.lua` | Link table queries — find all link names, find all node names, find records by link_name or node_path |
| `kb_link_mount_table.lua` | Link mount table queries — find all link names, find all mount paths, find records by link_name or mount_path |

### Database Files

| File | Purpose |
|------|---------|
| `knowledge_base.db` | Constructed knowledge base database (built by `construct_kb`) |
| `test_kb.db` | Test database used by `test_kb_query_support.lua` |
| `ltree.so` | SQLite ltree extension for path hierarchy queries |

### Test Drivers

| File | Purpose |
|------|---------|
| `test_kb_data_structures.lua` | Integration test exercising all subsystems through the aggregator facade. Requires `knowledge_base.db` |
| `test_kb_query_support.lua` | Unit tests for the CTE filter chain |
| `test_bit_s_expression.lua` | Unit tests for the S-expression tokenizer and evaluator |

## Usage

### Running Tests

```bash
# Integration test (requires constructed database)
luajit test_kb_data_structures.lua knowledge_base.db

# KB_Search filter tests
luajit test_kb_query_support.lua

# S-expression evaluator tests
luajit test_bit_s_expression.lua
```

### Opening a Constructed Database

```lua
local KB_Data_Structures = require('kb_data_structures')

local kb = KB_Data_Structures.new('knowledge_base.db', 'knowledge_base')
```

### Discovering Tables by Label

```lua
-- Find all status fields
local node_ids = kb:find_status_node_ids()
local paths = kb:find_path_values(node_ids)

-- Find with specific filters
local node_id = kb:find_status_node_id({
    kb         = "kb1",
    node_name  = "info2_status",
    properties = { prop3 = "val3" },
    node_path  = "*.header1_link.header1_name.KB_STATUS_FIELD.info2_status",
})
```

### Operating on Discovered Tables

```lua
-- Status table
local data = kb:get_status_data(path)
kb:set_status_data(path, { value = 42 })

-- Job queue
kb:push_job_data(path, { task = "process" })
local job = kb:peak_job_data(path)
kb:mark_job_completed(job.id)

-- Bit mask with named flags
local bit_data = kb:find_assemble_bit_data(node_ids, false, { "user_1" })
kb:set_flag_data(data_class, { F = 1, G = 0 })
kb:get_flag_data(data_class)

-- S-expression evaluation
local expr = "(or (and user_1:F user_1:J) (bit_changed user_1:G))"
local tokens = kb:s_tokenize(expr)
local result = kb:s_execute(tokens, bit_data)

-- RPC server
kb:rpc_server_push_rpc_queue(server_path, uuid, "action", data, tag, opts)
local job = kb:rpc_server_peak_server_queue(server_path)
kb:rpc_server_mark_job_completion(server_path, job.id)

-- RPC client
kb:rpc_client_push_and_claim_reply_data(client_path, uuid, server, action, tag, data)
local reply = kb:rpc_client_peak_and_claim_reply_data(client_path)
```

### KB_Search Filters

```lua
-- Chainable filter API
kb:clear_filters()
kb:search_kb("kb1")
kb:search_label("KB_STATUS_FIELD")
kb:search_name("info2_status")
kb:search_property_value("prop3", "val3")
kb:search_starting_path("kb1.header1_link")
kb:search_has_link()
kb:search_has_link_mount()
local results = kb:execute_kb_search()

-- Path decoding for link nodes
local decoded = kb:decode_link_nodes(path)

-- Description extraction
local descriptions = kb:find_description_paths(paths)
```

### Cleanup

```lua
kb:disconnect()
```

## Table Naming Convention

The constructor creates tables using a base name (typically `knowledge_base`)
with suffixes:

| Table | Name |
|-------|------|
| Main KB | `knowledge_base` |
| Status | `knowledge_base_status_table` |
| Job queue | `knowledge_base_job_queue` |
| Stream | `knowledge_base_stream_table` |
| Bit mask store | `knowledge_base_bit_mask_store` |
| RPC server queue | `knowledge_base_rpc_server_queue` |
| RPC client queue | `knowledge_base_rpc_client_queue` |
| Link table | `knowledge_base_link_table` |
| Link mount table | `knowledge_base_link_mount_table` |

## CTE Search Filters

The `kb_query_support` module builds SQL using a chain of CTEs that
progressively narrow results:

| Filter | SQL Effect |
|--------|------------|
| `search_kb(name)` | `WHERE knowledge_base = ?` |
| `search_label(label)` | `WHERE label = ?` |
| `search_name(name)` | `WHERE name = ?` |
| `search_property_key(key)` | `WHERE json_extract(properties, '$.key') IS NOT NULL` |
| `search_property_value(key, val)` | `WHERE json_extract(properties, '$.key') = ?` |
| `search_path(expr)` | `WHERE path LIKE ?` (with `*` → `%` conversion) |
| `search_starting_path(prefix)` | `WHERE path LIKE 'prefix%'` |
| `search_has_link()` | `WHERE has_link = 1` |
| `search_has_link_mount()` | `WHERE has_link_mount = 1` |

Filters are additive — each CTE reads from the previous, creating an AND
chain. This provides Cypher-like graph query capability over the hierarchical
knowledge base using standard SQLite.

## Related Projects

- **construct_kb/** — LuaJIT knowledge base constructor. Builds the SQLite
  database that this library operates on.
- **knowledge_base_c/** — C port of this library for embedded targets.
- **ChainTree** — Unified control framework (behavior trees, state machines,
  sequential control) that uses this library for runtime table access.

## License

MIT


