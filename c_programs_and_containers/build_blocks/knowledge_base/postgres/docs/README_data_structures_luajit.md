# LuaJIT PostgreSQL Knowledge Base Construction System

A LuaJIT port of the Python knowledge base construction framework. This system builds hierarchical knowledge bases in PostgreSQL using the `ltree` extension, along with satellite data tables (status, stream, job, RPC, document, bit mask) that stay synchronized with the knowledge base through declarative field definitions and automated installation checks.

## Prerequisites

- **LuaJIT** (Lua 5.1 compatible)
- **PostgreSQL** with the `ltree` extension available
- **libpq-dev** (PostgreSQL client library headers)

## Installing Required Modules

All Lua dependencies are installed via LuaRocks. **`sudo` is required** because LuaRocks installs shared libraries into system paths under `/usr/local/lib/lua/5.1/`.

```bash
# Install the PostgreSQL client library headers (needed to compile luadbi-postgresql)
sudo apt install libpq-dev

# Install LuaJIT Lua modules (sudo required for system-wide install)
sudo luarocks --lua-version 5.1 install luadbi
sudo luarocks --lua-version 5.1 install luadbi-postgresql
sudo luarocks --lua-version 5.1 install dkjson
```

Verify the installation:

```bash
luajit -e "require('DBI'); print('luadbi OK')"
luajit -e "require('dkjson'); print('dkjson OK')"
```

If LuaJIT cannot find the modules, ensure your paths include the 5.1 install locations:

```bash
export LUA_PATH="/usr/local/share/lua/5.1/?.lua;/usr/local/share/lua/5.1/?/init.lua;;"
export LUA_CPATH="/usr/local/lib/lua/5.1/?.so;;"
```

## Running the Tests

Set the PostgreSQL password as an environment variable and run the integration test:

```bash
POSTGRES_PASSWORD=yourpassword luajit test_construct_data_tables.lua
```

For the full three-pass unit test (build, modify, resize):

```bash
POSTGRES_PASSWORD=yourpassword luajit test_construct_data_tables.lua False True
```

For upload-only mode (skips table creation, connects to existing tables):

```bash
POSTGRES_PASSWORD=yourpassword luajit test_construct_data_tables.lua True
```

---

## Architecture Overview

### The Ltree Knowledge Base DSL

The knowledge base is a tree structure stored in PostgreSQL using the `ltree` extension. Each node has a dot-separated path that encodes its position in the hierarchy. The construction API is a stack-based DSL that enforces balanced tree construction at build time.

#### Ltree Path Structure

Every path is built from pairs of dot-separated components. Each pair consists of a **class** (the link/edge label) followed by an **instance** (the node name). The class identifies *what kind* of relationship or node it is; the instance identifies *which specific one*.

```
kb1.header1_link.header1_name.info1_link.info1_name
───  ────────────────────────  ───────────────────── 
 │        class    instance        class    instance
 │         ╰────────╯               ╰────────╯
 │         1st pair                 2nd pair
 │
 └── knowledge base root
```

The first component of every path is always the knowledge base name (e.g., `kb1`). This root element is pushed onto the stack when `add_kb()` is called and is never popped. All subsequent class/instance pairs are appended below it.

#### Multiple Knowledge Bases Per Database

A single PostgreSQL database can host multiple independent knowledge bases. Each KB is registered with `add_kb()` and gets its own root element in the path hierarchy. You switch between them with `select_kb()`:

```lua
kb:add_kb("irrigation", "Agricultural irrigation system")
kb:add_kb("hvac",       "HVAC control system")

-- Build the irrigation tree
kb:select_kb("irrigation")
kb:add_header_node("zone_class", "north_field", ...)
  kb:add_info_node("sensor_class", "moisture_1", ...)
kb:leave_header_node("zone_class", "north_field")

-- Switch to the HVAC tree
kb:select_kb("hvac")
kb:add_header_node("floor_class", "floor_1", ...)
  kb:add_info_node("thermostat_class", "lobby", ...)
kb:leave_header_node("floor_class", "floor_1")
```

This produces paths like:

```
irrigation.zone_class.north_field.sensor_class.moisture_1
hvac.floor_class.floor_1.thermostat_class.lobby
```

All knowledge bases share the same set of database tables, but are logically isolated by the `knowledge_base` column and the path root. The satellite tables (status, stream, job, RPC, etc.) query across all KBs by label, so a single `check_installation()` call synchronizes everything regardless of which KB the fields belong to.

The path stack is maintained independently for each knowledge base, so you can interleave construction across multiple KBs without interference. The `check_installation()` call at the end verifies that *every* KB's stack has been fully unwound.

#### Class/Instance Naming Conventions

Because each path pair represents a class and instance, a consistent naming convention makes the tree self-documenting:

| Class (link label) | Instance (node name) | Resulting path segment |
|---------------------|----------------------|------------------------|
| `zone_class` | `north_field` | `zone_class.north_field` |
| `KB_STATUS_FIELD` | `pump_pressure` | `KB_STATUS_FIELD.pump_pressure` |
| `KB_STREAM_FIELD` | `flow_rate` | `KB_STREAM_FIELD.flow_rate` |
| `KB_RPC_SERVER_FIELD` | `valve_control` | `KB_RPC_SERVER_FIELD.valve_control` |

The satellite table labels (`KB_STATUS_FIELD`, `KB_STREAM_FIELD`, `KB_JOB_QUEUE`, `KB_RPC_SERVER_FIELD`, `KB_RPC_CLIENT_FIELD`, `KB_JSONB_FIELD`, `KB_BIT_MASK`) are reserved class names used internally by the `add_*_field()` convenience functions. These functions call `add_info_node()` with the appropriate label, so the satellite field definitions are just regular info nodes in the tree — they follow the same class/instance pattern and participate in the same stack-based balancing.

#### Node Types

There are two fundamental node types:

**Header nodes** are structural (branch) nodes that can contain children. When you call `add_header_node`, the link and node name are pushed onto an internal path stack. All subsequent nodes become children of this header until `leave_header_node` pops it off.

**Info nodes** are leaf nodes. Calling `add_info_node` pushes the link and name onto the stack, records the node, then immediately pops both elements back off. Info nodes cannot have children.

Every node stores:

- **knowledge_base** — which KB it belongs to
- **label** — the link/edge label connecting it to its parent
- **name** — the node's own name
- **properties** — a JSON object for metadata
- **data** — a JSON object for payload data
- **path** — the full ltree path, computed from the stack

#### Stack-Based Path Management

The DSL maintains a path stack per knowledge base. The stack enforces that the tree is always balanced — every `add_header_node` must have a matching `leave_header_node`.

```
Operation                          Stack state (kb1)
─────────────────────────────────  ─────────────────────────
add_kb("kb1")                      [kb1]
add_header_node("h1_link", "h1")   [kb1, h1_link, h1]
  add_info_node("i1_link", "i1")   [kb1, h1_link, h1]  ← push+pop
  add_info_node("i2_link", "i2")   [kb1, h1_link, h1]  ← push+pop
leave_header_node("h1_link","h1")  [kb1]
```

The `leave_header_node` call verifies that the popped label and name match the expected values. If they don't, it raises an error immediately — catching mismatched push/pop pairs at construction time rather than at runtime.

#### Installation Check

After building the entire tree, `check_installation()` verifies that every KB's stack has been fully unwound back to just the root element `[kb_name]`. If any header node was opened but not closed, the check fails with a descriptive error showing the remaining stack contents.

#### Links and Link Mounts

The system supports cross-references between knowledge bases through **link mounts** and **link nodes**:

- `add_link_mount(name, description)` — declares a named mount point at the current path. This creates an entry in the `_link_mount` table and sets `has_link_mount = TRUE` on the current node.
- `add_link_node(link_name)` — creates a reference from the current path to an existing link mount. This creates an entry in the `_link` table and sets `has_link = TRUE` on the current node.

Link mounts must be created before link nodes can reference them.

#### Duplicate Path Detection

The DSL tracks all paths that have been created in a per-KB set. If you attempt to create a node at a path that already exists, it raises an error immediately. This catches copy-paste mistakes in large knowledge base definitions.

---

## Database Tables

The system creates a family of tables from a base name (e.g., `knowledge_base`). Each satellite table is built by its own constructor class and synchronized with the knowledge base through `check_installation()`.

### Core Tables

#### `knowledge_base` (main node table)

Stores every node in every knowledge base.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| knowledge_base | VARCHAR | Which KB this node belongs to |
| label | VARCHAR | Edge label (link type) |
| name | VARCHAR | Node name |
| properties | JSON | Node metadata |
| data | JSON | Node payload |
| has_link | BOOLEAN | Whether this node has outgoing links |
| has_link_mount | BOOLEAN | Whether this node is a link mount point |
| path | LTREE | Full hierarchical path (unique) |

#### `knowledge_base_info`

Registry of all knowledge bases.

| Column | Type | Description |
|--------|------|-------------|
| knowledge_base | VARCHAR | KB name (unique) |
| description | VARCHAR | Human-readable description |

#### `knowledge_base_link`

Cross-references between nodes via named links.

| Column | Type | Description |
|--------|------|-------------|
| link_name | VARCHAR | Name of the link (must exist in link_mount) |
| parent_node_kb | VARCHAR | Source KB |
| parent_path | LTREE | Source node path |

#### `knowledge_base_link_mount`

Named mount points that link nodes can reference.

| Column | Type | Description |
|--------|------|-------------|
| link_name | VARCHAR | Mount point name (unique) |
| knowledge_base | VARCHAR | KB where the mount lives |
| mount_path | LTREE | Path of the mount point |
| description | VARCHAR | Human-readable description |

### Satellite Tables

Each satellite table is populated by adding special info nodes to the knowledge base with a specific label. During `check_installation()`, the satellite table constructor queries the knowledge base for nodes with its label, then synchronizes its own table to match.

#### Status Table (`knowledge_base_status`)

**KB label:** `KB_STATUS_FIELD`

Simple key-value store for runtime status. One row per declared status field.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| data | JSON | Current status data |
| path | LTREE | Matches the KB node path (unique) |

**Build function:** `add_status_field(status_key, properties, description, initial_data)`

**Sync behavior:** Paths in the status table that don't exist in the KB are deleted. Paths in the KB that don't exist in the status table are inserted with empty JSON data.

#### Stream Table (`knowledge_base_stream`)

**KB label:** `KB_STREAM_FIELD`

Fixed-length circular buffer of timestamped records. The `stream_length` property in the KB node controls how many rows exist for each path.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| path | LTREE | Stream identifier |
| recorded_at | TIMESTAMPTZ | Timestamp of the record |
| valid | BOOLEAN | Whether this record contains valid data |
| data | JSONB | Record payload |

**Build function:** `add_stream_field(stream_key, stream_length, description)`

**Sync behavior:** Paths not in the KB are deleted. For each valid path, the row count is adjusted to match `stream_length` — excess rows are removed (oldest first), missing rows are inserted with `valid = FALSE` and empty data.

#### Job Table (`knowledge_base_job`)

**KB label:** `KB_JOB_QUEUE`

Fixed-length job queue with scheduling and lifecycle tracking. The `job_length` property controls queue depth.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| path | LTREE | Job queue identifier |
| schedule_at | TIMESTAMPTZ | When the job is scheduled |
| started_at | TIMESTAMPTZ | When processing began |
| completed_at | TIMESTAMPTZ | When processing finished |
| is_active | BOOLEAN | Whether the job is currently active |
| valid | BOOLEAN | Whether this slot contains a real job |
| data | JSONB | Job payload |

**Build function:** `add_job_field(job_key, job_length, description)`

**Sync behavior:** Same pattern as streams — invalid paths are purged, row counts are adjusted to match `job_length`.

#### RPC Server Table (`knowledge_base_rpc_server`)

**KB label:** `KB_RPC_SERVER_FIELD`

Inbound request queue for RPC servers. The `queue_depth` property controls how many request slots exist per server path.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| server_path | LTREE | Server endpoint identifier |
| request_id | UUID | Unique request identifier |
| rpc_action | TEXT | Action to perform |
| request_payload | JSONB | Request data |
| request_timestamp | TIMESTAMPTZ | When the request was made |
| transaction_tag | TEXT | Deduplication tag |
| state | TEXT | `'empty'`, `'new_job'`, or `'processing'` |
| priority | INTEGER | Request priority |
| processing_timestamp | TIMESTAMPTZ | When processing started |
| completed_timestamp | TIMESTAMPTZ | When processing finished |
| rpc_client_queue | LTREE | Return path for the response |

**Build function:** `add_rpc_server_field(rpc_server_key, queue_depth, description)`

**Sync behavior:** Unspecified paths are deleted (using a temp table for efficient batch operations). Queue depths are adjusted. All remaining records are reset to default values (`state = 'empty'`, empty payloads, fresh UUIDs).

#### RPC Client Table (`knowledge_base_rpc_client`)

**KB label:** `KB_RPC_CLIENT_FIELD`

Response queue for RPC clients. The `queue_depth` property controls slot count.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| request_id | UUID | Correlates to the original request |
| client_path | LTREE | Client endpoint identifier |
| server_path | LTREE | Which server handled the request |
| transaction_tag | TEXT | Deduplication tag |
| rpc_action | TEXT | Action that was performed |
| response_payload | JSONB | Response data |
| response_timestamp | TIMESTAMPTZ | When the response was recorded |
| is_new_result | BOOLEAN | Whether this result has been consumed |

**Build function:** `add_rpc_client_field(rpc_client_key, queue_depth, description)`

**Sync behavior:** Same pattern as RPC server — purge unspecified paths, adjust queue depths, restore defaults.

#### Document Table (`knowledge_base_document`)

**KB label:** `KB_JSONB_FIELD`

General-purpose document store with ltree paths, typed documents, and advisory locking.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| ltree | LTREE | Document path (unique) |
| type | TEXT | Document type/category |
| data | JSONB | Document content |
| locked_by | TEXT | Lock holder identifier |
| locked_at | TIMESTAMP | When the lock was acquired |
| lock_expires | TIMESTAMP | When the lock expires |
| created_at | TIMESTAMP | Creation time |
| updated_at | TIMESTAMP | Last modification time |

**Build function:** `add_jsonb_field(jsonb_key, type, description, data)`

**Sync behavior:** Compares current ltree paths against KB-declared paths. Missing paths are inserted with their declared type. Extra paths are deleted.

#### Bit Mask Table (`bit_mask_table`)

**KB label:** `KB_BIT_MASK`

64-bit atomic bit mask registers for distributed flag coordination.

| Column | Type | Description |
|--------|------|-------------|
| node_id | VARCHAR(255) | Register identifier (primary key) |
| bit_mask | BIGINT | 64-bit mask value |

**Build functions:**

1. `clear_bit_mask_flags()` — reset the flag definition accumulator
2. `add_bit_mask_flag(flag_name, bit_position, description)` — define a named bit
3. `create_bit_mask_entry(user_name, name, mask_size, bit_mask, description)` — validate flags, create the register, and record the definition in the KB

The bit mask builder validates that the number of defined flags matches `mask_size`, that no two flags share the same bit position, and that the initial mask value fits within the declared size.

---

## Synchronization Pattern

All satellite tables follow the same synchronization lifecycle:

1. **Declare** — During tree construction, call `add_*_field()` which creates an info node in the KB with a specific label and properties.
2. **Build** — After the tree is complete, call `check_installation()` on the facade.
3. **Reconcile** — Each satellite table queries the KB for nodes matching its label, compares against its own contents, then adds missing entries and removes orphaned ones.

This means the knowledge base tree is the single source of truth. You can change queue depths, add new fields, or remove old ones simply by modifying the construction script and re-running it. The satellite tables will adjust automatically.

## File Inventory

| File | Description |
|------|-------------|
| `knowledge_base_manager.lua` | Base PostgreSQL operations (connect, tables, CRUD) |
| `construct_kb.lua` | Stack-based KB construction DSL |
| `construct_data_tables.lua` | Facade composing all constructors |
| `construct_status_table.lua` | Status table constructor |
| `construct_stream_table.lua` | Stream table constructor |
| `construct_job_table.lua` | Job table constructor |
| `construct_rpc_server_table.lua` | RPC server table constructor |
| `construct_rpc_client_table.lua` | RPC client table constructor |
| `construct_jsonb_table.lua` | Document table constructor |
| `bit_mask_operations.lua` | Bit mask CRUD operations |
| `construct_bit_mask_store.lua` | Bit mask construction with flag validation |
| `test_knowledge_base_manager.lua` | Unit test for base manager |
| `test_construct_kb.lua` | Unit test for KB construction |
| `test_construct_data_tables.lua` | Integration test for full system |