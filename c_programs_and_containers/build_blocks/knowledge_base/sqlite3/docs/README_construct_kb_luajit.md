# SQLite KnowledgeBaseManager — LuaJIT Implementation

## Overview

LuaJIT FFI port of the Python KnowledgeBaseManager system with full ltree extension support. Provides hierarchical path queries, distributed control table management (status, job, stream, RPC client/server, bit masks), and a unified construction API — all running through a single shared SQLite3 FFI binding layer.

Designed for ChainTree distributed control architectures spanning from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.

## Requirements

- LuaJIT 2.1+
- libsqlite3 (shared library, typically already installed)
- `ltree.so` / `ltree.dylib` — custom SQLite extension for PostgreSQL-style ltree operations

## File Structure

```
sqlite3_helpers.lua              ← Shared FFI bindings, SQL helpers, JSON encode/decode
knowledge_base_manager.lua       ← Core KB CRUD + ltree query methods
construct_kb.lua                 ← Stack-based KB construction
bit_mask_operations.lua          ← Bit mask table CRUD
construct_bit_mask_store.lua     ← Bit mask + KB integration
construct_status_table.lua       ← Status table construction
construct_job_table.lua          ← Job queue table construction
construct_stream_table.lua       ← Stream buffer table construction
construct_rpc_client_table.lua   ← RPC client queue construction
construct_rpc_server_table.lua   ← RPC server queue construction
construct_data_tables.lua        ← Aggregator (unified API)
test_construct_data_tables.lua   ← Full test driver (3 tests)
test_construct_kb.lua            ← KB-only test
test_knowlege_base_manager.lua   ← Manager-only test
ltree.so                         ← ltree extension (platform-specific)
```

### Module Dependency Graph

```
sqlite3_helpers
├── knowledge_base_manager
│   └── construct_kb
│       └── construct_data_tables (aggregator)
│           ├── construct_status_table
│           ├── construct_job_table
│           ├── construct_stream_table
│           ├── construct_rpc_client_table
│           ├── construct_rpc_server_table
│           └── construct_bit_mask_store
│               └── bit_mask_operations
```

All database access flows through `sqlite3_helpers.lua`. LuaJIT's `require` caches modules, so `ffi.load('sqlite3')` is called exactly once.

## Quick Start

### Basic Knowledge Base

```lua
local KBM = require('knowledge_base_manager')

-- Auto-detects ltree extension from common locations
local kb = KBM.new('knowledge_base', 'database.db')

kb:add_kb('robotics', 'Robot control system')
kb:add_node('robotics', 'gate', 'Main Gate', {}, {},
    'system.GATE_main._0')
kb:add_node('robotics', 'sequence', 'Init',  {}, {},
    'system.GATE_main._0.SEQ_init._1')
kb:add_node('robotics', 'action', 'Home',    {}, {},
    'system.GATE_main._0.SEQ_init._1.ACT_home._0')

-- ltree queries
local gates = kb:find_by_pattern('*.*.GATE*.*', 'robotics')
local descendants = kb:find_descendants('system.GATE_main._0', 'robotics')
local depth = kb:get_node_depth('system.GATE_main._0.SEQ_init._1')

kb:disconnect()
```

### Full Data Tables (Unified API)

```lua
local CDT = require('construct_data_tables')

local kb = CDT.new('knowledge_base.db', 'knowledge_base')

kb:add_kb('kb1', 'First knowledge base')
kb:select_kb('kb1')

kb:add_header_node('sensor_link', 'sensor_module', { type = 'sensor' }, {})
kb:add_status_field('temperature', { unit = 'C' }, 'Temperature reading', { value = 0 })
kb:add_job_field('calibrate', 10, 'Calibration job queue')
kb:add_stream_field('data_log', 100, 'Sensor data stream')
kb:add_rpc_server_field('cmd_server', 25, 'Command server')
kb:add_rpc_client_field('cmd_client', 10, 'Command client')

kb:clear_bit_mask_flags()
kb:add_bit_mask_flag('enabled', 0, 'Sensor enabled')
kb:add_bit_mask_flag('fault',   1, 'Sensor fault')
kb:create_bit_mask_entry('admin', 'sensor_flags', 2, 0, 'Sensor status flags')

kb:leave_header_node('sensor_link', 'sensor_module')

kb:check_installation()
kb:disconnect()
```

### Explicit Extension Path

```lua
-- Specify installed location (no .so/.dylib suffix!)
local kb = KBM.new('my_kb', 'database.db', '/usr/local/lib/ltree')

-- Or local build
local kb = KBM.new('my_kb', 'database.db', './ltree')
```

## Ltree Extension

### Auto-Detection

When no path is specified, the following locations are searched in order:

1. `./ltree` (current directory)
2. `/usr/local/lib/ltree` (installed location)
3. `/usr/lib/ltree` (system location)

**Important**: Do NOT include the `.so`/`.dylib` suffix. SQLite's `load_extension()` appends it automatically. Passing `./ltree.so` would try to load `./ltree.so.so`.

### Building the Extension

```bash
cd /path/to/ltree/extension
make
make test
sudo make install    # installs to /usr/local/lib/ltree.so
```

### Verifying

```bash
ls -l /usr/local/lib/ltree.so
# or for local build:
ls -l ./ltree.so
```

## Ltree Query Methods

### Pattern Matching — `find_by_pattern(pattern, kb_name)`

```lua
-- Exact match
kb:find_by_pattern('kb.test.node', 'kb1')

-- Single wildcard (one label)
kb:find_by_pattern('kb.*.node', 'kb1')

-- Prefix matching
kb:find_by_pattern('kb.*.GATE*.*', 'kb1')

-- Quantified wildcards
kb:find_by_pattern('kb.*{2}.node', 'kb1')      -- exactly 2 levels
kb:find_by_pattern('kb.*{1,3}.node', 'kb1')    -- 1 to 3 levels
kb:find_by_pattern('kb.*{2,}.node', 'kb1')     -- 2 or more levels
```

### Pattern Reference

| Pattern | Description | Example Match |
|---------|-------------|---------------|
| `kb.test.node` | Exact path | `kb.test.node` |
| `kb.*.node` | Single wildcard | `kb.anything.node` |
| `*.*.*` | Any 3-level path | `any.three.levels` |
| `kb.*.GATE*.*` | Prefix match | `kb.test.GATE_root._0` |
| `kb.*{2}.node` | Exactly 2 levels | `kb.a.b.node` |
| `kb.*{1,3}.node` | 1–3 levels | `kb.a.node` to `kb.a.b.c.node` |
| `kb.*{2,}.node` | 2+ levels | `kb.a.b.node`, `kb.a.b.c.node` |
| `kb.*{,2}.node` | 0–2 levels | `kb.node` to `kb.a.b.node` |

### Hierarchy Queries

```lua
-- All descendants of a path
local desc = kb:find_descendants('system.GATE_root._0', 'kb1')

-- All ancestors of a path
local anc = kb:find_ancestors('system.GATE_root._0.SEQ_init._1', 'kb1')

-- Immediate children only
local children = kb:find_children('system.GATE_root._0', 'kb1')

-- Node depth
local depth = kb:get_node_depth('system.GATE_root._0.SEQ_init._1')

-- All nodes at a specific depth
local level3 = kb:find_by_depth(3, 'kb1')
```

## Running Tests

```bash
# Full test suite (3 tests: complete, modified fields, reduced queues)
luajit test_construct_data_tables.lua knowledge_base.db

# With upload_flag
luajit test_construct_data_tables.lua knowledge_base.db True

# With unit_test (runs all 3 tests)
luajit test_construct_data_tables.lua knowledge_base.db False True

# KB-only test
luajit test_construct_kb.lua

# Manager-only test
luajit test_knowlege_base_manager.lua
```

Inspect the resulting database:

```bash
sqlite3 knowledge_base.db ".tables"
sqlite3 knowledge_base.db "SELECT path, label, name FROM knowledge_base LIMIT 20"
```

## ChainTree Integration Example

```lua
local CDT = require('construct_data_tables')
local kb = CDT.new('robot.db', 'knowledge_base')

kb:add_kb('robot', 'Robot control system')
kb:select_kb('robot')

-- Build behavior tree
kb:add_header_node('GATE', 'root_selector', { type = 'selector' }, {})

  kb:add_header_node('SEQ', 'init_sequence', { type = 'sequence' }, {})
    kb:add_info_node('ACT', 'home_motors', { action = 'home' }, {})
    kb:add_info_node('ACT', 'enable_ctrl', { action = 'enable' }, {})
    kb:add_job_field('init_job', 5, 'Initialization job queue')
  kb:leave_header_node('SEQ', 'init_sequence')

  kb:add_header_node('SEQ', 'main_loop', { type = 'sequence' }, {})
    kb:add_info_node('ACT', 'read_sensors', { action = 'read' }, {})
    kb:add_stream_field('sensor_data', 100, 'Sensor data stream')
    kb:add_status_field('loop_status', {}, 'Main loop status', { running = false })
  kb:leave_header_node('SEQ', 'main_loop')

kb:leave_header_node('GATE', 'root_selector')

kb:check_installation()
kb:disconnect()
```

The ltree paths enable efficient traversal and pattern matching across the distributed control hierarchy:

```
kb.robot.GATE.root_selector
├── SEQ.init_sequence
│   ├── ACT.home_motors
│   └── ACT.enable_ctrl
└── SEQ.main_loop
    ├── ACT.read_sensors
    └── ...
```

## Differences from Python Version

| Aspect | Python | LuaJIT |
|--------|--------|--------|
| DB binding | `sqlite3` stdlib | FFI to `libsqlite3` |
| JSON | `json` stdlib | cjson or pure-Lua fallback |
| UUID | `uuid.uuid4()` | Pure-Lua `uuid4()` via `math.random` |
| Inheritance | `class Foo(Bar)` | Composition + delegation |
| Dict length | `len(d)` | `table_count(t)` helper |
| List append | `list.append(x)` | `t[#t + 1] = x` |
| List pop | `list.pop()` | `t[#t] = nil` |
| String join | `".".join(list)` | `table.concat(t, ".")` |
| Placeholders | `?` (same) | `?` (same) |

## Performance Notes

- Always specify `kb_name` to use the composite index on `(knowledge_base, path)`
- Use specific patterns rather than broad wildcards
- Prefer exact quantifiers (`{2}`) over open-ended ranges (`{2,}`)
- LuaJIT's FFI calls to libsqlite3 have near-zero overhead compared to Python's ctypes/CFFI
- The `sql_query` helper caches column names per statement for efficient row construction

## Troubleshooting

**Extension not loading — `ltree.so.so: cannot open`**: Don't include the `.so` suffix in the path. Use `'./ltree'` not `'./ltree.so'`.

**`attempt to redefine` error in ffi.cdef**: Ensure you have the latest `sqlite3_helpers.lua` which wraps `ffi.cdef` in `pcall()`.

**`module not found` on require**: Ensure all `.lua` files are in the same directory where you run `luajit`, or adjust `package.path`.

**ltree queries return no results**: Check that paths in the database match the pattern syntax (case-sensitive, dot-separated labels).

## License

MIT License — see repository for details.
