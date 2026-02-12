# Knowledge Base C Port — Test Suite

## Overview

Three test drivers exercise the KB runtime library from unit level through
full integration, mirroring the LuaJIT `test_kb_data_structures.lua` test.

## Prerequisites

```bash
sudo apt-get install libsqlite3-dev libcjson-dev
```

## Build and Run

```bash
# From the project root:
make clean && make all && make run-tests

# Or from the tests/ directory:
cd tests
make all
make run
```

## Test Drivers

### test_bit_s_expression

**Self-contained, no database required.**

Unit tests for the S-expression evaluator (`bit_s_expression.c`).
Exercises the recursive descent parser against bit mask data:

- Literal values: `0`, `1`, `true`, `false`
- Bit access: `(bit N)`, `(bit_changed N)`
- Boolean logic: `(and ...)`, `(or ...)`, `(not ...)`
- Conditionals: `(if cond then else)`, `(cond (test result) ...)`
- Nested expressions: `(and (bit 0) (not (bit 1)))`

```
40 assertions, 0 database dependencies
```

### test_kb_query_support

**Self-contained, uses in-memory SQLite database.**

Unit tests for the CTE progressive filter chain (`kb_query_support.c`).
Creates a 5-row in-memory KB table, then exercises:

- No-filter select (returns all rows)
- Label filter: `KB_STATUS_FIELD`, `article`
- Combined filters: `kb=kb1 + label=article`
- Name filter
- `has_link` filter
- `find_path_values` extraction
- `decode_link_nodes` path parsing (`kb.uuid1.link.uuid2.name` format)

```
24 assertions, in-memory database
```

### test_kb_data_structures

**Two modes: in-memory synthetic data OR real LuaJIT-constructed database.**

Integration test exercising all subsystems through the `kb_ds_t` aggregator
facade. Mirrors the LuaJIT `test_kb_data_structures.lua`.

#### Mode 1: In-memory (no arguments)

```bash
./test_kb_data_structures
```

Creates a self-contained in-memory database with synthetic tables and seed
data, then runs deterministic assertions against hardcoded paths:

- **Status table**: get/set/round-trip with JSON data
- **Job queue**: free count → push → queued count → peek → complete → free restored
- **RPC server**: push → peek → claim → complete → state counts
- **RPC client**: push_and_claim → state counts → peek_reply → clear_reply
- **Bit mask**: set bits → get bits → get mask → change mask → clear change mask
- **Link tables**: get by link_name, get by mount_name

```
42 assertions, in-memory database
```

#### Mode 2: Real database (with arguments)

```bash
./test_kb_data_structures knowledge_base.db
./test_kb_data_structures knowledge_base.db knowledge_base    # explicit table name
./test_kb_data_structures /path/to/my.db my_table_name        # custom table name
```

Opens the LuaJIT-constructed database file and runs discovery-based tests.
The table name defaults to `"knowledge_base"` (matching the LuaJIT constructor
convention `KB_Data_Structures.new(db_file, 'knowledge_base')`).

Discovery uses `KB_Search` label filters to locate paths dynamically, then
exercises each subsystem using the discovered paths:

- **KB_Search**: all nodes, find_path_values, find_description, label scans
  (`KB_STATUS_FIELD`, `KB_JOB_FIELD`, `KB_BIT_FIELD`, `KB_STREAM_FIELD`,
   `KB_RPC_SERVER_FIELD`, `KB_RPC_CLIENT_FIELD`, `KB_LINK_NODE`),
  `starting_path`, `has_link`, `has_link_mount`
- **Status data**: discover path → get → set → read back
- **Job queue**: discover path → clear → push → peek → complete → verify free restored
- **Bit mask**: discover path → read mask → set bit → read bit → clear change mask
- **RPC server**: discover path → push → peek → claim → complete
- **RPC client**: discover path → push_and_claim → peek_reply → clear_reply
- **Link tables**: query distinct link_name / mount_path from auxiliary tables
- **decode_link_nodes**: parse discovered paths into `kb_name + link pairs`

Subsystems whose tables don't exist in the database are gracefully skipped.

```
Dynamic assertion count, real database
```

## Test Macros

Defined in `test_common.h`:

| Macro | Purpose |
|-------|---------|
| `ASSERT_OK(rc, msg)` | Expect `KB_OK` return code |
| `ASSERT_ERR(rc, expected, msg)` | Expect specific error code |
| `ASSERT_EQ_INT(a, b, msg)` | Integer equality |
| `ASSERT_EQ_STR(a, b, msg)` | String equality |
| `ASSERT_NOT_NULL(ptr, msg)` | Non-null pointer |
| `ASSERT_TRUE(cond, msg)` | Boolean condition |
| `TEST_BEGIN(name)` | Print test section header |
| `TEST_END()` | Print pass/fail summary and return exit code |

## Diagnostic Output

`kb_common.c` includes SQLite error diagnostics. When a query fails, stderr
shows the actual SQLite error message and the SQL that failed:

```
  [SQL ERROR] prepare: no such table: knowledge_base_status_table
    SQL: SELECT data FROM knowledge_base_status_table WHERE path = ?
```

## Expected Results

```
Running test_kb_data_structures (in-memory)  →  42 passed, 0 failed
Running test_kb_query_support                →  24 passed, 0 failed
Running test_bit_s_expression                →  40 passed, 0 failed
                                        Total: 106 passed, 0 failed
```

