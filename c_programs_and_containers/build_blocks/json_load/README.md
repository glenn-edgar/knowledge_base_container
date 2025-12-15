# JSON to ChainTree Embedded C System

A lightweight, ROM-friendly system for parsing JSON into ChainTree behavior trees on resource-constrained embedded systems. Designed for targets from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.

## Features

- **Zero runtime allocation** - All data structures use pre-allocated buffers
- **ROM-resident data** - Generated headers produce `const` arrays for flash storage
- **Path-based JSON access** - Simple API: `json_path_int(&c, "config.timeout", 1000)`
- **Attached node data** - Custom fields accessible at runtime without schema changes
- **Configurable node types** - YAML-driven enum generation
- **Header-only C library** - No .c files to compile, just `#include`
- **C++ compatible** - `extern "C"` guards included

## Quick Start

```bash
# 1. Generate node types (one-time or when types change)
python3 generate_node_types.py chaintree_types.yaml -o chaintree_node_types.h

# 2. Generate tree data from JSON
python3 json_record_encoder.py my_tree.json -o my_tree_data.h

# 3. Compile your application
gcc -Wall -o app main.c
```

```c
#include "cfl_exception.h"
#include "json_record_reader.h"
#include "json_path.h"
#include "chaintree_nodes.h"
#include "chaintree_from_json.h"
#include "my_tree_data.h"

int main(void) {
    static ct_node_t nodes[64];
    ct_tree_t tree;
    
    // Build tree - size comes from data descriptor
    ct_build_from_data(&tree, nodes, my_tree_data.node_count, &my_tree_data);
    
    // Use tree...
    const ct_node_t* root = ct_get_root(&tree);
    printf("Root: %s\n", root->name);
    
    return 0;
}
```

---

## Python Tools

### json_record_encoder.py

Converts JSON files into C headers with flat record arrays.

**Usage:**
```bash
python3 json_record_encoder.py <input.json> [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Output .h file (stdout if not specified) |
| `-n, --name NAME` | Base name for arrays (default: derived from filename) |
| `-b, --binary FILE` | Output binary file for runtime loading |
| `--dump` | Debug: dump records to stdout |
| `--stats` | Print encoding statistics |
| `--include-types` | Include type definitions (standalone mode) |

**Examples:**
```bash
# Basic - names derived from filename
python3 json_record_encoder.py sensor_tree.json -o sensor_tree_data.h
# Generates: sensor_tree_strings, sensor_tree_records, sensor_tree_data

# Custom name
python3 json_record_encoder.py config.json -o data.h -n my_config

# Binary for runtime loading
python3 json_record_encoder.py tree.json -b tree.bin

# Both header and binary
python3 json_record_encoder.py tree.json -o tree_data.h -b tree.bin

# Multiple input files (shared string table)
python3 json_record_encoder.py tree1.json tree2.json -o combined.h

# Show statistics
python3 json_record_encoder.py tree.json --stats
```

**Generated Output:**
```c
static const char sensor_tree_strings[] = { ... };
static const json_record_t sensor_tree_records[71] = { ... };

static const json_data_t sensor_tree_data = {
    .records = sensor_tree_records,
    .strings = sensor_tree_strings,
    .record_count = 71,
    .string_size = 180,
    .node_count = 9
};

#define SENSOR_TREE_RECORDS_COUNT 71
#define SENSOR_TREE_STRINGS_SIZE 180
#define SENSOR_TREE_DATA_NODE_COUNT 9
```

---

### generate_node_types.py

Generates C header with node type enum and mapping functions from YAML config.

**Usage:**
```bash
python3 generate_node_types.py <config.yaml> -o <output.h>
```

**Options:**

| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Output .h file (stdout if not specified) |
| `--guard NAME` | Custom include guard name |

**YAML Config Format (chaintree_types.yaml):**
```yaml
types:
  - enum_name: CT_SEQUENCE
    json_name: sequence
    description: Run children in order, fail on first failure

  - enum_name: CT_SELECTOR
    json_name: selector
    description: Run children in order, succeed on first success

  - enum_name: CT_ACTION
    json_name: action
    description: Leaf node that performs an action

  # Add custom types here...
  - enum_name: CT_RETRY
    json_name: retry
    description: Retry child on failure up to N times

default_type: CT_ACTION
enum_name: ct_node_type_t
prefix: CT
```

**Generated Output:**
```c
typedef enum {
    CT_SEQUENCE = 0,
    CT_SELECTOR = 1,
    CT_ACTION = 2,
    CT_RETRY = 3,
    CT_TYPE_COUNT
} ct_node_type_t;

static inline ct_node_type_t ct_type_from_string(const char* s);
static inline const char* ct_type_name(ct_node_type_t type);

// Runtime introspection
static const ct_type_info_t ct_type_info[CT_TYPE_COUNT];
```

---

## C Runtime Headers

### Include Order

```c
#include "cfl_exception.h"        // 1. Exception handler (required)
#include "json_record_reader.h"   // 2. Core JSON reader
#include "json_path.h"            // 3. Path-based access
#include "chaintree_node_types.h" // 4. Generated node types
#include "chaintree_nodes.h"      // 5. Node structure
#include "chaintree_from_json.h"  // 6. Tree builder
#include "my_tree_data.h"         // 7. Generated data
```

---

### cfl_exception.h

Exception handling stub. Replace with your implementation.

```c
// Default: logs to stderr
#define EXCEPTION(msg) cfl_exception_handler(__FILE__, __func__, __LINE__, msg)

// Your implementation might:
// - Log and continue
// - Log and reset via watchdog
// - Store error and halt
// - longjmp to error handler
```

---

### json_record_reader.h

Core JSON record reader with cursor-based navigation.

**Types:**
```c
typedef struct {
    const json_record_t* records;
    const char* strings;
    uint32_t record_count;
    uint32_t string_size;
    uint32_t node_count;
} json_data_t;

typedef struct { ... } json_reader_t;
typedef struct { ... } json_cursor_t;
typedef struct { ... } json_array_iter_t;
```

**Initialization:**
```c
// From generated data descriptor (preferred)
json_reader_t reader;
json_cursor_t c;
json_cursor_init_from_data(&c, &reader, &my_tree_data);

// From separate components
json_reader_init(&reader, records, count, strings);
json_cursor_init(&c, &reader, &control);
```

**Value Access:**
```c
const char* s = json_get_string(&c);
int32_t i = json_get_int(&c);
float f = json_get_float(&c);
bool b = json_get_bool(&c);
uint8_t type = json_cursor_type(&c);  // JSON_TYPE_*
```

**Array Iteration:**
```c
json_array_iter_t it;
json_array_iter_init(&it, &cursor);

json_cursor_t elem;
while (json_array_iter_next(&it, &elem)) {
    const char* val = json_get_string(&elem);
}
```

---

### json_path.h

Path-based JSON access with dot notation and array indexing.

**Path Syntax:**
- `"key"` - Object key
- `"a.b.c"` - Nested keys
- `"arr[0]"` - Array index
- `"a.b[2].c[0].d"` - Mixed paths

**Optional Access (returns default on failure):**
```c
int32_t timeout = json_path_int(&c, "config.timeout", 1000);
float rate = json_path_float(&c, "sample_rate", 10.0f);
bool enabled = json_path_bool(&c, "flags.enabled", false);
const char* name = json_path_string(&c, "name", "unnamed");
```

**Required Access (throws exception on failure):**
```c
const char* type = json_path_string_ex(&c, "type");  // Throws if missing
int32_t id = json_path_int_ex(&c, "handler_id");
```

**Cursor Navigation:**
```c
json_cursor_t child;
if (json_path_cursor(&c, "children[0]", &child) == JSON_PATH_OK) {
    // Use child cursor...
}

// Check existence
if (json_path_exists(&c, "optional_field")) { ... }
```

**Array Iteration via Path:**
```c
json_array_iter_t it;
json_path_array_iter_init(&it, &c, "items");

json_cursor_t item;
while (json_array_iter_next(&it, &item)) {
    const char* name = json_path_string(&item, "name", NULL);
}
```

---

### chaintree_node_types.h (Generated)

Node type enum and mapping functions. Generated from YAML config.

```c
typedef enum {
    CT_SEQUENCE, CT_SELECTOR, CT_PARALLEL,
    CT_ACTION, CT_CONDITION, CT_STATE,
    CT_DECORATOR, CT_REPEAT, CT_INVERTER,
    CT_TIMEOUT, CT_RETRY, CT_GUARD,
    CT_TYPE_COUNT
} ct_node_type_t;

ct_node_type_t ct_type_from_string(const char* s);  // "sequence" -> CT_SEQUENCE
const char* ct_type_name(ct_node_type_t type);      // CT_SEQUENCE -> "sequence"

// Runtime introspection
ct_type_info_t ct_type_info[CT_TYPE_COUNT];  // {type, json_name, description}
```

---

### chaintree_nodes.h

ChainTree node structure and navigation.

**Node Structure:**
```c
typedef struct {
    uint16_t type;           // ct_node_type_t
    uint16_t first_child;    // Index, CT_NO_LINK if none
    uint16_t next_sibling;   // Index, CT_NO_LINK if none
    uint16_t parent;         // Index, CT_NO_LINK if root
    uint16_t handler_id;     // Maps to function pointer table
    uint16_t flags;          // User-defined
    const char* name;        // Points into string table
    uint32_t data_pos;       // Position for attached data access
} ct_node_t;

typedef struct {
    const ct_node_t* nodes;
    uint16_t node_count;
    uint16_t root_index;
    const void* data;        // json_data_t* for attached data
} ct_tree_t;
```

**Navigation:**
```c
const ct_node_t* root = ct_get_root(&tree);
const ct_node_t* child = ct_get_first_child(&tree, node);
const ct_node_t* sibling = ct_get_next_sibling(&tree, node);
const ct_node_t* parent = ct_get_parent(&tree, node);

bool leaf = ct_is_leaf(node);
bool has_kids = ct_has_children(node);
uint16_t count = ct_count_children(&tree, node);
```

**Child Iteration:**
```c
ct_child_iter_t it;
ct_child_iter_init(&it, &tree, parent);

const ct_node_t* child;
while ((child = ct_child_iter_next(&it)) != NULL) {
    printf("%s\n", child->name);
}
```

---

### chaintree_from_json.h

Build ChainTree from JSON records.

**Building:**
```c
static ct_node_t nodes[64];
ct_tree_t tree;

// From data descriptor (preferred)
ct_build_from_data(&tree, nodes, my_data.node_count, &my_data);

// From reader + control
ct_build_from_records(&tree, nodes, 64, &reader, &control);
```

**Attached Data Access:**
```c
json_reader_t reader;
json_cursor_t c;

if (ct_node_data(&tree, node, &reader, &c)) {
    int32_t timeout = json_path_int(&c, "timeout_ms", 1000);
    const char* sensor = json_path_string(&c, "sensor_id", NULL);
    int32_t min = json_path_int(&c, "thresholds.min", 0);
}
```

**Debug Helpers:**
```c
ct_dump_tree(&tree);   // Table format
ct_print_tree(&tree);  // Hierarchical format
```

---

### json_record_file.h

Runtime loading from binary files.

**Static Buffers (embedded):**
```c
static char string_buf[2048];
static json_record_t record_buf[256];
static record_control_t control_buf[16];

json_file_buffers_t bufs = {
    .strings = string_buf,  .strings_size = sizeof(string_buf),
    .records = record_buf,  .records_count = 256,
    .controls = control_buf, .controls_count = 16
};

json_data_t data;
record_control_t* controls;
uint32_t num_controls;

json_file_load("tree.bin", &bufs, &data, &controls, &num_controls);
```

**Dynamic Allocation (Linux/RTOS):**
```c
json_file_data_t* file = json_file_load_alloc("tree.bin");
// Use file->data...
json_file_free(file);
```

**Query File Info:**
```c
json_file_info_t info = json_file_get_info("tree.bin");
printf("Nodes: %u, Strings: %u bytes\n", info.node_count, info.string_size);
```

---

## JSON Format

### ChainTree Node Schema

```json
{
  "type": "<node_type>",
  "name": "<optional_string>",
  "handler_id": <optional_uint16>,
  "flags": <optional_uint16>,
  "children": [ <optional_array> ],
  
  "<custom_field>": <any_value>
}
```

| Field | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `type` | **Yes** | string | - | Node type from YAML config |
| `name` | No | string | NULL | Debug/display name |
| `handler_id` | No | int | 0 | Function pointer table index |
| `flags` | No | int | 0 | User-defined bitflags |
| `children` | No | array | [] | Child nodes |
| `*` | No | any | - | Custom attached data |

### Example Tree

```json
{
  "type": "sequence",
  "name": "main_loop",
  "timeout_ms": 10000,
  "children": [
    {
      "type": "condition",
      "name": "check_sensor",
      "handler_id": 1,
      "sensor_id": "temp_1",
      "thresholds": {"min": 10, "max": 100}
    },
    {
      "type": "selector",
      "name": "decide_action",
      "children": [
        {"type": "action", "name": "fast_path", "handler_id": 10},
        {"type": "action", "name": "slow_path", "handler_id": 11}
      ]
    },
    {
      "type": "action",
      "name": "cleanup",
      "handler_id": 30,
      "retry_count": 3
    }
  ]
}
```

---

## Memory Usage

### Per-Node Cost

| Field | Size | Notes |
|-------|------|-------|
| type | 2 bytes | |
| first_child | 2 bytes | |
| next_sibling | 2 bytes | |
| parent | 2 bytes | |
| handler_id | 2 bytes | |
| flags | 2 bytes | |
| name | 4/8 bytes | Pointer (32/64-bit) |
| data_pos | 4 bytes | Attached data position |
| **Total** | **20-24 bytes** | Per node |

### Example Tree (9 nodes)

| Component | Size |
|-----------|------|
| Records (71 × 8) | 568 bytes |
| Strings (deduplicated) | 180 bytes |
| Controls (1 × 8) | 8 bytes |
| **Total ROM** | **756 bytes** |
| Node buffer (9 × 20) | 180 bytes RAM |

---

## File Summary

| File | Type | Description |
|------|------|-------------|
| `json_record_encoder.py` | Tool | JSON → C header generator |
| `generate_node_types.py` | Tool | YAML → node types header |
| `chaintree_types.yaml` | Config | Node type definitions |
| `cfl_exception.h` | Runtime | Exception handler stub |
| `json_record_reader.h` | Runtime | Core JSON reader |
| `json_path.h` | Runtime | Path-based access |
| `chaintree_node_types.h` | Generated | Node type enum |
| `chaintree_nodes.h` | Runtime | Node structure |
| `chaintree_from_json.h` | Runtime | Tree builder |
| `json_record_file.h` | Runtime | Binary file loader |

---

## Adding Custom Node Types

1. Edit `chaintree_types.yaml`:
```yaml
types:
  # ... existing types ...
  - enum_name: CT_MY_TYPE
    json_name: my_type
    description: My custom node type
```

2. Regenerate header:
```bash
python3 generate_node_types.py chaintree_types.yaml -o chaintree_node_types.h
```

3. Use in JSON:
```json
{"type": "my_type", "name": "custom_node", "handler_id": 99}
```

---

## License

MIT License - See individual files for details.

