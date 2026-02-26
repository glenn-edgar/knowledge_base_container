# ChainTree Binary Generator

LuaJIT-based generator that converts ChainTree YAML configurations into compact binary format for embedded systems.

## Features

- **Binary format** - Compact, memory-mappable binary that can run directly from flash
- **Hash-based function resolution** - Functions identified by FNV-1a hash, no strings in binary
- **CRC32 integrity checking** - Header and data CRCs for validation
- **64-bit compatible** - Same binary works on 32-bit and 64-bit systems
- **Custom allocator support** - Use your own memory allocation functions

## Usage

```bash
luajit generate.lua <input.yaml> <output_dir> [options]

Options:
  --name=<name>   Base name for output files (default: from yaml filename)
  --verbose       Enable verbose output
```

### Example

```bash
luajit generate.lua chaintree_config.yaml output/ --name=my_tree --verbose
```

### Output Files

| File | Description |
|------|-------------|
| `<name>.bin` | Raw binary blob |
| `<name>_bin.h` | Binary as C uint8_t array |
| `<name>_hashes.h` | Hash #defines for functions/events/bitmasks |
| `<name>_resolver.h` | Template for user function resolution |

## Binary Format

### Header (96 bytes, padded)

```c
typedef struct __attribute__((packed)) {
    uint8_t  magic[4];              // "CTRB"
    uint16_t version;               // 0x0001
    uint16_t flags;                 // Reserved
    uint32_t total_size;            // Total binary size
    
    // Section offsets (13 × uint32_t)
    uint32_t nodes_offset;
    uint32_t link_table_offset;
    uint32_t kb_table_offset;
    uint32_t kb_aliases_offset;
    uint32_t main_func_hashes_offset;
    uint32_t one_shot_func_hashes_offset;
    uint32_t boolean_func_hashes_offset;
    uint32_t main_func_usage_offset;
    uint32_t event_hashes_offset;
    uint32_t bitmask_hashes_offset;
    uint32_t node_data_records_offset;
    uint32_t node_data_controls_offset;
    uint32_t node_data_strings_offset;
    
    // Counts (12 × uint16_t)
    uint16_t node_count;
    uint16_t link_table_size;
    uint16_t kb_count;
    uint16_t main_function_count;
    uint16_t one_shot_function_count;
    uint16_t boolean_function_count;
    uint16_t event_count;
    uint16_t bitmask_count;
    uint16_t node_data_records_count;
    uint16_t node_data_controls_count;
    uint16_t node_data_strings_size;
    uint16_t total_aliases_count;
    
    uint32_t unique_id_hash;
    uint32_t header_crc32;
} chaintree_binary_header_t;
```

### Node Structure (20 bytes each)

```c
typedef struct __attribute__((packed)) {
    uint16_t node_index;
    uint16_t parent_index;
    uint16_t depth;
    uint16_t link_start;
    uint16_t link_count;            // Bits 0-14: count, Bit 15: auto_start
    uint16_t main_function_index;
    uint16_t init_function_index;
    uint16_t aux_function_index;
    uint16_t term_function_index;
    uint16_t node_data_id;
} chaintree_binary_node_t;
```

### KB Info Structure (20 bytes each)

```c
typedef struct __attribute__((packed)) {
    uint32_t kb_name_hash;
    uint16_t root_node_index;
    uint16_t start_index;
    uint16_t node_count;
    uint16_t max_depth;
    uint16_t memory_factor;
    uint16_t alias_count;
    uint32_t aliases_offset;
} chaintree_binary_kb_info_t;
```

### Sections (in order)

1. **Header** (96 bytes)
2. **Nodes** - `node_count × 20 bytes`
3. **Link Table** - `link_table_size × 2 bytes`
4. **KB Table** - `kb_count × 20 bytes`
5. **KB Aliases** - `total_aliases_count × 8 bytes`
6. **Main Function Hashes** - `main_function_count × 4 bytes`
7. **One-Shot Function Hashes** - `one_shot_function_count × 4 bytes`
8. **Boolean Function Hashes** - `boolean_function_count × 4 bytes`
9. **Main Function Usage** - `main_function_count × 2 bytes`
10. **Event Hashes** - `event_count × 4 bytes`
11. **Bitmask Hashes** - `bitmask_count × 4 bytes`
12. **Node Data Records** - `node_data_records_count × 8 bytes`
13. **Node Data Controls** - `node_data_controls_count × 8 bytes`
14. **Node Data Strings** - `node_data_strings_size bytes`
15. **Data CRC32** (4 bytes)

All sections are 4-byte aligned.

## Hash Function

FNV-1a 32-bit with standard parameters:
- Offset basis: `0x811C9DC5`
- Prime: `0x01000193`

Special case: `CFL_NULL` always maps to hash `0x00000000`

Function hashes use typed names: `<function_name>_<type>` (lowercase)
- Main: `sequence_main`
- One-shot: `init_system_one_shot`
- Boolean: `check_ready_boolean`

## Runtime Loading

### Include Files

```c
#include "chaintree_binary_support.h"
#include "my_tree_hashes.h"
#include "my_tree_resolver.h"
```

### Implement Resolvers

```c
main_function_t resolve_main_function(uint32_t hash) {
    switch (hash) {
        case HASH_CFL_NULL:         return cfl_null_main_fn;
        case HASH_MAIN_SEQUENCE_MAIN: return sequence_main_fn;
        // ... other functions
        default: return NULL;
    }
}

one_shot_function_t resolve_one_shot_function(uint32_t hash) {
    switch (hash) {
        case HASH_CFL_NULL:           return cfl_null_one_shot_fn;
        case HASH_ONE_SHOT_INIT_ROOT: return init_root_fn;
        // ...
        default: return NULL;
    }
}

boolean_function_t resolve_boolean_function(uint32_t hash) {
    switch (hash) {
        case HASH_CFL_NULL:             return cfl_null_boolean_fn;
        case HASH_BOOLEAN_CHECK_READY:  return check_ready_fn;
        // ...
        default: return NULL;
    }
}
```

### Load Binary

```c
// Custom allocator
void* my_alloc(void *handle, size_t size) {
    return malloc(size);  // Or your allocator
}

void my_free(void *handle, void *ptr) {
    free(ptr);
}

// Setup
ct_allocator_t allocator = {
    .handle = NULL,
    .alloc = my_alloc,
    .free = my_free
};

ct_resolver_t resolver = {
    .resolve_main = resolve_main_function,
    .resolve_one_shot = resolve_one_shot_function,
    .resolve_boolean = resolve_boolean_function
};

// Load from embedded binary
extern const uint8_t my_tree_binary[];
extern const uint32_t MY_TREE_SIZE;

chaintree_runtime_t *runtime;
ct_load_result_t result = ct_load_binary(
    my_tree_binary,
    MY_TREE_SIZE,
    &allocator,
    &resolver,
    &runtime
);

if (result != CT_LOAD_OK) {
    // Handle error
}

// Use runtime...

// Cleanup
ct_unload(runtime, &allocator);
```

### Access Data

```c
// Get node
const chaintree_binary_node_t *node = CT_GET_NODE(runtime, node_index);

// Get link
uint16_t child_index = CT_GET_LINK(runtime, node->link_start + i);

// Check auto_start
bool auto_start = CT_NODE_AUTO_START(node);
uint16_t link_count = CT_NODE_LINK_COUNT(node);

// Get KB info
const chaintree_binary_kb_info_t *kb = CT_GET_KB(runtime, kb_index);

// Call function
runtime->main_functions[node->main_function_index](
    handle, node->aux_function_index, node->node_index,
    event_type, event_id, event_data
);
```

## File Structure

```
chaintree_binary/
├── generate.lua              # Main generator script
├── lib/
│   ├── yaml_parser.lua       # YAML parsing
│   ├── fnv1a.lua             # FNV-1a hash
│   ├── crc32.lua             # CRC32
│   ├── binary_writer.lua     # Binary packing
│   └── header_gen.lua        # Header file generation
├── chaintree_binary_support.h  # Runtime header
├── chaintree_binary_support.c  # Runtime loader implementation
└── test/
    └── test_config.yaml      # Test configuration
```

## Requirements

- LuaJIT 2.x (with JIT disabled for deterministic output)

## License

MIT License