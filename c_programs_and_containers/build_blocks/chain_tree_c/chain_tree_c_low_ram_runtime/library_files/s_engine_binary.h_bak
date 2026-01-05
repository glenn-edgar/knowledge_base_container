// ============================================================================
// s_engine_binary.h
// Binary Module Format Definitions and Parser API
// ============================================================================

#ifndef S_ENGINE_BINARY_H
#define S_ENGINE_BINARY_H

#include "s_engine_types.h"

// ============================================================================
// BINARY FORMAT CONSTANTS
// ============================================================================

#define SMOD_MAGIC      0x32444D53  // "SMD2" little-endian
#define SMOD_VERSION    0x0001

// ============================================================================
// ERROR CODES
// ============================================================================

#define BIN_OK                  0
#define BIN_ERR_NULL_INPUT      1
#define BIN_ERR_TOO_SMALL       2
#define BIN_ERR_BAD_MAGIC       3
#define BIN_ERR_BAD_VERSION     4
#define BIN_ERR_TRUNCATED       5
#define BIN_ERR_ALLOC           6
#define BIN_ERR_BAD_OFFSET      7

// ============================================================================
// BINARY FILE STRUCTURES (packed, little-endian)
// These match the Lua to_bin() output exactly
// ============================================================================

#pragma pack(push, 1)

typedef struct {
    uint32_t magic;             // SMOD_MAGIC
    uint16_t version;           // SMOD_VERSION
    uint16_t flags;             // reserved
    uint16_t tree_count;
    uint16_t oneshot_count;
    uint16_t boolean_count;
    uint16_t main_count;
    uint16_t string_count;
    uint16_t max_node_count;
    uint32_t module_name_offset;
    uint32_t string_blob_offset;
    uint32_t tree_dir_offset;
} bin_header_t;  // 32 bytes

typedef struct {
    uint32_t name_offset;       // into string blob
    uint16_t node_count;
    uint16_t param_count;
    uint32_t nodes_offset;      // file offset
    uint32_t params_offset;     // file offset
} bin_tree_entry_t;  // 16 bytes

typedef struct {
    uint8_t  type;
    uint8_t  child_count;
    uint16_t node_index;
    uint16_t first_child;
    uint16_t next_sibling;
    uint16_t fn_index;
    uint16_t param_offset;
    uint8_t  param_count;
    uint8_t  flags;             // bit 0 = is_default
} bin_node_t;  // 14 bytes

typedef struct {
    uint8_t  type;
    uint8_t  reserved[3];
    uint32_t value;             // union: i32/u32/f32 bits/str_index
} bin_param_t;  // 8 bytes

#pragma pack(pop)

// ============================================================================
// LOADED MODULE HANDLE
// Wraps module_def_t with allocation tracking for cleanup
// ============================================================================

typedef struct {
    module_def_t    def;            // The actual definition (copy this to use)
    
    // Allocation tracking for cleanup
    void*           alloc_block;    // Single block if using block allocator
    char*           string_pool;    // All strings in one allocation
    tree_def_t*     trees_alloc;    // Allocated trees array
    node_t**        nodes_alloc;    // Array of per-tree node allocations
    param_t**       params_alloc;   // Array of per-tree param allocations
    char**          names_alloc;    // Allocated name arrays [oneshot, boolean, main, strings]
    
    // Original buffer reference (if caller wants to keep it)
    const uint8_t*  source_data;
    uint32_t        source_size;
    
    // Status
    int             error_code;
    uint32_t        error_offset;   // File offset where error occurred
} bin_module_t;

// ============================================================================
// PARSER API
// ============================================================================

// Parse binary buffer into module definition
// Allocates all necessary structures using provided allocator
// Returns NULL on error, check bin_get_last_error()
//
// The returned bin_module_t contains a module_def_t that can be passed
// directly to module_create()
//
// Usage:
//   bin_module_t* bmod = bin_parse_module(data, size, &alloc, handle, ct_node_id);
//   if (!bmod) { handle error }
//   module_runtime_t* mod = module_create(&bmod->def, &registry, &alloc, handle, ct_node_id);
//
bin_module_t* bin_parse_module(
    const uint8_t* data,
    uint32_t size,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

// Free all memory allocated by bin_parse_module
void bin_free_module(
    bin_module_t* bmod,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

// Get human-readable error message
const char* bin_error_string(int error_code);

// ============================================================================
// VALIDATION API
// ============================================================================

// Quick validation without full parse (checks header only)
// Returns BIN_OK if header looks valid
int bin_validate_header(const uint8_t* data, uint32_t size);

// Get module info without full parse
// Returns BIN_OK on success, fills in info struct
typedef struct {
    const char* name;           // Points into data buffer (not copied)
    uint16_t    tree_count;
    uint16_t    max_node_count;
    uint16_t    oneshot_count;
    uint16_t    boolean_count;
    uint16_t    main_count;
    uint16_t    string_count;
    uint32_t    total_size;
} bin_module_info_t;

int bin_get_module_info(
    const uint8_t* data,
    uint32_t size,
    bin_module_info_t* info
);

#endif // S_ENGINE_BINARY_H