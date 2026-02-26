// ============================================================================
// s_expr_binary_v5_debug.h
// S-Expression Binary Module Loader - Debug/Inspection API
// Version 5.0
// ============================================================================
//
// Optional header for debugging and inspecting loaded binary modules.
// Include this in addition to s_expr_binary_v5.h when you need to:
//   - Inspect tree contents
//   - Look up strings, fields, constants
//   - Examine function hashes
//   - Validate binaries before loading
//
// ============================================================================

#ifndef S_EXPR_BINARY_V5_DEBUG_H
#define S_EXPR_BINARY_V5_DEBUG_H

#include "s_expr_binary_v5.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// TREE DEFINITION (for inspection)
// ============================================================================

typedef struct {
    s_expr_hash_t name_hash;
    s_expr_hash_t record_hash;
    const s_expr_param_t* params;  // Direct pointer into ROM
    uint16_t param_count;
    uint16_t node_count;
    uint16_t pointer_count;
} sexb_tree_def_t;

// ============================================================================
// FIELD INFO (for inspection)
// ============================================================================

typedef struct {
    s_expr_hash_t name_hash;
    uint16_t offset;
    uint16_t size;
    uint8_t type_tag;
    uint8_t flags;
} sexb_field_info_t;

// ============================================================================
// VALIDATION
// ============================================================================

// Validate binary without loading (checks magic, version, size, 64-bit mode)
bool sexb_validate(const uint8_t* data, uint32_t size);

// ============================================================================
// TREE ACCESS
// ============================================================================

// Get tree definition by index
bool sexb_get_tree(const sexb_module_t* mod, uint16_t index, sexb_tree_def_t* out);

// Find tree by name hash
bool sexb_find_tree(const sexb_module_t* mod, s_expr_hash_t name_hash, sexb_tree_def_t* out);

// ============================================================================
// FUNCTION HASH ACCESS
// ============================================================================

s_expr_hash_t sexb_get_oneshot_hash(const sexb_module_t* mod, uint16_t index);
s_expr_hash_t sexb_get_main_hash(const sexb_module_t* mod, uint16_t index);
s_expr_hash_t sexb_get_pred_hash(const sexb_module_t* mod, uint16_t index);

// ============================================================================
// STRING ACCESS
// ============================================================================

const char* sexb_get_string(const sexb_module_t* mod, uint16_t index);
uint16_t sexb_get_string_len(const sexb_module_t* mod, uint16_t index);

// ============================================================================
// FIELD ACCESS
// ============================================================================

// Find field by record hash and field hash
bool sexb_find_field(
    const sexb_module_t* mod,
    s_expr_hash_t record_hash,
    s_expr_hash_t field_hash,
    sexb_field_info_t* out
);

// Get record size by hash
uint16_t sexb_get_record_size(const sexb_module_t* mod, s_expr_hash_t record_hash);

// ============================================================================
// CONSTANT ACCESS
// ============================================================================

const void* sexb_find_constant(
    const sexb_module_t* mod,
    s_expr_hash_t hash,
    uint16_t* out_size
);

#ifdef __cplusplus
}
#endif

#endif // S_EXPR_BINARY_V5_DEBUG_H