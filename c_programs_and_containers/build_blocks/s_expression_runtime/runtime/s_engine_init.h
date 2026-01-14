// ============================================================================
// s_engine_init.h
// High-level S-Expression Engine Initialization API
// ============================================================================

#ifndef S_ENGINE_INIT_H
#define S_ENGINE_INIT_H

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_loader.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// ENGINE HANDLE
// ============================================================================

#ifndef S_ENGINE_MAX_TREES
#define S_ENGINE_MAX_TREES 16
#endif

typedef struct {
    s_expr_module_t           module;
    s_expr_loaded_module_t*   loaded;
    s_expr_tree_instance_t*   trees[S_ENGINE_MAX_TREES];
    uint16_t                  tree_count;
    s_expr_allocator_t        alloc;
    void*                     user_ctx;      // Opaque - user defined
    uint8_t                   error_code;
} s_engine_handle_t;

// ============================================================================
// INITIALIZATION FROM ROM
// ============================================================================

// Initialize engine from binary data in ROM/flash
// Binary data must remain valid for lifetime of engine
uint8_t s_engine_init_from_rom(
    s_engine_handle_t* handle,
    const uint8_t* binary_data,
    size_t binary_size,
    s_expr_allocator_t alloc,
    void* user_ctx
);

// ============================================================================
// INITIALIZATION FROM FILE
// ============================================================================

// Initialize engine from binary file
// File is loaded into RAM and owned by engine
uint8_t s_engine_init_from_file(
    s_engine_handle_t* handle,
    const char* filepath,
    s_expr_allocator_t alloc,
    void* user_ctx
);

// ============================================================================
// FUNCTION REGISTRATION
// ============================================================================

// Register function tables (call before validate)
void s_engine_register_oneshot(s_engine_handle_t* handle, const s_expr_fn_table_t* table);
void s_engine_register_main(s_engine_handle_t* handle, const s_expr_fn_table_t* table);
void s_engine_register_pred(s_engine_handle_t* handle, const s_expr_fn_table_t* table);

// Register built-in engine functions (SE_PIPELINE, SE_PRED_AND, etc.)
void s_engine_register_builtins(s_engine_handle_t* handle);

// Validate all functions resolved
uint8_t s_engine_validate(s_engine_handle_t* handle);

// ============================================================================
// TREE MANAGEMENT
// ============================================================================

// Create tree by index
s_expr_tree_instance_t* s_engine_create_tree(
    s_engine_handle_t* handle,
    uint16_t tree_index,
    uint32_t node_id
);

// Create tree by name hash
s_expr_tree_instance_t* s_engine_create_tree_by_hash(
    s_engine_handle_t* handle,
    s_expr_hash_t name_hash,
    uint32_t node_id
);

// Find tree by name hash (from already created trees)
s_expr_tree_instance_t* s_engine_find_tree(
    s_engine_handle_t* handle,
    s_expr_hash_t name_hash
);

// ============================================================================
// CLEANUP
// ============================================================================

void s_engine_free(s_engine_handle_t* handle);

// ============================================================================
// ACCESSORS
// ============================================================================

static inline void* s_engine_get_user_ctx(s_engine_handle_t* handle) {
    return handle ? handle->user_ctx : NULL;
}

static inline s_expr_module_t* s_engine_get_module(s_engine_handle_t* handle) {
    return handle ? &handle->module : NULL;
}

static inline uint16_t s_engine_get_tree_count(s_engine_handle_t* handle) {
    return handle ? handle->tree_count : 0;
}

static inline s_expr_tree_instance_t* s_engine_get_tree(s_engine_handle_t* handle, uint16_t idx) {
    return (handle && idx < handle->tree_count) ? handle->trees[idx] : NULL;
}

static inline const char* s_engine_error_str(s_engine_handle_t* handle) {
    if (!handle) return "NULL handle";
    if (handle->error_code != 0) {
        return s_expr_loader_error_str(handle->error_code);
    }
    return s_expr_error_str(handle->module.error_code);
}

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_INIT_H