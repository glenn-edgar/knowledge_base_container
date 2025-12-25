// ============================================================================
// s_engine_module.h
// Module Management API
// Version 2.5 - Two-tier architecture: shared module + tree instances
// ============================================================================
//
// ARCHITECTURE:
//   1. Initialize shared module once (resolves function tables)
//   2. Create tree instances as needed (each sized for its specific tree)
//   3. Multiple tree instances can run simultaneously
//
// USAGE:
//   #include "my_module.h"       // Generated header
//   #include "s_engine_module.h" // This file
//
// ============================================================================

#ifndef S_ENGINE_MODULE_H
#define S_ENGINE_MODULE_H

#include "s_engine_types.h"

// ============================================================================
// MODULE LIFECYCLE - THREE STEP PROCESS
// ============================================================================
//
// Step 1: Create module structure (allocates, doesn't resolve functions)
// Step 2: Load functions (call multiple times with different tables)
// Step 3: Validate all functions resolved
//
// ============================================================================

// Step 1: Create module structure
// - Allocates module and function pointer arrays
// - Returns NULL only on allocation failure
// - Does NOT resolve any functions yet
s_expr_module_t* s_expr_module_create(
    const s_expr_module_def_t* def,
    const s_expr_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

// Step 2: Load functions (call multiple times with different tables)
// - Returns count of functions successfully loaded from this table
// - Can be called multiple times (system fns, user fns, app fns, etc.)
// - Later loads can override earlier ones (last write wins)
uint16_t s_expr_module_load_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table);
uint16_t s_expr_module_load_boolean(s_expr_module_t* mod, const s_expr_fn_table_t* table);
uint16_t s_expr_module_load_main(s_expr_module_t* mod, const s_expr_fn_table_t* table);
void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t fn);

// Step 3: Validate all required functions are resolved
// - Returns S_EXPR_MOD_OK if all functions resolved
// - Returns error code if any function missing
// - Use s_expr_module_get_error_name() to find missing function
uint8_t s_expr_module_validate(s_expr_module_t* mod);

// Destroy module
// NOTE: All tree instances must be destroyed before calling this
void s_expr_module_deinit(s_expr_module_t* mod);

// ============================================================================
// MODULE ERROR HANDLING
// Check immediately after s_expr_module_init()
// ============================================================================

static inline uint8_t s_expr_module_get_error(const s_expr_module_t* mod) {
    return mod ? mod->error_code : S_EXPR_MOD_ERR_NULL_DEF;
}

static inline const char* s_expr_module_get_error_name(const s_expr_module_t* mod) {
    return mod ? mod->error_name : NULL;
}

static inline uint16_t s_expr_module_get_error_index(const s_expr_module_t* mod) {
    return mod ? mod->error_index : 0;
}

static inline const char* s_expr_module_error_str(uint8_t code) {
    switch (code) {
        case S_EXPR_MOD_OK:                    return "OK";
        case S_EXPR_MOD_ERR_ALLOC:             return "allocation failed";
        case S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND: return "oneshot function not found";
        case S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND: return "boolean function not found";
        case S_EXPR_MOD_ERR_MAIN_NOT_FOUND:    return "main function not found";
        case S_EXPR_MOD_ERR_INVALID_TREE:      return "invalid tree index";
        case S_EXPR_MOD_ERR_NULL_DEF:          return "null module definition";
        case S_EXPR_MOD_ERR_NULL_REGISTRY:     return "null registry";
        case S_EXPR_MOD_ERR_64BIT_MISMATCH:    return "64-bit mode mismatch";
        default:                               return "unknown error";
    }
}

// ============================================================================
// MODULE ACCESSORS
// ============================================================================

static inline bool s_expr_module_is_64bit(const s_expr_module_t* mod) {
    return mod && mod->def && mod->def->is_64bit;
}

static inline const char* s_expr_module_get_name(const s_expr_module_t* mod) {
    return (mod && mod->def) ? mod->def->name : NULL;
}

static inline uint16_t s_expr_module_tree_count(const s_expr_module_t* mod) {
    return (mod && mod->def) ? mod->def->tree_count : 0;
}

static inline const char* s_expr_module_tree_name(const s_expr_module_t* mod, uint16_t idx) {
    if (!mod || !mod->def || idx >= mod->def->tree_count) return NULL;
    return mod->def->trees[idx].name;
}

static inline const char* s_expr_module_get_string(const s_expr_module_t* mod, uint16_t str_index) {
    if (!mod || !mod->def || str_index >= mod->def->string_count) return NULL;
    return mod->def->strings[str_index];
}

// Find tree index by name (-1 if not found)
int16_t s_expr_module_find_tree(const s_expr_module_t* mod, const char* name);

// ============================================================================
// TREE INSTANCE LIFECYCLE (per-execution, can have many)
// ============================================================================

// Create tree instance by name
// - Allocates node_states sized to this specific tree's node_count
// - Returns NULL on error (invalid name or allocation failure)
s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    const char* tree_name,
    void* handle,
    uint16_t ct_node_id
);

// Create tree instance by index
s_expr_tree_instance_t* s_expr_tree_create_by_index(
    s_expr_module_t* mod,
    uint16_t tree_index,
    void* handle,
    uint16_t ct_node_id
);

// Destroy tree instance
void s_expr_tree_destroy(s_expr_tree_instance_t* inst);

// Reset tree instance (reinitialize all node states)
void s_expr_tree_reset(s_expr_tree_instance_t* inst);

// ============================================================================
// TREE INSTANCE ACCESSORS
// ============================================================================

static inline const char* s_expr_tree_get_name(const s_expr_tree_instance_t* inst) {
    return (inst && inst->tree_def) ? inst->tree_def->name : NULL;
}

static inline uint16_t s_expr_tree_node_count(const s_expr_tree_instance_t* inst) {
    return inst ? inst->node_count : 0;
}

static inline s_expr_node_state_t* s_expr_tree_get_state(
    s_expr_tree_instance_t* inst, 
    uint16_t node_index
) {
    if (!inst || node_index >= inst->node_count) return NULL;
    return &inst->node_states[node_index];
}

static inline const s_expr_param_t* s_expr_tree_get_params(
    const s_expr_tree_instance_t* inst,
    const s_expr_node_t* node
) {
    if (!inst || !node || !inst->tree_def) return NULL;
    return &inst->tree_def->params[node->param_offset];
}

static inline s_expr_module_t* s_expr_tree_get_module(const s_expr_tree_instance_t* inst) {
    return inst ? inst->module : NULL;
}

static inline void* s_expr_tree_get_handle(const s_expr_tree_instance_t* inst) {
    return inst ? inst->handle : NULL;
}

static inline uint16_t s_expr_tree_get_ct_node_id(const s_expr_tree_instance_t* inst) {
    return inst ? inst->ct_node_id : 0;
}

// Get string from module (convenience wrapper)
static inline const char* s_expr_tree_get_string(
    const s_expr_tree_instance_t* inst,
    uint16_t str_index
) {
    return (inst && inst->module) ? s_expr_module_get_string(inst->module, str_index) : NULL;
}

// ============================================================================
// FUNCTION INVOCATION HELPERS
// ============================================================================

void s_expr_call_oneshot(
    s_expr_tree_instance_t* inst,
    uint16_t fn_index,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
);

bool s_expr_call_boolean(
    s_expr_tree_instance_t* inst,
    uint16_t fn_index,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
);

s_expr_result_t s_expr_call_main(
    s_expr_tree_instance_t* inst,
    uint16_t fn_index,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
);

void s_expr_call_debug(s_expr_tree_instance_t* inst, const char* message);

#endif // S_ENGINE_MODULE_H