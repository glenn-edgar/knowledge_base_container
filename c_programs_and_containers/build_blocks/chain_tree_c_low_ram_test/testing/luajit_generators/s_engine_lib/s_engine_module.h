// ============================================================================
// s_engine_module.h
// Module Management API
// Version 2.2
// ============================================================================
//
// USAGE:
//   Include the generated module header BEFORE this file:
//
//     #include "motor_module.h"    // Generated - defines MODULE_IS_64BIT, types
//     #include "s_engine_module.h" // This file
//
//   Or, if not using a generated module, this will default to 32-bit mode.
//
// ============================================================================

#ifndef S_ENGINE_MODULE_H
#define S_ENGINE_MODULE_H

#include "s_engine_types.h"

// ============================================================================
// MODULE LIFECYCLE
// ============================================================================

// Create module runtime
// Allocates: node_states[], function pointer arrays
// Resolves function names against registry
// Returns NULL on error, check error_code via module_get_error()
module_runtime_t* module_create(
    const module_def_t* def,
    const module_registry_t* registry,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
);

// Destroy module, free all RAM
void module_destroy(module_runtime_t* mod);

// ============================================================================
// TREE SELECTION
// ============================================================================

// Select tree by index (0 to tree_count-1)
// Resets node_states for new tree
bool module_select_tree(module_runtime_t* mod, uint16_t tree_index);

// Select tree by name
bool module_select_tree_by_name(module_runtime_t* mod, const char* name);

// Reset current tree's node states to initial
void module_reset(module_runtime_t* mod);

// ============================================================================
// ACCESSORS
// ============================================================================

// Get node state by local index (0 to active_tree.node_count-1)
node_state_t* module_get_state(module_runtime_t* mod, uint16_t node_index);

// Get string by index
const char* module_get_string(module_runtime_t* mod, uint16_t str_index);

// Get params for a node
const param_t* module_get_params(module_runtime_t* mod, const node_t* node);

// Get active tree name
const char* module_active_tree_name(module_runtime_t* mod);

// Get active tree node count
uint16_t module_active_node_count(module_runtime_t* mod);

// ============================================================================
// ERROR HANDLING
// ============================================================================

// Get last error code
static inline uint8_t module_get_error(const module_runtime_t* mod) {
    return mod ? mod->error_code : MOD_ERR_NULL_DEF;
}

// Get error details (for MOD_ERR_*_NOT_FOUND errors)
static inline const char* module_get_error_name(const module_runtime_t* mod) {
    return mod ? mod->error_name : NULL;
}

static inline uint16_t module_get_error_index(const module_runtime_t* mod) {
    return mod ? mod->error_index : 0;
}

// Error code to string
static inline const char* module_error_str(uint8_t code) {
    switch (code) {
        case MOD_OK:                    return "OK";
        case MOD_ERR_ALLOC:             return "allocation failed";
        case MOD_ERR_ONESHOT_NOT_FOUND: return "oneshot function not found";
        case MOD_ERR_BOOLEAN_NOT_FOUND: return "boolean function not found";
        case MOD_ERR_MAIN_NOT_FOUND:    return "main function not found";
        case MOD_ERR_INVALID_TREE:      return "invalid tree index";
        case MOD_ERR_NULL_DEF:          return "null module definition";
        case MOD_ERR_NULL_REGISTRY:     return "null registry";
        default:                        return "unknown error";
    }
}

// ============================================================================
// MODULE INFO
// ============================================================================

// Check if module is 64-bit
static inline bool module_is_64bit(const module_runtime_t* mod) {
    return mod && mod->def && mod->def->is_64bit;
}

// Get module name
static inline const char* module_get_name(const module_runtime_t* mod) {
    return (mod && mod->def) ? mod->def->name : NULL;
}

// Get tree count
static inline uint16_t module_tree_count(const module_runtime_t* mod) {
    return (mod && mod->def) ? mod->def->tree_count : 0;
}

// Get tree name by index
static inline const char* module_tree_name(const module_runtime_t* mod, uint16_t idx) {
    if (!mod || !mod->def || idx >= mod->def->tree_count) return NULL;
    return mod->def->trees[idx].name;
}

// ============================================================================
// FUNCTION INVOCATION HELPERS
// ============================================================================

// Call oneshot function by index (with v2.2 param_count)
void module_call_oneshot(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

// Call boolean function by index
bool module_call_boolean(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

// Call main function by index
cfl_code_t module_call_main(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

// Call debug function
void module_call_debug(module_runtime_t* mod, const char* message);

#endif // S_ENGINE_MODULE_H