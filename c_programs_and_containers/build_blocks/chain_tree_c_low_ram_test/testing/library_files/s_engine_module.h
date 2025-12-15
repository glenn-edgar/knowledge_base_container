// ============================================================================
// s_engine_module.h
// Module Management API
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
// Returns NULL on error, check error_code
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

#endif // S_ENGINE_MODULE_H