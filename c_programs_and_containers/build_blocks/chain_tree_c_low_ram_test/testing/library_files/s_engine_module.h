// ============================================================================
// s_engine_module.h
// S-Expression Module Management API
// Version 2.7 - Pool table support, lifecycle management, incremental loading
// ============================================================================

#ifndef S_ENGINE_MODULE_H
#define S_ENGINE_MODULE_H

#include "s_engine_types.h"
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// MODULE INITIALIZATION (two-phase: init + validate)
// ============================================================================

// Phase 1: Initialize module structure (no function resolution yet)
uint8_t s_expr_module_init(
    s_expr_module_t* mod,
    const s_expr_module_def_t* def,
    s_expr_allocator_t alloc,
    void* handle
);

// Phase 2a: Load function entries (can be called multiple times)
// Returns number of entries loaded
uint16_t s_expr_module_load_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table);
uint16_t s_expr_module_load_boolean(s_expr_module_t* mod, const s_expr_fn_table_t* table);
uint16_t s_expr_module_load_main(s_expr_module_t* mod, const s_expr_fn_table_t* table);

// Phase 2b: Resolve all function references (call after all load calls)
// Returns 0 on success, error code on failure
uint8_t s_expr_module_validate(s_expr_module_t* mod);

// Free module resources
void s_expr_module_free(s_expr_module_t* mod);

// Set debug function (optional)
void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t debug_fn);

// ============================================================================
// ERROR ACCESSORS
// ============================================================================

static inline uint8_t s_expr_module_get_error(const s_expr_module_t* mod) {
    return mod ? mod->error_code : S_EXPR_MOD_ERR_ALLOC;
}

static inline uint16_t s_expr_module_get_error_index(const s_expr_module_t* mod) {
    return mod ? mod->error_index : 0;
}

static inline const char* s_expr_module_get_error_name(const s_expr_module_t* mod) {
    return mod ? mod->error_name : NULL;
}

const char* s_expr_module_error_str(uint8_t error_code);

// ============================================================================
// POOL TABLE MANAGEMENT
// ============================================================================

static inline void s_expr_module_set_pool_table(
    s_expr_module_t* mod,
    void** pool_table,
    uint16_t pool_count
) {
    if (mod) {
        mod->pool_table = pool_table;
        mod->pool_count = pool_count;
    }
}

static inline void** s_expr_module_get_pool_table(const s_expr_module_t* mod) {
    return mod ? mod->pool_table : NULL;
}

static inline uint16_t s_expr_module_get_pool_count(const s_expr_module_t* mod) {
    return mod ? mod->pool_count : 0;
}

// ============================================================================
// TREE INSTANCE CREATION
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    uint16_t tree_index,
    void* handle,
    uint16_t ct_node_id
);

void s_expr_tree_free(s_expr_tree_instance_t* inst);

// ============================================================================
// MODULE ACCESSORS
// ============================================================================

static inline const char* s_expr_module_get_name(const s_expr_module_t* mod) {
    return (mod && mod->def) ? mod->def->name : NULL;
}

static inline uint16_t s_expr_module_tree_count(const s_expr_module_t* mod) {
    return (mod && mod->def) ? mod->def->tree_count : 0;
}

static inline const char* s_expr_module_tree_name(const s_expr_module_t* mod, uint16_t index) {
    if (!mod || !mod->def || index >= mod->def->tree_count) return NULL;
    return mod->def->trees[index].name;
}

// ============================================================================
// TREE INSTANCE ACCESSORS
// ============================================================================

static inline const char* s_expr_tree_get_name(const s_expr_tree_instance_t* inst) {
    return (inst && inst->tree) ? inst->tree->name : NULL;
}

static inline uint16_t s_expr_tree_get_node_count(const s_expr_tree_instance_t* inst) {
    return inst ? inst->node_count : 0;
}

static inline s_expr_node_state_t* s_expr_tree_get_node_state(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
) {
    if (!inst || node_index >= inst->node_count) return NULL;
    return &inst->node_states[node_index];
}

static inline const s_expr_node_t* s_expr_tree_get_node(
    const s_expr_tree_instance_t* inst,
    uint16_t node_index
) {
    if (!inst || !inst->tree || node_index >= inst->tree->node_count) return NULL;
    return &inst->tree->nodes[node_index];
}

// ============================================================================
// POOL ACCESS FROM TREE INSTANCE
// ============================================================================

static inline void* s_expr_tree_get_pool_slot(
    const s_expr_tree_instance_t* inst,
    const s_expr_param_t* slot_param,
    size_t element_size
) {
    if (!inst || !inst->module || !inst->module->pool_table) return NULL;
    if (slot_param->type != S_EXPR_PARAM_SLOT) return NULL;
    
    uint16_t pool_id = slot_param->slot.pool_id;
    if (pool_id >= inst->module->pool_count) return NULL;
    
    uint8_t* pool = (uint8_t*)inst->module->pool_table[pool_id];
    if (!pool) return NULL;
    
    uint16_t slot_idx = slot_param->slot.slot_index;
    return pool + (slot_idx * element_size);
}

#define S_EXPR_TREE_GET_SLOT(inst, slot_param, type) \
    ((type*)s_expr_tree_get_pool_slot((inst), (slot_param), sizeof(type)))

// ============================================================================
// STRING ACCESS
// ============================================================================

static inline const char* s_expr_module_get_string(
    const s_expr_module_t* mod,
    uint16_t str_index
) {
    if (!mod || !mod->def || str_index >= mod->def->string_count) return NULL;
    return mod->def->strings[str_index];
}

static inline const char* s_expr_inst_get_string(
    const s_expr_tree_instance_t* inst,
    uint16_t str_index
) {
    if (!inst || !inst->module) return NULL;
    return s_expr_module_get_string(inst->module, str_index);
}

// ============================================================================
// FUNCTION NAME LOOKUP (for debugging)
// ============================================================================

static inline const char* s_expr_module_get_oneshot_name(
    const s_expr_module_t* mod,
    uint16_t fn_index
) {
    if (!mod || !mod->def || fn_index >= mod->def->oneshot_count) return NULL;
    return mod->def->oneshot_names[fn_index];
}

static inline const char* s_expr_module_get_boolean_name(
    const s_expr_module_t* mod,
    uint16_t fn_index
) {
    if (!mod || !mod->def || fn_index >= mod->def->boolean_count) return NULL;
    return mod->def->boolean_names[fn_index];
}

static inline const char* s_expr_module_get_main_name(
    const s_expr_module_t* mod,
    uint16_t fn_index
) {
    if (!mod || !mod->def || fn_index >= mod->def->main_count) return NULL;
    return mod->def->main_names[fn_index];
}

// ============================================================================
// NODE FLAG HELPERS
// ============================================================================

static inline bool s_expr_node_is_active(const s_expr_node_state_t* state) {
    return state && (state->flags & S_EXPR_NODE_FLAG_ACTIVE);
}

static inline bool s_expr_node_is_initialized(const s_expr_node_state_t* state) {
    return state && (state->flags & S_EXPR_NODE_FLAG_INITIALIZED);
}

static inline bool s_expr_node_is_running(const s_expr_node_state_t* state) {
    return state && S_EXPR_NODE_IS_RUNNING(state->flags);
}

static inline void s_expr_node_set_user_flags(
    s_expr_node_state_t* state,
    uint8_t user_flags
) {
    if (state) {
        state->flags = (state->flags & S_EXPR_NODE_FLAGS_SYSTEM) | 
                       (user_flags & S_EXPR_NODE_FLAGS_USER);
    }
}

static inline uint8_t s_expr_node_get_user_flags(const s_expr_node_state_t* state) {
    return state ? (state->flags & S_EXPR_NODE_FLAGS_USER) : 0;
}

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_MODULE_H