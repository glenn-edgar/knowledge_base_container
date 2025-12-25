// ============================================================================
// s_engine_module.c
// Module Management Implementation
// Version 2.5 - Two-tier architecture: shared module + tree instances
// ============================================================================

#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

static void* find_fn(const s_expr_fn_table_t* table, const char* name) {
    if (!table || !table->entries) return NULL;
    
    for (uint16_t i = 0; i < table->count; i++) {
        if (strcmp(table->entries[i].name, name) == 0) {
            return table->entries[i].fn_ptr;
        }
    }
    return NULL;
}

static void init_node_states(s_expr_tree_instance_t* inst) {
    for (uint16_t i = 0; i < inst->node_count; i++) {
        inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE;
        inst->node_states[i].state = 0;
        inst->node_states[i].user_data = 0;
    }
}

// ============================================================================
// MODULE CREATE (Step 1)
// ============================================================================

s_expr_module_t* s_expr_module_create(
    const s_expr_module_def_t* def,
    const s_expr_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    if (!def || !alloc || !alloc->malloc || !alloc->free) {
        return NULL;
    }
    
    // Allocate module structure
    s_expr_module_t* mod = (s_expr_module_t*)alloc->malloc(
        handle, ct_node_id, sizeof(s_expr_module_t)
    );
    if (!mod) {
        return NULL;
    }
    
    // Zero initialize
    memset(mod, 0, sizeof(s_expr_module_t));
    
    mod->def = def;
    mod->alloc = *alloc;
    mod->handle = handle;
    mod->error_code = S_EXPR_MOD_OK;
    
    // Verify 64-bit mode matches compilation
    #if MODULE_IS_64BIT
    if (!def->is_64bit) {
        mod->error_code = S_EXPR_MOD_ERR_64BIT_MISMATCH;
        return mod;
    }
    #else
    if (def->is_64bit) {
        mod->error_code = S_EXPR_MOD_ERR_64BIT_MISMATCH;
        return mod;
    }
    #endif
    
    // Allocate function pointer arrays (all NULL initially)
    if (def->oneshot_count > 0) {
        mod->oneshot_fns = (s_expr_oneshot_fn_t*)alloc->malloc(
            handle, ct_node_id, def->oneshot_count * sizeof(s_expr_oneshot_fn_t)
        );
        if (!mod->oneshot_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return mod;
        }
        memset(mod->oneshot_fns, 0, def->oneshot_count * sizeof(s_expr_oneshot_fn_t));
    }
    
    if (def->boolean_count > 0) {
        mod->boolean_fns = (s_expr_boolean_fn_t*)alloc->malloc(
            handle, ct_node_id, def->boolean_count * sizeof(s_expr_boolean_fn_t)
        );
        if (!mod->boolean_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return mod;
        }
        memset(mod->boolean_fns, 0, def->boolean_count * sizeof(s_expr_boolean_fn_t));
    }
    
    if (def->main_count > 0) {
        mod->main_fns = (s_expr_main_fn_t*)alloc->malloc(
            handle, ct_node_id, def->main_count * sizeof(s_expr_main_fn_t)
        );
        if (!mod->main_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return mod;
        }
        memset(mod->main_fns, 0, def->main_count * sizeof(s_expr_main_fn_t));
    }
    
    return mod;
}

// ============================================================================
// MODULE LOAD FUNCTIONS (Step 2)
// ============================================================================

uint16_t s_expr_module_load_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    if (!mod || !mod->def || !table || !table->entries) return 0;
    if (mod->error_code != S_EXPR_MOD_OK) return 0;
    
    uint16_t loaded = 0;
    
    for (uint16_t i = 0; i < mod->def->oneshot_count; i++) {
        const char* name = mod->def->oneshot_names[i];
        s_expr_oneshot_fn_t fn = (s_expr_oneshot_fn_t)find_fn(table, name);
        if (fn) {
            mod->oneshot_fns[i] = fn;
            loaded++;
        }
    }
    
    return loaded;
}

uint16_t s_expr_module_load_boolean(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    if (!mod || !mod->def || !table || !table->entries) return 0;
    if (mod->error_code != S_EXPR_MOD_OK) return 0;
    
    uint16_t loaded = 0;
    
    for (uint16_t i = 0; i < mod->def->boolean_count; i++) {
        const char* name = mod->def->boolean_names[i];
        s_expr_boolean_fn_t fn = (s_expr_boolean_fn_t)find_fn(table, name);
        if (fn) {
            mod->boolean_fns[i] = fn;
            loaded++;
        }
    }
    
    return loaded;
}

uint16_t s_expr_module_load_main(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    if (!mod || !mod->def || !table || !table->entries) return 0;
    if (mod->error_code != S_EXPR_MOD_OK) return 0;
    
    uint16_t loaded = 0;
    
    for (uint16_t i = 0; i < mod->def->main_count; i++) {
        const char* name = mod->def->main_names[i];
        s_expr_main_fn_t fn = (s_expr_main_fn_t)find_fn(table, name);
        if (fn) {
            mod->main_fns[i] = fn;
            loaded++;
        }
    }
    
    return loaded;
}

void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t fn) {
    if (mod) {
        mod->debug_fn = fn;
    }
}

// ============================================================================
// MODULE VALIDATE (Step 3)
// ============================================================================

uint8_t s_expr_module_validate(s_expr_module_t* mod) {
    if (!mod || !mod->def) return S_EXPR_MOD_ERR_NULL_DEF;
    
    // Already has an error from create
    if (mod->error_code != S_EXPR_MOD_OK) {
        return mod->error_code;
    }
    
    // Check all oneshot functions resolved
    for (uint16_t i = 0; i < mod->def->oneshot_count; i++) {
        if (!mod->oneshot_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = mod->def->oneshot_names[i];
            return mod->error_code;
        }
    }
    
    // Check all boolean functions resolved
    for (uint16_t i = 0; i < mod->def->boolean_count; i++) {
        if (!mod->boolean_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = mod->def->boolean_names[i];
            return mod->error_code;
        }
    }
    
    // Check all main functions resolved
    for (uint16_t i = 0; i < mod->def->main_count; i++) {
        if (!mod->main_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_MAIN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = mod->def->main_names[i];
            return mod->error_code;
        }
    }
    
    return S_EXPR_MOD_OK;
}

// ============================================================================
// MODULE DEINIT
// ============================================================================

void s_expr_module_deinit(s_expr_module_t* mod) {
    if (!mod) return;
    
    s_expr_free_fn_t free_fn = mod->alloc.free;
    void* handle = mod->handle;
    
    if (mod->oneshot_fns) {
        free_fn(handle, 0, mod->oneshot_fns);
    }
    if (mod->boolean_fns) {
        free_fn(handle, 0, mod->boolean_fns);
    }
    if (mod->main_fns) {
        free_fn(handle, 0, mod->main_fns);
    }
    
    free_fn(handle, 0, mod);
}

// ============================================================================
// TREE LOOKUP
// ============================================================================

int16_t s_expr_module_find_tree(const s_expr_module_t* mod, const char* name) {
    if (!mod || !mod->def || !name) return -1;
    
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
        if (strcmp(mod->def->trees[i].name, name) == 0) {
            return (int16_t)i;
        }
    }
    
    return -1;
}

// ============================================================================
// TREE INSTANCE CREATE
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create_by_index(
    s_expr_module_t* mod,
    uint16_t tree_index,
    void* handle,
    uint16_t ct_node_id
) {
    if (!mod || !mod->def) return NULL;
    if (tree_index >= mod->def->tree_count) return NULL;
    if (mod->error_code != S_EXPR_MOD_OK) return NULL;  // Don't create if module has errors
    
    const s_expr_tree_def_t* tree_def = &mod->def->trees[tree_index];
    
    // Allocate instance structure using the node's handle
    s_expr_tree_instance_t* inst = (s_expr_tree_instance_t*)mod->alloc.malloc(
        handle, ct_node_id, sizeof(s_expr_tree_instance_t)
    );
    if (!inst) return NULL;
    
    // Zero initialize
    memset(inst, 0, sizeof(s_expr_tree_instance_t));
    
    inst->module = mod;
    inst->tree_index = tree_index;
    inst->tree_def = tree_def;
    inst->node_count = tree_def->node_count;
    inst->handle = handle;
    inst->ct_node_id = ct_node_id;
    
    // Allocate node states sized to THIS tree (not max)
    inst->node_states = (s_expr_node_state_t*)mod->alloc.malloc(
        handle, ct_node_id, 
        tree_def->node_count * sizeof(s_expr_node_state_t)
    );
    if (!inst->node_states) {
        mod->alloc.free(handle, ct_node_id, inst);
        return NULL;
    }
    
    // Initialize node states
    init_node_states(inst);
    
    return inst;
}

s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    const char* tree_name,
    void* handle,
    uint16_t ct_node_id
) {
    int16_t idx = s_expr_module_find_tree(mod, tree_name);
    if (idx < 0) return NULL;
    
    return s_expr_tree_create_by_index(mod, (uint16_t)idx, handle, ct_node_id);
}

// ============================================================================
// TREE INSTANCE DESTROY
// ============================================================================

void s_expr_tree_destroy(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    s_expr_module_t* mod = inst->module;
    void* handle = inst->handle;
    uint16_t ct_node_id = inst->ct_node_id;
    
    if (inst->node_states) {
        mod->alloc.free(handle, ct_node_id, inst->node_states);
    }
    
    mod->alloc.free(handle, ct_node_id, inst);
}

// ============================================================================
// TREE INSTANCE RESET
// ============================================================================

void s_expr_tree_reset(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    init_node_states(inst);
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
) {
    if (!inst || !inst->module) return;
    if (fn_index >= inst->module->def->oneshot_count) return;
    if (!inst->module->oneshot_fns[fn_index]) return;
    
    inst->module->oneshot_fns[fn_index](
        inst, node, state,
        inst->current_event, inst->event_data,
        params, param_count
    );
}

bool s_expr_call_boolean(
    s_expr_tree_instance_t* inst,
    uint16_t fn_index,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
) {
    if (!inst || !inst->module) return false;
    if (fn_index >= inst->module->def->boolean_count) return false;
    if (!inst->module->boolean_fns[fn_index]) return false;
    
    return inst->module->boolean_fns[fn_index](
        inst, node, state,
        inst->current_event, inst->event_data,
        params, param_count
    );
}

s_expr_result_t s_expr_call_main(
    s_expr_tree_instance_t* inst,
    uint16_t fn_index,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
) {
    if (!inst || !inst->module) return SE_TERMINATE;
    if (fn_index >= inst->module->def->main_count) return SE_TERMINATE;
    if (!inst->module->main_fns[fn_index]) return SE_TERMINATE;
    
    return inst->module->main_fns[fn_index](
        inst, node, state,
        inst->current_event, inst->event_data,
        params, param_count
    );
}

void s_expr_call_debug(s_expr_tree_instance_t* inst, const char* message) {
    if (!inst || !inst->module || !inst->module->debug_fn) return;
    inst->module->debug_fn(inst, message);
}