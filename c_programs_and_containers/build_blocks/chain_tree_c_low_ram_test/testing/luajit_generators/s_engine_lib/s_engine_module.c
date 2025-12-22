// ============================================================================
// s_engine_module.c
// Module Management Implementation
// Version 2.2
// ============================================================================

#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

static void* find_fn(const fn_table_t* table, const char* name) {
    if (!table || !table->entries) return NULL;
    
    for (uint16_t i = 0; i < table->count; i++) {
        if (strcmp(table->entries[i].name, name) == 0) {
            return table->entries[i].fn_ptr;
        }
    }
    return NULL;
}

static void init_node_states(module_runtime_t* mod) {
    uint16_t count = mod->active_tree_def->node_count;
    
    for (uint16_t i = 0; i < count; i++) {
        mod->node_states[i].flags = NODE_FLAG_ACTIVE;
        mod->node_states[i].state = 0;
        mod->node_states[i].user_data = 0;
    }
}

// ============================================================================
// MODULE CREATE
// ============================================================================

module_runtime_t* module_create(
    const module_def_t* def,
    const module_registry_t* registry,
    const s_allocator_t* alloc,
    void* handle,
    uint16_t ct_node_id
) {
    if (!def) {
        return NULL;
    }
    if (!registry) {
        return NULL;
    }
    if (!alloc || !alloc->malloc || !alloc->free) {
        return NULL;
    }
    
    // Verify 64-bit mode matches compilation
    // This catches mismatched header/library combinations
    #if MODULE_IS_64BIT
    if (!def->is_64bit) {
        // Engine compiled for 64-bit, module is 32-bit
        return NULL;
    }
    #else
    if (def->is_64bit) {
        // Engine compiled for 32-bit, module is 64-bit
        return NULL;
    }
    #endif
    
    // Allocate runtime structure
    module_runtime_t* mod = (module_runtime_t*)alloc->malloc(
        handle, ct_node_id, sizeof(module_runtime_t)
    );
    if (!mod) {
        return NULL;
    }
    
    // Zero initialize
    memset(mod, 0, sizeof(module_runtime_t));
    
    mod->def = def;
    mod->handle = handle;
    mod->ct_node_id = ct_node_id;
    mod->alloc = *alloc;
    mod->error_code = MOD_OK;
    
    // Allocate node states for largest tree
    mod->node_states = (node_state_t*)alloc->malloc(
        handle, ct_node_id, def->max_node_count * sizeof(node_state_t)
    );
    if (!mod->node_states) {
        mod->error_code = MOD_ERR_ALLOC;
        goto fail;
    }
    
    // Allocate function pointer arrays
    if (def->oneshot_count > 0) {
        mod->oneshot_fns = (oneshot_fn_t*)alloc->malloc(
            handle, ct_node_id, def->oneshot_count * sizeof(oneshot_fn_t)
        );
        if (!mod->oneshot_fns) {
            mod->error_code = MOD_ERR_ALLOC;
            goto fail;
        }
    }
    
    if (def->boolean_count > 0) {
        mod->boolean_fns = (boolean_fn_t*)alloc->malloc(
            handle, ct_node_id, def->boolean_count * sizeof(boolean_fn_t)
        );
        if (!mod->boolean_fns) {
            mod->error_code = MOD_ERR_ALLOC;
            goto fail;
        }
    }
    
    if (def->main_count > 0) {
        mod->main_fns = (main_fn_t*)alloc->malloc(
            handle, ct_node_id, def->main_count * sizeof(main_fn_t)
        );
        if (!mod->main_fns) {
            mod->error_code = MOD_ERR_ALLOC;
            goto fail;
        }
    }
    
    // Resolve oneshot functions
    for (uint16_t i = 0; i < def->oneshot_count; i++) {
        const char* name = def->oneshot_names[i];
        oneshot_fn_t fn = (oneshot_fn_t)find_fn(&registry->oneshot, name);
        if (!fn) {
            mod->error_code = MOD_ERR_ONESHOT_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = name;
            goto fail;
        }
        mod->oneshot_fns[i] = fn;
    }
    
    // Resolve boolean functions
    for (uint16_t i = 0; i < def->boolean_count; i++) {
        const char* name = def->boolean_names[i];
        boolean_fn_t fn = (boolean_fn_t)find_fn(&registry->boolean, name);
        if (!fn) {
            mod->error_code = MOD_ERR_BOOLEAN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = name;
            goto fail;
        }
        mod->boolean_fns[i] = fn;
    }
    
    // Resolve main functions
    for (uint16_t i = 0; i < def->main_count; i++) {
        const char* name = def->main_names[i];
        main_fn_t fn = (main_fn_t)find_fn(&registry->main, name);
        if (!fn) {
            mod->error_code = MOD_ERR_MAIN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = name;
            goto fail;
        }
        mod->main_fns[i] = fn;
    }
    
    // Debug is optional
    mod->debug_fn = registry->debug;
    
    // Select first tree by default
    mod->active_tree = 0;
    mod->active_tree_def = &def->trees[0];
    init_node_states(mod);
    
    return mod;

fail:
    module_destroy(mod);
    return NULL;
}

// ============================================================================
// MODULE DESTROY
// ============================================================================

void module_destroy(module_runtime_t* mod) {
    if (!mod) return;
    
    s_free_fn_t free_fn = mod->alloc.free;
    void* handle = mod->handle;
    uint16_t ct_node_id = mod->ct_node_id;
    
    if (mod->oneshot_fns) {
        free_fn(handle, ct_node_id, mod->oneshot_fns);
    }
    if (mod->boolean_fns) {
        free_fn(handle, ct_node_id, mod->boolean_fns);
    }
    if (mod->main_fns) {
        free_fn(handle, ct_node_id, mod->main_fns);
    }
    if (mod->node_states) {
        free_fn(handle, ct_node_id, mod->node_states);
    }
    
    free_fn(handle, ct_node_id, mod);
}

// ============================================================================
// TREE SELECTION
// ============================================================================

bool module_select_tree(module_runtime_t* mod, uint16_t tree_index) {
    if (!mod) return false;
    
    if (tree_index >= mod->def->tree_count) {
        mod->error_code = MOD_ERR_INVALID_TREE;
        return false;
    }
    
    mod->active_tree = tree_index;
    mod->active_tree_def = &mod->def->trees[tree_index];
    init_node_states(mod);
    mod->error_code = MOD_OK;
    
    return true;
}

bool module_select_tree_by_name(module_runtime_t* mod, const char* name) {
    if (!mod || !name) return false;
    
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
        if (strcmp(mod->def->trees[i].name, name) == 0) {
            return module_select_tree(mod, i);
        }
    }
    
    mod->error_code = MOD_ERR_INVALID_TREE;
    return false;
}

void module_reset(module_runtime_t* mod) {
    if (!mod) return;
    init_node_states(mod);
}

// ============================================================================
// ACCESSORS
// ============================================================================

node_state_t* module_get_state(module_runtime_t* mod, uint16_t node_index) {
    if (!mod) return NULL;
    if (node_index >= mod->active_tree_def->node_count) return NULL;
    return &mod->node_states[node_index];
}

const char* module_get_string(module_runtime_t* mod, uint16_t str_index) {
    if (!mod) return NULL;
    if (str_index >= mod->def->string_count) return NULL;
    return mod->def->strings[str_index];
}

const param_t* module_get_params(module_runtime_t* mod, const node_t* node) {
    if (!mod || !node) return NULL;
    return &mod->active_tree_def->params[node->param_offset];
}

const char* module_active_tree_name(module_runtime_t* mod) {
    if (!mod) return NULL;
    return mod->active_tree_def->name;
}

uint16_t module_active_node_count(module_runtime_t* mod) {
    if (!mod) return 0;
    return mod->active_tree_def->node_count;
}

// ============================================================================
// FUNCTION INVOCATION HELPERS
// ============================================================================

void module_call_oneshot(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
) {
    if (!mod) return;
    if (fn_index >= mod->def->oneshot_count) return;
    if (!mod->oneshot_fns[fn_index]) return;
    
    mod->oneshot_fns[fn_index](mod, node, state, event_id, event_data, params, param_count);
}

bool module_call_boolean(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
) {
    if (!mod) return false;
    if (fn_index >= mod->def->boolean_count) return false;
    if (!mod->boolean_fns[fn_index]) return false;
    
    return mod->boolean_fns[fn_index](mod, node, state, event_id, event_data, params, param_count);
}

cfl_code_t module_call_main(
    module_runtime_t* mod,
    uint16_t fn_index,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
) {
    if (!mod) return CFL_TERMINATE;
    if (fn_index >= mod->def->main_count) return CFL_TERMINATE;
    if (!mod->main_fns[fn_index]) return CFL_TERMINATE;
    
    return mod->main_fns[fn_index](mod, node, state, event_id, event_data, params, param_count);
}

void module_call_debug(module_runtime_t* mod, const char* message) {
    if (!mod || !mod->debug_fn) return;
    mod->debug_fn(mod, message);
}