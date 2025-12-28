// ============================================================================
// s_engine_module.c
// S-Expression Module Management Implementation
// Version 2.8 - Pool table support, lifecycle management, incremental loading
//               Updated for pointer-sized user_data
// ============================================================================

#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// INTERNAL: Function registry storage for incremental loading
// ============================================================================

#define MAX_REGISTRY_TABLES 8

typedef struct {
    const s_expr_fn_table_t* tables[MAX_REGISTRY_TABLES];
    uint16_t table_count;
} s_expr_registry_list_t;

// Static storage for registries (kept until validate is called)
static s_expr_registry_list_t oneshot_registry;
static s_expr_registry_list_t boolean_registry;
static s_expr_registry_list_t main_registry;

// ============================================================================
// HELPER: Lookup function by name across multiple tables
// ============================================================================

static void* lookup_function(const s_expr_registry_list_t* reg, const char* name) {
    if (!reg || !name) return NULL;
    
    for (uint16_t t = 0; t < reg->table_count; t++) {
        const s_expr_fn_table_t* table = reg->tables[t];
        if (!table || !table->entries) continue;
        
        for (uint16_t i = 0; i < table->count; i++) {
            if (table->entries[i].name && strcmp(table->entries[i].name, name) == 0) {
                return table->entries[i].fn_ptr;
            }
        }
    }
    return NULL;
}

// ============================================================================
// ERROR STRING
// ============================================================================

const char* s_expr_module_error_str(uint8_t error_code) {
    switch (error_code) {
        case S_EXPR_MOD_OK:                    return "OK";
        case S_EXPR_MOD_ERR_ALLOC:             return "Allocation failed";
        case S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND: return "Oneshot function not found";
        case S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND: return "Boolean function not found";
        case S_EXPR_MOD_ERR_MAIN_NOT_FOUND:    return "Main function not found";
        case S_EXPR_MOD_ERR_INVALID_TREE:      return "Invalid tree index";
        case S_EXPR_MOD_ERR_NULL_DEF:          return "Null module definition";
        case S_EXPR_MOD_ERR_NULL_REGISTRY:     return "Null function registry";
        case S_EXPR_MOD_ERR_64BIT_MISMATCH:    return "64-bit mode mismatch";
        default:                               return "Unknown error";
    }
}

// ============================================================================
// MODULE INITIALIZATION (Phase 1)
// ============================================================================

uint8_t s_expr_module_init(
    s_expr_module_t* mod,
    const s_expr_module_def_t* def,
    s_expr_allocator_t alloc,
    void* handle
) {
    if (!mod) return S_EXPR_MOD_ERR_ALLOC;
    
    // Clear module and registries
    memset(mod, 0, sizeof(*mod));
    memset(&oneshot_registry, 0, sizeof(oneshot_registry));
    memset(&boolean_registry, 0, sizeof(boolean_registry));
    memset(&main_registry, 0, sizeof(main_registry));
    
    mod->alloc = alloc;
    mod->handle = handle;
    
    if (!def) {
        mod->error_code = S_EXPR_MOD_ERR_NULL_DEF;
        return S_EXPR_MOD_ERR_NULL_DEF;
    }
    
    // Check 64-bit mode match
    if (def->is_64bit != (MODULE_IS_64BIT != 0)) {
        mod->error_code = S_EXPR_MOD_ERR_64BIT_MISMATCH;
        return S_EXPR_MOD_ERR_64BIT_MISMATCH;
    }
    
    mod->def = def;
    
    // Allocate function pointer arrays (filled during validate)
    if (def->oneshot_count > 0) {
        size_t size = def->oneshot_count * sizeof(s_expr_oneshot_fn_t);
        mod->oneshot_fns = (s_expr_oneshot_fn_t*)alloc.malloc(handle, 0, size);
        if (!mod->oneshot_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return S_EXPR_MOD_ERR_ALLOC;
        }
        memset(mod->oneshot_fns, 0, size);
    }
    
    if (def->boolean_count > 0) {
        size_t size = def->boolean_count * sizeof(s_expr_boolean_fn_t);
        mod->boolean_fns = (s_expr_boolean_fn_t*)alloc.malloc(handle, 0, size);
        if (!mod->boolean_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return S_EXPR_MOD_ERR_ALLOC;
        }
        memset(mod->boolean_fns, 0, size);
    }
    
    if (def->main_count > 0) {
        size_t size = def->main_count * sizeof(s_expr_main_fn_t);
        mod->main_fns = (s_expr_main_fn_t*)alloc.malloc(handle, 0, size);
        if (!mod->main_fns) {
            mod->error_code = S_EXPR_MOD_ERR_ALLOC;
            return S_EXPR_MOD_ERR_ALLOC;
        }
        memset(mod->main_fns, 0, size);
    }
    
    mod->error_code = S_EXPR_MOD_OK;
    return S_EXPR_MOD_OK;
}

// ============================================================================
// FUNCTION LOADING (Phase 2a) - can be called multiple times
// ============================================================================

uint16_t s_expr_module_load_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || oneshot_registry.table_count >= MAX_REGISTRY_TABLES) return 0;
    oneshot_registry.tables[oneshot_registry.table_count++] = table;
    return table->count;
}

uint16_t s_expr_module_load_boolean(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || boolean_registry.table_count >= MAX_REGISTRY_TABLES) return 0;
    boolean_registry.tables[boolean_registry.table_count++] = table;
    return table->count;
}

uint16_t s_expr_module_load_main(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || main_registry.table_count >= MAX_REGISTRY_TABLES) return 0;
    main_registry.tables[main_registry.table_count++] = table;
    return table->count;
}

// ============================================================================
// FUNCTION RESOLUTION (Phase 2b)
// ============================================================================

uint8_t s_expr_module_validate(s_expr_module_t* mod) {
    if (!mod || !mod->def) {
        return S_EXPR_MOD_ERR_NULL_DEF;
    }
    
    const s_expr_module_def_t* def = mod->def;
    
    // Resolve oneshot functions
    for (uint16_t i = 0; i < def->oneshot_count; i++) {
        mod->oneshot_fns[i] = (s_expr_oneshot_fn_t)lookup_function(
            &oneshot_registry, def->oneshot_names[i]
        );
        if (!mod->oneshot_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = def->oneshot_names[i];
            return S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND;
        }
    }
    
    // Resolve boolean functions
    for (uint16_t i = 0; i < def->boolean_count; i++) {
        mod->boolean_fns[i] = (s_expr_boolean_fn_t)lookup_function(
            &boolean_registry, def->boolean_names[i]
        );
        if (!mod->boolean_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = def->boolean_names[i];
            return S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND;
        }
    }
    
    // Resolve main functions
    for (uint16_t i = 0; i < def->main_count; i++) {
        mod->main_fns[i] = (s_expr_main_fn_t)lookup_function(
            &main_registry, def->main_names[i]
        );
        if (!mod->main_fns[i]) {
            mod->error_code = S_EXPR_MOD_ERR_MAIN_NOT_FOUND;
            mod->error_index = i;
            mod->error_name = def->main_names[i];
            return S_EXPR_MOD_ERR_MAIN_NOT_FOUND;
        }
    }
    
    mod->error_code = S_EXPR_MOD_OK;
    return S_EXPR_MOD_OK;
}

// ============================================================================
// MODULE FREE
// ============================================================================

void s_expr_module_free(s_expr_module_t* mod) {
    if (!mod) return;
    
    if (mod->oneshot_fns) {
        mod->alloc.free(mod->handle, 0, mod->oneshot_fns);
        mod->oneshot_fns = NULL;
    }
    
    if (mod->boolean_fns) {
        mod->alloc.free(mod->handle, 0, mod->boolean_fns);
        mod->boolean_fns = NULL;
    }
    
    if (mod->main_fns) {
        mod->alloc.free(mod->handle, 0, mod->main_fns);
        mod->main_fns = NULL;
    }
    
    mod->def = NULL;
    mod->debug_fn = NULL;
    mod->pool_table = NULL;
    mod->pool_count = 0;
}

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t debug_fn) {
    if (mod) {
        mod->debug_fn = debug_fn;
    }
}

// ============================================================================
// TREE INSTANCE CREATION
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    uint16_t tree_index,
    void* handle,
    uint16_t ct_node_id
) {
    if (!mod || !mod->def) return NULL;
    if (tree_index >= mod->def->tree_count) return NULL;
    
    const s_expr_tree_def_t* tree_def = &mod->def->trees[tree_index];
    
    // Allocate instance
    s_expr_tree_instance_t* inst = (s_expr_tree_instance_t*)mod->alloc.malloc(
        handle ? handle : mod->handle, ct_node_id, sizeof(s_expr_tree_instance_t)
    );
    if (!inst) return NULL;
    
    memset(inst, 0, sizeof(*inst));
    
    // Set up instance
    inst->module = mod;
    inst->tree_index = tree_index;
    inst->tree = tree_def;
    inst->node_count = tree_def->node_count;
    inst->handle = handle ? handle : mod->handle;
    inst->ct_node_id = ct_node_id;
    
    // Allocate node states
    if (tree_def->node_count > 0) {
        size_t state_size = tree_def->node_count * sizeof(s_expr_node_state_t);
        inst->node_states = (s_expr_node_state_t*)mod->alloc.malloc(
            inst->handle, ct_node_id, state_size
        );
        if (!inst->node_states) {
            mod->alloc.free(inst->handle, ct_node_id, inst);
            return NULL;
        }
        
        // Initialize all nodes to ACTIVE (not yet INITIALIZED)
        for (uint16_t i = 0; i < tree_def->node_count; i++) {
            inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE;
            inst->node_states[i].state = 0;
            memset(inst->node_states[i].reserved, 0, sizeof(inst->node_states[i].reserved));
            inst->node_states[i].user_data.u64 = 0;
        }
    }
    
    return inst;
}

// ============================================================================
// TREE INSTANCE FREE
// ============================================================================

void s_expr_tree_free(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    s_expr_module_t* mod = inst->module;
    if (!mod) return;
    
    if (inst->node_states) {
        mod->alloc.free(inst->handle, inst->ct_node_id, inst->node_states);
    }
    
    mod->alloc.free(inst->handle, inst->ct_node_id, inst);
}