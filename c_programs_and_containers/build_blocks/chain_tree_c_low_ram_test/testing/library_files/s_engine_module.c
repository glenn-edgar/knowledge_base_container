// ============================================================================
// s_engine_v3_module.c
// S-Expression Module Management Implementation - Version 3.0
// ============================================================================

#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// INTERNAL: Function registry storage
// ============================================================================

#define MAX_REGISTRY_TABLES 8

typedef struct {
    const s_expr_fn_table_t* tables[MAX_REGISTRY_TABLES];
    uint16_t count;
} s_expr_registry_t;

static s_expr_registry_t oneshot_registry;
static s_expr_registry_t main_registry;
static s_expr_registry_t pred_registry;

// ============================================================================
// INTERNAL: Lookup function by hash
// ============================================================================

static void* lookup_by_hash(const s_expr_registry_t* reg, uint32_t hash) {
    if (!reg) return NULL;
    
    for (uint16_t t = 0; t < reg->count; t++) {
        const s_expr_fn_table_t* table = reg->tables[t];
        if (!table || !table->entries) continue;
        
        for (uint16_t i = 0; i < table->count; i++) {
            if (table->entries[i].hash == hash) {
                return table->entries[i].fn_ptr;
            }
        }
    }
    return NULL;
}

// ============================================================================
// ERROR STRING
// ============================================================================

const char* s_expr_error_str(uint8_t error_code) {
    switch (error_code) {
        case S_EXPR_ERR_OK:               return "OK";
        case S_EXPR_ERR_ALLOC:            return "Allocation failed";
        case S_EXPR_ERR_NULL_DEF:         return "Null module definition";
        case S_EXPR_ERR_64BIT_MISMATCH:   return "64-bit mode mismatch";
        case S_EXPR_ERR_ONESHOT_NOT_FOUND: return "Oneshot function not found";
        case S_EXPR_ERR_MAIN_NOT_FOUND:   return "Main function not found";
        case S_EXPR_ERR_PRED_NOT_FOUND:   return "Predicate function not found";
        case S_EXPR_ERR_INVALID_TREE:     return "Invalid tree index";
        case S_EXPR_ERR_NOT_POINTER_CALL: return "Pointer access outside pt_m_call";
        case S_EXPR_ERR_POINTER_INDEX:    return "Pointer index out of range";
        case S_EXPR_ERR_NO_BLACKBOARD:    return "No blackboard bound";
        default:                          return "Unknown error";
    }
}

// ============================================================================
// MODULE INIT
// ============================================================================

uint8_t s_expr_module_init(
    s_expr_module_t* mod,
    const s_expr_module_def_t* def,
    s_expr_allocator_t alloc
) {
    if (!mod) return S_EXPR_ERR_ALLOC;
    
    // Clear everything
    memset(mod, 0, sizeof(*mod));
    memset(&oneshot_registry, 0, sizeof(oneshot_registry));
    memset(&main_registry, 0, sizeof(main_registry));
    memset(&pred_registry, 0, sizeof(pred_registry));
    
    mod->alloc = alloc;
    
    if (!def) {
        mod->error_code = S_EXPR_ERR_NULL_DEF;
        return S_EXPR_ERR_NULL_DEF;
    }
    
    // Check 64-bit mode match
    if (def->is_64bit != (MODULE_IS_64BIT != 0)) {
        mod->error_code = S_EXPR_ERR_64BIT_MISMATCH;
        return S_EXPR_ERR_64BIT_MISMATCH;
    }
    
    mod->def = def;
    
    // Allocate function pointer arrays
    if (def->oneshot_count > 0) {
        size_t size = def->oneshot_count * sizeof(s_expr_oneshot_fn_t);
        mod->oneshot_fns = (s_expr_oneshot_fn_t*)alloc.malloc(alloc.ctx, size);
        if (!mod->oneshot_fns) {
            mod->error_code = S_EXPR_ERR_ALLOC;
            return S_EXPR_ERR_ALLOC;
        }
        memset(mod->oneshot_fns, 0, size);
    }
    
    if (def->main_count > 0) {
        size_t size = def->main_count * sizeof(s_expr_main_fn_t);
        mod->main_fns = (s_expr_main_fn_t*)alloc.malloc(alloc.ctx, size);
        if (!mod->main_fns) {
            mod->error_code = S_EXPR_ERR_ALLOC;
            return S_EXPR_ERR_ALLOC;
        }
        memset(mod->main_fns, 0, size);
    }
    
    if (def->pred_count > 0) {
        size_t size = def->pred_count * sizeof(s_expr_pred_fn_t);
        mod->pred_fns = (s_expr_pred_fn_t*)alloc.malloc(alloc.ctx, size);
        if (!mod->pred_fns) {
            mod->error_code = S_EXPR_ERR_ALLOC;
            return S_EXPR_ERR_ALLOC;
        }
        memset(mod->pred_fns, 0, size);
    }
    
    mod->error_code = S_EXPR_ERR_OK;
    return S_EXPR_ERR_OK;
}

// ============================================================================
// FUNCTION REGISTRATION
// ============================================================================

void s_expr_module_register_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || oneshot_registry.count >= MAX_REGISTRY_TABLES) return;
    oneshot_registry.tables[oneshot_registry.count++] = table;
}

void s_expr_module_register_main(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || main_registry.count >= MAX_REGISTRY_TABLES) return;
    main_registry.tables[main_registry.count++] = table;
}

void s_expr_module_register_pred(s_expr_module_t* mod, const s_expr_fn_table_t* table) {
    (void)mod;
    if (!table || pred_registry.count >= MAX_REGISTRY_TABLES) return;
    pred_registry.tables[pred_registry.count++] = table;
}

// ============================================================================
// MODULE VALIDATE
// ============================================================================

uint8_t s_expr_module_validate(s_expr_module_t* mod) {
    if (!mod || !mod->def) return S_EXPR_ERR_NULL_DEF;
    
    const s_expr_module_def_t* def = mod->def;
    
    // Resolve oneshot functions by hash
    for (uint16_t i = 0; i < def->oneshot_count; i++) {
        uint32_t hash = def->oneshot_hashes[i];
        mod->oneshot_fns[i] = (s_expr_oneshot_fn_t)lookup_by_hash(&oneshot_registry, hash);
        if (!mod->oneshot_fns[i]) {
            mod->error_code = S_EXPR_ERR_ONESHOT_NOT_FOUND;
            mod->error_index = i;
            mod->error_hash = hash;
            return S_EXPR_ERR_ONESHOT_NOT_FOUND;
        }
    }
    
    // Resolve main functions by hash
    for (uint16_t i = 0; i < def->main_count; i++) {
        uint32_t hash = def->main_hashes[i];
        mod->main_fns[i] = (s_expr_main_fn_t)lookup_by_hash(&main_registry, hash);
        if (!mod->main_fns[i]) {
            mod->error_code = S_EXPR_ERR_MAIN_NOT_FOUND;
            mod->error_index = i;
            mod->error_hash = hash;
            return S_EXPR_ERR_MAIN_NOT_FOUND;
        }
    }
    
    // Resolve predicate functions by hash
    for (uint16_t i = 0; i < def->pred_count; i++) {
        uint32_t hash = def->pred_hashes[i];
        mod->pred_fns[i] = (s_expr_pred_fn_t)lookup_by_hash(&pred_registry, hash);
        if (!mod->pred_fns[i]) {
            mod->error_code = S_EXPR_ERR_PRED_NOT_FOUND;
            mod->error_index = i;
            mod->error_hash = hash;
            return S_EXPR_ERR_PRED_NOT_FOUND;
        }
    }
    
    mod->error_code = S_EXPR_ERR_OK;
    return S_EXPR_ERR_OK;
}

// ============================================================================
// MODULE FREE
// ============================================================================

void s_expr_module_free(s_expr_module_t* mod) {
    if (!mod) return;
    
    s_expr_allocator_t* alloc = &mod->alloc;
    
    if (mod->oneshot_fns) {
        alloc->free(alloc->ctx, mod->oneshot_fns);
        mod->oneshot_fns = NULL;
    }
    
    if (mod->main_fns) {
        alloc->free(alloc->ctx, mod->main_fns);
        mod->main_fns = NULL;
    }
    
    if (mod->pred_fns) {
        alloc->free(alloc->ctx, mod->pred_fns);
        mod->pred_fns = NULL;
    }
    
    mod->def = NULL;
}

// ============================================================================
// SET CALLBACKS
// ============================================================================

void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t fn) {
    if (mod) mod->debug_fn = fn;
}

void s_expr_module_set_error(s_expr_module_t* mod, s_expr_error_fn_t fn) {
    if (mod) mod->error_fn = fn;
}

void s_expr_module_set_pools(s_expr_module_t* mod, void** pools, uint16_t count) {
    if (mod) {
        mod->pool_table = pools;
        mod->pool_count = count;
    }
}

// ============================================================================
// TREE INSTANCE CREATE
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    uint16_t tree_index,
    uint32_t ct_node_id
) {
    if (!mod || !mod->def) return NULL;
    if (tree_index >= mod->def->tree_count) return NULL;
    
    const s_expr_tree_def_t* tree_def = &mod->def->trees[tree_index];
    s_expr_allocator_t* alloc = &mod->alloc;
    
    // Allocate instance
    s_expr_tree_instance_t* inst = (s_expr_tree_instance_t*)alloc->malloc(
        alloc->ctx, sizeof(s_expr_tree_instance_t)
    );
    if (!inst) return NULL;
    
    memset(inst, 0, sizeof(*inst));
    inst->ct_node_id = ct_node_id;
    inst->module = mod;
    inst->tree = tree_def;
    inst->tree_index = tree_index;
    inst->node_count = tree_def->func_node_count;
    inst->pointer_count = tree_def->pointer_count;
    
    // Allocate node states
    if (tree_def->func_node_count > 0) {
        size_t size = tree_def->func_node_count * sizeof(s_expr_node_state_t);
        inst->node_states = (s_expr_node_state_t*)alloc->malloc(alloc->ctx, size);
        if (!inst->node_states) {
            alloc->free(alloc->ctx, inst);
            return NULL;
        }
        
        // Initialize all nodes to ACTIVE
        for (uint16_t i = 0; i < tree_def->func_node_count; i++) {
            inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE;
            inst->node_states[i].state = 0;
            memset(inst->node_states[i].reserved, 0, sizeof(inst->node_states[i].reserved));
            inst->node_states[i].user_data.u64 = 0;
        }
    }
    
    // Allocate pointer array
    if (tree_def->pointer_count > 0) {
        size_t size = tree_def->pointer_count * sizeof(void*);
        inst->pointer_array = (void**)alloc->malloc(alloc->ctx, size);
        if (!inst->pointer_array) {
            if (inst->node_states) alloc->free(alloc->ctx, inst->node_states);
            alloc->free(alloc->ctx, inst);
            return NULL;
        }
        memset(inst->pointer_array, 0, size);
    }
    
    return inst;
}

// ============================================================================
// TREE INSTANCE CREATE BY HASH
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create_by_hash(
    s_expr_module_t* mod,
    uint32_t name_hash,
    uint32_t ct_node_id;
) {
    if (!mod || !mod->def) return NULL;
    
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
        if (mod->def->trees[i].name_hash == name_hash) {
            return s_expr_tree_create(mod, i, ct_node_id);
        }
    }
    return NULL;
}

// ============================================================================
// TREE INSTANCE FREE
// ============================================================================

void s_expr_tree_free(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    s_expr_module_t* mod = inst->module;
    if (!mod) return;
    
    s_expr_allocator_t* alloc = &mod->alloc;
    
    // Free any allocated pointers
    if (inst->pointer_array) {
        for (uint16_t i = 0; i < inst->pointer_count; i++) {
            if (inst->pointer_array[i]) {
                alloc->free(alloc->ctx, inst->pointer_array[i]);
            }
        }
        alloc->free(alloc->ctx, inst->pointer_array);
    }
    
    if (inst->node_states) {
        alloc->free(alloc->ctx, inst->node_states);
    }
    
    alloc->free(alloc->ctx, inst);
}

// ============================================================================
// BLACKBOARD BINDING
// ============================================================================

void s_expr_tree_bind_blackboard(
    s_expr_tree_instance_t* inst,
    void* blackboard,
    uint16_t size
) {
    if (inst) {
        inst->blackboard = blackboard;
        inst->blackboard_size = size;
    }
}

// ============================================================================
// USER CONTEXT
// ============================================================================

void s_expr_tree_set_user_ctx(s_expr_tree_instance_t* inst, void* ctx) {
    if (inst) inst->user_ctx = ctx;
}

void* s_expr_tree_get_user_ctx(s_expr_tree_instance_t* inst) {
    return inst ? inst->user_ctx : NULL;
}

// ============================================================================
// NODE STATE ACCESS (current node)
// ============================================================================

static inline s_expr_node_state_t* get_current_state(s_expr_tree_instance_t* inst) {
    if (!inst || inst->current_node_index >= inst->node_count) return NULL;
    return &inst->node_states[inst->current_node_index];
}

uint8_t s_expr_get_user_flags(s_expr_tree_instance_t* inst) {
    s_expr_node_state_t* state = get_current_state(inst);
    return state ? (state->flags & S_EXPR_NODE_FLAGS_USER) : 0;
}

void s_expr_set_user_flags(s_expr_tree_instance_t* inst, uint8_t flags) {
    s_expr_node_state_t* state = get_current_state(inst);
    if (state) {
        // Only allow setting user bits (4-7), preserve system bits (0-3)
        state->flags = (state->flags & S_EXPR_NODE_FLAGS_SYSTEM) | (flags & S_EXPR_NODE_FLAGS_USER);
    }
}

uint8_t s_expr_get_state(s_expr_tree_instance_t* inst) {
    s_expr_node_state_t* state = get_current_state(inst);
    return state ? state->state : 0;
}

void s_expr_set_state(s_expr_tree_instance_t* inst, uint8_t st) {
    s_expr_node_state_t* state = get_current_state(inst);
    if (state) state->state = st;
}

void* s_expr_get_user_ptr(s_expr_tree_instance_t* inst) {
    s_expr_node_state_t* state = get_current_state(inst);
    return state ? state->user_data.ptr : NULL;
}

void s_expr_set_user_ptr(s_expr_tree_instance_t* inst, void* ptr) {
    s_expr_node_state_t* state = get_current_state(inst);
    if (state) state->user_data.ptr = ptr;
}

uint64_t s_expr_get_user_u64(s_expr_tree_instance_t* inst) {
    s_expr_node_state_t* state = get_current_state(inst);
    return state ? state->user_data.u64 : 0;
}

void s_expr_set_user_u64(s_expr_tree_instance_t* inst, uint64_t val) {
    s_expr_node_state_t* state = get_current_state(inst);
    if (state) state->user_data.u64 = val;
}

double s_expr_get_user_f64(s_expr_tree_instance_t* inst) {
    s_expr_node_state_t* state = get_current_state(inst);
    return state ? state->user_data.f64 : 0.0;
}

void s_expr_set_user_f64(s_expr_tree_instance_t* inst, double val) {
    s_expr_node_state_t* state = get_current_state(inst);
    if (state) state->user_data.f64 = val;
}
// ============================================================================
// INTERNAL: Report error
// ============================================================================

static void report_error(s_expr_tree_instance_t* inst, uint8_t code, const char* msg) {
    if (inst && inst->module && inst->module->error_fn) {
        inst->module->error_fn(inst, code, msg);
    }
}

// ============================================================================
// POINTER ACCESS
// ============================================================================

bool s_expr_is_pointer_call(s_expr_tree_instance_t* inst) {
    return inst && inst->in_pointer_call;
}

void** s_expr_get_pointer_slot(s_expr_tree_instance_t* inst, uint16_t param_index) {
    if (!inst) return NULL;
    
    // Must be inside a pt_m_call
    if (!inst->in_pointer_call) {
        report_error(inst, S_EXPR_ERR_NOT_POINTER_CALL, 
                     "s_expr_get_pointer_slot: not in pointer call");
        return NULL;
    }
    
    // Calculate actual pointer index
    uint16_t ptr_idx = inst->pointer_base + param_index;
    
    if (ptr_idx >= inst->pointer_count) {
        report_error(inst, S_EXPR_ERR_POINTER_INDEX,
                     "s_expr_get_pointer_slot: index out of range");
        return NULL;
    }
    
    return &inst->pointer_array[ptr_idx];
}

void* s_expr_pointer_alloc(s_expr_tree_instance_t* inst, uint16_t param_index, size_t size) {
    void** slot = s_expr_get_pointer_slot(inst, param_index);
    if (!slot) return NULL;
    
    // Free existing if any
    if (*slot) {
        inst->module->alloc.free(inst->module->alloc.ctx, *slot);
    }
    
    // Allocate new
    *slot = inst->module->alloc.malloc(inst->module->alloc.ctx, size);
    return *slot;
}

void s_expr_pointer_free(s_expr_tree_instance_t* inst, uint16_t param_index) {
    void** slot = s_expr_get_pointer_slot(inst, param_index);
    if (!slot || !*slot) return;
    
    inst->module->alloc.free(inst->module->alloc.ctx, *slot);
    *slot = NULL;
}

// ============================================================================
// BLACKBOARD ACCESS
// ============================================================================

void* s_expr_get_field_ptr(s_expr_tree_instance_t* inst, const s_expr_param_t* field_param) {
    if (!inst || !field_param) return NULL;
    
    if ((field_param->type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        return NULL;
    }
    
    if (!inst->blackboard) {
        report_error(inst, S_EXPR_ERR_NO_BLACKBOARD,
                     "s_expr_get_field_ptr: no blackboard bound");
        return NULL;
    }
    
    uint16_t offset = field_param->field.offset;
    
    // Bounds check
    if (offset + field_param->field.size > inst->blackboard_size) {
        report_error(inst, S_EXPR_ERR_NO_BLACKBOARD,
                     "s_expr_get_field_ptr: field exceeds blackboard size");
        return NULL;
    }
    
    return (uint8_t*)inst->blackboard + offset;
}

// ============================================================================
// POOL ACCESS (legacy)
// ============================================================================

void* s_expr_get_slot_ptr(s_expr_tree_instance_t* inst, const s_expr_param_t* slot_param, size_t elem_size) {
    if (!inst || !slot_param) return NULL;
    if (!inst->module || !inst->module->pool_table) return NULL;
    
    if ((slot_param->type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_SLOT) {
        return NULL;
    }
    
    uint16_t pool_id = slot_param->slot.pool_id;
    uint16_t slot_idx = slot_param->slot.slot_index;
    
    if (pool_id >= inst->module->pool_count) return NULL;
    
    uint8_t* pool = (uint8_t*)inst->module->pool_table[pool_id];
    if (!pool) return NULL;
    
    return pool + (slot_idx * elem_size);
}