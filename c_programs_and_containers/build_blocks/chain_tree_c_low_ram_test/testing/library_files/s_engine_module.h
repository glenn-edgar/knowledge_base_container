// ============================================================================
// s_engine_v3_module.h
// S-Expression Module Management API - Version 3.0
// ============================================================================

#ifndef S_ENGINE_V3_MODULE_H
#define S_ENGINE_V3_MODULE_H

#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// MODULE LIFECYCLE
// ============================================================================

// Initialize module with definition and allocator
uint8_t s_expr_module_init(
    s_expr_module_t* mod,
    const s_expr_module_def_t* def,
    s_expr_allocator_t alloc
);

// Register function tables (call before validate)
void s_expr_module_register_oneshot(s_expr_module_t* mod, const s_expr_fn_table_t* table);
void s_expr_module_register_main(s_expr_module_t* mod, const s_expr_fn_table_t* table);
void s_expr_module_register_pred(s_expr_module_t* mod, const s_expr_fn_table_t* table);

// Validate all functions are resolved
uint8_t s_expr_module_validate(s_expr_module_t* mod);

// Free module resources
void s_expr_module_free(s_expr_module_t* mod);

// Set callbacks
void s_expr_module_set_debug(s_expr_module_t* mod, s_expr_debug_fn_t fn);
void s_expr_module_set_error(s_expr_module_t* mod, s_expr_error_fn_t fn);

// Set pool table (legacy support)
void s_expr_module_set_pools(s_expr_module_t* mod, void** pools, uint16_t count);

// Error info
const char* s_expr_error_str(uint8_t error_code);

// ============================================================================
// TREE INSTANCE LIFECYCLE
// ============================================================================

s_expr_tree_instance_t* s_expr_tree_create(
    s_expr_module_t* mod,
    uint16_t tree_index,
    uint32_t ct_node_id    // <-- ADD THIS
);

s_expr_tree_instance_t* s_expr_tree_create_by_hash(
    s_expr_module_t* mod,
    uint32_t name_hash,
    uint32_t ct_node_id
);

// Free tree instance
void s_expr_tree_free(s_expr_tree_instance_t* inst);

// Bind blackboard (record) to tree
// blackboard must remain valid for lifetime of tree instance
void s_expr_tree_bind_blackboard(
    s_expr_tree_instance_t* inst,
    void* blackboard,
    uint16_t size
);

// Set user context
void s_expr_tree_set_user_ctx(s_expr_tree_instance_t* inst, void* ctx);
void* s_expr_tree_get_user_ctx(s_expr_tree_instance_t* inst);

// ============================================================================
// NODE STATE ACCESS (for use inside function callbacks)
// These operate on the CURRENT node being executed
// ============================================================================

// Get/set user flags (bits 4-7 only, system bits protected)
uint8_t s_expr_get_user_flags(s_expr_tree_instance_t* inst);
void s_expr_set_user_flags(s_expr_tree_instance_t* inst, uint8_t flags);

// Get/set user state byte
uint8_t s_expr_get_state(s_expr_tree_instance_t* inst);
void s_expr_set_state(s_expr_tree_instance_t* inst, uint8_t state);

// Get/set user data (general purpose storage)
void* s_expr_get_user_ptr(s_expr_tree_instance_t* inst);
void s_expr_set_user_ptr(s_expr_tree_instance_t* inst, void* ptr);
uint64_t s_expr_get_user_u64(s_expr_tree_instance_t* inst);
void s_expr_set_user_u64(s_expr_tree_instance_t* inst, uint64_t val);
double s_expr_get_user_f64(s_expr_tree_instance_t* inst);
void s_expr_set_user_f64(s_expr_tree_instance_t* inst, double val);
// ============================================================================
// POINTER ACCESS (for pt_m_call only)
// These error if called outside a pt_m_call context
// ============================================================================

// Get pointer slot for param at relative index (0 = first param after func ref)
// Returns pointer to the slot (void**) so user can read/write
void** s_expr_get_pointer_slot(s_expr_tree_instance_t* inst, uint16_t param_index);

// Allocate memory and store in pointer slot
void* s_expr_pointer_alloc(s_expr_tree_instance_t* inst, uint16_t param_index, size_t size);

// Free memory in pointer slot (sets slot to NULL)
void s_expr_pointer_free(s_expr_tree_instance_t* inst, uint16_t param_index);

// Check if currently in a pointer-capable call
bool s_expr_is_pointer_call(s_expr_tree_instance_t* inst);

// ============================================================================
// BLACKBOARD ACCESS (for field_ref parameters)
// ============================================================================

// Get pointer to field in blackboard
void* s_expr_get_field_ptr(s_expr_tree_instance_t* inst, const s_expr_param_t* field_param);

// Type-safe field access macros
#define S_EXPR_GET_FIELD(inst, param, type) \
    ((type*)s_expr_get_field_ptr((inst), (param)))

// ============================================================================
// POOL ACCESS (legacy support)
// ============================================================================

void* s_expr_get_slot_ptr(s_expr_tree_instance_t* inst, const s_expr_param_t* slot_param, size_t elem_size);

#define S_EXPR_GET_SLOT(inst, param, type) \
    ((type*)s_expr_get_slot_ptr((inst), (param), sizeof(type)))

// ============================================================================
// PARAMETER ACCESS HELPERS
// ============================================================================

// Get value from parameter
static inline ct_int_t s_expr_param_int(const s_expr_param_t* p) { return p->i; }
static inline ct_uint_t s_expr_param_uint(const s_expr_param_t* p) { return p->u; }
static inline ct_float_t s_expr_param_float(const s_expr_param_t* p) { return p->f; }
static inline uint32_t s_expr_param_str_hash(const s_expr_param_t* p) { return p->str_hash; }
static inline uint16_t s_expr_param_func_idx(const s_expr_param_t* p) { return p->func_idx; }
static inline uint16_t s_expr_param_brace_offset(const s_expr_param_t* p) { return p->brace_idx; }

// Get opcode (lower 4 bits)
static inline uint8_t s_expr_param_opcode(const s_expr_param_t* p) {
    return p->type & S_EXPR_OPCODE_MASK;
}

// Check flags
static inline bool s_expr_param_is_pointer(const s_expr_param_t* p) {
    return (p->type & S_EXPR_FLAG_POINTER) != 0;
}

static inline bool s_expr_param_survives_reset(const s_expr_param_t* p) {
    return (p->type & S_EXPR_FLAG_SURVIVES_RESET) != 0;
}

// ============================================================================
// PARAMETER NAVIGATION
// ============================================================================

// Skip past a parameter (handles braces)
static inline uint16_t s_expr_skip_param(const s_expr_param_t* params, uint16_t idx) {
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    if (opcode == S_EXPR_PARAM_OPEN || opcode == S_EXPR_PARAM_OPEN_CALL) {
        return idx + params[idx].brace_idx + 1;
    }
    return idx + 1;
}

// Get contents of braced expression
// Returns pointer to first element inside braces, sets count
static inline const s_expr_param_t* s_expr_brace_contents(
    const s_expr_param_t* params,
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = open_idx + params[open_idx].brace_idx;
    *out_count = close_idx - open_idx - 1;
    return &params[open_idx + 1];
}

// For OPEN_CALL: get function ref and args
static inline const s_expr_param_t* s_expr_call_func(
    const s_expr_param_t* params,
    uint16_t open_idx
) {
    return &params[open_idx + 1];
}

static inline const s_expr_param_t* s_expr_call_args(
    const s_expr_param_t* params,
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = open_idx + params[open_idx].brace_idx;
    *out_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    return (*out_count > 0) ? &params[open_idx + 2] : NULL;
}

// ============================================================================
// MODULE ACCESSORS
// ============================================================================

static inline uint16_t s_expr_module_tree_count(const s_expr_module_t* mod) {
    return (mod && mod->def) ? mod->def->tree_count : 0;
}

static inline uint32_t s_expr_module_tree_hash(const s_expr_module_t* mod, uint16_t idx) {
    if (!mod || !mod->def || idx >= mod->def->tree_count) return 0;
    return mod->def->trees[idx].name_hash;
}

// ============================================================================
// TREE INSTANCE ACCESSORS
// ============================================================================

static inline uint32_t s_expr_tree_name_hash(const s_expr_tree_instance_t* inst) {
    return (inst && inst->tree) ? inst->tree->name_hash : 0;
}

static inline uint16_t s_expr_tree_node_count(const s_expr_tree_instance_t* inst) {
    return inst ? inst->node_count : 0;
}

static inline const s_expr_param_t* s_expr_tree_params(const s_expr_tree_instance_t* inst) {
    return (inst && inst->tree) ? inst->tree->params : NULL;
}

static inline uint16_t s_expr_tree_param_count(const s_expr_tree_instance_t* inst) {
    return (inst && inst->tree) ? inst->tree->param_count : 0;
}

void s_expr_build_fn_table(
    const s_expr_fn_entry_named_t* named,
    s_expr_fn_entry_t* out,
    uint16_t count
);
#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_V3_MODULE_H