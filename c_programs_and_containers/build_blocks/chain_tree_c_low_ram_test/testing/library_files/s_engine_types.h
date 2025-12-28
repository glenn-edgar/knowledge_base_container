// ============================================================================
// s_engine_types.h
// S-Expression Engine Type Definitions
// Version 2.8 - Relative brace offsets for sub-array compatibility
// ============================================================================
//
// ARCHITECTURE:
//   s_expr_module_t        - Shared module (function tables, created once)
//   s_expr_tree_instance_t - Per-execution instance (node states per tree)
//
// BRACE INDEXING:
//   brace_idx stores RELATIVE OFFSET, not absolute index
//   - For OPEN/OPEN_CALL: offset to matching CLOSE (close_idx = open_idx + brace_idx)
//   - For CLOSE: offset back to matching OPEN (open_idx = close_idx - brace_idx)
//   This allows sub-arrays to work correctly when passed to nested functions.
//
// USAGE:
//   The generated module header MUST be included BEFORE this file.
//   The module header defines: MODULE_IS_64BIT, ct_int_t, ct_uint_t, ct_float_t
//
// ============================================================================

#ifndef S_ENGINE_TYPES_H
#define S_ENGINE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// 64-BIT CONFIGURATION
// ============================================================================

#ifndef MODULE_IS_64BIT
    #define MODULE_IS_64BIT 0
#endif

#ifndef CT_TYPES_DEFINED
    #if MODULE_IS_64BIT
        typedef int64_t  ct_int_t;
        typedef uint64_t ct_uint_t;
        typedef double   ct_float_t;
    #else
        typedef int32_t  ct_int_t;
        typedef uint32_t ct_uint_t;
        typedef float    ct_float_t;
    #endif
    #define CT_TYPES_DEFINED 1
#endif

#if MODULE_IS_64BIT
    _Static_assert(sizeof(ct_int_t) == 8, "ct_int_t must be 8 bytes for 64-bit mode");
    _Static_assert(sizeof(ct_uint_t) == 8, "ct_uint_t must be 8 bytes for 64-bit mode");
    _Static_assert(sizeof(ct_float_t) == 8, "ct_float_t must be 8 bytes for 64-bit mode");
#else
    _Static_assert(sizeof(ct_int_t) == 4, "ct_int_t must be 4 bytes for 32-bit mode");
    _Static_assert(sizeof(ct_uint_t) == 4, "ct_uint_t must be 4 bytes for 32-bit mode");
    _Static_assert(sizeof(ct_float_t) == 4, "ct_float_t must be 4 bytes for 32-bit mode");
#endif

// ============================================================================
// RESULT CODES
// ============================================================================

typedef enum {
    SE_CONTINUE           = 0,
    SE_HALT               = 1,
    SE_TERMINATE          = 2,
    SE_RESET              = 3,
    SE_DISABLE            = 4,
    SE_FUNCTION_TERMINATE = 5,
} s_expr_result_t;

// ============================================================================
// LIFECYCLE EVENTS
// ============================================================================

#define S_EXPR_EVENT_INIT       0xFFFE  // Sent on first execution (before normal event)
#define S_EXPR_EVENT_TERMINATE  0xFFFF  // Sent before node is disabled/reset

// ============================================================================
// TABLE SELECTORS (upper 2 bits of node type)
// ============================================================================

#define S_EXPR_TABLE_OPCODE    0x00
#define S_EXPR_TABLE_ONESHOT   0x40
#define S_EXPR_TABLE_BOOLEAN   0x80
#define S_EXPR_TABLE_MAIN      0xC0
#define S_EXPR_TABLE_MASK      0xC0
#define S_EXPR_OPCODE_MASK     0x3F

// ============================================================================
// OPCODES (when table == TABLE_OPCODE)
// ============================================================================

#define S_EXPR_OP_PIPELINE     0x01
#define S_EXPR_OP_IF           0x02
#define S_EXPR_OP_IF_ELSE      0x03
#define S_EXPR_OP_COND         0x04
#define S_EXPR_OP_DISPATCH     0x05
#define S_EXPR_OP_AND          0x06
#define S_EXPR_OP_OR           0x07
#define S_EXPR_OP_NOT          0x08
#define S_EXPR_OP_QUOTE        0x09
#define S_EXPR_OP_DEBUG        0x0A
#define S_EXPR_OP_CLAUSE       0x0B
#define S_EXPR_OP_CASE         0x0C
#define S_EXPR_OP_XOR          0x0D
#define S_EXPR_OP_NAND         0x0E
#define S_EXPR_OP_NOR          0x0F

// ============================================================================
// PARAMETER TYPES
// ============================================================================

#define S_EXPR_PARAM_INT       0x00
#define S_EXPR_PARAM_UINT      0x01
#define S_EXPR_PARAM_FLOAT     0x02
#define S_EXPR_PARAM_STRING    0x03
#define S_EXPR_PARAM_MAIN      0x04
#define S_EXPR_PARAM_ONESHOT   0x05
#define S_EXPR_PARAM_PRED      0x06
#define S_EXPR_PARAM_OPEN      0x07
#define S_EXPR_PARAM_CLOSE     0x08
#define S_EXPR_PARAM_OPEN_CALL 0x09
#define S_EXPR_PARAM_SLOT      0x0A

// ============================================================================
// PARAMETER TYPE PREDICATES
// ============================================================================

#define S_EXPR_PARAM_IS_NUMERIC(t)   ((t) <= S_EXPR_PARAM_FLOAT)
#define S_EXPR_PARAM_IS_FUNC_REF(t)  ((t) >= S_EXPR_PARAM_MAIN && (t) <= S_EXPR_PARAM_PRED)
#define S_EXPR_PARAM_IS_BRACE(t)     ((t) >= S_EXPR_PARAM_OPEN && (t) <= S_EXPR_PARAM_OPEN_CALL)
#define S_EXPR_PARAM_IS_OPEN(t)      ((t) == S_EXPR_PARAM_OPEN || (t) == S_EXPR_PARAM_OPEN_CALL)
#define S_EXPR_PARAM_IS_CALLABLE(t)  ((t) == S_EXPR_PARAM_OPEN_CALL)
#define S_EXPR_PARAM_IS_SLOT(t)      ((t) == S_EXPR_PARAM_SLOT)

// ============================================================================
// SENTINEL VALUES
// ============================================================================

#define S_EXPR_NO_CHILD        0xFFFF
#define S_EXPR_NO_SIBLING      0xFFFF

// ============================================================================
// NODE FLAGS (runtime state) - 8 bits
//
// Bit allocation:
//   [0] ACTIVE      - Node executes on tick
//   [1] INITIALIZED - Init event has been sent
//   [2] SUSPENDED   - Reserved for future use (skip but don't terminate)
//   [3] ERROR       - Reserved for future use (node in error state)
//   [4-7]           - Available for user/application use
//
// ============================================================================

#define S_EXPR_NODE_FLAG_ACTIVE        0x01  // bit 0
#define S_EXPR_NODE_FLAG_INITIALIZED   0x02  // bit 1
#define S_EXPR_NODE_FLAG_SUSPENDED     0x04  // bit 2 (reserved)
#define S_EXPR_NODE_FLAG_ERROR         0x08  // bit 3 (reserved)

// System-reserved flags mask (bits 0-3)
#define S_EXPR_NODE_FLAGS_SYSTEM       0x0F

// User-available flags mask (bits 4-7)
#define S_EXPR_NODE_FLAGS_USER         0xF0

// Convenience: check if node is active and initialized
#define S_EXPR_NODE_IS_RUNNING(flags) \
    (((flags) & (S_EXPR_NODE_FLAG_ACTIVE | S_EXPR_NODE_FLAG_INITIALIZED)) == \
     (S_EXPR_NODE_FLAG_ACTIVE | S_EXPR_NODE_FLAG_INITIALIZED))

// ============================================================================
// SLOT REFERENCE STRUCTURE
// ============================================================================

typedef struct {
    uint16_t pool_id;
    uint16_t slot_index;
} s_expr_slot_ref_t;

_Static_assert(sizeof(s_expr_slot_ref_t) == 4, "s_expr_slot_ref_t should be 4 bytes");

// ============================================================================
// PARAMETER STRUCTURE
// ============================================================================

typedef struct {
    uint8_t  type;
    uint8_t  reserved[3];
    union {
        ct_int_t         i;
        ct_uint_t        u;
        ct_float_t       f;
        uint16_t         str_index;
        uint16_t         func_idx;
        uint16_t         brace_idx;   // RELATIVE OFFSET to matching brace
        s_expr_slot_ref_t slot;
    };
} s_expr_param_t;

#if MODULE_IS_64BIT
    _Static_assert(sizeof(s_expr_param_t) == 16, "s_expr_param_t should be 16 bytes in 64-bit mode");
#else
    _Static_assert(sizeof(s_expr_param_t) == 8, "s_expr_param_t should be 8 bytes in 32-bit mode");
#endif

// ============================================================================
// NODE STRUCTURE (ROM - 14 bytes)
// ============================================================================

typedef struct {
    uint8_t  type;
    uint8_t  child_count;
    uint16_t node_index;
    uint16_t first_child;
    uint16_t next_sibling;
    uint16_t fn_index;
    uint16_t param_offset;
    uint8_t  param_count;
    uint8_t  reserved;       // bit 0 = is_default flag
} s_expr_node_t;

_Static_assert(sizeof(s_expr_node_t) == 14, "s_expr_node_t should be 14 bytes");

// ============================================================================
// TREE DEFINITION (ROM)
// ============================================================================

typedef struct {
    const char*             name;
    const s_expr_node_t*    nodes;
    uint16_t                node_count;
    uint16_t                root_index;
    const s_expr_param_t*   params;
    uint16_t                param_count;
} s_expr_tree_def_t;

// ============================================================================
// MODULE DEFINITION (ROM - generated by DSL compiler)
// ============================================================================

typedef struct {
    const char*               name;
    const s_expr_tree_def_t*  trees;
    uint16_t                  tree_count;
    bool                      is_64bit;
    
    const char* const*        oneshot_names;
    const char* const*        boolean_names;
    const char* const*        main_names;
    const char* const*        strings;
    
    uint16_t                  oneshot_count;
    uint16_t                  boolean_count;
    uint16_t                  main_count;
    uint16_t                  string_count;
    
    uint16_t                  max_node_count;  // For reference only
} s_expr_module_def_t;

// ============================================================================
// NODE STATE (RAM - per active tree node)
// ============================================================================

typedef struct {
    uint8_t  flags;
    uint8_t  state;
    uint8_t  reserved[6];  // padding for 8-byte alignment of user_data
    union {
        void*    ptr;
        uint64_t u64;
        int64_t  i64;
    } user_data;
} s_expr_node_state_t;

_Static_assert(sizeof(s_expr_node_state_t) == 16, "s_expr_node_state_t should be 16 bytes");

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

typedef struct s_expr_module s_expr_module_t;
typedef struct s_expr_tree_instance s_expr_tree_instance_t;

// ============================================================================
// FUNCTION SIGNATURES
// ============================================================================

typedef void (*s_expr_oneshot_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* params,
    uint8_t param_count
);

typedef bool (*s_expr_boolean_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* params,
    uint8_t param_count
);

typedef s_expr_result_t (*s_expr_main_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* params,
    uint8_t param_count
);

typedef void (*s_expr_debug_fn_t)(
    s_expr_tree_instance_t* inst,
    const char* message
);

// ============================================================================
// MEMORY ALLOCATOR INTERFACE
// ============================================================================

typedef void* (*s_expr_malloc_fn_t)(void* handle, uint16_t ct_node_id, size_t size);
typedef void  (*s_expr_free_fn_t)(void* handle, uint16_t ct_node_id, void* ptr);

typedef struct {
    s_expr_malloc_fn_t  malloc;
    s_expr_free_fn_t    free;
} s_expr_allocator_t;

// ============================================================================
// FUNCTION REGISTRATION
// ============================================================================

typedef struct {
    const char* name;
    void*       fn_ptr;
} s_expr_fn_entry_t;

typedef struct {
    const s_expr_fn_entry_t*  entries;
    uint16_t                  count;
} s_expr_fn_table_t;

// ============================================================================
// SHARED MODULE (created once per module definition)
// Holds resolved function tables, shared by all tree instances
// ============================================================================

struct s_expr_module {
    // ROM definition
    const s_expr_module_def_t*   def;
    
    // Resolved function pointers (shared by all tree instances)
    s_expr_oneshot_fn_t*         oneshot_fns;
    s_expr_boolean_fn_t*         boolean_fns;
    s_expr_main_fn_t*            main_fns;
    s_expr_debug_fn_t            debug_fn;
    
    // Pool table (optional - set via s_expr_module_set_pool_table)
    void**                       pool_table;
    uint16_t                     pool_count;
    
    // Allocator functions (for creating tree instances)
    s_expr_allocator_t           alloc;
    
    // ChainTree runtime handle (for allocations)
    void*                        handle;
    
    // Initialization error state (checked immediately after init)
    uint8_t                      error_code;
    uint16_t                     error_index;
    const char*                  error_name;
};

// ============================================================================
// TREE INSTANCE (created per-execution, many can exist simultaneously)
// Each ChainTree node that runs a tree gets its own instance
// ============================================================================

struct s_expr_tree_instance {
    // Parent module (for function tables, strings, etc.)
    s_expr_module_t*            module;
    
    // Active tree definition
    uint16_t                    tree_index;
    const s_expr_tree_def_t*    tree;
    
    // Per-node state (sized to tree->node_count)
    s_expr_node_state_t*        node_states;
    uint16_t                    node_count;
    
    // Execution context (set during tick)
    uint16_t                    current_event;
    void*                       event_data;
    
    // ChainTree context (which CT node owns this instance)
    void*                       handle;
    uint16_t                    ct_node_id;
};

// ============================================================================
// ERROR CODES
// ============================================================================

#define S_EXPR_MOD_OK                    0
#define S_EXPR_MOD_ERR_ALLOC             1
#define S_EXPR_MOD_ERR_ONESHOT_NOT_FOUND 2
#define S_EXPR_MOD_ERR_BOOLEAN_NOT_FOUND 3
#define S_EXPR_MOD_ERR_MAIN_NOT_FOUND    4
#define S_EXPR_MOD_ERR_INVALID_TREE      5
#define S_EXPR_MOD_ERR_NULL_DEF          6
#define S_EXPR_MOD_ERR_NULL_REGISTRY     7
#define S_EXPR_MOD_ERR_64BIT_MISMATCH    8

// ============================================================================
// PARAMETER ACCESS HELPERS
// ============================================================================

static inline ct_int_t s_expr_param_get_int(const s_expr_param_t* p) {
    return p->i;
}

static inline ct_uint_t s_expr_param_get_uint(const s_expr_param_t* p) {
    return p->u;
}

static inline ct_float_t s_expr_param_get_float(const s_expr_param_t* p) {
    return p->f;
}

static inline uint16_t s_expr_param_get_str_index(const s_expr_param_t* p) {
    return p->str_index;
}

static inline uint16_t s_expr_param_get_func_idx(const s_expr_param_t* p) {
    return p->func_idx;
}

static inline uint16_t s_expr_param_get_brace_offset(const s_expr_param_t* p) {
    return p->brace_idx;
}

static inline bool s_expr_param_is_callable(const s_expr_param_t* p) {
    return p->type == S_EXPR_PARAM_OPEN_CALL;
}

// Skip past a braced expression (open_idx + offset + 1)
// NOTE: brace_idx is now a RELATIVE OFFSET
static inline uint16_t s_expr_param_skip_brace(const s_expr_param_t* params, uint16_t open_idx) {
    return open_idx + params[open_idx].brace_idx + 1;
}

// Get contents of a braced expression
// NOTE: brace_idx is now a RELATIVE OFFSET
static inline const s_expr_param_t* s_expr_param_brace_contents(
    const s_expr_param_t* params, 
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = open_idx + params[open_idx].brace_idx;
    *out_count = close_idx - open_idx - 1;
    return &params[open_idx + 1];
}

// ============================================================================
// SLOT ACCESS HELPERS
// ============================================================================

static inline s_expr_slot_ref_t s_expr_param_get_slot(const s_expr_param_t* p) {
    return p->slot;
}

static inline uint16_t s_expr_param_get_pool_id(const s_expr_param_t* p) {
    return p->slot.pool_id;
}

static inline uint16_t s_expr_param_get_slot_index(const s_expr_param_t* p) {
    return p->slot.slot_index;
}

static inline bool s_expr_param_is_slot(const s_expr_param_t* p) {
    return p->type == S_EXPR_PARAM_SLOT;
}

// ============================================================================
// POOL ACCESS HELPERS
// Requires pool_table[] to be defined in generated pools.c
// ============================================================================

// Generic pool access - caller casts to correct type
static inline void* s_expr_pool_get_ptr(
    void* const* pool_table,
    const s_expr_param_t* slot_param,
    size_t element_size
) {
    uint16_t pool_id = slot_param->slot.pool_id;
    uint16_t slot_idx = slot_param->slot.slot_index;
    uint8_t* pool = (uint8_t*)pool_table[pool_id];
    return pool + (slot_idx * element_size);
}

// Type-safe macro for pool access
#define S_EXPR_POOL_GET(pool_table, slot_param, type) \
    ((type*)s_expr_pool_get_ptr((pool_table), (slot_param), sizeof(type)))

// Direct pool/slot access when you have the values
#define S_EXPR_POOL_SLOT(pool_table, pool_id, slot_idx, type) \
    (&((type*)(pool_table)[pool_id])[slot_idx])

// ============================================================================
// STRING ACCESS HELPER
// ============================================================================

static inline const char* s_expr_tree_get_string(
    const s_expr_tree_instance_t* inst,
    uint16_t str_index
) {
    if (!inst || !inst->module || !inst->module->def) return NULL;
    if (str_index >= inst->module->def->string_count) return NULL;
    return inst->module->def->strings[str_index];
}

// ============================================================================
// S-EXPRESSION EVALUATION HELPERS
// NOTE: brace_idx is now a RELATIVE OFFSET
// ============================================================================

static inline const s_expr_param_t* s_expr_sexpr_get_func(
    const s_expr_param_t* params,
    uint16_t open_idx
) {
    return &params[open_idx + 1];
}

static inline const s_expr_param_t* s_expr_sexpr_get_args(
    const s_expr_param_t* params,
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = open_idx + params[open_idx].brace_idx;
    *out_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    return &params[open_idx + 2];
}

// ============================================================================
// FUNCTION INVOCATION HELPERS (inline for performance)
// ============================================================================

static inline s_expr_result_t s_expr_invoke_main_ref(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != S_EXPR_PARAM_MAIN) {
        return SE_TERMINATE;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= inst->module->def->main_count || inst->module->main_fns[idx] == NULL) {
        return SE_TERMINATE;
    }
    return inst->module->main_fns[idx](inst, node, state, event_id, event_data, args, arg_count);
}

static inline void s_expr_invoke_oneshot_ref(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != S_EXPR_PARAM_ONESHOT) {
        return;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= inst->module->def->oneshot_count || inst->module->oneshot_fns[idx] == NULL) {
        return;
    }
    inst->module->oneshot_fns[idx](inst, node, state, event_id, event_data, args, arg_count);
}

static inline bool s_expr_invoke_pred_ref(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const s_expr_param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != S_EXPR_PARAM_PRED) {
        return false;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= inst->module->def->boolean_count || inst->module->boolean_fns[idx] == NULL) {
        return false;
    }
    return inst->module->boolean_fns[idx](inst, node, state, event_id, event_data, args, arg_count);
}

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_TYPES_H