// ============================================================================
// s_engine_types.h
// S-Expression Engine Type Definitions
// Version 2.2 - Compile-time 32/64-bit selection
// ============================================================================
//
// USAGE:
//   The generated module header MUST be included BEFORE this file.
//   The module header defines: MODULE_IS_64BIT, ct_int_t, ct_uint_t, ct_float_t
//
//   Example:
//     #include "motor_module.h"    // Generated - defines types
//     #include "s_engine_types.h"  // This file
//
// ============================================================================

#ifndef S_ENGINE_TYPES_H
#define S_ENGINE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

// ============================================================================
// 64-BIT CONFIGURATION
// If MODULE_IS_64BIT is not defined, default to 32-bit mode
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

// Verify type sizes match configuration
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
// CONTROL CODES
// ============================================================================

typedef enum {
    CFL_CONTINUE           = 0,
    CFL_HALT               = 1,
    CFL_TERMINATE          = 2,
    CFL_RESET              = 3,
    CFL_DISABLE            = 4,
    CFL_FUNCTION_TERMINATE = 5,
} cfl_code_t;

// ============================================================================
// TABLE SELECTORS (upper 2 bits of node type)
// ============================================================================

#define TABLE_OPCODE    0x00
#define TABLE_ONESHOT   0x40
#define TABLE_BOOLEAN   0x80
#define TABLE_MAIN      0xC0
#define TABLE_MASK      0xC0
#define OPCODE_MASK     0x3F

// ============================================================================
// OPCODES (when table == TABLE_OPCODE)
// ============================================================================

#define OP_PIPELINE     0x01
#define OP_IF           0x02
#define OP_IF_ELSE      0x03
#define OP_COND         0x04
#define OP_DISPATCH     0x05
#define OP_AND          0x06
#define OP_OR           0x07
#define OP_NOT          0x08
#define OP_QUOTE        0x09
#define OP_DEBUG        0x0A
#define OP_CLAUSE       0x0B
#define OP_CASE         0x0C

// ============================================================================
// PARAMETER TYPES
// ============================================================================

#define PARAM_INT       0x00    // ct_int_t
#define PARAM_UINT      0x01    // ct_uint_t
#define PARAM_FLOAT     0x02    // ct_float_t
#define PARAM_STRING    0x03    // string table index
#define PARAM_MAIN      0x04    // main function table index
#define PARAM_ONESHOT   0x05    // oneshot function table index
#define PARAM_PRED      0x06    // predicate function table index
#define PARAM_OPEN      0x07    // open brace - data list
#define PARAM_CLOSE     0x08    // close brace
#define PARAM_OPEN_CALL 0x09    // open brace - callable S-expression

// ============================================================================
// PARAMETER TYPE PREDICATES
// ============================================================================

#define PARAM_IS_NUMERIC(t)   ((t) <= PARAM_FLOAT)
#define PARAM_IS_FUNC_REF(t)  ((t) >= PARAM_MAIN && (t) <= PARAM_PRED)
#define PARAM_IS_BRACE(t)     ((t) >= PARAM_OPEN && (t) <= PARAM_OPEN_CALL)
#define PARAM_IS_OPEN(t)      ((t) == PARAM_OPEN || (t) == PARAM_OPEN_CALL)
#define PARAM_IS_CALLABLE(t)  ((t) == PARAM_OPEN_CALL)

// ============================================================================
// SENTINEL VALUES
// ============================================================================

#define NO_CHILD        0xFFFF
#define NO_SIBLING      0xFFFF

// ============================================================================
// NODE FLAGS (runtime state)
// ============================================================================

#define NODE_FLAG_ACTIVE        0x80
#define NODE_FLAG_INITIALIZED   0x40
#define NODE_FLAG_DISABLED      0x20
#define NODE_FLAG_ONESHOT_FIRED 0x10

// ============================================================================
// PARAMETER STRUCTURE
// Size: 8 bytes (32-bit mode) or 12 bytes (64-bit mode)
// ============================================================================

typedef struct {
    uint8_t  type;
    uint8_t  reserved[3];       // padding for alignment
    union {
        ct_int_t   i;           // PARAM_INT
        ct_uint_t  u;           // PARAM_UINT
        ct_float_t f;           // PARAM_FLOAT
        uint16_t   str_index;   // PARAM_STRING
        uint16_t   func_idx;    // PARAM_MAIN, PARAM_ONESHOT, PARAM_PRED
        uint16_t   brace_idx;   // PARAM_OPEN, PARAM_CLOSE, PARAM_OPEN_CALL
    };
} param_t;

// Verify param_t size
#if MODULE_IS_64BIT
    _Static_assert(sizeof(param_t) == 16, "param_t should be 16 bytes in 64-bit mode");
#else
    _Static_assert(sizeof(param_t) == 8, "param_t should be 8 bytes in 32-bit mode");
#endif

// ============================================================================
// NODE STRUCTURE (ROM - 14 bytes, same for both modes)
// ============================================================================

typedef struct {
    uint8_t  type;           // [table:2][opcode:6]
    uint8_t  child_count;
    uint16_t node_index;     // local to tree
    uint16_t first_child;
    uint16_t next_sibling;
    uint16_t fn_index;
    uint16_t param_offset;
    uint8_t  param_count;
    uint8_t  reserved;       // bit 0 = is_default flag
} node_t;

_Static_assert(sizeof(node_t) == 14, "node_t should be 14 bytes");

// ============================================================================
// TREE DEFINITION (ROM)
// ============================================================================

typedef struct {
    const char*    name;
    const node_t*  nodes;
    uint16_t       node_count;
    uint16_t       root_index;
    const param_t* params;
    uint16_t       param_count;
} tree_def_t;

// ============================================================================
// MODULE DEFINITION (ROM)
// ============================================================================

typedef struct {
    const char*           name;
    const tree_def_t*     trees;
    uint16_t              tree_count;
    bool                  is_64bit;
    
    const char* const*    oneshot_names;
    const char* const*    boolean_names;
    const char* const*    main_names;
    const char* const*    strings;
    
    uint16_t              oneshot_count;
    uint16_t              boolean_count;
    uint16_t              main_count;
    uint16_t              string_count;
    
    uint16_t              max_node_count;
} module_def_t;

// ============================================================================
// NODE STATE (RAM - per active tree node)
// ============================================================================

typedef struct {
    uint8_t  flags;
    uint8_t  state;          // user state machine
    uint16_t user_data;      // application scratch
} node_state_t;

_Static_assert(sizeof(node_state_t) == 4, "node_state_t should be 4 bytes");

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

typedef struct module_runtime module_runtime_t;

// ============================================================================
// FUNCTION SIGNATURES
// ============================================================================

typedef void (*oneshot_fn_t)(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

typedef bool (*boolean_fn_t)(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

typedef cfl_code_t (*main_fn_t)(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params,
    uint8_t param_count
);

typedef void (*debug_fn_t)(
    module_runtime_t* mod,
    const char* message
);

// ============================================================================
// MEMORY ALLOCATOR INTERFACE
// ============================================================================

typedef void* (*s_malloc_fn_t)(void* handle, uint16_t ct_node_id, size_t size);
typedef void  (*s_free_fn_t)(void* handle, uint16_t ct_node_id, void* ptr);

typedef struct {
    s_malloc_fn_t  malloc;
    s_free_fn_t    free;
} s_allocator_t;

// ============================================================================
// FUNCTION REGISTRATION
// ============================================================================

typedef struct {
    const char* name;
    void*       fn_ptr;
} fn_entry_t;

typedef struct {
    const fn_entry_t*  entries;
    uint16_t           count;
} fn_table_t;

typedef struct {
    fn_table_t  oneshot;
    fn_table_t  boolean;
    fn_table_t  main;
    debug_fn_t  debug;
} module_registry_t;

// ============================================================================
// MODULE RUNTIME (RAM)
// ============================================================================

struct module_runtime {
    // ROM definition
    const module_def_t*   def;
    
    // Resolved function pointers
    oneshot_fn_t*         oneshot_fns;
    boolean_fn_t*         boolean_fns;
    main_fn_t*            main_fns;
    debug_fn_t            debug_fn;
    
    // Per-node state (sized for max_node_count)
    node_state_t*         node_states;
    
    // Active tree
    uint16_t              active_tree;
    const tree_def_t*     active_tree_def;
    
    // Execution context
    uint16_t              current_event;
    void*                 event_data;
    
    // ChainTree context
    void*                 handle;          // user context
    uint16_t              ct_node_id;      // ChainTree node owning this module
    
    // Allocator
    s_allocator_t         alloc;
    
    // Status
    uint8_t               flags;
    uint8_t               error_code;
    uint16_t              error_index;
    const char*           error_name;
};

// ============================================================================
// ERROR CODES
// ============================================================================

#define MOD_OK                    0
#define MOD_ERR_ALLOC             1
#define MOD_ERR_ONESHOT_NOT_FOUND 2
#define MOD_ERR_BOOLEAN_NOT_FOUND 3
#define MOD_ERR_MAIN_NOT_FOUND    4
#define MOD_ERR_INVALID_TREE      5
#define MOD_ERR_NULL_DEF          6
#define MOD_ERR_NULL_REGISTRY     7

// ============================================================================
// PARAMETER ACCESS HELPERS
// ============================================================================

// Get signed integer value
static inline ct_int_t param_get_int(const param_t* p) {
    return p->i;
}

// Get unsigned integer value
static inline ct_uint_t param_get_uint(const param_t* p) {
    return p->u;
}

// Get float value
static inline ct_float_t param_get_float(const param_t* p) {
    return p->f;
}

// Get string index
static inline uint16_t param_get_str_index(const param_t* p) {
    return p->str_index;
}

// Get function index (for PARAM_MAIN, PARAM_ONESHOT, PARAM_PRED)
static inline uint16_t param_get_func_idx(const param_t* p) {
    return p->func_idx;
}

// Get matching brace index (for PARAM_OPEN, PARAM_OPEN_CALL, PARAM_CLOSE)
static inline uint16_t param_get_brace_idx(const param_t* p) {
    return p->brace_idx;
}

// Check if open brace is callable
static inline bool param_is_callable(const param_t* p) {
    return p->type == PARAM_OPEN_CALL;
}

// Skip to after matching close brace
static inline uint16_t param_skip_brace(const param_t* params, uint16_t open_idx) {
    return params[open_idx].brace_idx + 1;
}

// Get contents between braces (returns pointer to first element, sets count)
static inline const param_t* param_brace_contents(
    const param_t* params, 
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = params[open_idx].brace_idx;
    *out_count = close_idx - open_idx - 1;
    return &params[open_idx + 1];
}

// ============================================================================
// S-EXPRESSION EVALUATION HELPERS
// ============================================================================

// For callable S-expr: get the function param and args
// Returns: function param at params[open_idx + 1]
//          args start at params[open_idx + 2]
//          arg_count = close_idx - open_idx - 2
static inline const param_t* sexpr_get_func(
    const param_t* params,
    uint16_t open_idx
) {
    return &params[open_idx + 1];
}

static inline const param_t* sexpr_get_args(
    const param_t* params,
    uint16_t open_idx,
    uint16_t* out_count
) {
    uint16_t close_idx = params[open_idx].brace_idx;
    *out_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    return &params[open_idx + 2];
}

// Invoke a main function from a PARAM_MAIN reference
static inline cfl_code_t invoke_main_ref(
    module_runtime_t* mod,
    const param_t* func_param,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != PARAM_MAIN) {
        return CFL_TERMINATE;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= mod->def->main_count || mod->main_fns[idx] == NULL) {
        return CFL_TERMINATE;
    }
    return mod->main_fns[idx](mod, node, state, event_id, event_data, args, arg_count);
}

// Invoke a oneshot function from a PARAM_ONESHOT reference
static inline void invoke_oneshot_ref(
    module_runtime_t* mod,
    const param_t* func_param,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != PARAM_ONESHOT) {
        return;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= mod->def->oneshot_count || mod->oneshot_fns[idx] == NULL) {
        return;
    }
    mod->oneshot_fns[idx](mod, node, state, event_id, event_data, args, arg_count);
}

// Invoke a predicate function from a PARAM_PRED reference
static inline bool invoke_pred_ref(
    module_runtime_t* mod,
    const param_t* func_param,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* args,
    uint8_t arg_count
) {
    if (func_param->type != PARAM_PRED) {
        return false;
    }
    uint16_t idx = func_param->func_idx;
    if (idx >= mod->def->boolean_count || mod->boolean_fns[idx] == NULL) {
        return false;
    }
    return mod->boolean_fns[idx](mod, node, state, event_id, event_data, args, arg_count);
}

#endif // S_ENGINE_TYPES_H