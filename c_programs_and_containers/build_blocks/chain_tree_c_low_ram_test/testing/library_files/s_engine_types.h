// ============================================================================
// s_engine_v3_types.h
// S-Expression Engine Type Definitions - Version 3.0
// Flat parameter model, hash-based function tables, record/field blackboards
// ============================================================================

#ifndef S_ENGINE_V3_TYPES_H
#define S_ENGINE_V3_TYPES_H

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

// ============================================================================
// RESULT CODES (returned by main functions)
// ============================================================================

typedef enum {
    SE_CONTINUE           = 0,  // Continue to next sibling
    SE_HALT               = 1,  // Stop current sequence, return HALT to parent
    SE_TERMINATE          = 2,  // Tree terminates
    SE_RESET              = 3,  // Reset tree state
    SE_DISABLE            = 4,  // Disable this node
    SE_FUNCTION_TERMINATE = 5,  // Function requests tree termination
    SE_SKIP_CONTINUE      = 6,  // Skip remaining siblings, return CONTINUE
    SE_FUNCTION_HALT      = 7,  // Function-level halt
    SE_FUNCTION_RESET     = 8,  // Function-level reset
} s_expr_result_t;

// ============================================================================
// EVENT TYPES (passed to functions)
// ============================================================================

typedef enum {
    SE_EVENT_TICK      = 0,     // Normal tick event
    SE_EVENT_INIT      = 1,     // First execution of node
    SE_EVENT_TERMINATE = 2,     // Node being terminated
    SE_EVENT_USER      = 3,     // User-defined event (check event_id)
} s_expr_event_type_t;

// ============================================================================
// PARAMETER TYPE OPCODES (bits 3:0 of type byte)
// ============================================================================

#define S_EXPR_PARAM_INT         0x00
#define S_EXPR_PARAM_UINT        0x01
#define S_EXPR_PARAM_FLOAT       0x02
#define S_EXPR_PARAM_STR_HASH    0x03
#define S_EXPR_PARAM_SLOT        0x04
#define S_EXPR_PARAM_OPEN        0x05
#define S_EXPR_PARAM_CLOSE       0x06
#define S_EXPR_PARAM_OPEN_CALL   0x07
#define S_EXPR_PARAM_ONESHOT     0x08
#define S_EXPR_PARAM_MAIN        0x09
#define S_EXPR_PARAM_PRED        0x0A
#define S_EXPR_PARAM_FIELD       0x0B

// ============================================================================
// TYPE FLAGS (upper bits of type byte)
// ============================================================================

#define S_EXPR_FLAG_SURVIVES_RESET 0x10  // bit 4: io_call (survives reset)
#define S_EXPR_FLAG_POINTER        0x80  // bit 7: pt_m_call (pointer-capable)
#define S_EXPR_OPCODE_MASK         0x0F  // bits 3:0

// ============================================================================
// PARAMETER TYPE PREDICATES
// ============================================================================

#define S_EXPR_PARAM_IS_NUMERIC(t)   (((t) & S_EXPR_OPCODE_MASK) <= S_EXPR_PARAM_FLOAT)
#define S_EXPR_PARAM_IS_FUNC_REF(t)  (((t) & S_EXPR_OPCODE_MASK) >= S_EXPR_PARAM_ONESHOT && \
                                      ((t) & S_EXPR_OPCODE_MASK) <= S_EXPR_PARAM_PRED)
#define S_EXPR_PARAM_IS_OPEN(t)      (((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_OPEN || \
                                      ((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_OPEN_CALL)
#define S_EXPR_PARAM_IS_CLOSE(t)     (((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_CLOSE)
#define S_EXPR_PARAM_IS_CALLABLE(t)  (((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_OPEN_CALL)
#define S_EXPR_PARAM_IS_SLOT(t)      (((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_SLOT)
#define S_EXPR_PARAM_IS_FIELD(t)     (((t) & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_FIELD)
#define S_EXPR_PARAM_HAS_POINTER(t)  (((t) & S_EXPR_FLAG_POINTER) != 0)
#define S_EXPR_PARAM_SURVIVES_RESET(t) (((t) & S_EXPR_FLAG_SURVIVES_RESET) != 0)

// ============================================================================
// NODE FLAGS (runtime state per func_node)
// ============================================================================

#define S_EXPR_NODE_FLAG_ACTIVE        0x01  // bit 0: node executes on tick
#define S_EXPR_NODE_FLAG_INITIALIZED   0x02  // bit 1: init event sent
#define S_EXPR_NODE_FLAG_EVER_INIT     0x04  // bit 2: survives reset (io_call)
#define S_EXPR_NODE_FLAG_ERROR         0x08  // bit 3: node in error state

// System-reserved flags (bits 0-3)
#define S_EXPR_NODE_FLAGS_SYSTEM       0x0F
// User-available flags (bits 4-7)
#define S_EXPR_NODE_FLAGS_USER         0xF0

// ============================================================================
// SLOT REFERENCE STRUCTURE
// ============================================================================

typedef struct {
    uint16_t pool_id;
    uint16_t slot_index;
} s_expr_slot_ref_t;

// ============================================================================
// FIELD REFERENCE STRUCTURE (for record/blackboard access)
// ============================================================================

typedef struct {
    uint16_t offset;
    uint16_t size;
} s_expr_field_ref_t;

// ============================================================================
// PARAMETER STRUCTURE (matches DSL output)
// ============================================================================

typedef struct {
    uint8_t  type;              // opcode + flags
    uint8_t  index_to_pointer;  // base index into pointer array (for pt_m_call)
    uint16_t node_index;        // func_node index (for function refs)
    uint8_t  reserved[4];
    union {
        ct_int_t          i;
        ct_uint_t         u;
        ct_float_t        f;
        uint32_t          str_hash;
        uint16_t          func_idx;
        uint16_t          brace_idx;    // relative offset to matching brace
        s_expr_slot_ref_t slot;
        s_expr_field_ref_t field;
    };
} s_expr_param_t;

// ============================================================================
// FIELD DESCRIPTOR (for runtime field lookup)
// ============================================================================

typedef struct {
    uint32_t name_hash;
    uint16_t offset;
    uint16_t size;
} s_expr_field_desc_t;

// ============================================================================
// RECORD DESCRIPTOR (for runtime record management)
// ============================================================================

typedef struct {
    uint32_t name_hash;
    uint16_t total_size;
    uint16_t field_count;
    const s_expr_field_desc_t* fields;
} s_expr_record_desc_t;

// ============================================================================
// TREE DEFINITION (ROM - generated by DSL)
// ============================================================================

typedef struct {
    uint32_t name_hash;
    uint32_t record_hash;           // bound blackboard record (0 if none)
    const s_expr_param_t* params;
    uint16_t param_count;
    uint16_t func_node_count;
    uint16_t pointer_count;
} s_expr_tree_def_t;

// ============================================================================
// MODULE DEFINITION (ROM - generated by DSL)
// ============================================================================

typedef struct {
    uint32_t name_hash;
    const s_expr_tree_def_t* trees;
    uint16_t tree_count;
    bool     is_64bit;
    
    const uint32_t* oneshot_hashes;
    const uint32_t* main_hashes;
    const uint32_t* pred_hashes;
    
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    
    uint16_t max_func_node_count;
    uint16_t max_pointer_count;
    uint16_t max_param_count;
    
    // Optional: record descriptors
    const s_expr_record_desc_t* records;
    uint16_t record_count;
} s_expr_module_def_t;

// ============================================================================
// NODE STATE (RAM - per func_node in tree instance)
// ============================================================================

typedef struct {
    uint8_t  flags;
    uint8_t  state;         // user state (0-255)
    uint8_t  reserved[6];
    union {
        void*    ptr;
        uint64_t u64;
        int64_t  i64;
        double   f64;
    } user_data;
} s_expr_node_state_t;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

typedef struct s_expr_module s_expr_module_t;
typedef struct s_expr_tree_instance s_expr_tree_instance_t;

// ============================================================================
// FUNCTION SIGNATURES
// All user functions receive same signature for consistency
// ============================================================================

// Main function: returns result code
typedef s_expr_result_t (*s_expr_main_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t*   params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void*                   event_data
);

// Oneshot function: no return (runs once)
typedef void (*s_expr_oneshot_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t*   params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void*                   event_data
);

// Predicate function: returns bool
typedef bool (*s_expr_pred_fn_t)(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t*   params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void*                   event_data
);

// Debug callback
typedef void (*s_expr_debug_fn_t)(
    s_expr_tree_instance_t* inst,
    const char* message
);

// Error callback (for pointer access violations, etc.)
typedef void (*s_expr_error_fn_t)(
    s_expr_tree_instance_t* inst,
    uint8_t error_code,
    const char* message
);

// ============================================================================
// MEMORY ALLOCATOR INTERFACE
// ============================================================================

typedef void* (*s_expr_malloc_fn_t)(void* ctx, size_t size);
typedef void  (*s_expr_free_fn_t)(void* ctx, void* ptr);

typedef struct {
    s_expr_malloc_fn_t malloc;
    s_expr_free_fn_t   free;
    void*              ctx;
} s_expr_allocator_t;

// ============================================================================
// FUNCTION REGISTRATION (hash -> function pointer)
// ============================================================================

typedef struct {
    uint32_t hash;
    void*    fn_ptr;
} s_expr_fn_entry_t;

typedef struct {
    const s_expr_fn_entry_t* entries;
    uint16_t                 count;
} s_expr_fn_table_t;

// ============================================================================
// SHARED MODULE (created once per module definition)
// ============================================================================

struct s_expr_module {
    const s_expr_module_def_t* def;
    
    // Resolved function pointers (indexed by func_idx)
    s_expr_oneshot_fn_t* oneshot_fns;
    s_expr_main_fn_t*    main_fns;
    s_expr_pred_fn_t*    pred_fns;
    
    // Callbacks
    s_expr_debug_fn_t    debug_fn;
    s_expr_error_fn_t    error_fn;
    
    // Allocator
    s_expr_allocator_t   alloc;
    
    // Pool table (legacy support)
    void**               pool_table;
    uint16_t             pool_count;
    
    // Error state
    uint8_t              error_code;
    uint16_t             error_index;
    uint32_t             error_hash;
};

// ============================================================================
// TREE INSTANCE (created per-execution)
// ============================================================================

sstruct s_expr_tree_instance {
    s_expr_module_t*          module;
    const s_expr_tree_def_t*  tree;
    uint16_t                  tree_index;
    
    // Per func_node state
    s_expr_node_state_t*      node_states;
    uint16_t                  node_count;
    
    // Pointer array (for pt_m_call)
    void**                    pointer_array;
    uint16_t                  pointer_count;
    
    // Blackboard (record binding)
    void*                     blackboard;
    uint16_t                  blackboard_size;
    
    // Current execution context
    uint16_t                  current_node_index;
    uint16_t                  current_event_id;
    void*                     current_event_data;
    bool                      in_pointer_call;
    uint8_t                   pointer_base;
    
    // ChainTree node identifier
    uint32_t                  ct_node_id;    // <-- ADD THIS
    
    // User context
    void*                     user_ctx;
};

// ============================================================================
// ERROR CODES
// ============================================================================

#define S_EXPR_ERR_OK                    0
#define S_EXPR_ERR_ALLOC                 1
#define S_EXPR_ERR_NULL_DEF              2
#define S_EXPR_ERR_64BIT_MISMATCH        3
#define S_EXPR_ERR_ONESHOT_NOT_FOUND     4
#define S_EXPR_ERR_MAIN_NOT_FOUND        5
#define S_EXPR_ERR_PRED_NOT_FOUND        6
#define S_EXPR_ERR_INVALID_TREE          7
#define S_EXPR_ERR_NOT_POINTER_CALL      8   // tried to access pointer in non-pt_m_call
#define S_EXPR_ERR_POINTER_INDEX         9   // pointer index out of range
#define S_EXPR_ERR_NO_BLACKBOARD         10  // field_ref but no blackboard bound

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_V3_TYPES_H