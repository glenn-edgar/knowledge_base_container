// ============================================================================
// s_engine_types.h
// S-Expression Engine Type Definitions
// ============================================================================

#ifndef S_ENGINE_TYPES_H
#define S_ENGINE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

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

#define PARAM_INT32     0x00
#define PARAM_UINT32    0x01
#define PARAM_FLOAT32   0x02
#define PARAM_STRING    0x03

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
// STRUCTURES (ROM - from .h file)
// ============================================================================

typedef struct {
    uint8_t  type;
    union {
        int32_t  i32;
        uint32_t u32;
        float    f32;
        uint16_t str_index;
    };
} param_t;

typedef struct {
    uint8_t  type;           // [table:2][opcode:6]
    uint8_t  child_count;
    uint16_t node_index;     // local to tree
    uint16_t first_child;
    uint16_t next_sibling;
    uint16_t fn_index;
    uint16_t param_offset;
    uint8_t  param_count;
    uint8_t  reserved;       // is_default flag in bit 0
} node_t;

typedef struct {
    const char*    name;
    const node_t*  nodes;
    uint16_t       node_count;
    uint16_t       root_index;
    const param_t* params;
    uint16_t       param_count;
} tree_def_t;

typedef struct {
    const char*           name;
    const tree_def_t*     trees;
    uint16_t              tree_count;
    
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
    const param_t* params
);

typedef bool (*boolean_fn_t)(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params
);

typedef uint8_t (*main_fn_t)(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    uint16_t event_id,
    void* event_data,
    const param_t* params
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

#endif // S_ENGINE_TYPES_H