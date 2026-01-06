// ============================================================================
// s_engine_types.h
// ChainTree S-Expression Engine Core Types
// 
// This header defines types used by generated code and the runtime engine.
// Include this before any generated module headers.
// ============================================================================

#ifndef S_ENGINE_TYPES_H
#define S_ENGINE_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

// ============================================================================
// RESULT CODES
// ============================================================================

typedef enum {
    SE_CONTINUE = 0,            // Continue execution
    SE_HALT = 1,                // Halt current node, resume next tick
    SE_FUNCTION_TERMINATE = 2,  // Node completed successfully
    SE_RESET = 3,               // Reset node state
    SE_ERROR = 4,               // Error occurred
} s_result_t;

// ============================================================================
// FUNCTION TYPES
// ============================================================================

typedef enum {
    S_FUNC_ONESHOT = 1,         // One-shot function (no return)
    S_FUNC_MAIN = 2,            // Main function (returns s_result_t)
    S_FUNC_PRED = 3,            // Predicate function (returns bool)
    S_FUNC_PT_MAIN = 4,         // Protothread main
    S_FUNC_INIT_ONE = 5,        // Init oneshot (survives reset)
    S_FUNC_BIT_PRED = 6,        // Bit block predicate
} s_func_type_t;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

typedef struct s_engine s_engine_t;
typedef struct s_engine_ctx s_engine_ctx_t;
typedef struct s_node s_node_t;
typedef struct s_tree s_tree_t;
typedef struct s_module s_module_t;

// ============================================================================
// FUNCTION POINTERS
// ============================================================================

typedef void (*s_oneshot_fn)(s_engine_ctx_t* ctx);
typedef s_result_t (*s_main_fn)(s_engine_ctx_t* ctx);
typedef bool (*s_pred_fn)(s_engine_ctx_t* ctx);

// ============================================================================
// PARAMETER TYPES
// ============================================================================

typedef enum {
    S_PARAM_INT = 1,
    S_PARAM_UINT = 2,
    S_PARAM_FLOAT = 3,
    S_PARAM_STR_IDX = 4,
    S_PARAM_FIELD_REF = 5,
    S_PARAM_NESTED_REF = 6,
    S_PARAM_CONST_REF = 7,
    S_PARAM_RESULT = 8,
    S_PARAM_LIST_START = 9,
    S_PARAM_LIST_END = 10,
    S_PARAM_CALL_START = 11,
    S_PARAM_CALL_END = 12,
    S_PARAM_INT64 = 13,
    S_PARAM_UINT64 = 14,
    S_PARAM_DOUBLE = 15,
} s_param_type_t;

// Parameter value union
typedef union {
    int32_t i32;
    uint32_t u32;
    float f32;
    int64_t i64;
    uint64_t u64;
    double f64;
    uint32_t str_idx;
    uint32_t field_hash;
    uint32_t const_idx;
    uint32_t result_code;
} s_param_value_t;

// ============================================================================
// NODE STATE
// ============================================================================

typedef struct {
    uint32_t pc;                // Program counter / protothread state
    uint32_t user_flags;        // User-defined flags
    int32_t wait_count;         // Wait counter for delays
    uint32_t wait_target;       // Target tick/time for waits
    uint8_t enabled : 1;        // Node enabled
    uint8_t initialized : 1;    // Init oneshot executed
    uint8_t reserved : 6;
} s_node_state_t;

// ============================================================================
// EXECUTION CONTEXT
// ============================================================================

struct s_engine_ctx {
    // Engine reference
    s_engine_t* engine;
    
    // Current tree/node
    const s_tree_t* tree;
    s_node_t* node;
    s_node_state_t* node_state;
    
    // Blackboard
    void* blackboard;
    size_t blackboard_size;
    
    // Module reference (for string/const lookup)
    const s_module_t* module;
    
    // Parameter access
    const uint8_t* params;          // Current parameter bytecode
    uint32_t param_pos;             // Current read position
    uint32_t param_count;           // Total parameters
    
    // Event info
    uint32_t event_id;
    uint32_t event_data;
    
    // Runtime bits (64-bit mask)
    uint64_t runtime_bits;
    
    // Current tick
    uint32_t current_tick;
    
    // Error info
    uint32_t error_code;
    const char* error_msg;
};

// ============================================================================
// MODULE DEFINITION (Static, ROM-based)
// ============================================================================

// Tree definition
typedef struct {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t node_count;
    const uint8_t* bytecode;
    uint32_t bytecode_size;
} s_tree_def_t;

// Field definition
typedef struct {
    uint32_t name_hash;
    uint8_t type_tag;
    uint8_t flags;
    uint16_t offset;
    uint16_t size;
    uint16_t aux;
} s_field_def_t;

// Record definition
typedef struct {
    uint32_t name_hash;
    uint16_t field_count;
    uint16_t size;
    const s_field_def_t* fields;
} s_record_def_t;

// Constant definition
typedef struct {
    uint32_t name_hash;
    uint16_t record_index;
    uint16_t data_size;
    const void* data;
} s_const_def_t;

// Module definition (used by static headers and binary loader)
typedef struct s_expr_module_def {
    uint32_t name_hash;
    
    // Trees
    uint16_t tree_count;
    const s_tree_def_t* trees;
    
    // Records
    uint16_t record_count;
    const s_record_def_t* records;
    
    // Constants
    uint16_t const_count;
    const s_const_def_t* constants;
    
    // Strings
    uint16_t string_count;
    const char* const* strings;
    
    // Function hashes
    uint16_t oneshot_count;
    uint16_t main_count;
    uint16_t pred_count;
    const uint32_t* oneshot_hashes;
    const uint32_t* main_hashes;
    const uint32_t* pred_hashes;
} s_expr_module_def_t;

// ============================================================================
// ENGINE STRUCTURE
// ============================================================================

// Function registry entry
typedef struct {
    uint32_t hash;
    union {
        s_oneshot_fn oneshot;
        s_main_fn main;
        s_pred_fn pred;
        void* ptr;
    } fn;
} s_func_entry_t;

// Engine configuration
typedef struct {
    uint16_t max_oneshot_funcs;
    uint16_t max_main_funcs;
    uint16_t max_pred_funcs;
    uint16_t max_nodes;
    uint16_t max_trees;
} s_engine_config_t;

// Default configuration
#define S_ENGINE_DEFAULT_CONFIG { \
    .max_oneshot_funcs = 128, \
    .max_main_funcs = 64, \
    .max_pred_funcs = 64, \
    .max_nodes = 256, \
    .max_trees = 16, \
}

// ============================================================================
// HASH FUNCTION (FNV-1a 32-bit)
// ============================================================================

#define S_FNV_OFFSET_BASIS 0x811c9dc5
#define S_FNV_PRIME        0x01000193

static inline uint32_t s_hash32(const char* str) {
    uint32_t hash = S_FNV_OFFSET_BASIS;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= S_FNV_PRIME;
    }
    return hash;
}

static inline uint32_t s_hash32_n(const char* str, size_t len) {
    uint32_t hash = S_FNV_OFFSET_BASIS;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)str[i];
        hash *= S_FNV_PRIME;
    }
    return hash;
}

// ============================================================================
// PARAMETER ACCESS HELPERS
// ============================================================================

// Read next parameter type
static inline s_param_type_t s_ctx_peek_param_type(s_engine_ctx_t* ctx) {
    if (!ctx->params || ctx->param_pos >= ctx->param_count) {
        return 0;
    }
    return (s_param_type_t)ctx->params[ctx->param_pos];
}

// Read int32 parameter
static inline int32_t s_ctx_read_int(s_engine_ctx_t* ctx) {
    if (!ctx->params) return 0;
    ctx->param_pos++;  // skip opcode
    const uint8_t* p = ctx->params + ctx->param_pos;
    int32_t v = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
    ctx->param_pos += 4;
    return v;
}

// Read uint32 parameter
static inline uint32_t s_ctx_read_uint(s_engine_ctx_t* ctx) {
    if (!ctx->params) return 0;
    ctx->param_pos++;
    const uint8_t* p = ctx->params + ctx->param_pos;
    uint32_t v = p[0] | (p[1] << 8) | (p[2] << 16) | ((uint32_t)p[3] << 24);
    ctx->param_pos += 4;
    return v;
}

// Read float parameter
static inline float s_ctx_read_float(s_engine_ctx_t* ctx) {
    uint32_t bits = s_ctx_read_uint(ctx);
    float f;
    memcpy(&f, &bits, sizeof(f));
    return f;
}

// Read field reference (returns hash)
static inline uint32_t s_ctx_read_field_ref(s_engine_ctx_t* ctx) {
    return s_ctx_read_uint(ctx);
}

// Read string index
static inline uint32_t s_ctx_read_str_idx(s_engine_ctx_t* ctx) {
    return s_ctx_read_uint(ctx);
}

// Read result code
static inline s_result_t s_ctx_read_result(s_engine_ctx_t* ctx) {
    return (s_result_t)s_ctx_read_uint(ctx);
}

// ============================================================================
// BLACKBOARD ACCESS HELPERS
// ============================================================================

// Get field pointer by offset
static inline void* s_ctx_get_field_ptr(s_engine_ctx_t* ctx, uint16_t offset) {
    if (!ctx->blackboard || offset >= ctx->blackboard_size) {
        return NULL;
    }
    return (uint8_t*)ctx->blackboard + offset;
}

// Get field by hash (requires record definition)
static inline void* s_ctx_get_field_by_hash(
    s_engine_ctx_t* ctx,
    const s_record_def_t* rec,
    uint32_t field_hash
) {
    if (!rec || !rec->fields) return NULL;
    
    for (uint16_t i = 0; i < rec->field_count; i++) {
        if (rec->fields[i].name_hash == field_hash) {
            return s_ctx_get_field_ptr(ctx, rec->fields[i].offset);
        }
    }
    return NULL;
}

// ============================================================================
// BIT OPERATIONS
// ============================================================================

static inline void s_ctx_set_bit(s_engine_ctx_t* ctx, uint8_t bit) {
    if (bit < 64) {
        ctx->runtime_bits |= (1ULL << bit);
    }
}

static inline void s_ctx_clear_bit(s_engine_ctx_t* ctx, uint8_t bit) {
    if (bit < 64) {
        ctx->runtime_bits &= ~(1ULL << bit);
    }
}

static inline bool s_ctx_read_bit(s_engine_ctx_t* ctx, uint8_t bit) {
    if (bit < 64) {
        return (ctx->runtime_bits & (1ULL << bit)) != 0;
    }
    return false;
}

// ============================================================================
// STRING ACCESS
// ============================================================================

static inline const char* s_ctx_get_string(s_engine_ctx_t* ctx, uint32_t index) {
    if (!ctx->module || !ctx->module->strings) return NULL;
    if (index >= ctx->module->string_count) return NULL;
    return ctx->module->strings[index];
}

// ============================================================================
// CONSTANT ACCESS
// ============================================================================

static inline const void* s_ctx_get_const(s_engine_ctx_t* ctx, uint32_t index) {
    if (!ctx->module || !ctx->module->constants) return NULL;
    if (index >= ctx->module->const_count) return NULL;
    return ctx->module->constants[index].data;
}

static inline const s_const_def_t* s_ctx_find_const(s_engine_ctx_t* ctx, uint32_t hash) {
    if (!ctx->module || !ctx->module->constants) return NULL;
    
    for (uint16_t i = 0; i < ctx->module->const_count; i++) {
        if (ctx->module->constants[i].name_hash == hash) {
            return &ctx->module->constants[i];
        }
    }
    return NULL;
}

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_TYPES_H

