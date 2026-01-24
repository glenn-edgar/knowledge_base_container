// ============================================================================
// s_engine_stack.h
// Per-Tree Parameter Stack for S-Expression Engine
//
// Minimal stack of s_expr_param_t elements with Lua-style accessors.
// Stack is attached after tree instantiation and reclaimed on termination.
// Reset at start of each tick.
// ============================================================================

#ifndef S_ENGINE_STACK_H
#define S_ENGINE_STACK_H

#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// STACK STRUCTURE
// ============================================================================

typedef struct s_expr_stack {
    s_expr_param_t* data;           // Stack storage
    uint16_t        capacity;       // Maximum number of params
    uint16_t        sp;             // Stack pointer (next free slot)
} s_expr_stack_t;

// ============================================================================
// LIFECYCLE
// ============================================================================

// Create a stack with given capacity
s_expr_stack_t* s_expr_stack_create(
    s_expr_allocator_t* alloc,
    uint16_t capacity
);

// Free stack
void s_expr_stack_free(
    s_expr_stack_t* stack,
    s_expr_allocator_t* alloc
);

// Reset stack to empty (call at tick start)
void s_expr_stack_reset(s_expr_stack_t* stack);

// ============================================================================
// TREE INTEGRATION
// ============================================================================

// Attach stack to tree (owned = free on tree destroy)
void s_expr_tree_attach_stack(
    s_expr_tree_instance_t* inst,
    s_expr_stack_t* stack,
    bool owned
);

// Create stack and attach to tree in one call (owned = true)
bool s_expr_tree_stack_create_and_attach(
    s_expr_tree_instance_t* inst,
    uint16_t capacity
);

// Terminate stack - frees if owned, clears pointer
void s_expr_tree_stack_terminate(s_expr_tree_instance_t* inst);

// ============================================================================
// PUSH OPERATIONS (value → stack)
// ============================================================================

// Push param onto stack, returns true on success
bool s_expr_stack_push(s_expr_stack_t* stack, const s_expr_param_t* param);

// Push typed values
bool s_expr_stack_push_int(s_expr_stack_t* stack, ct_int_t val);
bool s_expr_stack_push_uint(s_expr_stack_t* stack, ct_uint_t val);
bool s_expr_stack_push_float(s_expr_stack_t* stack, ct_float_t val);
bool s_expr_stack_push_hash(s_expr_stack_t* stack, s_expr_hash_t hash);
bool s_expr_stack_push_ptr(s_expr_stack_t* stack, void* ptr);

// Push copy of value at index
bool s_expr_stack_pushvalue(s_expr_stack_t* stack, int idx);

// ============================================================================
// POP OPERATIONS
// ============================================================================

// Pop param from stack, returns pointer or NULL on underflow
const s_expr_param_t* s_expr_stack_pop(s_expr_stack_t* stack);

// Pop n elements from stack
void s_expr_stack_popn(s_expr_stack_t* stack, uint16_t n);

// ============================================================================
// INDEX CONVERSION
// Positive indices: 1-based from bottom (1 = first pushed)
// Negative indices: from top (-1 = top, -2 = below top)
// Returns NULL if out of range
// ============================================================================

// Get param at index (does not pop)
const s_expr_param_t* s_expr_stack_get(s_expr_stack_t* stack, int idx);

// Convert relative index to absolute (internal helper, exposed for convenience)
int s_expr_stack_absindex(s_expr_stack_t* stack, int idx);

// ============================================================================
// TYPE CHECKING (Lua-style)
// ============================================================================

// Get type opcode at index, returns -1 if invalid
int s_expr_stack_type(s_expr_stack_t* stack, int idx);

// Type predicates
bool s_expr_stack_isint(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isuint(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isfloat(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isnumeric(s_expr_stack_t* stack, int idx);
bool s_expr_stack_ishash(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isptr(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isfield(s_expr_stack_t* stack, int idx);
bool s_expr_stack_isresult(s_expr_stack_t* stack, int idx);

// ============================================================================
// VALUE ACCESSORS (Lua-style "to" functions)
// Return value at index without popping
// Return 0/NULL if type mismatch or invalid index
// ============================================================================

ct_int_t      s_expr_stack_toint(s_expr_stack_t* stack, int idx);
ct_uint_t     s_expr_stack_touint(s_expr_stack_t* stack, int idx);
ct_float_t    s_expr_stack_tofloat(s_expr_stack_t* stack, int idx);
s_expr_hash_t s_expr_stack_tohash(s_expr_stack_t* stack, int idx);
void*         s_expr_stack_toptr(s_expr_stack_t* stack, int idx);

// Get as number (converts int/uint/float to ct_float_t)
ct_float_t    s_expr_stack_tonumber(s_expr_stack_t* stack, int idx);

// ============================================================================
// STACK MANIPULATION (Lua-style)
// ============================================================================

// Get current stack depth
uint16_t s_expr_stack_gettop(s_expr_stack_t* stack);

// Set stack depth (pops if smaller, does nothing if larger)
void s_expr_stack_settop(s_expr_stack_t* stack, int idx);

// Insert top element at idx, shifting elements up
bool s_expr_stack_insert(s_expr_stack_t* stack, int idx);

// Remove element at idx, shifting elements down
bool s_expr_stack_remove(s_expr_stack_t* stack, int idx);

// Replace element at idx with top, then pop
bool s_expr_stack_replace(s_expr_stack_t* stack, int idx);

// Copy value from 'from' index to 'to' index (no pop)
bool s_expr_stack_copy(s_expr_stack_t* stack, int from, int to);

// Rotate n elements starting at idx
// Positive n: rotate toward top
// Negative n: rotate toward bottom
bool s_expr_stack_rotate(s_expr_stack_t* stack, int idx, int n);

// Swap top two elements (convenience)
bool s_expr_stack_swap(s_expr_stack_t* stack);

// Duplicate top element (convenience, same as pushvalue(-1))
bool s_expr_stack_dup(s_expr_stack_t* stack);

// ============================================================================
// STATUS (inline)
// ============================================================================

static inline uint16_t s_expr_stack_depth(const s_expr_stack_t* stack) {
    return stack ? stack->sp : 0;
}

static inline bool s_expr_stack_is_empty(const s_expr_stack_t* stack) {
    return !stack || stack->sp == 0;
}

static inline uint16_t s_expr_stack_capacity(const s_expr_stack_t* stack) {
    return stack ? stack->capacity : 0;
}

static inline uint16_t s_expr_stack_available(const s_expr_stack_t* stack) {
    return stack ? (stack->capacity - stack->sp) : 0;
}

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_STACK_H