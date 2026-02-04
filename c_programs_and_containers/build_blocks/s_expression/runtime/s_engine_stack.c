// ============================================================================
// s_engine_stack.c
// Per-Tree Parameter Stack Implementation
//
// Lua-style stack for S-Expression Engine parameter passing.
// Positive indices: 1-based from bottom (1 = first pushed)
// Negative indices: from top (-1 = top, -2 = below top)
// ============================================================================

#include "s_engine_stack.h"
#include "s_engine_exception.h"
#include <string.h>

// ============================================================================
// LIFECYCLE
// ============================================================================

s_expr_stack_t* s_expr_stack_create(
    s_expr_allocator_t* alloc,
    uint16_t capacity
) {
    if (!alloc || !alloc->malloc || !alloc->free) {
        EXCEPTION("s_expr_stack_create: invalid allocator");
        return NULL;
    }
    
    if (capacity == 0) {
        EXCEPTION("s_expr_stack_create: capacity must be > 0");
        return NULL;
    }
    
    s_expr_stack_t* stack = (s_expr_stack_t*)alloc->malloc(
        alloc->ctx, sizeof(s_expr_stack_t)
    );
    if (!stack) {
        EXCEPTION("s_expr_stack_create: failed to allocate stack");
        return NULL;
    }
    
    size_t data_size = (size_t)capacity * sizeof(s_expr_param_t);
    stack->data = (s_expr_param_t*)alloc->malloc(alloc->ctx, data_size);
    if (!stack->data) {
        EXCEPTION("s_expr_stack_create: failed to allocate stack data");
        alloc->free(alloc->ctx, stack);
        return NULL;
    }
    
    stack->capacity = capacity;
    stack->sp = 0;
    
    return stack;
}

void s_expr_stack_free(
    s_expr_stack_t* stack,
    s_expr_allocator_t* alloc
) {
    if (!stack) return;
    
    if (!alloc || !alloc->free) {
        EXCEPTION("s_expr_stack_free: invalid allocator");
        return;
    }
    
    if (stack->data) {
        alloc->free(alloc->ctx, stack->data);
    }
    alloc->free(alloc->ctx, stack);
}

void s_expr_stack_reset(s_expr_stack_t* stack) {
    if (stack) {
        stack->sp = 0;
    }
}

// ============================================================================
// TREE INTEGRATION
// ============================================================================

void s_expr_tree_attach_stack(
    s_expr_tree_instance_t* inst,
    s_expr_stack_t* stack,
    bool owned
) {
    if (!inst) {
        EXCEPTION("s_expr_tree_attach_stack: NULL instance");
        return;
    }
    
    inst->stack = stack;
    inst->stack_owned = owned;
}

bool s_expr_tree_stack_create_and_attach(
    s_expr_tree_instance_t* inst,
    uint16_t capacity
) {
    if (!inst) {
        EXCEPTION("s_expr_tree_stack_create_and_attach: NULL instance");
        return false;
    }
    
    if (!inst->module) {
        EXCEPTION("s_expr_tree_stack_create_and_attach: NULL module");
        return false;
    }
    
    s_expr_allocator_t* alloc = &inst->module->alloc;
    
    s_expr_stack_t* stack = s_expr_stack_create(alloc, capacity);
    if (!stack) {
        // EXCEPTION already called by s_expr_stack_create
        return false;
    }
    
    inst->stack = stack;
    inst->stack_owned = true;
    
    return true;
}

void s_expr_tree_stack_terminate(s_expr_tree_instance_t* inst) {
    if (!inst) {
        EXCEPTION("s_expr_tree_stack_terminate: NULL instance");
        return;
    }
    
    if (!inst->stack) {
        return;
    }
    
    if (inst->stack_owned) {
        if (!inst->module) {
            EXCEPTION("s_expr_tree_stack_terminate: NULL module, cannot free owned stack");
            return;
        }
        s_expr_stack_free(inst->stack, &inst->module->alloc);
    }
    
    inst->stack = NULL;
    inst->stack_owned = false;
}

// ============================================================================
// INDEX CONVERSION
// Lua-style: positive 1-based from bottom, negative from top
// ============================================================================

int s_expr_stack_absindex(s_expr_stack_t* stack, int idx) {
    if (!stack || stack->sp == 0) return 0;
    
    if (idx > 0) {
        // 1-based from bottom
        return idx;
    } else if (idx < 0) {
        // Negative: from top (-1 = top)
        return (int)stack->sp + idx + 1;
    }
    return 0;  // idx == 0 is invalid
}

// Internal: convert to 0-based array index, returns -1 if invalid
static int stack_to_array_idx(s_expr_stack_t* stack, int idx) {
    if (!stack || stack->sp == 0) return -1;
    
    int abs_idx = s_expr_stack_absindex(stack, idx);
    if (abs_idx < 1 || abs_idx > (int)stack->sp) {
        return -1;
    }
    return abs_idx - 1;  // Convert to 0-based
}

const s_expr_param_t* s_expr_stack_get(s_expr_stack_t* stack, int idx) {
    int arr_idx = stack_to_array_idx(stack, idx);
    if (arr_idx < 0) return NULL;
    return &stack->data[arr_idx];
}

// ============================================================================
// PUSH OPERATIONS
// ============================================================================

bool s_expr_stack_push(s_expr_stack_t* stack, const s_expr_param_t* param) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push: NULL stack");
        return false;
    }
    if (!param) {
        EXCEPTION("s_expr_stack_push: NULL param");
        return false;
    }
    
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push: overflow");
        return false;
    }
    
    stack->data[stack->sp] = *param;
    stack->sp++;
    return true;
}

bool s_expr_stack_push_int(s_expr_stack_t* stack, ct_int_t val) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push_int: NULL stack");
        return false;
    }
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push_int: overflow");
        return false;
    }
    
    s_expr_param_t* p = &stack->data[stack->sp];
    memset(p, 0, sizeof(*p));
    p->type = S_EXPR_PARAM_INT;
    p->int_val = val;
    stack->sp++;
    return true;
}

bool s_expr_stack_push_uint(s_expr_stack_t* stack, ct_uint_t val) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push_uint: NULL stack");
        return false;
    }
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push_uint: overflow");
        return false;
    }
    
    s_expr_param_t* p = &stack->data[stack->sp];
    memset(p, 0, sizeof(*p));
    p->type = S_EXPR_PARAM_UINT;
    p->uint_val = val;
    stack->sp++;
    return true;
}

bool s_expr_stack_push_float(s_expr_stack_t* stack, ct_float_t val) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push_float: NULL stack");
        return false;
    }
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push_float: overflow");
        return false;
    }
    
    s_expr_param_t* p = &stack->data[stack->sp];
    memset(p, 0, sizeof(*p));
    p->type = S_EXPR_PARAM_FLOAT;
    p->float_val = val;
    stack->sp++;
    return true;
}

bool s_expr_stack_push_hash(s_expr_stack_t* stack, s_expr_hash_t hash) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push_hash: NULL stack");
        return false;
    }
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push_hash: overflow");
        return false;
    }
    
    s_expr_param_t* p = &stack->data[stack->sp];
    memset(p, 0, sizeof(*p));
    p->type = S_EXPR_PARAM_STR_HASH;
    p->str_hash = hash;
    stack->sp++;
    return true;
}

bool s_expr_stack_push_ptr(s_expr_stack_t* stack, void* ptr) {
    if (!stack) {
        EXCEPTION("s_expr_stack_push_ptr: NULL stack");
        return false;
    }
    if (stack->sp >= stack->capacity) {
        EXCEPTION("s_expr_stack_push_ptr: overflow");
        return false;
    }
    
    s_expr_param_t* p = &stack->data[stack->sp];
    memset(p, 0, sizeof(*p));
    p->type = S_EXPR_PARAM_SLOT | S_EXPR_FLAG_POINTER;
    // Store pointer in the union - use str_hash field for 64-bit storage
    // This works because s_expr_hash_t is 8 bytes on 64-bit
#if MODULE_IS_64BIT
    p->str_hash = (s_expr_hash_t)(uintptr_t)ptr;
#else
    p->uint_val = (ct_uint_t)(uintptr_t)ptr;
#endif
    stack->sp++;
    return true;
}

bool s_expr_stack_pushvalue(s_expr_stack_t* stack, int idx) {
    if (!stack) {
        EXCEPTION("s_expr_stack_pushvalue: NULL stack");
        return false;
    }
    
    const s_expr_param_t* src = s_expr_stack_get(stack, idx);
    if (!src) {
        EXCEPTION("s_expr_stack_pushvalue: invalid index");
        return false;
    }
    return s_expr_stack_push(stack, src);
}

// ============================================================================
// POP OPERATIONS
// ============================================================================

const s_expr_param_t* s_expr_stack_pop(s_expr_stack_t* stack) {
    if (!stack) {
        EXCEPTION("s_expr_stack_pop: NULL stack");
        return NULL;
    }
    
    if (stack->sp == 0) {
        EXCEPTION("s_expr_stack_pop: underflow");
        return NULL;
    }
    
    stack->sp--;
    return &stack->data[stack->sp];
}

void s_expr_stack_popn(s_expr_stack_t* stack, uint16_t n) {
    if (!stack) {
        EXCEPTION("s_expr_stack_popn: NULL stack");
        return;
    }
    
    if (n > stack->sp) {
        // Pop all - not an error, just saturate
        stack->sp = 0;
    } else {
        stack->sp -= n;
    }
}

// ============================================================================
// TYPE CHECKING
// ============================================================================

int s_expr_stack_type(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return -1;
    return p->type & S_EXPR_OPCODE_MASK;
}

bool s_expr_stack_isint(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_INT;
}

bool s_expr_stack_isuint(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_UINT;
}

bool s_expr_stack_isfloat(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_FLOAT;
}

bool s_expr_stack_isnumeric(s_expr_stack_t* stack, int idx) {
    int type = s_expr_stack_type(stack, idx);
    return type == S_EXPR_PARAM_INT || 
           type == S_EXPR_PARAM_UINT || 
           type == S_EXPR_PARAM_FLOAT;
}

bool s_expr_stack_ishash(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_STR_HASH;
}

bool s_expr_stack_isptr(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return false;
    return (p->type & S_EXPR_FLAG_POINTER) != 0;
}

bool s_expr_stack_isfield(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_FIELD;
}

bool s_expr_stack_isresult(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_type(stack, idx) == S_EXPR_PARAM_RESULT;
}

// ============================================================================
// VALUE ACCESSORS
// Return converted value, 0/NULL on type mismatch or invalid index.
// Use type predicates first for strict checking.
// ============================================================================

ct_int_t s_expr_stack_toint(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return 0;
    
    int type = p->type & S_EXPR_OPCODE_MASK;
    switch (type) {
        case S_EXPR_PARAM_INT:
        case S_EXPR_PARAM_RESULT:
            return p->int_val;
        case S_EXPR_PARAM_UINT:
            return (ct_int_t)p->uint_val;
        case S_EXPR_PARAM_FLOAT:
            return (ct_int_t)p->float_val;
        default:
            return 0;
    }
}

ct_uint_t s_expr_stack_touint(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return 0;
    
    int type = p->type & S_EXPR_OPCODE_MASK;
    switch (type) {
        case S_EXPR_PARAM_UINT:
            return p->uint_val;
        case S_EXPR_PARAM_INT:
            return (ct_uint_t)p->int_val;
        case S_EXPR_PARAM_FLOAT:
            return (ct_uint_t)p->float_val;
        default:
            return 0;
    }
}

ct_float_t s_expr_stack_tofloat(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return 0.0f;
    
    int type = p->type & S_EXPR_OPCODE_MASK;
    switch (type) {
        case S_EXPR_PARAM_FLOAT:
            return p->float_val;
        case S_EXPR_PARAM_INT:
            return (ct_float_t)p->int_val;
        case S_EXPR_PARAM_UINT:
            return (ct_float_t)p->uint_val;
        default:
            return 0.0f;
    }
}

s_expr_hash_t s_expr_stack_tohash(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return 0;
    
    if ((p->type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_STR_HASH) {
        return p->str_hash;
    }
    return 0;
}

void* s_expr_stack_toptr(s_expr_stack_t* stack, int idx) {
    const s_expr_param_t* p = s_expr_stack_get(stack, idx);
    if (!p) return NULL;
    
    if (!(p->type & S_EXPR_FLAG_POINTER)) {
        return NULL;
    }
    
#if MODULE_IS_64BIT
    return (void*)(uintptr_t)p->str_hash;
#else
    return (void*)(uintptr_t)p->uint_val;
#endif
}

ct_float_t s_expr_stack_tonumber(s_expr_stack_t* stack, int idx) {
    return s_expr_stack_tofloat(stack, idx);
}

// ============================================================================
// STACK MANIPULATION
// ============================================================================

uint16_t s_expr_stack_gettop(s_expr_stack_t* stack) {
    return stack ? stack->sp : 0;
}

void s_expr_stack_settop(s_expr_stack_t* stack, int idx) {
    if (!stack) {
        EXCEPTION("s_expr_stack_settop: NULL stack");
        return;
    }
    
    if (idx >= 0) {
        if ((uint16_t)idx < stack->sp) {
            stack->sp = (uint16_t)idx;
        }
        // If idx > sp, Lua would push nils, but we don't have nil type
        // So we just do nothing if trying to grow
    } else {
        // Negative: relative to current top
        int new_top = (int)stack->sp + idx + 1;
        if (new_top < 0) new_top = 0;
        stack->sp = (uint16_t)new_top;
    }
}

bool s_expr_stack_insert(s_expr_stack_t* stack, int idx) {
    if (!stack) {
        EXCEPTION("s_expr_stack_insert: NULL stack");
        return false;
    }
    if (stack->sp == 0) {
        EXCEPTION("s_expr_stack_insert: empty stack");
        return false;
    }
    
    int arr_idx = stack_to_array_idx(stack, idx);
    if (arr_idx < 0) {
        EXCEPTION("s_expr_stack_insert: invalid index");
        return false;
    }
    
    // Save top element
    s_expr_param_t top = stack->data[stack->sp - 1];
    
    // Shift elements up from idx to sp-1
    for (int i = (int)stack->sp - 1; i > arr_idx; i--) {
        stack->data[i] = stack->data[i - 1];
    }
    
    // Insert saved top at idx
    stack->data[arr_idx] = top;
    return true;
}

bool s_expr_stack_remove(s_expr_stack_t* stack, int idx) {
    if (!stack) {
        EXCEPTION("s_expr_stack_remove: NULL stack");
        return false;
    }
    if (stack->sp == 0) {
        EXCEPTION("s_expr_stack_remove: empty stack");
        return false;
    }
    
    int arr_idx = stack_to_array_idx(stack, idx);
    if (arr_idx < 0) {
        EXCEPTION("s_expr_stack_remove: invalid index");
        return false;
    }
    
    // Shift elements down
    for (int i = arr_idx; i < (int)stack->sp - 1; i++) {
        stack->data[i] = stack->data[i + 1];
    }
    
    stack->sp--;
    return true;
}

bool s_expr_stack_replace(s_expr_stack_t* stack, int idx) {
    if (!stack) {
        EXCEPTION("s_expr_stack_replace: NULL stack");
        return false;
    }
    if (stack->sp == 0) {
        EXCEPTION("s_expr_stack_replace: empty stack");
        return false;
    }
    
    int arr_idx = stack_to_array_idx(stack, idx);
    if (arr_idx < 0) {
        EXCEPTION("s_expr_stack_replace: invalid index");
        return false;
    }
    
    // Replace element at idx with top, then pop
    stack->data[arr_idx] = stack->data[stack->sp - 1];
    stack->sp--;
    return true;
}

bool s_expr_stack_copy(s_expr_stack_t* stack, int from, int to) {
    if (!stack) {
        EXCEPTION("s_expr_stack_copy: NULL stack");
        return false;
    }
    
    int from_arr = stack_to_array_idx(stack, from);
    int to_arr = stack_to_array_idx(stack, to);
    
    if (from_arr < 0) {
        EXCEPTION("s_expr_stack_copy: invalid 'from' index");
        return false;
    }
    if (to_arr < 0) {
        EXCEPTION("s_expr_stack_copy: invalid 'to' index");
        return false;
    }
    
    stack->data[to_arr] = stack->data[from_arr];
    return true;
}

bool s_expr_stack_rotate(s_expr_stack_t* stack, int idx, int n) {
    if (!stack) {
        EXCEPTION("s_expr_stack_rotate: NULL stack");
        return false;
    }
    if (stack->sp == 0) {
        EXCEPTION("s_expr_stack_rotate: empty stack");
        return false;
    }
    
    int arr_idx = stack_to_array_idx(stack, idx);
    if (arr_idx < 0) {
        EXCEPTION("s_expr_stack_rotate: invalid index");
        return false;
    }
    
    int count = (int)stack->sp - arr_idx;
    if (count <= 1) return true;  // Nothing to rotate
    
    // Normalize n to positive rotation count within range
    n = n % count;
    if (n < 0) n += count;
    if (n == 0) return true;
    
    // Use in-place reversal algorithm to avoid VLA/heap allocation
    // rotate(arr, n) = reverse(0, count-1); reverse(0, n-1); reverse(n, count-1)
    // But we want to rotate toward top, so adjust:
    // For positive n (rotate toward top): elements move up, top wraps to idx
    
    // Simple bubble rotation - O(n*count) but no extra memory
    // For small stacks this is fine; for large stacks could use reversal
    if (n <= count / 2) {
        // Rotate right by n
        for (int r = 0; r < n; r++) {
            s_expr_param_t temp = stack->data[stack->sp - 1];
            for (int i = (int)stack->sp - 1; i > arr_idx; i--) {
                stack->data[i] = stack->data[i - 1];
            }
            stack->data[arr_idx] = temp;
        }
    } else {
        // Rotate left by (count - n) is faster
        int left_n = count - n;
        for (int r = 0; r < left_n; r++) {
            s_expr_param_t temp = stack->data[arr_idx];
            for (int i = arr_idx; i < (int)stack->sp - 1; i++) {
                stack->data[i] = stack->data[i + 1];
            }
            stack->data[stack->sp - 1] = temp;
        }
    }
    
    return true;
}

bool s_expr_stack_swap(s_expr_stack_t* stack) {
    if (!stack) {
        EXCEPTION("s_expr_stack_swap: NULL stack");
        return false;
    }
    if (stack->sp < 2) {
        EXCEPTION("s_expr_stack_swap: need at least 2 elements");
        return false;
    }
    
    s_expr_param_t temp = stack->data[stack->sp - 1];
    stack->data[stack->sp - 1] = stack->data[stack->sp - 2];
    stack->data[stack->sp - 2] = temp;
    return true;
}

bool s_expr_stack_dup(s_expr_stack_t* stack) {
    if (!stack) {
        EXCEPTION("s_expr_stack_dup: NULL stack");
        return false;
    }
    return s_expr_stack_pushvalue(stack, -1);
}