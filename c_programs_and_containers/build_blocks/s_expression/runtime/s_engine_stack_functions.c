// ============================================================================
// s_engine_stack_ops.c
// S-Expression Engine Stack-Based Arithmetic Operations
// 
// Oneshot functions that operate on the parameter stack for expression
// evaluation. These provide calculator-style operations within tree nodes.
//
// Stack Notation: [-n, +m] means pop n values, push m values
// All operations use ct_int_t, ct_uint_t, ct_float_t native types
// ============================================================================

#include "s_engine_stack_functions.h"
#include "s_engine_stack.h"
#include <math.h>

// ============================================================================
// BASIC ARITHMETIC [-2, +1]
// ============================================================================

void se_stack_add(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push sum
}

void se_stack_sub(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push difference (second - top)
}

void se_stack_mul(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push product
}

void se_stack_div(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push quotient (float division)
}

void se_stack_mod(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push remainder (fmod)
}

void se_stack_idiv(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push integer quotient (truncates)
}

void se_stack_imod(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push integer remainder
}

// ============================================================================
// UNARY ARITHMETIC [-1, +1]
// ============================================================================

void se_stack_neg(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push negated value
}

void se_stack_abs(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push absolute value
}

void se_stack_inc(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push value + 1
}

void se_stack_dec(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push value - 1
}

// ============================================================================
// BITWISE OPERATIONS [-2, +1]
// ============================================================================

void se_stack_band(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push bitwise AND
}

void se_stack_bor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push bitwise OR
}

void se_stack_bxor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push bitwise XOR
}

void se_stack_shl(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push a << b
}

void se_stack_shr(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push logical shift right (unsigned)
}

void se_stack_sar(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push arithmetic shift right (signed)
}

// ============================================================================
// UNARY BITWISE [-1, +1]
// ============================================================================

void se_stack_bnot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push bitwise NOT
}

// ============================================================================
// COMPARISON [-2, +1] - push 1 (true) or 0 (false)
// ============================================================================

void se_stack_eq(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if equal, 0 otherwise
}

void se_stack_ne(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if not equal, 0 otherwise
}

void se_stack_lt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if a < b, 0 otherwise
}

void se_stack_le(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if a <= b, 0 otherwise
}

void se_stack_gt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if a > b, 0 otherwise
}

void se_stack_ge(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if a >= b, 0 otherwise
}

// ============================================================================
// LOGICAL OPERATIONS [-2, +1]
// ============================================================================

void se_stack_and(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if both non-zero, 0 otherwise
}

void se_stack_or(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push 1 if either non-zero, 0 otherwise
}

// ============================================================================
// UNARY LOGICAL [-1, +1]
// ============================================================================

void se_stack_not(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push 1 if zero, 0 if non-zero
}

// ============================================================================
// MATH FUNCTIONS [-1, +1]
// ============================================================================

void se_stack_sqrt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push sqrt
}

void se_stack_exp(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push e^x
}

void se_stack_log(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push natural log
}

void se_stack_log10(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push log base 10
}

void se_stack_sin(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push sin (radians)
}

void se_stack_cos(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push cos (radians)
}

void se_stack_tan(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push tan (radians)
}

void se_stack_asin(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push asin
}

void se_stack_acos(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push acos
}

void se_stack_atan(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push atan
}

void se_stack_floor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push floor
}

void se_stack_ceil(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push ceil
}

void se_stack_round(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push round to nearest
}

void se_stack_trunc(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push truncate toward zero
}

// ============================================================================
// MATH FUNCTIONS [-2, +1]
// ============================================================================

void se_stack_pow(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push a^b
}

void se_stack_atan2(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push atan2(y, x)
}

void se_stack_min(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push min(a, b)
}

void se_stack_max(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two, push max(a, b)
}

// ============================================================================
// MATH FUNCTIONS [-3, +1]
// ============================================================================

void se_stack_clamp(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop three, push clamp(val, min, max)
}

// ============================================================================
// TYPE CONVERSION [-1, +1]
// ============================================================================

void se_stack_toint(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push as ct_int_t
}

void se_stack_touint(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push as ct_uint_t
}

void se_stack_tofloat(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, push as ct_float_t
}

// ============================================================================
// CONSTANT PUSH [+1]
// ============================================================================

void se_stack_push_const(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: read const from params[0], push to stack
}

// ============================================================================
// IMMEDIATE OPERATIONS [-1, +1]
// ============================================================================

void se_stack_addi(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, add params[0], push result
}

void se_stack_subi(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, subtract params[0], push result
}

void se_stack_muli(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, multiply params[0], push result
}

void se_stack_divi(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, divide by params[0], push result
}

void se_stack_modi(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, mod by params[0], push result
}

void se_stack_shli(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, shift left by params[0], push result
}

void se_stack_shri(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, logical shift right by params[0], push result
}

void se_stack_sari(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, arithmetic shift right by params[0], push result
}

void se_stack_bandi(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, AND with params[0], push result
}

void se_stack_bori(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, OR with params[0], push result
}

void se_stack_bxori(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop one, XOR with params[0], push result
}

// ============================================================================
// BLACKBOARD FIELD OPERATIONS - LOAD [+1]
// ============================================================================

void se_stack_load_int(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: read field offset/size from params[0], load as int, push
}

void se_stack_load_uint(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: read field offset/size from params[0], load as uint, push
}

void se_stack_load_float(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: read field offset/size from params[0], load as float, push
}

void se_stack_load_ptr64(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: read field offset from params[0], load 8-byte ptr, push
}

// ============================================================================
// BLACKBOARD FIELD OPERATIONS - STORE [-1]
// ============================================================================

void se_stack_store_int(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop, store as int to field at params[0] offset
}

void se_stack_store_uint(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop, store as uint to field at params[0] offset
}

void se_stack_store_float(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop, store as float to field at params[0] offset
}

void se_stack_store_ptr64(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop, store as 8-byte ptr to field at params[0] offset
}

// ============================================================================
// STACK MANIPULATION
// ============================================================================

void se_stack_drop(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop and discard one value
}

void se_stack_drop2(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop and discard two values
}

void se_stack_dropn(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop and discard n values (n from params[0])
}

void se_stack_dup(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: duplicate top (a -- a a)
}

void se_stack_dup2(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: duplicate top two (a b -- a b a b)
}

void se_stack_swap(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: swap top two (a b -- b a)
}

void se_stack_over(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: copy second to top (a b -- a b a)
}

void se_stack_rot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: rotate three (a b c -- b c a)
}

void se_stack_nrot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: reverse rotate (a b c -- c a b)
}

void se_stack_pick(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: copy nth to top (n from params[0])
}

void se_stack_roll(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: rotate n items (n from params[0])
}

// ============================================================================
// CONDITIONAL OPERATIONS
// ============================================================================

void se_stack_select(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: (cond a b -- result) if cond!=0 then a else b
}

// ============================================================================
// HASH OPERATIONS
// ============================================================================

void se_stack_push_hash(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: push hash from params[0]
}

void se_stack_hash_eq(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    // TODO: pop two hashes, push 1 if equal, 0 otherwise
}

// ============================================================================
// REGISTRATION
// ============================================================================

// Function table entries
static s_expr_fn_entry_t s_stack_ops_entries[] = {
    // Basic arithmetic
    { 0x00000000, (void*)se_stack_add },      // SE_STACK_ADD
    { 0x00000000, (void*)se_stack_sub },      // SE_STACK_SUB
    { 0x00000000, (void*)se_stack_mul },      // SE_STACK_MUL
    { 0x00000000, (void*)se_stack_div },      // SE_STACK_DIV
    { 0x00000000, (void*)se_stack_mod },      // SE_STACK_MOD
    { 0x00000000, (void*)se_stack_idiv },     // SE_STACK_IDIV
    { 0x00000000, (void*)se_stack_imod },     // SE_STACK_IMOD
    
    // Unary arithmetic
    { 0x00000000, (void*)se_stack_neg },      // SE_STACK_NEG
    { 0x00000000, (void*)se_stack_abs },      // SE_STACK_ABS
    { 0x00000000, (void*)se_stack_inc },      // SE_STACK_INC
    { 0x00000000, (void*)se_stack_dec },      // SE_STACK_DEC
    
    // Bitwise
    { 0x00000000, (void*)se_stack_band },     // SE_STACK_BAND
    { 0x00000000, (void*)se_stack_bor },      // SE_STACK_BOR
    { 0x00000000, (void*)se_stack_bxor },     // SE_STACK_BXOR
    { 0x00000000, (void*)se_stack_shl },      // SE_STACK_SHL
    { 0x00000000, (void*)se_stack_shr },      // SE_STACK_SHR
    { 0x00000000, (void*)se_stack_sar },      // SE_STACK_SAR
    { 0x00000000, (void*)se_stack_bnot },     // SE_STACK_BNOT
    
    // Comparison
    { 0x00000000, (void*)se_stack_eq },       // SE_STACK_EQ
    { 0x00000000, (void*)se_stack_ne },       // SE_STACK_NE
    { 0x00000000, (void*)se_stack_lt },       // SE_STACK_LT
    { 0x00000000, (void*)se_stack_le },       // SE_STACK_LE
    { 0x00000000, (void*)se_stack_gt },       // SE_STACK_GT
    { 0x00000000, (void*)se_stack_ge },       // SE_STACK_GE
    
    // Logical
    { 0x00000000, (void*)se_stack_and },      // SE_STACK_AND
    { 0x00000000, (void*)se_stack_or },       // SE_STACK_OR
    { 0x00000000, (void*)se_stack_not },      // SE_STACK_NOT
    
    // Math functions
    { 0x00000000, (void*)se_stack_sqrt },     // SE_STACK_SQRT
    { 0x00000000, (void*)se_stack_exp },      // SE_STACK_EXP
    { 0x00000000, (void*)se_stack_log },      // SE_STACK_LOG
    { 0x00000000, (void*)se_stack_log10 },    // SE_STACK_LOG10
    { 0x00000000, (void*)se_stack_sin },      // SE_STACK_SIN
    { 0x00000000, (void*)se_stack_cos },      // SE_STACK_COS
    { 0x00000000, (void*)se_stack_tan },      // SE_STACK_TAN
    { 0x00000000, (void*)se_stack_asin },     // SE_STACK_ASIN
    { 0x00000000, (void*)se_stack_acos },     // SE_STACK_ACOS
    { 0x00000000, (void*)se_stack_atan },     // SE_STACK_ATAN
    { 0x00000000, (void*)se_stack_floor },    // SE_STACK_FLOOR
    { 0x00000000, (void*)se_stack_ceil },     // SE_STACK_CEIL
    { 0x00000000, (void*)se_stack_round },    // SE_STACK_ROUND
    { 0x00000000, (void*)se_stack_trunc },    // SE_STACK_TRUNC
    { 0x00000000, (void*)se_stack_pow },      // SE_STACK_POW
    { 0x00000000, (void*)se_stack_atan2 },    // SE_STACK_ATAN2
    { 0x00000000, (void*)se_stack_min },      // SE_STACK_MIN
    { 0x00000000, (void*)se_stack_max },      // SE_STACK_MAX
    { 0x00000000, (void*)se_stack_clamp },    // SE_STACK_CLAMP
    
    // Type conversion
    { 0x00000000, (void*)se_stack_toint },    // SE_STACK_TOINT
    { 0x00000000, (void*)se_stack_touint },   // SE_STACK_TOUINT
    { 0x00000000, (void*)se_stack_tofloat },  // SE_STACK_TOFLOAT
    
    // Constant/immediate
    { 0x00000000, (void*)se_stack_push_const }, // SE_STACK_PUSH_CONST
    { 0x00000000, (void*)se_stack_addi },     // SE_STACK_ADDI
    { 0x00000000, (void*)se_stack_subi },     // SE_STACK_SUBI
    { 0x00000000, (void*)se_stack_muli },     // SE_STACK_MULI
    { 0x00000000, (void*)se_stack_divi },     // SE_STACK_DIVI
    { 0x00000000, (void*)se_stack_modi },     // SE_STACK_MODI
    { 0x00000000, (void*)se_stack_shli },     // SE_STACK_SHLI
    { 0x00000000, (void*)se_stack_shri },     // SE_STACK_SHRI
    { 0x00000000, (void*)se_stack_sari },     // SE_STACK_SARI
    { 0x00000000, (void*)se_stack_bandi },    // SE_STACK_BANDI
    { 0x00000000, (void*)se_stack_bori },     // SE_STACK_BORI
    { 0x00000000, (void*)se_stack_bxori },    // SE_STACK_BXORI
    
    // Field operations
    { 0x00000000, (void*)se_stack_load_int },   // SE_STACK_LOAD_INT
    { 0x00000000, (void*)se_stack_load_uint },  // SE_STACK_LOAD_UINT
    { 0x00000000, (void*)se_stack_load_float }, // SE_STACK_LOAD_FLOAT
    { 0x00000000, (void*)se_stack_load_ptr64 }, // SE_STACK_LOAD_PTR64
    { 0x00000000, (void*)se_stack_store_int },  // SE_STACK_STORE_INT
    { 0x00000000, (void*)se_stack_store_uint }, // SE_STACK_STORE_UINT
    { 0x00000000, (void*)se_stack_store_float },// SE_STACK_STORE_FLOAT
    { 0x00000000, (void*)se_stack_store_ptr64 },// SE_STACK_STORE_PTR64
    
    // Stack manipulation
    { 0x00000000, (void*)se_stack_drop },     // SE_STACK_DROP
    { 0x00000000, (void*)se_stack_drop2 },    // SE_STACK_DROP2
    { 0x00000000, (void*)se_stack_dropn },    // SE_STACK_DROPN
    { 0x00000000, (void*)se_stack_dup },      // SE_STACK_DUP
    { 0x00000000, (void*)se_stack_dup2 },     // SE_STACK_DUP2
    { 0x00000000, (void*)se_stack_swap },     // SE_STACK_SWAP
    { 0x00000000, (void*)se_stack_over },     // SE_STACK_OVER
    { 0x00000000, (void*)se_stack_rot },      // SE_STACK_ROT
    { 0x00000000, (void*)se_stack_nrot },     // SE_STACK_NROT
    { 0x00000000, (void*)se_stack_pick },     // SE_STACK_PICK
    { 0x00000000, (void*)se_stack_roll },     // SE_STACK_ROLL
    
    // Conditional
    { 0x00000000, (void*)se_stack_select },   // SE_STACK_SELECT
    
    // Hash
    { 0x00000000, (void*)se_stack_push_hash }, // SE_STACK_PUSH_HASH
    { 0x00000000, (void*)se_stack_hash_eq },   // SE_STACK_HASH_EQ
};

// NOTE: Hash values (0x00000000) need to be filled in with actual FNV-1a hashes
// of the function names (e.g., "SE_STACK_ADD" -> fnv1a_32("SE_STACK_ADD"))

static const s_expr_fn_table_t s_stack_ops_table = {
    .entries = s_stack_ops_entries,
    .count = sizeof(s_stack_ops_entries) / sizeof(s_stack_ops_entries[0])
};

const s_expr_fn_table_t* s_engine_get_stack_ops_table(void) {
    return &s_stack_ops_table;
}

void s_engine_register_stack_ops(s_expr_module_t* module) {
    // TODO: Register with module's oneshot function table
    // s_expr_module_register_oneshot(module, &s_stack_ops_table);
}