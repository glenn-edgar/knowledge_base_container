// ============================================================================
// s_engine_builtins.c
// Built-in S-Expression Engine Functions Implementation
// ============================================================================

#include "s_engine_builtins.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_exception.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

// Predicates
static bool se_pred_and(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_or(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_not(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_nor(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_nand(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_xor(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_true(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_false(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_check_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Main functions
static s_expr_result_t se_pipeline(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_tick_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_time_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_wait_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_nop(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_if_then_else(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_trigger_on_change(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_machine(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_actions(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_field_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_event_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
// main Result code functions
static s_expr_result_t se_return_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_disable(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_skip_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
// Oneshots
static void se_log(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static s_expr_fn_entry_t builtin_oneshot_entries[] = {
    { SE_LOG_HASH, (void*)se_log },
};

static s_expr_fn_entry_t builtin_main_entries[] = {
    { SE_PIPELINE_HASH, (void*)se_pipeline },
    { SE_TICK_DELAY_HASH, (void*)se_tick_delay },
    { SE_TIME_DELAY_HASH, (void*)se_time_delay },
    { SE_WAIT_EVENT_HASH, (void*)se_wait_event },
    { SE_NOP_HASH, (void*)se_nop },
    { SE_IF_THEN_ELSE_HASH, (void*)se_if_then_else },
    { SE_TRIGGER_ON_CHANGE_HASH, (void*)se_trigger_on_change },
    { SE_STATE_MACHINE_HASH, (void*)se_state_machine },
    { SE_STATE_ACTIONS_HASH, (void*)se_state_actions },
    { SE_FIELD_DISPATCH_HASH, (void*)se_field_dispatch },
    { SE_EVENT_DISPATCH_HASH, (void*)se_event_dispatch },
    { SE_DISPATCH_HASH, (void*)se_dispatch },
    // Result code functions
    { SE_RETURN_CONTINUE_HASH, (void*)se_return_continue },
    { SE_RETURN_HALT_HASH, (void*)se_return_halt },
    { SE_RETURN_TERMINATE_HASH, (void*)se_return_terminate },
    { SE_RETURN_RESET_HASH, (void*)se_return_reset },
    { SE_RETURN_DISABLE_HASH, (void*)se_return_disable },
    { SE_RETURN_SKIP_CONTINUE_HASH, (void*)se_return_skip_continue },
    { SE_RETURN_FUNCTION_HALT_HASH, (void*)se_return_function_halt },
    { SE_RETURN_FUNCTION_RESET_HASH, (void*)se_return_function_reset },
    { SE_RETURN_FUNCTION_TERMINATE_HASH, (void*)se_return_function_terminate },
};

static s_expr_fn_entry_t builtin_pred_entries[] = {
    { SE_PRED_AND_HASH, (void*)se_pred_and },
    { SE_PRED_OR_HASH, (void*)se_pred_or },
    { SE_PRED_NOT_HASH, (void*)se_pred_not },
    { SE_PRED_NOR_HASH, (void*)se_pred_nor },
    { SE_PRED_NAND_HASH, (void*)se_pred_nand },
    { SE_PRED_XOR_HASH, (void*)se_pred_xor },
    { SE_TRUE_HASH, (void*)se_true },
    { SE_FALSE_HASH, (void*)se_false },
    { SE_CHECK_EVENT_HASH, (void*)se_check_event },
};

static const s_expr_fn_table_t builtin_oneshot_table = {
    .entries = builtin_oneshot_entries,
    .count = sizeof(builtin_oneshot_entries) / sizeof(builtin_oneshot_entries[0])
};

static const s_expr_fn_table_t builtin_main_table = {
    .entries = builtin_main_entries,
    .count = sizeof(builtin_main_entries) / sizeof(builtin_main_entries[0])
};

static const s_expr_fn_table_t builtin_pred_table = {
    .entries = builtin_pred_entries,
    .count = sizeof(builtin_pred_entries) / sizeof(builtin_pred_entries[0])
};

// ============================================================================
// TABLE ACCESSORS
// ============================================================================

const s_expr_fn_table_t* s_engine_builtin_oneshot_table(void) {
    return &builtin_oneshot_table;
}

const s_expr_fn_table_t* s_engine_builtin_main_table(void) {
    return &builtin_main_table;
}

const s_expr_fn_table_t* s_engine_builtin_pred_table(void) {
    return &builtin_pred_table;
}

// ============================================================================
// PREDICATE IMPLEMENTATIONS
// ============================================================================

// SE_PRED_AND - all children must be true (short-circuit)
static bool se_pred_and(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (!s_expr_invoke_pred(inst, params, i)) {
                return false;  // Short-circuit on first false
            }
        }
        i = s_expr_skip_param(params, i);
    }
    return true;  // All true (or no predicates)
}

// SE_PRED_OR - any child true (short-circuit)
static bool se_pred_or(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (s_expr_invoke_pred(inst, params, i)) {
                return true;  // Short-circuit on first true
            }
        }
        i = s_expr_skip_param(params, i);
    }
    return false;  // None true (or no predicates)
}

// SE_PRED_NOT - invert single child
static bool se_pred_not(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    // Find first predicate and invert it
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            return !s_expr_invoke_pred(inst, params, i);
        }
        i = s_expr_skip_param(params, i);
    }
    return true;  // No predicate found, default true
}

// SE_PRED_NOR - NOT(any child true) = all false
static bool se_pred_nor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    return !se_pred_or(inst, params, param_count, event_type, event_id, event_data);
}

// SE_PRED_NAND - NOT(all children true) = at least one false
static bool se_pred_nand(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    return !se_pred_and(inst, params, param_count, event_type, event_id, event_data);
}

// SE_PRED_XOR - exactly one child true
static bool se_pred_xor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    int true_count = 0;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (s_expr_invoke_pred(inst, params, i)) {
                true_count++;
                if (true_count > 1) {
                    return false;  // More than one true
                }
            }
        }
        i = s_expr_skip_param(params, i);
    }
    
    return (true_count == 1);
}

// SE_TRUE - always true
static bool se_true(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return true;
}

// SE_FALSE - always false
static bool se_false(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return false;
}

// SE_CHECK_EVENT - check if current event matches any of the given IDs
// params: [event_id, event_id, ...]
static bool se_check_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)event_type;
    (void)event_data;
    
    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            if ((uint16_t)params[i].int_val == event_id) {
                return true;
            }
        }
    }
    return false;
}

// ============================================================================
// MAIN FUNCTION IMPLEMENTATIONS
// ============================================================================

// SE_PIPELINE - execute children in sequence
// Returns first non-CONTINUE result, or CONTINUE if all succeed
static s_expr_result_t se_pipeline(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        printf("param[%d] = %d\n", i, params[i].type);
        if (s_expr_param_is_action(&params[i])) {
            s_expr_result_t r = s_expr_invoke_any(inst, params, i);
            if (r != SE_CONTINUE) {
                return r;
            }
        }
        i = s_expr_skip_param(params, i);
    }
    

    
    return s_expr_find_result(params, param_count);
}

// SE_TICK_DELAY - wait for N ticks (pt_m_call)
// params: [tick_count]
// Uses u64 slot for remaining ticks
static s_expr_result_t se_tick_delay(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        uint32_t ticks = (param_count > 0) ? (uint32_t)params[0].uint_val : 0;
        s_expr_set_u64(inst, (uint64_t)ticks);
        return (ticks > 0) ? SE_HALT : SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK event - decrement counter
    uint64_t remaining = s_expr_get_u64(inst);
    if (remaining > 0) {
        remaining--;
        s_expr_set_u64(inst, remaining);
        if (remaining > 0) {
            return SE_HALT;
        }
    }
    
    return SE_CONTINUE;
}

// SE_TIME_DELAY - wait for N seconds (pt_m_call)
// params: [seconds as float]
// Uses f64 slot for remaining time
// Expects event_data to point to delta_time (double*) or uses default 0.001
static s_expr_result_t se_time_delay(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    
    if (event_type == SE_EVENT_INIT) {
        double seconds = (param_count > 0) ? (double)params[0].float_val : 0.0;
        s_expr_set_f64(inst, seconds);
        return (seconds > 0.0) ? SE_HALT : SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK event - subtract delta time
    double remaining = s_expr_get_f64(inst);
    double dt = event_data ? *(double*)event_data : 0.001;  // Default 1ms
    
    remaining -= dt;
    
    if (remaining > 0.0) {
        s_expr_set_f64(inst, remaining);
        return SE_HALT;
    }
    
    return SE_CONTINUE;
}

// SE_WAIT_EVENT - wait for specific event N times (pt_m_call)
// params: [event_id, count]
// Uses u64 slot: upper 32 bits = target event, lower 32 bits = remaining count
static s_expr_result_t se_wait_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        uint32_t target_event = (param_count > 0) ? (uint32_t)params[0].int_val : 0;
        uint32_t count = (param_count > 1) ? (uint32_t)params[1].int_val : 1;
        
        uint64_t state = ((uint64_t)target_event << 32) | count;
        s_expr_set_u64(inst, state);
        
        return (count > 0) ? SE_HALT : SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK event - check if event matches
    uint64_t state = s_expr_get_u64(inst);
    uint32_t target_event = (uint32_t)(state >> 32);
    uint32_t remaining = (uint32_t)(state & 0xFFFFFFFF);
    
    if (event_id == target_event && remaining > 0) {
        remaining--;
        if (remaining == 0) {
            return SE_CONTINUE;
        }
        state = ((uint64_t)target_event << 32) | remaining;
        s_expr_set_u64(inst, state);
    }
    
    return SE_HALT;
}

// SE_NOP - do nothing, return CONTINUE
static s_expr_result_t se_nop(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return SE_CONTINUE;
}

// SE_IF_THEN_ELSE - conditional execution
// params: [predicate] [then_action] [else_action]
static s_expr_result_t se_if_then_else(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    if (param_count == 0) return SE_CONTINUE;
    
    uint16_t idx = 0;
    
    // First: evaluate predicate
    bool condition = false;
    if (s_expr_param_is_predicate(&params[idx])) {
        condition = s_expr_invoke_pred(inst, params, idx);
        idx = s_expr_skip_param(params, idx);
    }
    
    if (idx >= param_count) return SE_CONTINUE;
    
    // Second: then branch
    uint16_t then_idx = idx;
    idx = s_expr_skip_param(params, idx);
    
    // Third: else branch (optional)
    uint16_t else_idx = idx;
    bool has_else = (idx < param_count);
    
    if (condition) {
        return s_expr_invoke_any(inst, params, then_idx);
    } else if (has_else) {
        return s_expr_invoke_any(inst, params, else_idx);
    }
    
    return SE_CONTINUE;
}

// SE_TRIGGER_ON_CHANGE - execute action on state transition
// params: [initial_state (int)] [predicate] [rising_action] [falling_action]
// Uses node state to track previous predicate value
static s_expr_result_t se_trigger_on_change(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    if (param_count < 2) return SE_CONTINUE;
    
    uint16_t idx = 0;
    
    // First param: initial state
    uint8_t initial_state = (uint8_t)params[0].int_val;
    idx++;
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, initial_state);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Second param: predicate
    if (idx >= param_count) return SE_CONTINUE;
    
    bool current = false;
    if (s_expr_param_is_predicate(&params[idx])) {
        current = s_expr_invoke_pred(inst, params, idx);
        idx = s_expr_skip_param(params, idx);
    }
    
    uint8_t prev = s_expr_get_state(inst);
    s_expr_set_state(inst, current ? 1 : 0);
    
    // Detect transitions
    bool rising = (prev == 0 && current);
    bool falling = (prev != 0 && !current);
    
    if (idx >= param_count) return SE_CONTINUE;
    
    // Third param: rising action
    uint16_t rising_idx = idx;
    idx = s_expr_skip_param(params, idx);
    
    // Fourth param: falling action (optional)
    uint16_t falling_idx = idx;
    bool has_falling = (idx < param_count);
    
    if (rising && s_expr_param_is_action(&params[rising_idx])) {
        return s_expr_invoke_any(inst, params, rising_idx);
    } else if (falling && has_falling && s_expr_param_is_action(&params[falling_idx])) {
        return s_expr_invoke_any(inst, params, falling_idx);
    }
    
    return SE_CONTINUE;
}

// SE_STATE_MACHINE - dispatch to state handler based on field value
// params: [field_ref] [state_0_action] [state_1_action] ...
static s_expr_result_t se_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    // First param: field reference for state
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("se_state_machine: first param must be field_ref");
        return SE_CONTINUE;
    }
    
    int32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!state_ptr) return SE_CONTINUE;
    
    int32_t state = *state_ptr;
    if (state < 0) return SE_CONTINUE;
    
    // Find state handler at index 'state' (skip field ref)
    uint16_t idx = s_expr_skip_param(params, 0);
    int32_t state_idx = 0;
    
    while (idx < param_count) {
        if (state_idx == state) {
            if (s_expr_param_is_action(&params[idx])) {
                return s_expr_invoke_any(inst, params, idx);
            }
            return SE_CONTINUE;
        }
        idx = s_expr_skip_param(params, idx);
        state_idx++;
    }
    
    return SE_CONTINUE;  // State out of range
}

// SE_STATE_ACTIONS - execute actions and return result code
// params: [actions...] [result_code]
static s_expr_result_t se_state_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    // Execute all actions
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_action(&params[i])) {
            s_expr_result_t r = s_expr_invoke_any(inst, params, i);
            if (r != SE_CONTINUE && r != SE_HALT) {
                return r;  // Propagate non-normal results
            }
        }
        i = s_expr_skip_param(params, i);
    }
    
    // Return result code if present
    return s_expr_find_result(params, param_count);
}

// SE_FIELD_DISPATCH - dispatch based on field value
// params: [field_ref] [case: value action] [case: value action] ...
static s_expr_result_t se_field_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    // First param: field reference
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("se_field_dispatch: first param must be field_ref");
        return SE_CONTINUE;
    }
    
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) return SE_CONTINUE;
    
    int32_t val = *val_ptr;
    
    // Iterate through cases
    uint16_t idx = s_expr_skip_param(params, 0);
    
    while (idx < param_count) {
        opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            // Case list: (value action)
            uint16_t case_count;
            const s_expr_param_t* case_params = s_expr_brace_contents(params, idx, &case_count);
            
            if (case_count >= 2) {
                int32_t case_val = case_params[0].int_val;
                if (case_val == val) {
                    // Found matching case - execute action
                    uint16_t action_idx = s_expr_skip_param(case_params, 0);
                    if (action_idx < case_count) {
                        return s_expr_invoke_any(inst, case_params, action_idx);
                    }
                    return SE_CONTINUE;
                }
            }
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return SE_CONTINUE;  // No matching case
}

// SE_EVENT_DISPATCH - dispatch based on event_id
// params: [case: event_id action] [case: event_id action] ...
static s_expr_result_t se_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_data;
    
    uint16_t idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            // Case list: (event_id action)
            uint16_t case_count;
            const s_expr_param_t* case_params = s_expr_brace_contents(params, idx, &case_count);
            
            if (case_count >= 2) {
                uint16_t case_event = (uint16_t)case_params[0].int_val;
                if (case_event == event_id) {
                    // Found matching case - execute action
                    uint16_t action_idx = s_expr_skip_param(case_params, 0);
                    if (action_idx < case_count) {
                        return s_expr_invoke_any(inst, case_params, action_idx);
                    }
                    return SE_CONTINUE;
                }
            }
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return SE_CONTINUE;  // No matching case
}

// SE_DISPATCH - generic value dispatch (value passed as first int param)
// params: [value] [case: match action] [case: match action] ...
static s_expr_result_t se_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    // First param: value to dispatch on
    int32_t val = params[0].int_val;
    
    uint16_t idx = s_expr_skip_param(params, 0);
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            // Case list: (match_value action)
            uint16_t case_count;
            const s_expr_param_t* case_params = s_expr_brace_contents(params, idx, &case_count);
            
            if (case_count >= 2) {
                int32_t case_val = case_params[0].int_val;
                if (case_val == val) {
                    // Found matching case - execute action
                    uint16_t action_idx = s_expr_skip_param(case_params, 0);
                    if (action_idx < case_count) {
                        return s_expr_invoke_any(inst, case_params, action_idx);
                    }
                    return SE_CONTINUE;
                }
            }
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return SE_CONTINUE;  // No matching case
}

// ============================================================================
// ONESHOT IMPLEMENTATIONS
// ============================================================================

// SE_LOG - log a message with timestamp
// params: [str_ptr]
static void se_log(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    if (param_count < 1) return;
    
    const char* msg = s_expr_get_string(inst, &params[0]);
    if (!msg) return;
    
    // Get timestamp
    double timestamp = 0.0;
    s_expr_module_t* mod = inst->module;
    if (mod && mod->alloc.get_time) {
        timestamp = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    // Use module's debug callback if available
    if (mod && mod->debug_fn) {
        // Format with timestamp into buffer
        char buf[256];
        snprintf(buf, sizeof(buf), "[%.6f] %s", timestamp, msg);
        mod->debug_fn(inst, buf);
    } else {
        // Fallback to printf if available
        #ifndef S_ENGINE_NO_STDIO
        printf("[SE_LOG %.6f] %s\n", timestamp, msg);
        #endif
    }
}
// ============================================================================
// RESULT CODE FUNCTION IMPLEMENTATIONS
// ============================================================================

// ============================================================================
// RESULT CODE FUNCTION IMPLEMENTATIONS
// ============================================================================

static s_expr_result_t se_return_continue(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_continue: %d\n", SE_CONTINUE);
    return SE_CONTINUE;
}

static s_expr_result_t se_return_halt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_halt: %d\n", SE_HALT);
    return SE_HALT;
}

static s_expr_result_t se_return_terminate(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_terminate: %d\n", SE_TERMINATE);
    return SE_TERMINATE;
}

static s_expr_result_t se_return_reset(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_reset: %d\n", SE_RESET);
    return SE_RESET;
}

static s_expr_result_t se_return_disable(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_disable: %d\n", SE_DISABLE);
    return SE_DISABLE;
}

static s_expr_result_t se_return_skip_continue(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_skip_continue: %d\n", SE_SKIP_CONTINUE);
    return SE_SKIP_CONTINUE;
}

static s_expr_result_t se_return_function_halt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_function_halt: %d\n", SE_FUNCTION_HALT);
    return SE_FUNCTION_HALT;
}

static s_expr_result_t se_return_function_reset(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    //printf("se_return_function_reset: %d\n", SE_FUNCTION_RESET);
    return SE_FUNCTION_RESET;
}

static s_expr_result_t se_return_function_terminate(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    (void)params;
    (void)param_count;
    (void)event_type;
    (void)event_id;
    (void)event_data;
   // printf("se_return_function_terminate: %d\n", SE_FUNCTION_TERMINATE);
    return SE_FUNCTION_TERMINATE;
}