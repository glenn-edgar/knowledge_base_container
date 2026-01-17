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

// Result code functions
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
// UNIFIED BODY EXECUTION HELPER
// ============================================================================

// Execute a body: run oneshots, run main functions, find result
// This handles mixed oneshot + main function sequences consistently
static s_expr_result_t s_expr_execute_body(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
) {
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_oneshot(&params[i])) {
            // Execute oneshot (fire and forget)
            s_expr_invoke_oneshot(inst, params, i);
        }
        else if (s_expr_param_is_main(&params[i])) {
            // Execute main function
            s_expr_result_t r = s_expr_invoke_main(inst, params, i);
            // Continue on CONTINUE or DISABLE, stop on anything else
            if (r != SE_CONTINUE && r != SE_DISABLE) {
                return r;
            }
        }
        // Skip predicates, literals, field refs, etc.
        i = s_expr_skip_param(params, i);
    }
    
    return s_expr_find_result(params, param_count);
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

// SE_PIPELINE - sequence of actions
static s_expr_result_t se_pipeline(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    // Pipeline doesn't process during INIT/TERMINATE
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    return s_expr_execute_body(inst, params, param_count);
}

// SE_TICK_DELAY - wait for N ticks
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
        ticks++;  // Probably established on a tick event
        s_expr_set_u64(inst, (uint64_t)ticks);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // TICK event - decrement counter
    uint64_t remaining = s_expr_get_u64(inst);
    
    if (remaining > 0) {
        remaining--;
        s_expr_set_u64(inst, remaining);
        return SE_HALT;
    }
    
    return SE_DISABLE;
}

// SE_TIME_DELAY - wait for N seconds
// params: [seconds as float]
// Uses f64 slot for target time
static s_expr_result_t se_time_delay(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    s_expr_module_t* mod = inst->module;
    
    if (event_type == SE_EVENT_INIT) {
        double seconds = (param_count > 0) ? (double)params[0].float_val : 0.0;
        
        if (seconds <= 0.0) {
            return SE_CONTINUE;  // No delay
        }
        
        // Get current time and compute target time
        double now = 0.0;
        if (mod && mod->alloc.get_time) {
            now = mod->alloc.get_time(mod->alloc.ctx);
        }
        
        double target_time = now + seconds;
        s_expr_set_f64(inst, target_time);
        
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_id != SE_EVENT_TICK) {
        return SE_HALT;
    }
    
    // TICK event - check if target time reached
    double target_time = s_expr_get_f64(inst);
    
    double now = 0.0;
    if (mod && mod->alloc.get_time) {
        now = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (now >= target_time) {
        return SE_DISABLE;  // Delay complete
    }
    
    return SE_HALT;  // Still waiting
}

// SE_WAIT_EVENT - wait for specific event N times
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
        
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Check if event matches
    uint64_t state = s_expr_get_u64(inst);
    uint32_t target_event = (uint32_t)(state >> 32);
    uint32_t remaining = (uint32_t)(state & 0xFFFFFFFF);
    
    if (remaining <= 0) {
        return SE_DISABLE;
    }
    
    if (event_id == target_event) {
        remaining--;
        state = ((uint64_t)target_event << 32) | remaining;
        s_expr_set_u64(inst, state);
        
        if (remaining == 0) {
            return SE_DISABLE;
        }
    }
    
    return SE_HALT;
}

// SE_NOP - do nothing, return DISABLE
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
    return SE_DISABLE;
}

// SE_IF_THEN_ELSE - conditional execution
// params: [predicate] [then_body...] [else_body...]
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
    uint16_t then_end = s_expr_skip_param(params, idx);
    idx = then_end;
    
    // Third: else branch (optional)
    uint16_t else_idx = idx;
    uint16_t else_end = (idx < param_count) ? s_expr_skip_param(params, idx) : idx;
    bool has_else = (idx < param_count);
    
    if (condition) {
        return s_expr_execute_body(inst, &params[then_idx], then_end - then_idx);
    } else if (has_else) {
        return s_expr_execute_body(inst, &params[else_idx], else_end - else_idx);
    }
    
    return SE_CONTINUE;
}

// SE_TRIGGER_ON_CHANGE - execute action on state transition
// params: [initial_state (int)] [predicate] [rising_action] [falling_action]
// Uses node state to track previous predicate value
// Restarts actions on transitions so oneshots fire each time
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
    
    // Calculate parameter positions once
    const uint16_t init_idx = 0;
    const uint16_t pred_idx = 1;
    const uint16_t rising_idx = s_expr_skip_param(params, pred_idx);
    const uint16_t falling_idx = s_expr_skip_param(params, rising_idx);
    const bool has_falling = (falling_idx < param_count);
    
    // -------------------------------------------------------------------------
    // INIT: Validate and set initial state
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        uint8_t type0 = params[init_idx].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("se_trigger_on_change: param[0] must be INT or UINT");
            return SE_CONTINUE;
        }
        
        if (!s_expr_param_is_predicate(&params[pred_idx])) {
            EXCEPTION("se_trigger_on_change: param[1] must be predicate");
            return SE_CONTINUE;
        }
        
        int32_t initial_state = (int32_t)params[init_idx].int_val;
        s_expr_set_state(inst, initial_state ? 1 : 0);
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Evaluate predicate and dispatch on change
    // -------------------------------------------------------------------------
    bool current = s_expr_invoke_pred(inst, params, pred_idx);
    uint8_t prev = s_expr_get_state(inst);
    
    // Detect transitions
    bool rising = (prev == 0 && current);
    bool falling = (prev != 0 && !current);
    
    // Update state
    s_expr_set_state(inst, current ? 1 : 0);
    
    if (rising) {
        // Restart action so oneshots fire again
        uint16_t rising_end = s_expr_skip_param(params, rising_idx);
        s_expr_restart_actions(inst, &params[rising_idx], rising_end - rising_idx);
        return s_expr_execute_body(inst, &params[rising_idx], rising_end - rising_idx);
    } 
    else if (falling && has_falling) {
        // Restart action so oneshots fire again
        uint16_t falling_end = s_expr_skip_param(params, falling_idx);
        s_expr_restart_actions(inst, &params[falling_idx], falling_end - falling_idx);
        return s_expr_execute_body(inst, &params[falling_idx], falling_end - falling_idx);
    }
    
    return SE_CONTINUE;
}
static s_expr_result_t se_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    // First param: field reference for state
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("se_state_machine: first param must be field_ref");
        return SE_CONTINUE;
    }
    
    // TERMINATE: Clean up current state
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    // INIT: Validate and reset state tracking
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // TICK: Read state and dispatch
    int32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!state_ptr) return SE_CONTINUE;
    
    int32_t state = *state_ptr;
    if (state < 0) return SE_CONTINUE;
    
    // Find state handler at index 'state' (skip field ref)
    uint16_t idx = s_expr_skip_param(params, 0);
    int32_t state_idx = 0;
    uint16_t action_idx = 0;
    
    while (idx < param_count) {
        if (state_idx == state) {
            action_idx = idx;
            break;
        }
        idx = s_expr_skip_param(params, idx);
        state_idx++;
    }
    
    if (action_idx == 0) {
        // State out of range - terminate previous if any
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    // Handle state transition
    if (action_idx != prev_action_idx) {
        // Terminate previous state
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        // Enable new state (will get INIT on first invoke)
        s_expr_enable_actions(inst, &params[action_idx],
            s_expr_skip_param(params, action_idx) - action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // Execute current state handler
    uint16_t action_end = s_expr_skip_param(params, action_idx);
    return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
}

// SE_STATE_ACTIONS - sequence within a state (like pipeline)
static s_expr_result_t se_state_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    return s_expr_execute_body(inst, params, param_count);
}

static s_expr_result_t se_field_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    (void)event_data;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    // TERMINATE: Clean up current branch
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx], 
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    // INIT: Validate field parameter
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("se_field_dispatch: missing field parameter");
            return SE_CONTINUE;
        }
        uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            EXCEPTION("se_field_dispatch: first param must be field_ref");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0);  // No active branch yet
        return SE_CONTINUE;
    }
    
    // TICK: Read field value and dispatch
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) return SE_CONTINUE;
    
    int32_t val = *val_ptr;
    
    // Scan: [field] [int, action] [int, action] ...
    uint16_t idx = s_expr_skip_param(params, 0);
    uint16_t action_idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_val = (int32_t)params[idx].int_val;
            uint16_t this_action_idx = idx + 1;
            
            if (case_val == val && this_action_idx < param_count) {
                action_idx = this_action_idx;
                break;
            }
            
            idx = s_expr_skip_param(params, idx);  // Skip INT
            idx = s_expr_skip_param(params, idx);  // Skip action
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    if (action_idx == 0) {
        // No match - terminate previous branch if any
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    // Handle branch transition
    if (action_idx != prev_action_idx) {
        // Terminate previous branch
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        // Enable new branch (will get INIT on first invoke)
        s_expr_enable_actions(inst, &params[action_idx],
            s_expr_skip_param(params, action_idx) - action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // Execute current branch
    uint16_t action_end = s_expr_skip_param(params, action_idx);
    return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
}

static s_expr_result_t se_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Scan: [int, action] [int, action] ...
    uint16_t idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_event = (int32_t)params[idx].int_val;
            uint16_t action_idx = idx + 1;
            
            if (case_event == (int32_t)event_id && action_idx < param_count) {
                uint16_t action_end = s_expr_skip_param(params, action_idx);
                return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
            }
            
            idx = s_expr_skip_param(params, idx);  // Skip INT
            idx = s_expr_skip_param(params, idx);  // Skip action
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    return SE_CONTINUE;
}
// SE_DISPATCH - generic value dispatch
// params: [value] [case: (match_value body...)] [case: (match_value body...)] ...
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
            // Case list: (match_value body...)
            uint16_t case_count;
            const s_expr_param_t* case_params = s_expr_brace_contents(params, idx, &case_count);
            
            if (case_count >= 2) {
                int32_t case_val = case_params[0].int_val;
                if (case_val == val) {
                    // Execute body (everything after the match value)
                    uint16_t body_start = s_expr_skip_param(case_params, 0);
                    return s_expr_execute_body(inst, &case_params[body_start], case_count - body_start);
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
   
    if (param_count < 1){
        EXCEPTION("SE_LOG: param_count < 1");
    }
    
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
        snprintf(buf, sizeof(buf), " Time Stamp:[%.8f] %s", timestamp, msg);
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
    return SE_FUNCTION_TERMINATE;
}

// Test function to dump list structure
static s_expr_result_t se_dump_list(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    printf("[dump_list] param_count=%d\n", param_count);
    
    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            uint16_t brace_idx = params[i].brace_idx;
            printf("  [%d] OPEN brace_idx=%d (close at %d)\n", i, brace_idx, i + brace_idx);
        } else if (opcode == S_EXPR_PARAM_CLOSE) {
            printf("  [%d] CLOSE\n", i);
        } else if (opcode == S_EXPR_PARAM_INT) {
            printf("  [%d] INT %d\n", i, (int)params[i].int_val);
        } else if (opcode == S_EXPR_PARAM_STR_IDX) {
            const char* s = s_expr_get_string(inst, &params[i]);
            printf("  [%d] STR \"%s\"\n", i, s ? s : "?");
        } else {
            printf("  [%d] opcode=0x%02X\n", i, opcode);
        }
    }
    
    return SE_CONTINUE;
}