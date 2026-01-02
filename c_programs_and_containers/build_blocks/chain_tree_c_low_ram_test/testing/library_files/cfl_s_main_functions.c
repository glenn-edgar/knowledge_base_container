#include "cfl_s_main_functions.h"
#include "cfl_common_functions.h"
#include <stdio.h>
#include <stdlib.h>

static s_expr_result_t cfl_wait_event_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT: Load counter from param
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("CFL_WAIT_EVENT: Missing count parameter");
            return SE_DISABLE;
        }
        
        uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_WAIT_EVENT: param[0] must be INT or UINT");
            return SE_DISABLE;
        }
        uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
        if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_WAIT_EVENT: param[1] must be INT or UINT");
            return SE_DISABLE;
        }        
        int64_t count = (int64_t)s_expr_param_int(&params[1]);
        s_expr_set_user_u64(inst, (uint64_t)count);
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    if (event_id != s_expr_param_int(&params[0])) {
        return SE_HALT;
    }
    // -------------------------------------------------------------------------
    // TICK: Decrement counter
    // -------------------------------------------------------------------------
    int64_t remaining = (int64_t)s_expr_get_user_u64(inst);
    
    if (remaining <= 1) {
        return SE_DISABLE;
    }
    
    s_expr_set_user_u64(inst, (uint64_t)(remaining - 1));
    return SE_HALT;
}
// ============================================================================
// CFL_TICK_DELAY: Halt for N ticks, then disable
// Params: [0] = tick count (int/uint)
//
// Returns:
//   SE_HALT while counting down
//   SE_DISABLE when complete
// ============================================================================

static s_expr_result_t cfl_tick_delay_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT: Load counter from param
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("CFL_TICK_DELAY: Missing count parameter");
            return SE_DISABLE;
        }
        
        uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_TICK_DELAY: param[0] must be INT or UINT");
            return SE_DISABLE;
        }
        
        int64_t count = (int64_t)s_expr_param_int(&params[0]);
        s_expr_set_user_u64(inst, (uint64_t)count);
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    if (event_id != CFL_TIMER_EVENT) {
        return SE_HALT;
    }
    // -------------------------------------------------------------------------
    // TICK: Decrement counter
    // -------------------------------------------------------------------------
    int64_t remaining = (int64_t)s_expr_get_user_u64(inst);
    
    if (remaining <= 1) {
        return SE_DISABLE;
    }
    
    s_expr_set_user_u64(inst, (uint64_t)(remaining - 1));
    return SE_HALT;
}
// ============================================================================
// CFL_TIME_DELAY - Delay for specified time duration
//
// Usage in DSL:
//   main("CFL_TIME_DELAY", flt(1.5))   -- 1.5 seconds
//
// Behavior:
//   INIT: Calculate deadline = current_time + delay, store in user_data.f64
//   TICK: Check if current_time >= deadline
//     Not reached: return SE_HALT (block pipeline)
//     Reached: return SE_DISABLE (done, deactivate)
// ============================================================================

static s_expr_result_t cfl_time_delay_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        return SE_DISABLE;
    }
    
    // -------------------------------------------------------------------------
    // INIT: Calculate and store end time
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            return SE_DISABLE;
        }
        
        uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
        double delay;
        
        if (type0 == S_EXPR_PARAM_FLOAT) {
            delay = (double)s_expr_param_float(&params[0]);
        } else if (type0 == S_EXPR_PARAM_INT || type0 == S_EXPR_PARAM_UINT) {
            delay = (double)s_expr_param_int(&params[0]);
        } else {
            return SE_DISABLE;
        }
        
        double now = cfl_timer_get_timestamp(runtime_handle->timer_handle);
        s_expr_set_user_f64(inst, now + delay);
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Check if time elapsed
    // -------------------------------------------------------------------------
    double end_time = s_expr_get_user_f64(inst);
    double now = cfl_timer_get_timestamp(runtime_handle->timer_handle);
    
    if (now >= end_time) {
        return SE_DISABLE;
    }
    
    return SE_HALT;
}


// ============================================================================
// CFL_STATE_MACHINE: Execute one branch based on state value
// ============================================================================

// Helper: Find param index for state N
static bool cfl_sm_find_state_idx(
    const s_expr_param_t* params,
    uint16_t param_count,
    int32_t state_num,
    uint16_t* out_idx
) {
    int32_t block_idx = 0;
    for (uint16_t i = 1; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            if (block_idx == state_num) {
                *out_idx = i;
                return true;
            }
            i += params[i].brace_idx + 1;
            block_idx++;
        } else {
            i++;
        }
    }
    return false;
}

// Helper: Count state branches
static uint16_t cfl_sm_count_states(
    const s_expr_param_t* params,
    uint16_t param_count
) {
    uint16_t num_states = 0;
    for (uint16_t i = 1; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            num_states++;
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
    return num_states;
}

static void cfl_sm_init_branch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        uint16_t close_idx = idx + params[idx].brace_idx;
        uint16_t inner_count = (close_idx > idx + 1) ? (close_idx - idx - 1) : 0;
        const s_expr_param_t* inner_params = &params[idx + 1];
        
        // Reset all main functions in branch (clear INITIALIZED, set ACTIVE)
        s_expr_enable_actions(inst, inner_params, inner_count);
    }
}
// Helper: Terminate branch contents
static void cfl_sm_terminate_branch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        uint16_t close_idx = idx + params[idx].brace_idx;
        uint16_t inner_count = (close_idx > idx + 1) ? (close_idx - idx - 1) : 0;
        const s_expr_param_t* inner_params = &params[idx + 1];
        s_expr_restart_actions(inst, inner_params, inner_count);
    }
}

static s_expr_result_t cfl_state_machine_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Validate field_ref param
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_FIELD) {
        return SE_TERMINATE;
    }
    
    // Get state pointer from blackboard
    int32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!state_ptr) {
        return SE_TERMINATE;
    }
    
    uint16_t num_states = cfl_sm_count_states(params, param_count);
    
    // -------------------------------------------------------------------------
    // INIT: Store initial state, validate
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        int32_t initial_state = *state_ptr;
        
        if (initial_state < 0 || initial_state >= (int32_t)num_states) {
            EXCEPTION("CFL_STATE_MACHINE: invalid initial state");
            return SE_TERMINATE;
        }
        
        s_expr_set_state(inst, (uint8_t)initial_state);
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Clean up current branch
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        uint8_t prev_state = s_expr_get_state(inst);
        uint16_t prev_idx;
        
        if (cfl_sm_find_state_idx(params, param_count, prev_state, &prev_idx)) {
            cfl_sm_terminate_branch(inst, params, prev_idx);
        }
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Handle state changes, execute current branch
    // -------------------------------------------------------------------------
    int32_t current_state = *state_ptr;
    uint8_t prev_state = s_expr_get_state(inst);
    
    if (current_state != (int32_t)prev_state) {
        // Validate new state
        if (current_state < 0 || current_state >= (int32_t)num_states) {
            return SE_TERMINATE;
        }
        
        // Terminate old branch
        uint16_t prev_idx;
        if (cfl_sm_find_state_idx(params, param_count, prev_state, &prev_idx)) {
            cfl_sm_terminate_branch(inst, params, prev_idx);
        }
        
        // Initialize new branch (reset flags so they get INIT event)
        uint16_t new_idx;
        if (cfl_sm_find_state_idx(params, param_count, current_state, &new_idx)) {
            cfl_sm_init_branch(inst, params, new_idx);  // <-- ADD THIS
        }
        
        // Update stored state
        s_expr_set_state(inst, (uint8_t)current_state);
    }
    
    // Find and execute current state branch
    uint16_t current_idx;
    if (!cfl_sm_find_state_idx(params, param_count, current_state, &current_idx)) {
        EXCEPTION("CFL_STATE_MACHINE: invalid current state");
        return SE_TERMINATE;
    }
    
    return s_expr_invoke_any(inst, params, current_idx);
}

// ============================================================================
// CFL_STATE_ACTIONS: Execute all actions in sequence, return code from last int
// Params: list of actions (callables), optionally ending with int return code
//
// DSL usage:
//   m_call("CFL_STATE_ACTIONS")
//       m_call("action_1") ... end_call(...)
//       m_call("action_2") ... end_call(...)
//       int(SE_CONTINUE)  -- return code
//   end_call(...)
// ============================================================================

static s_expr_result_t cfl_state_actions_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT: Nothing special
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Clean up child actions
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_restart_actions(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Execute all actions in sequence
    // -------------------------------------------------------------------------
    for (uint16_t i = 0; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            s_expr_result_t r = s_expr_invoke_any(inst, params, i);
            
            switch (r) {
                case SE_DISABLE:
                case SE_CONTINUE:
                    break;  // keep going
                    
                case SE_HALT:
                case SE_TERMINATE:
                case SE_RESET:
                case SE_FUNCTION_TERMINATE:
                case SE_SKIP_CONTINUE:
                case SE_FUNCTION_HALT:
                case SE_FUNCTION_RESET:
                    return r;  // propagate up (no restart yet)
                    
                default:
                    // EXCEPTION("CFL_STATE_ACTIONS: invalid result");
                    break;
            }
            
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
    
   // In cfl_state_actions_main - find return code
for (int16_t i = param_count - 1; i >= 0; i--) {
    uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
    
    // Look for RESULT type first
    if (opcode == S_EXPR_PARAM_RESULT) {
        return (s_expr_result_t)params[i].int_val;
    }
}

// Fallback to last INT/UINT for backwards compatibility
for (int16_t i = param_count - 1; i >= 0; i--) {
    uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
        return (s_expr_result_t)s_expr_param_int(&params[i]);
    }
}

return SE_CONTINUE;
    
    return SE_CONTINUE;  // No restart - still in this state
}

// Store action_idx directly instead of case_num for faster lookup
static s_expr_result_t cfl_dispatch_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    // Use user_flags to store previous action_idx (16 bits)
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    // -------------------------------------------------------------------------
    // TERMINATE: Clean up current branch
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            cfl_dispatch_terminate_branch(inst, params, prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // INIT: Clear previous action index
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Dispatch based on event_id
    // -------------------------------------------------------------------------
    int32_t key = (int32_t)event_id;
    uint16_t default_action_idx = 0;
    uint16_t match_action_idx = 0;
    
    // Scan for matching case
    for (uint16_t i = 0; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            uint16_t case_val_idx = i + 1;
            uint8_t val_opcode = params[case_val_idx].type & S_EXPR_OPCODE_MASK;
            
            if (val_opcode == S_EXPR_PARAM_INT || val_opcode == S_EXPR_PARAM_UINT) {
                int32_t case_val = (int32_t)s_expr_param_int(&params[case_val_idx]);
                uint16_t action_idx = i + 2;
                
                if (case_val == 0) {
                    default_action_idx = action_idx;
                } else if (case_val == key) {
                    match_action_idx = action_idx;
                    break;  // Found match, stop scanning
                }
            }
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
    
    // Use match or default
    uint16_t action_idx = match_action_idx ? match_action_idx : default_action_idx;
    if (action_idx == 0) {
        return SE_CONTINUE;  // No case matched
    }
    
    // Handle branch transition
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            cfl_dispatch_terminate_branch(inst, params, prev_action_idx);
        }
        cfl_dispatch_init_branch(inst, params, action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    return s_expr_invoke_any(inst, params, action_idx);
}
// ============================================================================
// CFL_EVENT_DISPATCH: Switch/case on event_id
// Params: list of cases, each case is: list(int(event_val), action)
//         event_val = 0 is the default case
//
// DSL usage:
//   m_call("CFL_EVENT_DISPATCH")
//       list() int(CFL_TIMER_EVENT) m_call("handle_timer") end_call(...) end_list()
//       list() int(CFL_BUTTON_EVENT) m_call("handle_button") end_call(...) end_list()
//       list() int(0) m_call("handle_default") end_call(...) end_list()  -- default
//   end_call(...)
// ============================================================================

static s_expr_result_t cfl_event_dispatch_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT / TERMINATE: Nothing special
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Dispatch based on event_id
    // -------------------------------------------------------------------------
    int32_t key = (int32_t)event_id;
    uint16_t default_idx = 0;
    
    // Scan for matching case: list(int(event_val), action)
    for (uint16_t i = 0; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            uint16_t case_val_idx = i + 1;
            uint8_t val_opcode = params[case_val_idx].type & S_EXPR_OPCODE_MASK;
            
            if (val_opcode == S_EXPR_PARAM_INT || val_opcode == S_EXPR_PARAM_UINT) {
                int32_t case_val = (int32_t)s_expr_param_int(&params[case_val_idx]);
                uint16_t action_idx = i + 2;
                
                // 0 = default case
                if (case_val == 0) {
                    default_idx = action_idx;
                }
                // Match found
                else if (case_val == key) {
                    return s_expr_invoke_any(inst, params, action_idx);
                }
            }
            
            // Skip to end of this list
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
    
    // No match - try default
    if (default_idx > 0) {
        return s_expr_invoke_any(inst, params, default_idx);
    }
    
    return SE_CONTINUE;
}




 
// ============================================================================
// DF_CONTROL: if (pred) then_action else else_action
// Params: [0] = predicate, [1] = then_action, [2] = else_action
// ============================================================================

// ============================================================================
// CFL_TRIGGER_ON_CHANGE: Edge-triggered if-then-else
// Params: [0] = initial_state (int: 0=inactive, non-zero=active)
//         [1] = predicate
//         [2] = then_action (called when pred becomes true)
//         [3] = else_action (called when pred becomes false)
//
// Behavior: Tracks state transitions, fires actions only on change
// ============================================================================

#define CFL_TRIGGER_FLAG_ACTIVE  0x80  // bit 7: currently in active state

// ============================================================================
// Helper: Restart a single callable (terminate + re-enable)
// ============================================================================




static void cfl_restart_single_action(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        uint16_t close_idx = idx + params[idx].brace_idx;
        uint16_t inner_count = (close_idx > idx + 1) ? (close_idx - idx - 1) : 0;
        const s_expr_param_t* inner_params = &params[idx + 1];
        s_expr_restart_actions(inst, inner_params, inner_count);
    }
}
// ============================================================================
// CFL_TRIGGER_ON_CHANGE: Edge-triggered if-then-else with branch restart
// Params: [0] = initial_state (int: 0=inactive, non-zero=active)
//         [1] = predicate
//         [2] = then_action (restarted + invoked when pred becomes true)
//         [3] = else_action (invoked when pred becomes false)
// ============================================================================

#define CFL_TRIGGER_FLAG_ACTIVE  0x80  // bit 7: currently in active state

static s_expr_result_t cfl_trigger_on_change_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Calculate parameter positions once
    const uint16_t init_idx = 0;
    const uint16_t pred_idx = 1;
    const uint16_t then_idx = s_expr_skip_param(params, pred_idx);
    const uint16_t else_idx = s_expr_skip_param(params, then_idx);
    
    // -------------------------------------------------------------------------
    // INIT: Validate parameters and set initial state
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        // Validate param count
        if (s_expr_count_params(params, param_count) < 4) {
            EXCEPTION("CFL_TRIGGER_ON_CHANGE requires 4 parameters: initial_state, pred, then, else");
            return SE_DISABLE;
        }
        
        // Validate initial_state type
        uint8_t type0 = params[init_idx].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_TRIGGER_ON_CHANGE param[0] must be INT or UINT");
            return SE_DISABLE;
        }
        
        // Validate predicate
        if (!s_expr_param_is_predicate(&params[pred_idx])) {
            EXCEPTION("CFL_TRIGGER_ON_CHANGE param[1] must be predicate");
            return SE_DISABLE;
        }
        
        // Validate then_action
        if (!s_expr_param_is_action(&params[then_idx])) {
            EXCEPTION("CFL_TRIGGER_ON_CHANGE param[2] must be action");
            return SE_DISABLE;
        }
        
        // Validate else_action
        if (!s_expr_param_is_action(&params[else_idx])) {
            EXCEPTION("CFL_TRIGGER_ON_CHANGE param[3] must be action");
            return SE_DISABLE;
        }
        
        // Set initial state from param[0]
        int32_t initial_state = (int32_t)s_expr_param_int(&params[init_idx]);
        if (initial_state != 0) {
            s_expr_set_user_flags(inst, CFL_TRIGGER_FLAG_ACTIVE);
        } else {
            s_expr_set_user_flags(inst, 0);
        }
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Cleanup
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Evaluate predicate and dispatch on change
    // -------------------------------------------------------------------------
    bool pred_result = s_expr_invoke_pred(inst, params, pred_idx);
    uint8_t flags = s_expr_get_user_flags(inst);
    bool was_active = (flags & CFL_TRIGGER_FLAG_ACTIVE) != 0;
    
    if (pred_result) {
        // Condition true - activate if not already active
        if (!was_active) {
            // Restart then branch (terminate + re-enable for fresh INIT)
            cfl_restart_single_action(inst, params, then_idx);
            // Now invoke it (will get INIT event)
            s_expr_invoke_any(inst, params, then_idx);
            s_expr_set_user_flags(inst, flags | CFL_TRIGGER_FLAG_ACTIVE);
        }
    } else {
        // Condition false - deactivate if was active
        if (was_active) {
            cfl_restart_single_action(inst, params, else_idx);
            s_expr_invoke_any(inst, params, else_idx);
            s_expr_set_user_flags(inst, flags & ~CFL_TRIGGER_FLAG_ACTIVE);
        }
    }
    
    return SE_CONTINUE;
}
// ============================================================================
// CFL_WAIT_CHILD_DISABLED: Wait until child node is disabled, then disable self
// Params: [0] = child_node_index (int/uint)
//
// Returns:
//   SE_HALT while child is enabled
//   SE_DISABLE when child becomes disabled
// ============================================================================

static s_expr_result_t cfl_wait_child_disabled_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // -------------------------------------------------------------------------
    // INIT: Validate parameters
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("CFL_WAIT_CHILD_DISABLED: requires child_node_index");
            return SE_DISABLE;
        }
        
        uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_WAIT_CHILD_DISABLED: param[0] must be INT or UINT");
            return SE_DISABLE;
        }
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Check if child is still enabled
    // -------------------------------------------------------------------------
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_WAIT_CHILD_DISABLED: no runtime handle");
        return SE_TERMINATE;
    }
    
    uint16_t child_node_index = (uint16_t)s_expr_param_uint(&params[0]);
    
    if (cfl_child_is_enabled(runtime_handle, inst->ct_node_id, child_node_index)) {
        return SE_HALT;  // Still waiting
    }
    
    return SE_DISABLE;  // Child disabled, we're done
}

// ============================================================================
// CFL_S_IF_THEN_ELSE: Conditional execution based on predicate
// Params: [0] = predicate (bool)
//         [1] = then_action (executed if predicate true)
//         [2] = else_action (executed if predicate false)
//
// Evaluates predicate each tick and executes appropriate branch
// ============================================================================

static s_expr_result_t cfl_s_if_then_else_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Calculate parameter positions once
    const uint16_t pred_idx = 0;
    const uint16_t then_idx = s_expr_skip_param(params, pred_idx);
    const uint16_t else_idx = s_expr_skip_param(params, then_idx);
    
    // -------------------------------------------------------------------------
    // INIT: Validate parameters
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_INIT) {
        if (s_expr_count_params(params, param_count) < 3) {
            EXCEPTION("CFL_S_IF_THEN_ELSE requires 3 parameters: pred, then, else");
            return SE_DISABLE;
        }
        
        if (!s_expr_param_is_predicate(&params[pred_idx])) {
            EXCEPTION("CFL_S_IF_THEN_ELSE param[0] must be predicate");
            return SE_DISABLE;
        }
        
        if (!s_expr_param_is_action(&params[then_idx])) {
            EXCEPTION("CFL_S_IF_THEN_ELSE param[1] must be action");
            return SE_DISABLE;
        }
        
        if (!s_expr_param_is_action(&params[else_idx])) {
            EXCEPTION("CFL_S_IF_THEN_ELSE param[2] must be action");
            return SE_DISABLE;
        }
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Evaluate predicate and execute appropriate branch
    // -------------------------------------------------------------------------
    bool pred_result = s_expr_invoke_pred(inst, params, pred_idx);
    
    if (pred_result) {
        return s_expr_invoke_any(inst, params, then_idx);
    } else {
        return s_expr_invoke_any(inst, params, else_idx);
    }
}
// ============================================================================
// SYSTEM MAIN ENTRIES (named for readability)
// ============================================================================

static const s_expr_fn_entry_named_t system_main_entries_named[] = {
    { "CFL_TICK_DELAY",           (void*)cfl_tick_delay_main },
    { "CFL_TIME_DELAY",           (void*)cfl_time_delay_main },
    { "CFL_STATE_ACTIONS",        (void*)cfl_state_actions_main },
    { "CFL_STATE_MACHINE",        (void*)cfl_state_machine_main },
    { "CFL_WAIT_CHILD_DISABLED",  (void*)cfl_wait_child_disabled_main },
    { "CFL_DISPATCH",             (void*)cfl_dispatch_main },
    { "CFL_EVENT_DISPATCH",       (void*)cfl_event_dispatch_main },
    { "CFL_TRIGGER_ON_CHANGE",    (void*)cfl_trigger_on_change_main },
    { "CFL_S_IF_THEN_ELSE",       (void*)cfl_s_if_then_else_main },
    { "CFL_WAIT_EVENT",           (void*)cfl_wait_event_main },
    // Add more system main functions here
};

// ============================================================================
// HASH TABLE (populated at runtime)
// ============================================================================

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

static s_expr_fn_entry_t system_main_entries[ARRAY_COUNT(system_main_entries_named)];
static s_expr_fn_table_t system_main_table;

// ============================================================================
// LOAD FUNCTION
// ============================================================================

static bool system_main_initialized = false;

void cfl_load_main_s_functions(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        printf("ERROR: cfl_load_main_s_functions called with invalid handle\n");
        return;
    }
    
    // Initialize hash table once
    if (!system_main_initialized) {
        s_expr_build_fn_table(
            system_main_entries_named,
            system_main_entries,
            ARRAY_COUNT(system_main_entries_named)
        );
        
        system_main_table.entries = system_main_entries;
        system_main_table.count = ARRAY_COUNT(system_main_entries);
        
        system_main_initialized = true;
    }
    
    // Register to all modules
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_register_main(modules[i], &system_main_table);
    }
}