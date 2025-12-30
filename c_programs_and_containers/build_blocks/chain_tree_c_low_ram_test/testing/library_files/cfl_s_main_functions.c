#include "cfl_s_main_functions.h"
#include "cfl_common_functions.h"
#include <stdio.h>
#include <stdlib.h>
static s_expr_result_t cfl_enable_children_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
     (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;

    if (event_id == S_EXPR_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {

        return SE_CONTINUE;
    }
    
    cfl_enable_all_children(runtime_handle,inst->ct_node_id);
    return SE_CONTINUE;
}

static s_expr_result_t cfl_disable_children_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
     (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;

    if (event_id == S_EXPR_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {

        return SE_CONTINUE;
    }

    cfl_disable_all_children(runtime_handle,inst->ct_node_id);
    return SE_CONTINUE;
}


static s_expr_result_t cfl_tick_delay_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    // Use high bit of user_data to track initialization
    const uint64_t INIT_FLAG = (uint64_t)1 << 63;
    
    if (!(state->user_data.u64 & INIT_FLAG)) {
        // Not initialized - set count and flag
        uint64_t count = (uint64_t)s_expr_param_get_int(&params[0]);
        state->user_data.u64 = count | INIT_FLAG;
        return SE_HALT;
    }
    if(event_id != CFL_TIMER_EVENT)
    {
        return SE_HALT;
    }
    // Get count (mask off init flag)
    uint64_t count = state->user_data.u64 & ~INIT_FLAG;
    
    if (count > 0) {
        state->user_data.u64 = (count - 1) | INIT_FLAG;
        return SE_HALT;
    }
    
    // Done - clear init flag for next state
    state->user_data.u64 = 0;
    return SE_DISABLE;
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
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)event_data;
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)inst->handle;
    
    // -------------------------------------------------------------------------
    // INIT: Calculate and store deadline
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("CFL_TIME_DELAY: Missing delay parameter");
            return SE_DISABLE;
        }
        
        double delay;
        uint8_t type = params[0].type;
        
        if (type == S_EXPR_PARAM_FLOAT) {
            delay = (double)s_expr_param_get_float(&params[0]);
        } else if (type == S_EXPR_PARAM_INT) {
            delay = (double)s_expr_param_get_int(&params[0]);
        } else if (type == S_EXPR_PARAM_UINT) {
            delay = (double)s_expr_param_get_uint(&params[0]);
        } else {
            EXCEPTION("CFL_TIME_DELAY: Parameter must be FLOAT, INT, or UINT");
            return SE_DISABLE;
        }
        
        // Store deadline (current time + delay)
        double now = cfl_timer_get_timestamp(runtime->timer_handle);
        state->user_data.f64 = now + delay;
        
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Nothing to clean up
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Check if deadline reached
    // -------------------------------------------------------------------------
    double now = cfl_timer_get_timestamp(runtime->timer_handle);
    double deadline = state->user_data.f64;
    
    if (now >= deadline) {
        return SE_DISABLE;  // Done - deactivate node
    }
    
    return SE_FUNCTION_HALT;  // Still waiting
}

static s_expr_result_t cfl_state_machine_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_id; (void)event_data;
    
    int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!slot_ptr) return SE_TERMINATE;
    
    int current_state = *slot_ptr;
    int block_idx = 0;
    for (uint8_t i = 1; i < param_count; i++) {
        
        if (params[i].type == S_EXPR_PARAM_OPEN_CALL) {
            
            
            if (block_idx == current_state) {
                
                s_expr_result_t r = s_expr_invoke_any(inst, node, state, params, i);
                
                
                
                return r;
            }
            i += params[i].brace_idx;
            
            block_idx++;
        }
    }
    
    
    return SE_TERMINATE;
}
static s_expr_result_t cfl_state_actions_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_id; (void)event_data;
    
   
    
    bool oneshots_done = (state->flags & S_EXPR_NODE_FLAGS_USER) != 0;
    
    if (!oneshots_done) {
        for (uint8_t i = 0; i < param_count; i++) {
            if (params[i].type == S_EXPR_PARAM_OPEN_CALL &&
                params[i + 1].type == S_EXPR_PARAM_ONESHOT) {
                
                s_expr_invoke_any(inst, node, state, params, i);
                i += params[i].brace_idx;
            }
        }
        state->flags |= S_EXPR_NODE_FLAGS_USER;
    }
    
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_OPEN_CALL &&
            params[i + 1].type == S_EXPR_PARAM_MAIN) {
            
            s_expr_result_t r = s_expr_invoke_any(inst, node, state, params, i);
          
            if (r == SE_HALT) return SE_HALT;
            i += params[i].brace_idx;
        }
    }
    
  
    for (int i = param_count - 1; i >= 0; i--) {
        if (params[i].type == S_EXPR_PARAM_INT || params[i].type == S_EXPR_PARAM_UINT) {
            s_expr_result_t rc = (s_expr_result_t)s_expr_param_get_int(&params[i]);
         
            state->flags &= ~S_EXPR_NODE_FLAGS_USER;  // Clear for next state
            return rc;
        }
    }
    
   
    state->flags &= ~S_EXPR_NODE_FLAGS_USER;
    return SE_CONTINUE;
}

static s_expr_result_t s_internal_event(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    if(event_id != S_EXPR_EVENT_INIT){
        cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
        if(param_count != 2){
            EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
            return SE_TERMINATE;
        }
        if((params[0].type != S_EXPR_PARAM_INT) && (params[0].type != S_EXPR_PARAM_UINT)){
            EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
            return SE_TERMINATE;
        }
        if((params[1].type != S_EXPR_PARAM_INT) && (params[1].type != S_EXPR_PARAM_UINT)){
            EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
            return SE_TERMINATE;
        }
        
        cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, (unsigned)params[0].i, (cfl_int_t)params[1].i);
        return SE_CONTINUE;
    }
    return SE_CONTINUE;
}
static const s_expr_fn_entry_t user_main_entries[] = {
    { "CFL_ENABLE_CHILDREN", (void*)cfl_enable_children_main },
    { "CFL_DISABLE_CHILDREN", (void*)cfl_disable_children_main },
    { "CFL_TICK_DELAY", (void*)cfl_tick_delay_main },
    { "CFL_TIME_DELAY",(void*)cfl_time_delay_main },
    { "CFL_STATE_ACTIONS",(void*)cfl_state_actions_main },
    { "CFL_STATE_MACHINE",(void*)cfl_state_machine_main },
    { "CFL_INTERNAL_EVENT",(void*)s_internal_event },
    // Add more user main functions here
};


static const s_expr_fn_table_t user_main_table = {
    .entries = user_main_entries,
    .count = sizeof(user_main_entries) / sizeof(user_main_entries[0])
};

// ============================================================================
// LOAD FUNCTION
// ============================================================================

void cfl_load_main_s_functions(cfl_runtime_handle_t* handle) {
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_modules;
    
    if (!mod) {
        printf("ERROR: load_user_s_functions called before module init\n");
        return;
    }
    
    
    s_expr_module_load_main(mod, &user_main_table);
    
    
           
}