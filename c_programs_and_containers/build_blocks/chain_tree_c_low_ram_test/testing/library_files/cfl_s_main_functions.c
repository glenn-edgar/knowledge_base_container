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
    
    // Use high bit of i32 as init flag
    #define TICK_DELAY_INIT_FLAG (1 << 30)
    
    // -------------------------------------------------------------------------
    // INIT: Reset state (flow node context)
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_INIT) {
        state->user_data.i32 = 0;
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Clean up
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        state->user_data.i32 = 0;
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // First call detection (works for both flow node AND m_call)
    // -------------------------------------------------------------------------
    bool initialized = (state->user_data.i32 & TICK_DELAY_INIT_FLAG) != 0;
    
    if (!initialized) {
        if (param_count < 1) {
            EXCEPTION("CFL_TICK_DELAY: Missing count parameter");
            return SE_DISABLE;
        }
        
        int32_t count = (int32_t)s_expr_param_get_int(&params[0]);
        state->user_data.i32 = count | TICK_DELAY_INIT_FLAG;
        return SE_HALT;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Decrement counter
    // -------------------------------------------------------------------------
    int32_t remaining = state->user_data.i32 & ~TICK_DELAY_INIT_FLAG;
    
    if (remaining <= 1) {
        state->user_data.i32 = 0;  // Reset for next use
        return SE_DISABLE;
    }
    
    state->user_data.i32 = (remaining - 1) | TICK_DELAY_INIT_FLAG;
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

static s_expr_result_t cfl_tick_delay_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)event_data;
    
    // Use high bit of i64 as init flag
    #define TICK_DELAY_INIT_FLAG (1LL << 62)
    
    // -------------------------------------------------------------------------
    // INIT: Reset state (flow node context)
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_INIT) {
      
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Clean up
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        state->user_data.i64 = 0;
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // First call detection (works for both flow node AND m_call)
    // -------------------------------------------------------------------------
    bool initialized = (state->user_data.i64 & TICK_DELAY_INIT_FLAG) != 0;
    
    if (!initialized) {
        if (param_count < 1) {
            EXCEPTION("CFL_TICK_DELAY: Missing count parameter");
            return SE_DISABLE;
        }
        
        int64_t count = (int64_t)s_expr_param_get_int(&params[0]);
        state->user_data.i64 = count | TICK_DELAY_INIT_FLAG;
        return SE_HALT;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Decrement counter
    // -------------------------------------------------------------------------
    int64_t remaining = state->user_data.i64 & ~TICK_DELAY_INIT_FLAG;
    
    if (remaining <= 1) {
        state->user_data.i64 = 0;
        return SE_DISABLE;
    }
    
    state->user_data.i64 = (remaining - 1) | TICK_DELAY_INIT_FLAG;
    return SE_HALT;
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
    bool oneshots_done = (state->flags & S_EXPR_NODE_FLAGS_USER) != 0;
    
    printf(">>> STATE_ACTIONS: oneshots_done=%d\n", oneshots_done);
    
    // Execute oneshots once per state entry
    if (!oneshots_done) {
        for (uint8_t i = 0; i < param_count; i++) {
            if (params[i].type == S_EXPR_PARAM_OPEN_CALL &&
                params[i + 1].type == S_EXPR_PARAM_ONESHOT) {
                printf(">>> STATE_ACTIONS: calling oneshot at i=%d\n", i);
                s_expr_invoke_any(inst, node, state, params, i);
                i += params[i].brace_idx;
            }
        }
        state->flags |= S_EXPR_NODE_FLAGS_USER;
    }
    
    // Execute mains every tick
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_OPEN_CALL &&
            params[i + 1].type == S_EXPR_PARAM_MAIN) {
            printf(">>> STATE_ACTIONS: calling main at i=%d, func_idx=%d\n", 
                   i, params[i + 1].func_idx);
            s_expr_result_t r = s_expr_invoke_any(inst, node, state, params, i);
            printf(">>> STATE_ACTIONS: main returned %d\n", r);
            if (r == SE_HALT) {
                printf(">>> STATE_ACTIONS: returning SE_HALT\n");
                return SE_HALT;
            }
            i += params[i].brace_idx;
        }
    }
    
    printf(">>> STATE_ACTIONS: all mains complete, finding return code\n");
    
    // Find return code (last INT/UINT)
    for (int i = param_count - 1; i >= 0; i--) {
        if (params[i].type == S_EXPR_PARAM_INT || params[i].type == S_EXPR_PARAM_UINT) {
            int ret = (int)s_expr_param_get_int(&params[i]);
            printf(">>> STATE_ACTIONS: found return code %d at i=%d\n", ret, i);
            state->flags &= ~S_EXPR_NODE_FLAGS_USER;
            return (s_expr_result_t)ret;
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

static s_expr_result_t cfl_dispatch_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_id; (void)event_data;
    if (param_count < 1 || params[0].type != S_EXPR_PARAM_SLOT) {
        return SE_TERMINATE;
    }
    
    int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!slot_ptr) return SE_TERMINATE;
    
    int32_t key = *slot_ptr;
    uint8_t default_idx = 0;
    
    // Scan for matching case: list(int(case_val), m_call(...))
    for (uint8_t i = 1; i < param_count; i++) {
        printf(">>> DISPATCH: i=%d, param_type=0x%02X\n", i, params[i].type);
        
        if (params[i].type == S_EXPR_PARAM_OPEN) {
            uint8_t case_val_idx = i + 1;
            printf(">>> DISPATCH: OPEN at i=%d, case_val_idx=%d, case_type=0x%02X\n", 
                   i, case_val_idx, params[case_val_idx].type);
            
            if (params[case_val_idx].type == S_EXPR_PARAM_INT ||
                params[case_val_idx].type == S_EXPR_PARAM_UINT) {
                
                int32_t case_val = (int32_t)s_expr_param_get_int(&params[case_val_idx]);
                printf(">>> DISPATCH: case_val=%d, key=%d\n", case_val, key);
                
                if (case_val == 0) {
                    default_idx = i + 2;
                    printf(">>> DISPATCH: default case at m_call_idx=%d\n", default_idx);
                }
                else if (case_val == key) {
                    printf(">>> DISPATCH: MATCH! invoking m_call at idx=%d\n", i + 2);
                    return s_expr_invoke_any(inst, node, state, params, i + 2);
                }
            }
            
            uint8_t skip = params[i].brace_idx;
            printf(">>> DISPATCH: skipping %d params (i=%d -> i=%d)\n", skip, i, i + skip);
            i += skip;
        }
    }
    
    printf(">>> DISPATCH: no match, default_idx=%d\n", default_idx);
    
    // No match - try default
    if (default_idx > 0) {
        return s_expr_invoke_any(inst, node, state, params, default_idx);
    }
    
    return SE_CONTINUE;
}

static s_expr_result_t cfl_event_dispatch_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_data;
    
    int32_t key = (int32_t)event_id;
    uint8_t default_idx = 0;
    
    // Scan for matching case: list(int(event_val), m_call(...))
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_OPEN) {
            uint8_t case_val_idx = i + 1;
            if (params[case_val_idx].type == S_EXPR_PARAM_INT ||
                params[case_val_idx].type == S_EXPR_PARAM_UINT) {
                
                int32_t case_val = (int32_t)s_expr_param_get_int(&params[case_val_idx]);
                
                // 0 = default case
                if (case_val == 0) {
                    default_idx = i + 2;
                }
                // Match found
                else if (case_val == key) {
                    return s_expr_invoke_any(inst, node, state, params, i + 2);
                }
            }
            
            i += params[i].brace_idx;
        }
    }
    
    // No match - try default
    if (default_idx > 0) {
        return s_expr_invoke_any(inst, node, state, params, default_idx);
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
    { "CFL_DISPATCH",(void*)cfl_dispatch_main },
    { "CFL_EVENT_DISPATCH",(void*)cfl_event_dispatch_main },
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