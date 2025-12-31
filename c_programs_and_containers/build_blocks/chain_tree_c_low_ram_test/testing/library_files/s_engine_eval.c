// ============================================================================
// s_engine_v3_eval.c
// S-Expression Evaluator Implementation - Version 3.0
// Flat parameter walker
// ============================================================================

#include "s_engine_eval.h"
#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static s_expr_result_t eval_params(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// INTERNAL: Get node state by index
// ============================================================================

static inline s_expr_node_state_t* get_node_state(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
) {
    if (!inst || node_index >= inst->node_count) return NULL;
    return &inst->node_states[node_index];
}

// ============================================================================
// INTERNAL: Dispatch oneshot function
// ============================================================================

static void dispatch_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_param_t* args,
    uint16_t arg_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    s_expr_module_t* mod = inst->module;
    uint16_t func_idx = func_param->func_idx;
    uint16_t node_idx = func_param->node_index;
    bool survives_reset = (func_param->type & S_EXPR_FLAG_SURVIVES_RESET) != 0;
    
    // Get node state
    s_expr_node_state_t* state = get_node_state(inst, node_idx);
    if (!state) return;
    
    // Check if already run
    // io_call (survives_reset): check EVER_INIT
    // o_call: check INITIALIZED
    uint8_t check_flag = survives_reset ? S_EXPR_NODE_FLAG_EVER_INIT : S_EXPR_NODE_FLAG_INITIALIZED;
    
    if (state->flags & check_flag) {
        return;  // Already executed
    }
    
    // Mark as executed
    state->flags |= check_flag;
    
    // Set current node for state access functions
    uint16_t saved_node = inst->current_node_index;
    inst->current_node_index = node_idx;
    
    // Dispatch
    if (func_idx < mod->def->oneshot_count && mod->oneshot_fns[func_idx]) {
        mod->oneshot_fns[func_idx](inst, args, arg_count, event_type, event_id, event_data);
    }
    
    inst->current_node_index = saved_node;
}

// ============================================================================
// INTERNAL: Dispatch predicate function
// ============================================================================

static bool dispatch_pred(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_param_t* args,
    uint16_t arg_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    s_expr_module_t* mod = inst->module;
    uint16_t func_idx = func_param->func_idx;
    uint16_t node_idx = func_param->node_index;
    
    // Set current node
    uint16_t saved_node = inst->current_node_index;
    inst->current_node_index = node_idx;
    
    bool result = false;
    if (func_idx < mod->def->pred_count && mod->pred_fns[func_idx]) {
        result = mod->pred_fns[func_idx](inst, args, arg_count, event_type, event_id, event_data);
    }
    
    inst->current_node_index = saved_node;
    return result;
}

// ============================================================================
// INTERNAL: Dispatch main function
// ============================================================================

static s_expr_result_t dispatch_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* func_param,
    const s_expr_param_t* args,
    uint16_t arg_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    s_expr_module_t* mod = inst->module;
    uint16_t func_idx = func_param->func_idx;
    uint16_t node_idx = func_param->node_index;
    bool is_pointer_call = (func_param->type & S_EXPR_FLAG_POINTER) != 0;
    uint8_t pointer_base = func_param->index_to_pointer;
    
    // Get node state
    s_expr_node_state_t* state = get_node_state(inst, node_idx);
    if (!state) return SE_CONTINUE;
    
    // Skip if not active
    if (!(state->flags & S_EXPR_NODE_FLAG_ACTIVE)) {
        return SE_CONTINUE;
    }
    
    // Save context
    uint16_t saved_node = inst->current_node_index;
    bool saved_in_ptr = inst->in_pointer_call;
    uint8_t saved_ptr_base = inst->pointer_base;
    
    // Set context
    inst->current_node_index = node_idx;
    if (is_pointer_call) {
        inst->in_pointer_call = true;
        inst->pointer_base = pointer_base;
    }
    
    s_expr_result_t result = SE_CONTINUE;
    s_expr_main_fn_t fn = NULL;
    
    if (func_idx < mod->def->main_count) {
        fn = mod->main_fns[func_idx];
    }
    
    if (!fn) {
        inst->current_node_index = saved_node;
        inst->in_pointer_call = saved_in_ptr;
        inst->pointer_base = saved_ptr_base;
        return SE_CONTINUE;
    }
    
    // Check if INIT event needed
    if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
        state->flags |= S_EXPR_NODE_FLAG_INITIALIZED;
        
        result = fn(inst, args, arg_count, SE_EVENT_INIT, event_id, event_data);
        
        if (result == SE_DISABLE) {
            // Send terminate, deactivate
            fn(inst, args, arg_count, SE_EVENT_TERMINATE, event_id, event_data);
            state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
            
            inst->current_node_index = saved_node;
            inst->in_pointer_call = saved_in_ptr;
            inst->pointer_base = saved_ptr_base;
            return SE_DISABLE;
        }
    }
    
    // Normal tick
    result = fn(inst, args, arg_count, event_type, event_id, event_data);
    
    // Handle disable
    if (result == SE_DISABLE) {
        fn(inst, args, arg_count, SE_EVENT_TERMINATE, event_id, event_data);
        state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
    }
    
    // Restore context
    inst->current_node_index = saved_node;
    inst->in_pointer_call = saved_in_ptr;
    inst->pointer_base = saved_ptr_base;
    
    return result;
}

// ============================================================================
// INTERNAL: Evaluate a single callable (OPEN_CALL ... CLOSE)
// Returns result and sets *out_skip to index after CLOSE
// ============================================================================

static s_expr_result_t eval_callable(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t open_idx,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data,
    uint16_t* out_skip
) {
    // Get close index via relative offset
    uint16_t close_idx = open_idx + params[open_idx].brace_idx;
    *out_skip = close_idx + 1;
    
    // Function ref is right after OPEN_CALL
    const s_expr_param_t* func_param = &params[open_idx + 1];
    uint8_t func_opcode = func_param->type & S_EXPR_OPCODE_MASK;
    
    // Calculate args (between func_ref and CLOSE)
    uint16_t arg_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    const s_expr_param_t* args = (arg_count > 0) ? &params[open_idx + 2] : NULL;
    
    // Dispatch based on function type
    switch (func_opcode) {
        case S_EXPR_PARAM_ONESHOT:
            dispatch_oneshot(inst, func_param, args, arg_count, event_type, event_id, event_data);
            return SE_CONTINUE;
            
        case S_EXPR_PARAM_PRED: {
            bool result = dispatch_pred(inst, func_param, args, arg_count, event_type, event_id, event_data);
            return result ? SE_CONTINUE : SE_HALT;
        }
        
        case S_EXPR_PARAM_MAIN:
            return dispatch_main(inst, func_param, args, arg_count, event_type, event_id, event_data);
            
        default:
            return SE_CONTINUE;
    }
}

// ============================================================================
// INTERNAL: Walk parameter array, execute callables in sequence
// ============================================================================

static s_expr_result_t eval_params(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    uint16_t idx = 0;
    
    while (idx < count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            // Callable expression
            uint16_t skip;
            s_expr_result_t result = eval_callable(
                inst, params, idx, event_type, event_id, event_data, &skip
            );
            
            // Handle control flow
            switch (result) {
                case SE_CONTINUE:
                case SE_DISABLE:
                    // Continue to next
                    idx = skip;
                    break;
                    
                case SE_HALT:
                case SE_TERMINATE:
                case SE_RESET:
                case SE_FUNCTION_TERMINATE:
                case SE_FUNCTION_HALT:
                case SE_FUNCTION_RESET:
                    // Propagate up
                    return result;
                    
                case SE_SKIP_CONTINUE:
                    // Skip remaining, return CONTINUE
                    return SE_CONTINUE;
                    
                default:
                    idx = skip;
                    break;
            }
        } else if (opcode == S_EXPR_PARAM_OPEN) {
            // Plain list - skip it
            idx = idx + params[idx].brace_idx + 1;
        } else {
            // Other param - skip
            idx++;
        }
    }
    
    return SE_CONTINUE;
}

// ============================================================================
// PUBLIC: Tree tick
// ============================================================================

s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
) {
    if (!inst || !inst->tree || !inst->tree->params) {
        return SE_TERMINATE;
    }
    
    // Store event context
    inst->current_event_id = event_id;
    inst->current_event_data = event_data;
    
    // Evaluate all params
    s_expr_result_t result = eval_params(
        inst,
        inst->tree->params,
        inst->tree->param_count,
        SE_EVENT_TICK,
        event_id,
        event_data
    );
    
    // Handle SE_RESET
    if (result == SE_RESET || result == SE_FUNCTION_RESET) {
        s_expr_tree_reset(inst);
        return SE_CONTINUE;
    }
    
    return result;
}

// ============================================================================
// PUBLIC: Reset tree
// ============================================================================

void s_expr_tree_reset(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    for (uint16_t i = 0; i < inst->node_count; i++) {
        // Preserve EVER_INIT (for io_call), clear INITIALIZED
        uint8_t ever_init = inst->node_states[i].flags & S_EXPR_NODE_FLAG_EVER_INIT;
        inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE | ever_init;
        inst->node_states[i].state = 0;
        inst->node_states[i].user_data.u64 = 0;
    }
}

// ============================================================================
// PUBLIC: Terminate tree
// ============================================================================

void s_expr_tree_terminate(s_expr_tree_instance_t* inst) {
    if (!inst || !inst->tree) return;
    
    // Walk params to find all main nodes and send TERMINATE
    const s_expr_param_t* params = inst->tree->params;
    uint16_t count = inst->tree->param_count;
    
    for (uint16_t idx = 0; idx < count; idx++) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_MAIN) {
            uint16_t node_idx = params[idx].node_index;
            s_expr_node_state_t* state = get_node_state(inst, node_idx);
            
            if (state && (state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
                uint16_t func_idx = params[idx].func_idx;
                
                if (func_idx < inst->module->def->main_count) {
                    s_expr_main_fn_t fn = inst->module->main_fns[func_idx];
                    if (fn) {
                        inst->current_node_index = node_idx;
                        fn(inst, NULL, 0, SE_EVENT_TERMINATE, 0, NULL);
                    }
                }
            }
        }
    }
    
    // Clear all states
    for (uint16_t i = 0; i < inst->node_count; i++) {
        inst->node_states[i].flags = 0;
        inst->node_states[i].state = 0;
        inst->node_states[i].user_data.u64 = 0;
    }
}

// ============================================================================
// PUBLIC: Full reset
// ============================================================================

void s_expr_tree_full_reset(s_expr_tree_instance_t* inst) {
    s_expr_tree_terminate(inst);
    s_expr_tree_init_states(inst);
}

// ============================================================================
// PUBLIC: Initialize states
// ============================================================================

void s_expr_tree_init_states(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    for (uint16_t i = 0; i < inst->node_count; i++) {
        inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE;
        inst->node_states[i].state = 0;
        memset(inst->node_states[i].reserved, 0, sizeof(inst->node_states[i].reserved));
        inst->node_states[i].user_data.u64 = 0;
    }
}

// ============================================================================
// PUBLIC: Invoke main callable
// ============================================================================

s_expr_result_t s_expr_invoke_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    if (!inst || !params) return SE_TERMINATE;
    
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        // Braced callable
        uint16_t close_idx = idx + params[idx].brace_idx;
        const s_expr_param_t* func_param = &params[idx + 1];
        uint16_t arg_count = (close_idx > idx + 2) ? (close_idx - idx - 2) : 0;
        const s_expr_param_t* args = (arg_count > 0) ? &params[idx + 2] : NULL;
        
        return dispatch_main(inst, func_param, args, arg_count,
                            SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    } else if (opcode == S_EXPR_PARAM_MAIN) {
        // Bare function ref
        return dispatch_main(inst, &params[idx], NULL, 0,
                            SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    }
    
    return SE_TERMINATE;
}

// ============================================================================
// PUBLIC: Invoke oneshot callable
// ============================================================================

void s_expr_invoke_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    if (!inst || !params) return;
    
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        uint16_t close_idx = idx + params[idx].brace_idx;
        const s_expr_param_t* func_param = &params[idx + 1];
        uint16_t arg_count = (close_idx > idx + 2) ? (close_idx - idx - 2) : 0;
        const s_expr_param_t* args = (arg_count > 0) ? &params[idx + 2] : NULL;
        
        dispatch_oneshot(inst, func_param, args, arg_count,
                        SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    } else if (opcode == S_EXPR_PARAM_ONESHOT) {
        dispatch_oneshot(inst, &params[idx], NULL, 0,
                        SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    }
}

// ============================================================================
// PUBLIC: Invoke predicate callable
// ============================================================================

bool s_expr_invoke_pred(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    if (!inst || !params) return false;
    
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        uint16_t close_idx = idx + params[idx].brace_idx;
        const s_expr_param_t* func_param = &params[idx + 1];
        uint16_t arg_count = (close_idx > idx + 2) ? (close_idx - idx - 2) : 0;
        const s_expr_param_t* args = (arg_count > 0) ? &params[idx + 2] : NULL;
        
        return dispatch_pred(inst, func_param, args, arg_count,
                            SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    } else if (opcode == S_EXPR_PARAM_PRED) {
        return dispatch_pred(inst, &params[idx], NULL, 0,
                            SE_EVENT_TICK, inst->current_event_id, inst->current_event_data);
    }
    
    return false;
}

// ============================================================================
// PUBLIC: Invoke any callable
// ============================================================================

s_expr_result_t s_expr_invoke_any(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t idx
) {
    if (!inst || !params) return SE_TERMINATE;
    
    uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
    
    // For OPEN_CALL, check the function type inside
    if (opcode == S_EXPR_PARAM_OPEN_CALL) {
        const s_expr_param_t* func_param = &params[idx + 1];
        uint8_t func_opcode = func_param->type & S_EXPR_OPCODE_MASK;
        
        switch (func_opcode) {
            case S_EXPR_PARAM_MAIN:
                return s_expr_invoke_main(inst, params, idx);
            case S_EXPR_PARAM_ONESHOT:
                s_expr_invoke_oneshot(inst, params, idx);
                return SE_CONTINUE;
            case S_EXPR_PARAM_PRED:
                return s_expr_invoke_pred(inst, params, idx) ? SE_CONTINUE : SE_HALT;
            default:
                return SE_TERMINATE;
        }
    }
    
    // Bare function ref
    switch (opcode) {
        case S_EXPR_PARAM_MAIN:
            return s_expr_invoke_main(inst, params, idx);
        case S_EXPR_PARAM_ONESHOT:
            s_expr_invoke_oneshot(inst, params, idx);
            return SE_CONTINUE;
        case S_EXPR_PARAM_PRED:
            return s_expr_invoke_pred(inst, params, idx) ? SE_CONTINUE : SE_HALT;
        default:
            return SE_TERMINATE;
    }
}

// ============================================================================
// PUBLIC: Count logical parameters
// ============================================================================

uint16_t s_expr_count_params(const s_expr_param_t* params, uint16_t count) {
    if (!params || count == 0) return 0;
    
    uint16_t logical_count = 0;
    uint16_t idx = 0;
    
    while (idx < count) {
        logical_count++;
        idx = s_expr_skip_param(params, idx);
    }
    
    return logical_count;
}

// ============================================================================
// PUBLIC: Find parameter by opcode
// ============================================================================

uint16_t s_expr_find_param(const s_expr_param_t* params, uint16_t count, uint8_t opcode) {
    if (!params) return UINT16_MAX;
    
    for (uint16_t idx = 0; idx < count; ) {
        if ((params[idx].type & S_EXPR_OPCODE_MASK) == opcode) {
            return idx;
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return UINT16_MAX;
}

// ============================================================================
// PUBLIC: Iterate parameters
// ============================================================================

void s_expr_iterate_params(
    const s_expr_param_t* params,
    uint16_t count,
    s_expr_param_iter_fn callback,
    void* ctx
) {
    if (!params || !callback) return;
    
    uint16_t idx = 0;
    while (idx < count) {
        if (!callback(params, idx, ctx)) {
            break;
        }
        idx = s_expr_skip_param(params, idx);
    }
}

// ============================================================================
// Runtime helper: Restart actions by walking params
// Call from inside any function to terminate and re-enable callables
// ============================================================================

void s_expr_restart_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
) {
    for (uint16_t i = 0; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            const s_expr_param_t* func_param = &params[i + 1];
            uint8_t func_opcode = func_param->type & S_EXPR_OPCODE_MASK;
            
            if (func_opcode == S_EXPR_PARAM_MAIN) {
                uint16_t node_idx = func_param->node_index;
                
                if (node_idx < inst->node_count) {
                    s_expr_node_state_t* node_state = &inst->node_states[node_idx];
                    
                    // Terminate if initialized
                    if (node_state->flags & S_EXPR_NODE_FLAG_INITIALIZED) {
                        uint16_t func_idx = func_param->func_idx;
                        s_expr_module_t* mod = inst->module;
                        
                        if (func_idx < mod->def->main_count && mod->main_fns[func_idx]) {
                            uint16_t close_idx = i + params[i].brace_idx;
                            uint16_t arg_count = (close_idx > i + 2) ? (close_idx - i - 2) : 0;
                            const s_expr_param_t* args = (arg_count > 0) ? &params[i + 2] : NULL;
                            
                            uint16_t saved_node = inst->current_node_index;
                            inst->current_node_index = node_idx;
                            
                            mod->main_fns[func_idx](inst, args, arg_count,
                                                    SE_EVENT_TERMINATE, 0, NULL);
                            
                            inst->current_node_index = saved_node;
                        }
                    }
                    
                    // Reset: clear INITIALIZED, keep EVER_INIT, set ACTIVE
                    uint8_t ever_init = node_state->flags & S_EXPR_NODE_FLAG_EVER_INIT;
                    node_state->flags = S_EXPR_NODE_FLAG_ACTIVE | ever_init;
                    node_state->state = 0;
                    node_state->user_data.u64 = 0;
                }
            }
            
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
}

// Reset flags so actions get INIT event on next invoke (no TERMINATE sent)
void s_expr_enable_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
) {
    for (uint16_t i = 0; i < param_count; ) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_CALL) {
            const s_expr_param_t* func_param = &params[i + 1];
            uint8_t func_opcode = func_param->type & S_EXPR_OPCODE_MASK;
            
            if (func_opcode == S_EXPR_PARAM_MAIN) {
                uint16_t node_idx = func_param->node_index;
                
                if (node_idx < inst->node_count) {
                    s_expr_node_state_t* node_state = &inst->node_states[node_idx];
                    
                    // Clear INITIALIZED, keep EVER_INIT, set ACTIVE
                    uint8_t ever_init = node_state->flags & S_EXPR_NODE_FLAG_EVER_INIT;
                    node_state->flags = S_EXPR_NODE_FLAG_ACTIVE | ever_init;
                }
            }
            
            i += params[i].brace_idx + 1;
        } else {
            i++;
        }
    }
}