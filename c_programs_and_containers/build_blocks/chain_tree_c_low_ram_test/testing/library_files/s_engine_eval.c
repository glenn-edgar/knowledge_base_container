// ============================================================================
// s_engine_eval.c
// S-Expression Tree Evaluation Engine
// Version 2.7 - Lifecycle events for main functions only
// ============================================================================

#include "s_engine_eval.h"
#include "s_engine_module.h"
#include <string.h>
#include <stdint.h>

// ============================================================================
// INTERNAL: Evaluate a single node
// ============================================================================

static s_expr_result_t eval_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index,
    uint16_t event_id,
    void* event_data
) {
    if (!inst || node_index >= inst->node_count) {
        return SE_TERMINATE;
    }
    
    const s_expr_node_t* node = &inst->tree->nodes[node_index];
    s_expr_node_state_t* state = &inst->node_states[node_index];
    
    // Skip inactive nodes
    if (!(state->flags & S_EXPR_NODE_FLAG_ACTIVE)) {
        return SE_CONTINUE;
    }
    
    s_expr_module_t* mod = inst->module;
    
    // Get parameters (stored in tree definition)
    const s_expr_param_t* params = NULL;
    uint8_t param_count = 0;
    if (node->param_count > 0 && inst->tree->params) {
        params = &inst->tree->params[node->param_offset];
        param_count = node->param_count;
    }
    
    // Dispatch based on node type
    switch (node->type) {
        case S_EXPR_TABLE_ONESHOT: {
            // Oneshot: no lifecycle events, just execute
            if (node->fn_index < mod->def->oneshot_count && mod->oneshot_fns) {
                s_expr_oneshot_fn_t fn = mod->oneshot_fns[node->fn_index];
                if (fn) {
                    fn(inst, node, state, event_id, event_data, params, param_count);
                }
            }
            return SE_CONTINUE;
        }
        
        case S_EXPR_TABLE_BOOLEAN: {
            // Boolean: no lifecycle events, just evaluate
            if (node->fn_index < mod->def->boolean_count && mod->boolean_fns) {
                s_expr_boolean_fn_t fn = mod->boolean_fns[node->fn_index];
                if (fn) {
                    bool result = fn(inst, node, state, event_id, event_data, params, param_count);
                    return result ? SE_CONTINUE : SE_HALT;
                }
            }
            return SE_HALT;
        }
        
        case S_EXPR_TABLE_MAIN: {
            if (node->fn_index >= mod->def->main_count || !mod->main_fns) {
                return SE_CONTINUE;
            }
            
            s_expr_main_fn_t fn = mod->main_fns[node->fn_index];
            if (!fn) {
                return SE_CONTINUE;
            }
            
            // Main functions receive lifecycle events
            // Check if init event needed (first execution)
            if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
                state->flags |= S_EXPR_NODE_FLAG_INITIALIZED;
                
                // Send init event
                s_expr_result_t init_result = fn(inst, node, state, 
                    S_EXPR_EVENT_INIT, event_data, params, param_count);
                
                // If init returns SE_DISABLE, send terminate and deactivate
                if (init_result == SE_DISABLE) {
                    fn(inst, node, state, S_EXPR_EVENT_TERMINATE, event_data, params, param_count);
                    state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
                    return SE_DISABLE;
                }
            }
            
            // Normal execution
            s_expr_result_t result = fn(inst, node, state, event_id, event_data, params, param_count);
            
            // If result is SE_DISABLE, send terminate event
            if (result == SE_DISABLE) {
                fn(inst, node, state, S_EXPR_EVENT_TERMINATE, event_data, params, param_count);
                state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
            }
            
            return result;
        }
        
        default:
            return SE_CONTINUE;
    }
}

// ============================================================================
// PUBLIC: Tree tick
// ============================================================================

s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
) {
    if (!inst || !inst->tree || inst->node_count == 0) {
        return SE_TERMINATE;
    }
    
    // Evaluate root node (index 0)
    s_expr_result_t result = eval_node(inst, 0, event_id, event_data);
    
    // Handle SE_RESET
    if (result == SE_RESET) {
        s_expr_tree_reset(inst);
        return SE_CONTINUE;
    }
    
    return result;
}

// ============================================================================
// PUBLIC: Terminate all nodes (sends terminate to main functions only)
// ============================================================================

void s_expr_tree_terminate(s_expr_tree_instance_t* inst) {
    if (!inst || !inst->tree) return;
    
    s_expr_module_t* mod = inst->module;
    
    // Walk nodes in reverse order (children before parents)
    for (int i = (int)inst->node_count - 1; i >= 0; i--) {
        s_expr_node_state_t* state = &inst->node_states[i];
        
        // Only terminate initialized main nodes
        if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
            continue;
        }
        
        const s_expr_node_t* node = &inst->tree->nodes[i];
        
        // Only main functions receive terminate events
        if (node->type != S_EXPR_TABLE_MAIN) {
            state->flags = 0;
            continue;
        }
        
        if (node->fn_index >= mod->def->main_count || !mod->main_fns) {
            state->flags = 0;
            continue;
        }
        
        s_expr_main_fn_t fn = mod->main_fns[node->fn_index];
        if (fn) {
            const s_expr_param_t* params = NULL;
            uint8_t param_count = 0;
            if (node->param_count > 0 && inst->tree->params) {
                params = &inst->tree->params[node->param_offset];
                param_count = node->param_count;
            }
            
            fn(inst, node, state, S_EXPR_EVENT_TERMINATE, NULL, params, param_count);
        }
        
        state->flags = 0;
    }
}

// ============================================================================
// PUBLIC: Reset tree (terminate all, then reinitialize)
// ============================================================================

void s_expr_tree_reset(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    // Terminate all nodes
    s_expr_tree_terminate(inst);
    
    // Reinitialize all nodes to ACTIVE
    s_expr_tree_init_states(inst);
}

// ============================================================================
// PUBLIC: Initialize node states
// ============================================================================

void s_expr_tree_init_states(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    
    for (uint16_t i = 0; i < inst->node_count; i++) {
        inst->node_states[i].flags = S_EXPR_NODE_FLAG_ACTIVE;
        inst->node_states[i].state = 0;
        inst->node_states[i].user_data = 0;
    }
}

// ============================================================================
// PUBLIC: Parameter helpers
// ============================================================================

uint16_t s_expr_find_param_type(
    const s_expr_param_t* params,
    uint8_t param_count,
    uint8_t param_type
) {
    if (!params) return UINT16_MAX;
    
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == param_type) {
            return i;
        }
    }
    return UINT16_MAX;  // Not found
}

uint8_t s_expr_count_param_type(
    const s_expr_param_t* params,
    uint8_t param_count,
    uint8_t param_type
) {
    if (!params) return 0;
    
    uint8_t count = 0;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == param_type) {
            count++;
        }
    }
    return count;
}