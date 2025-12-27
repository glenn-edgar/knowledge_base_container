// ============================================================================
// s_engine_eval.c
// S-Expression Tree Evaluation Engine
// Version 2.8 - Complete tree walker with all control flow opcodes
// ============================================================================

#include "s_engine_eval.h"
#include "s_engine_module.h"
#include <string.h>
#include <stdint.h>

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static s_expr_result_t eval_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index,
    uint16_t event_id,
    void* event_data
);

static bool eval_bool_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index,
    uint16_t event_id,
    void* event_data
);

// ============================================================================
// INTERNAL: Get child by index
// ============================================================================

static uint16_t get_child_index(
    s_expr_tree_instance_t* inst,
    uint16_t parent_index,
    uint8_t child_num
) {
    const s_expr_node_t* parent = &inst->tree->nodes[parent_index];
    uint16_t child_idx = parent->first_child;
    
    for (uint8_t i = 0; i < child_num && child_idx != S_EXPR_NO_CHILD; i++) {
        child_idx = inst->tree->nodes[child_idx].next_sibling;
    }
    
    return child_idx;
}

// ============================================================================
// INTERNAL: Evaluate boolean expression node
// ============================================================================

static bool eval_bool_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index,
    uint16_t event_id,
    void* event_data
) {
    if (!inst || node_index >= inst->node_count) {
        return false;
    }
    
    const s_expr_node_t* node = &inst->tree->nodes[node_index];
    s_expr_node_state_t* state = &inst->node_states[node_index];
    s_expr_module_t* mod = inst->module;
    
    uint8_t table = node->type & S_EXPR_TABLE_MASK;
    uint8_t opcode = node->type & S_EXPR_OPCODE_MASK;
    
    // Get parameters
    const s_expr_param_t* params = NULL;
    uint8_t param_count = 0;
    if (node->param_count > 0 && inst->tree->params) {
        params = &inst->tree->params[node->param_offset];
        param_count = node->param_count;
    }
    
    // Boolean function call
    if (table == S_EXPR_TABLE_BOOLEAN) {
        if (node->fn_index < mod->def->boolean_count && mod->boolean_fns) {
            s_expr_boolean_fn_t fn = mod->boolean_fns[node->fn_index];
            if (fn) {
                return fn(inst, node, state, event_id, event_data, params, param_count);
            }
        }
        return false;
    }
    
    // Boolean opcodes
    if (table == S_EXPR_TABLE_OPCODE) {
        switch (opcode) {
            case S_EXPR_OP_AND: {
                // All children must be true
                uint16_t child_idx = node->first_child;
                while (child_idx != S_EXPR_NO_CHILD) {
                    if (!eval_bool_node(inst, child_idx, event_id, event_data)) {
                        return false;
                    }
                    child_idx = inst->tree->nodes[child_idx].next_sibling;
                }
                return true;
            }
            
            case S_EXPR_OP_OR: {
                // Any child must be true
                uint16_t child_idx = node->first_child;
                while (child_idx != S_EXPR_NO_CHILD) {
                    if (eval_bool_node(inst, child_idx, event_id, event_data)) {
                        return true;
                    }
                    child_idx = inst->tree->nodes[child_idx].next_sibling;
                }
                return false;
            }
            
            case S_EXPR_OP_NOT: {
                // Negate single child
                uint16_t child_idx = node->first_child;
                if (child_idx == S_EXPR_NO_CHILD) {
                    return true;
                }
                return !eval_bool_node(inst, child_idx, event_id, event_data);
            }
            
            case S_EXPR_OP_XOR: {
                // Odd number of true children
                uint16_t child_idx = node->first_child;
                bool result = false;
                while (child_idx != S_EXPR_NO_CHILD) {
                    if (eval_bool_node(inst, child_idx, event_id, event_data)) {
                        result = !result;
                    }
                    child_idx = inst->tree->nodes[child_idx].next_sibling;
                }
                return result;
            }
            
            case S_EXPR_OP_NAND: {
                // NOT AND
                uint16_t child_idx = node->first_child;
                while (child_idx != S_EXPR_NO_CHILD) {
                    if (!eval_bool_node(inst, child_idx, event_id, event_data)) {
                        return true;  // Short-circuit: one false -> NAND is true
                    }
                    child_idx = inst->tree->nodes[child_idx].next_sibling;
                }
                return false;  // All true -> NAND is false
            }
            
            case S_EXPR_OP_NOR: {
                // NOT OR
                uint16_t child_idx = node->first_child;
                while (child_idx != S_EXPR_NO_CHILD) {
                    if (eval_bool_node(inst, child_idx, event_id, event_data)) {
                        return false;  // Short-circuit: one true -> NOR is false
                    }
                    child_idx = inst->tree->nodes[child_idx].next_sibling;
                }
                return true;  // All false -> NOR is true
            }
            
            default:
                return false;
        }
    }
    
    return false;
}

// ============================================================================
// INTERNAL: Evaluate a single node (control flow)
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
    
    uint8_t table = node->type & S_EXPR_TABLE_MASK;
    uint8_t opcode = node->type & S_EXPR_OPCODE_MASK;
    
    // Get parameters
    const s_expr_param_t* params = NULL;
    uint8_t param_count = 0;
    if (node->param_count > 0 && inst->tree->params) {
        params = &inst->tree->params[node->param_offset];
        param_count = node->param_count;
    }
    
    // Dispatch based on table selector
    switch (table) {
        case S_EXPR_TABLE_ONESHOT: {
            // Oneshot: execute only once (first tick)
            if (state->flags & S_EXPR_NODE_FLAG_INITIALIZED) {
                // Already executed, skip
                return SE_CONTINUE;
            }
            
            // Mark as executed
            state->flags |= S_EXPR_NODE_FLAG_INITIALIZED;
            
            if (node->fn_index < mod->def->oneshot_count && mod->oneshot_fns) {
                s_expr_oneshot_fn_t fn = mod->oneshot_fns[node->fn_index];
                if (fn) {
                    fn(inst, node, state, event_id, event_data, params, param_count);
                }
            }
            return SE_CONTINUE;
        }
        
        case S_EXPR_TABLE_BOOLEAN: {
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
            
            // Check if init event needed
            if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
                state->flags |= S_EXPR_NODE_FLAG_INITIALIZED;
                
                s_expr_result_t init_result = fn(inst, node, state, 
                    S_EXPR_EVENT_INIT, event_data, params, param_count);
                
                if (init_result == SE_DISABLE) {
                    fn(inst, node, state, S_EXPR_EVENT_TERMINATE, event_data, params, param_count);
                    state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
                    return SE_DISABLE;
                }
            }
            
            s_expr_result_t result = fn(inst, node, state, event_id, event_data, params, param_count);
            
            if (result == SE_DISABLE) {
                fn(inst, node, state, S_EXPR_EVENT_TERMINATE, event_data, params, param_count);
                state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
            }
            
            return result;
        }
        
        case S_EXPR_TABLE_OPCODE: {
            // Control flow opcodes
            switch (opcode) {
                case S_EXPR_OP_QUOTE: {
                    return (s_expr_result_t)node->fn_index;
                }
                
                case S_EXPR_OP_PIPELINE: {
                    uint16_t child_idx = node->first_child;
                    while (child_idx != S_EXPR_NO_CHILD) {
                        s_expr_result_t result = eval_node(inst, child_idx, event_id, event_data);
                        if (result != SE_CONTINUE) {
                            return result;
                        }
                        child_idx = inst->tree->nodes[child_idx].next_sibling;
                    }
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_IF: {
                    uint16_t cond_idx = get_child_index(inst, node_index, 0);
                    uint16_t then_idx = get_child_index(inst, node_index, 1);
                    
                    if (cond_idx == S_EXPR_NO_CHILD) {
                        return SE_CONTINUE;
                    }
                    
                    bool cond_result = eval_bool_node(inst, cond_idx, event_id, event_data);
                    
                    if (cond_result && then_idx != S_EXPR_NO_CHILD) {
                        return eval_node(inst, then_idx, event_id, event_data);
                    }
                    
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_IF_ELSE: {
                    uint16_t cond_idx = get_child_index(inst, node_index, 0);
                    uint16_t then_idx = get_child_index(inst, node_index, 1);
                    uint16_t else_idx = get_child_index(inst, node_index, 2);
                    
                    if (cond_idx == S_EXPR_NO_CHILD) {
                        return SE_CONTINUE;
                    }
                    
                    bool cond_result = eval_bool_node(inst, cond_idx, event_id, event_data);
                    
                    if (cond_result) {
                        if (then_idx != S_EXPR_NO_CHILD) {
                            return eval_node(inst, then_idx, event_id, event_data);
                        }
                    } else {
                        if (else_idx != S_EXPR_NO_CHILD) {
                            return eval_node(inst, else_idx, event_id, event_data);
                        }
                    }
                    
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_COND: {
                    uint16_t child_idx = node->first_child;
                    while (child_idx != S_EXPR_NO_CHILD) {
                        const s_expr_node_t* clause = &inst->tree->nodes[child_idx];
                        
                        bool is_default = (clause->reserved & 0x01) != 0;
                        
                        if (is_default) {
                            uint16_t action_idx = clause->first_child;
                            if (action_idx != S_EXPR_NO_CHILD) {
                                return eval_node(inst, action_idx, event_id, event_data);
                            }
                            return SE_CONTINUE;
                        }
                        
                        uint16_t cond_idx = clause->first_child;
                        if (cond_idx != S_EXPR_NO_CHILD) {
                            bool cond_result = eval_bool_node(inst, cond_idx, event_id, event_data);
                            if (cond_result) {
                                uint16_t action_idx = inst->tree->nodes[cond_idx].next_sibling;
                                if (action_idx != S_EXPR_NO_CHILD) {
                                    return eval_node(inst, action_idx, event_id, event_data);
                                }
                                return SE_CONTINUE;
                            }
                        }
                        
                        child_idx = clause->next_sibling;
                    }
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_DISPATCH: {
                    if (param_count == 0) {
                        return SE_CONTINUE;
                    }
                    
                    const char* key = s_expr_module_get_string(mod, params[0].str_index);
                    if (!key) {
                        return SE_CONTINUE;
                    }
                    
                    uint16_t child_idx = node->first_child;
                    uint16_t default_case_idx = S_EXPR_NO_CHILD;
                    
                    while (child_idx != S_EXPR_NO_CHILD) {
                        const s_expr_node_t* case_node = &inst->tree->nodes[child_idx];
                        
                        bool is_default = (case_node->reserved & 0x01) != 0;
                        
                        if (is_default) {
                            default_case_idx = child_idx;
                            child_idx = case_node->next_sibling;
                            continue;
                        }
                        
                        const s_expr_param_t* case_params = NULL;
                        if (case_node->param_count > 0 && inst->tree->params) {
                            case_params = &inst->tree->params[case_node->param_offset];
                        }
                        
                        for (uint8_t i = 0; i < case_node->param_count; i++) {
                            const char* pattern = s_expr_module_get_string(mod, case_params[i].str_index);
                            if (pattern && strcmp(key, pattern) == 0) {
                                uint16_t action_idx = case_node->first_child;
                                if (action_idx != S_EXPR_NO_CHILD) {
                                    return eval_node(inst, action_idx, event_id, event_data);
                                }
                                return SE_CONTINUE;
                            }
                        }
                        
                        child_idx = case_node->next_sibling;
                    }
                    
                    if (default_case_idx != S_EXPR_NO_CHILD) {
                        const s_expr_node_t* case_node = &inst->tree->nodes[default_case_idx];
                        uint16_t action_idx = case_node->first_child;
                        if (action_idx != S_EXPR_NO_CHILD) {
                            return eval_node(inst, action_idx, event_id, event_data);
                        }
                    }
                    
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_DEBUG: {
                    if (mod->debug_fn && param_count > 0) {
                        const char* msg = s_expr_module_get_string(mod, params[0].str_index);
                        if (msg) {
                            mod->debug_fn(inst, msg);
                        }
                    }
                    
                    uint16_t child_idx = node->first_child;
                    if (child_idx != S_EXPR_NO_CHILD) {
                        return eval_node(inst, child_idx, event_id, event_data);
                    }
                    return SE_CONTINUE;
                }
                
                case S_EXPR_OP_AND:
                case S_EXPR_OP_OR:
                case S_EXPR_OP_NOT:
                case S_EXPR_OP_XOR:
                case S_EXPR_OP_NAND:
                case S_EXPR_OP_NOR: {
                    bool result = eval_bool_node(inst, node_index, event_id, event_data);
                    return result ? SE_CONTINUE : SE_HALT;
                }
                
                default:
                    return SE_CONTINUE;
            }
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
    
    // Store event context
    inst->current_event = event_id;
    inst->event_data = event_data;
    
    // Evaluate root node
    s_expr_result_t result = eval_node(inst, 0, event_id, event_data);
    
    // Handle SE_RESET
    if (result == SE_RESET) {
        s_expr_tree_reset(inst);
        return SE_CONTINUE;
    }
    
    return result;
}

// ============================================================================
// PUBLIC: Exposed node evaluators
// ============================================================================

s_expr_result_t s_expr_eval_node(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
) {
    return eval_node(inst, node_index, inst->current_event, inst->event_data);
}

bool s_expr_eval_bool(
    s_expr_tree_instance_t* inst,
    uint16_t node_index
) {
    return eval_bool_node(inst, node_index, inst->current_event, inst->event_data);
}

// ============================================================================
// PUBLIC: Terminate all nodes
// ============================================================================

void s_expr_tree_terminate(s_expr_tree_instance_t* inst) {
    if (!inst || !inst->tree) return;
    
    s_expr_module_t* mod = inst->module;
    
    for (int i = (int)inst->node_count - 1; i >= 0; i--) {
        s_expr_node_state_t* state = &inst->node_states[i];
        
        if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
            continue;
        }
        
        const s_expr_node_t* node = &inst->tree->nodes[i];
        
        if ((node->type & S_EXPR_TABLE_MASK) != S_EXPR_TABLE_MAIN) {
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
// PUBLIC: Reset tree
// ============================================================================

void s_expr_tree_reset(s_expr_tree_instance_t* inst) {
    if (!inst) return;
    s_expr_tree_terminate(inst);
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
    return UINT16_MAX;
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