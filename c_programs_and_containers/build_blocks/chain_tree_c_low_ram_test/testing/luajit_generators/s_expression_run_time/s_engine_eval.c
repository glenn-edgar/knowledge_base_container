// ============================================================================
// s_engine_eval.c
// S-Expression Evaluator Implementation
// Version 2.5 - Two-tier architecture: operates on tree instances
// ============================================================================

#include "s_engine_eval.h"
#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// HELPER MACROS
// ============================================================================

#define NODE_TABLE(n)   ((n)->type & S_EXPR_TABLE_MASK)
#define NODE_OPCODE(n)  ((n)->type & S_EXPR_OPCODE_MASK)
#define IS_DEFAULT(n)   ((n)->reserved & 0x01)

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static s_expr_result_t eval_pipeline(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_if(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_if_else(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_cond(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_dispatch(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_debug(s_expr_tree_instance_t* inst, const s_expr_node_t* node);

static bool eval_and(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_or(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_not(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_xor(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_nand(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_nor(s_expr_tree_instance_t* inst, const s_expr_node_t* node);

static s_expr_result_t eval_oneshot(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static bool eval_boolean(s_expr_tree_instance_t* inst, const s_expr_node_t* node);
static s_expr_result_t eval_main(s_expr_tree_instance_t* inst, const s_expr_node_t* node);

// ============================================================================
// NODE ACCESS HELPERS
// ============================================================================

static inline const s_expr_node_t* get_node(s_expr_tree_instance_t* inst, uint16_t index) {
    return &inst->tree_def->nodes[index];
}

static inline s_expr_node_state_t* get_state(s_expr_tree_instance_t* inst, uint16_t index) {
    return &inst->node_states[index];
}

static inline const s_expr_param_t* get_params(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    return &inst->tree_def->params[node->param_offset];
}

static inline const char* get_string(s_expr_tree_instance_t* inst, uint16_t str_index) {
    return inst->module->def->strings[str_index];
}

// ============================================================================
// PARAMETER NAVIGATION
// ============================================================================

uint16_t s_expr_skip_param(const s_expr_param_t* params, uint16_t idx) {
    uint8_t type = params[idx].type;
    
    if (S_EXPR_PARAM_IS_OPEN(type)) {
        return params[idx].brace_idx + 1;
    }
    
    return idx + 1;
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

s_expr_result_t s_expr_tree_tick(
    s_expr_tree_instance_t* inst,
    uint16_t event_id,
    void* event_data
) {
    if (!inst || !inst->tree_def || !inst->module) {
        return SE_HALT;
    }
    
    // Set execution context
    inst->current_event = event_id;
    inst->event_data = event_data;
    
    // Evaluate from root
    return s_expr_eval_node(inst, inst->tree_def->root_index);
}

// ============================================================================
// GENERIC NODE EVALUATOR
// ============================================================================

s_expr_result_t s_expr_eval_node(s_expr_tree_instance_t* inst, uint16_t node_index) {
    if (node_index == S_EXPR_NO_CHILD) {
        return SE_HALT;
    }
    
    const s_expr_node_t* node = get_node(inst, node_index);
    uint8_t table = NODE_TABLE(node);
    uint8_t opcode = NODE_OPCODE(node);
    
    if (table == S_EXPR_TABLE_OPCODE) {
        switch (opcode) {
            case S_EXPR_OP_PIPELINE:   return eval_pipeline(inst, node);
            case S_EXPR_OP_IF:         return eval_if(inst, node);
            case S_EXPR_OP_IF_ELSE:    return eval_if_else(inst, node);
            case S_EXPR_OP_COND:       return eval_cond(inst, node);
            case S_EXPR_OP_DISPATCH:   return eval_dispatch(inst, node);
            case S_EXPR_OP_DEBUG:      return eval_debug(inst, node);
            case S_EXPR_OP_QUOTE:      return (s_expr_result_t)node->fn_index;
            default:                   return SE_HALT;
        }
    } else {
        switch (table) {
            case S_EXPR_TABLE_ONESHOT:
                return eval_oneshot(inst, node);
            case S_EXPR_TABLE_BOOLEAN:
                return eval_boolean(inst, node) ? SE_CONTINUE : SE_HALT;
            case S_EXPR_TABLE_MAIN:
                return eval_main(inst, node);
            default:
                return SE_HALT;
        }
    }
}

// ============================================================================
// BOOLEAN NODE EVALUATOR
// ============================================================================

bool s_expr_eval_bool(s_expr_tree_instance_t* inst, uint16_t node_index) {
    if (node_index == S_EXPR_NO_CHILD) {
        return false;
    }
    
    const s_expr_node_t* node = get_node(inst, node_index);
    uint8_t table = NODE_TABLE(node);
    uint8_t opcode = NODE_OPCODE(node);
    
    if (table == S_EXPR_TABLE_OPCODE) {
        switch (opcode) {
            case S_EXPR_OP_AND:    return eval_and(inst, node);
            case S_EXPR_OP_OR:     return eval_or(inst, node);
            case S_EXPR_OP_NOT:    return eval_not(inst, node);
            case S_EXPR_OP_XOR:    return eval_xor(inst, node);
            case S_EXPR_OP_NAND:   return eval_nand(inst, node);
            case S_EXPR_OP_NOR:    return eval_nor(inst, node);
            default:               return false;
        }
    } else if (table == S_EXPR_TABLE_BOOLEAN) {
        return eval_boolean(inst, node);
    }
    
    return false;
}

// ============================================================================
// S-EXPRESSION EVALUATION
// ============================================================================

s_expr_result_t s_expr_eval_sexpr(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint16_t open_idx
) {
    if (params[open_idx].type != S_EXPR_PARAM_OPEN_CALL) {
        return SE_TERMINATE;
    }
    
    const s_expr_param_t* func_param = &params[open_idx + 1];
    uint16_t close_idx = params[open_idx].brace_idx;
    const s_expr_param_t* args = &params[open_idx + 2];
    uint8_t arg_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    
    s_expr_module_t* mod = inst->module;
    
    switch (func_param->type) {
        case S_EXPR_PARAM_MAIN: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->main_count || !mod->main_fns[fn_idx]) {
                return SE_TERMINATE;
            }
            return mod->main_fns[fn_idx](
                inst, node, state, 
                inst->current_event, inst->event_data,
                args, arg_count
            );
        }
        
        case S_EXPR_PARAM_ONESHOT: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->oneshot_count || !mod->oneshot_fns[fn_idx]) {
                return SE_TERMINATE;
            }
            mod->oneshot_fns[fn_idx](
                inst, node, state,
                inst->current_event, inst->event_data,
                args, arg_count
            );
            return SE_CONTINUE;
        }
        
        case S_EXPR_PARAM_PRED: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->boolean_count || !mod->boolean_fns[fn_idx]) {
                return SE_TERMINATE;
            }
            bool result = mod->boolean_fns[fn_idx](
                inst, node, state,
                inst->current_event, inst->event_data,
                args, arg_count
            );
            return result ? SE_CONTINUE : SE_HALT;
        }
        
        default:
            return SE_TERMINATE;
    }
}

// ============================================================================
// PIPELINE
// ============================================================================

static s_expr_result_t eval_pipeline(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    uint16_t child_idx = node->first_child;
    s_expr_result_t result = SE_CONTINUE;
    
    while (child_idx != S_EXPR_NO_CHILD) {
        result = s_expr_eval_node(inst, child_idx);
        
        if (result != SE_CONTINUE) {
            return result;
        }
        
        const s_expr_node_t* child = get_node(inst, child_idx);
        child_idx = child->next_sibling;
    }
    
    return result;
}

// ============================================================================
// IF (condition + then)
// ============================================================================

static s_expr_result_t eval_if(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    if (node->child_count < 2) {
        return SE_HALT;
    }
    
    uint16_t cond_idx = node->first_child;
    const s_expr_node_t* cond_node = get_node(inst, cond_idx);
    uint16_t then_idx = cond_node->next_sibling;
    
    if (s_expr_eval_bool(inst, cond_idx)) {
        return s_expr_eval_node(inst, then_idx);
    }
    
    return SE_CONTINUE;
}

// ============================================================================
// IF-ELSE (condition + then + else)
// ============================================================================

static s_expr_result_t eval_if_else(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    if (node->child_count < 3) {
        return SE_HALT;
    }
    
    uint16_t cond_idx = node->first_child;
    const s_expr_node_t* cond_node = get_node(inst, cond_idx);
    uint16_t then_idx = cond_node->next_sibling;
    const s_expr_node_t* then_node = get_node(inst, then_idx);
    uint16_t else_idx = then_node->next_sibling;
    
    if (s_expr_eval_bool(inst, cond_idx)) {
        return s_expr_eval_node(inst, then_idx);
    } else {
        return s_expr_eval_node(inst, else_idx);
    }
}

// ============================================================================
// COND (multi-way conditional)
// ============================================================================

static s_expr_result_t eval_cond(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    uint16_t clause_idx = node->first_child;
    
    while (clause_idx != S_EXPR_NO_CHILD) {
        const s_expr_node_t* clause = get_node(inst, clause_idx);
        
        if (IS_DEFAULT(clause)) {
            return s_expr_eval_node(inst, clause->first_child);
        }
        
        uint16_t cond_idx = clause->first_child;
        const s_expr_node_t* cond_node = get_node(inst, cond_idx);
        uint16_t action_idx = cond_node->next_sibling;
        
        if (s_expr_eval_bool(inst, cond_idx)) {
            return s_expr_eval_node(inst, action_idx);
        }
        
        clause_idx = clause->next_sibling;
    }
    
    return SE_HALT;
}

// ============================================================================
// DISPATCH (event-based switching)
// ============================================================================

static s_expr_result_t eval_dispatch(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    const s_expr_param_t* params = get_params(inst, node);
    uint16_t key_str_idx = params[0].str_index;
    const char* key_name = get_string(inst, key_str_idx);
    
    uint16_t event_id = inst->current_event;
    
    uint16_t case_idx = node->first_child;
    
    while (case_idx != S_EXPR_NO_CHILD) {
        const s_expr_node_t* case_node = get_node(inst, case_idx);
        
        if (IS_DEFAULT(case_node)) {
            return s_expr_eval_node(inst, case_node->first_child);
        }
        
        const s_expr_param_t* case_params = get_params(inst, case_node);
        
        for (uint8_t i = 0; i < case_node->param_count; i++) {
            uint16_t pattern_str_idx = case_params[i].str_index;
            const char* pattern = get_string(inst, pattern_str_idx);
            
            (void)pattern;
            (void)event_id;
            (void)key_name;
            // TODO: Application-specific matching logic
        }
        
        case_idx = case_node->next_sibling;
    }
    
    return SE_HALT;
}

// ============================================================================
// DEBUG
// ============================================================================

static s_expr_result_t eval_debug(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    if (inst->module->debug_fn) {
        const s_expr_param_t* params = get_params(inst, node);
        uint16_t msg_idx = params[0].str_index;
        const char* message = get_string(inst, msg_idx);
        inst->module->debug_fn(inst, message);
    }
    
    return s_expr_eval_node(inst, node->first_child);
}

// ============================================================================
// BOOLEAN OPERATORS
// ============================================================================

static bool eval_and(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    uint16_t child_idx = node->first_child;
    
    while (child_idx != S_EXPR_NO_CHILD) {
        if (!s_expr_eval_bool(inst, child_idx)) {
            return false;
        }
        
        const s_expr_node_t* child = get_node(inst, child_idx);
        child_idx = child->next_sibling;
    }
    
    return true;
}

static bool eval_or(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    uint16_t child_idx = node->first_child;
    
    while (child_idx != S_EXPR_NO_CHILD) {
        if (s_expr_eval_bool(inst, child_idx)) {
            return true;
        }
        
        const s_expr_node_t* child = get_node(inst, child_idx);
        child_idx = child->next_sibling;
    }
    
    return false;
}

static bool eval_not(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    return !s_expr_eval_bool(inst, node->first_child);
}

static bool eval_xor(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    uint16_t child_idx = node->first_child;
    bool result = false;
    
    while (child_idx != S_EXPR_NO_CHILD) {
        if (s_expr_eval_bool(inst, child_idx)) {
            result = !result;
        }
        
        const s_expr_node_t* child = get_node(inst, child_idx);
        child_idx = child->next_sibling;
    }
    
    return result;
}

static bool eval_nand(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    return !eval_and(inst, node);
}

static bool eval_nor(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    return !eval_or(inst, node);
}

// ============================================================================
// FUNCTION CALLS
// ============================================================================

static s_expr_result_t eval_oneshot(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    s_expr_node_state_t* state = get_state(inst, node->node_index);
    
    if (!(state->flags & S_EXPR_NODE_FLAG_ACTIVE)) {
        return SE_CONTINUE;
    }
    
    if (state->flags & S_EXPR_NODE_FLAG_ONESHOT_FIRED) {
        return SE_CONTINUE;
    }
    
    state->flags |= S_EXPR_NODE_FLAG_ONESHOT_FIRED;
    
    s_expr_oneshot_fn_t fn = inst->module->oneshot_fns[node->fn_index];
    const s_expr_param_t* params = get_params(inst, node);
    
    fn(inst, node, state, inst->current_event, inst->event_data, 
       params, node->param_count);
    
    return SE_CONTINUE;
}

static bool eval_boolean(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    s_expr_node_state_t* state = get_state(inst, node->node_index);
    
    if (!(state->flags & S_EXPR_NODE_FLAG_ACTIVE)) {
        return false;
    }
    
    s_expr_boolean_fn_t fn = inst->module->boolean_fns[node->fn_index];
    const s_expr_param_t* params = get_params(inst, node);
    
    return fn(inst, node, state, inst->current_event, inst->event_data,
              params, node->param_count);
}

static s_expr_result_t eval_main(s_expr_tree_instance_t* inst, const s_expr_node_t* node) {
    s_expr_node_state_t* state = get_state(inst, node->node_index);
    
    if (!(state->flags & S_EXPR_NODE_FLAG_ACTIVE)) {
        return SE_CONTINUE;
    }
    
    if (!(state->flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
        state->flags |= S_EXPR_NODE_FLAG_INITIALIZED;
    }
    
    s_expr_main_fn_t fn = inst->module->main_fns[node->fn_index];
    const s_expr_param_t* params = get_params(inst, node);
    
    s_expr_result_t result = fn(inst, node, state, inst->current_event, inst->event_data,
                           params, node->param_count);
    
    if (result == SE_DISABLE) {
        state->flags &= ~S_EXPR_NODE_FLAG_ACTIVE;
        return SE_CONTINUE;
    }
    
    return result;
}