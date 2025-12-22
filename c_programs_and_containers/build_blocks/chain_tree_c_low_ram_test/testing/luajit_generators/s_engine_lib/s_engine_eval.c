// ============================================================================
// s_engine_eval.c
// S-Expression Evaluator Implementation
// Version 2.2 - param_count, callable S-expressions
// ============================================================================

#include "s_engine_eval.h"
#include "s_engine_module.h"
#include <string.h>

// ============================================================================
// HELPER MACROS
// ============================================================================

#define NODE_TABLE(n)   ((n)->type & TABLE_MASK)
#define NODE_OPCODE(n)  ((n)->type & OPCODE_MASK)
#define IS_DEFAULT(n)   ((n)->reserved & 0x01)

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static cfl_code_t eval_pipeline(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_if(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_if_else(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_cond(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_dispatch(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_debug(module_runtime_t* mod, const node_t* node);

static bool eval_and(module_runtime_t* mod, const node_t* node);
static bool eval_or(module_runtime_t* mod, const node_t* node);
static bool eval_not(module_runtime_t* mod, const node_t* node);

static cfl_code_t eval_oneshot(module_runtime_t* mod, const node_t* node);
static bool eval_boolean(module_runtime_t* mod, const node_t* node);
static cfl_code_t eval_main(module_runtime_t* mod, const node_t* node);

// ============================================================================
// NODE ACCESS HELPERS
// ============================================================================

static inline const node_t* get_node(module_runtime_t* mod, uint16_t index) {
    return &mod->active_tree_def->nodes[index];
}

static inline node_state_t* get_state(module_runtime_t* mod, uint16_t index) {
    return &mod->node_states[index];
}

static inline const param_t* get_params(module_runtime_t* mod, const node_t* node) {
    return &mod->active_tree_def->params[node->param_offset];
}

// ============================================================================
// PARAMETER NAVIGATION (v2.2)
// ============================================================================

// Skip over a single parameter, handling braces
uint16_t skip_param(const param_t* params, uint16_t idx) {
    uint8_t type = params[idx].type;
    
    if (PARAM_IS_OPEN(type)) {
        // Jump to after matching close brace
        return params[idx].brace_idx + 1;
    }
    
    // Simple parameter - just advance by 1
    return idx + 1;
}

// Count arguments in a callable S-expr (between open and close)
static uint16_t count_sexpr_args(const param_t* params, uint16_t open_idx) {
    uint16_t close_idx = params[open_idx].brace_idx;
    uint16_t count = 0;
    
    // Start after open brace and function ref
    uint16_t idx = open_idx + 2;
    
    while (idx < close_idx) {
        count++;
        idx = skip_param(params, idx);
    }
    
    return count;
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

cfl_code_t module_tick(
    module_runtime_t* mod,
    uint16_t event_id,
    void* event_data
) {
    if (!mod || !mod->active_tree_def) {
        return CFL_HALT;
    }
    
    // Set execution context
    mod->current_event = event_id;
    mod->event_data = event_data;
    
    // Evaluate from root
    return eval_node(mod, mod->active_tree_def->root_index);
}

// ============================================================================
// GENERIC NODE EVALUATOR
// ============================================================================

cfl_code_t eval_node(module_runtime_t* mod, uint16_t node_index) {
    if (node_index == NO_CHILD) {
        return CFL_HALT;
    }
    
    const node_t* node = get_node(mod, node_index);
    uint8_t table = NODE_TABLE(node);
    uint8_t opcode = NODE_OPCODE(node);
    
    if (table == TABLE_OPCODE) {
        // Built-in opcode
        switch (opcode) {
            case OP_PIPELINE:   return eval_pipeline(mod, node);
            case OP_IF:         return eval_if(mod, node);
            case OP_IF_ELSE:    return eval_if_else(mod, node);
            case OP_COND:       return eval_cond(mod, node);
            case OP_DISPATCH:   return eval_dispatch(mod, node);
            case OP_DEBUG:      return eval_debug(mod, node);
            case OP_QUOTE:      return (cfl_code_t)node->fn_index;  // fn_index holds control code
            default:            return CFL_HALT;
        }
    } else {
        // Function call
        switch (table) {
            case TABLE_ONESHOT:
                return eval_oneshot(mod, node);
            case TABLE_BOOLEAN:
                // Boolean in control context - true=CONTINUE, false=HALT
                return eval_boolean(mod, node) ? CFL_CONTINUE : CFL_HALT;
            case TABLE_MAIN:
                return eval_main(mod, node);
            default:
                return CFL_HALT;
        }
    }
}

// ============================================================================
// BOOLEAN NODE EVALUATOR
// ============================================================================

bool eval_bool(module_runtime_t* mod, uint16_t node_index) {
    if (node_index == NO_CHILD) {
        return false;
    }
    
    const node_t* node = get_node(mod, node_index);
    uint8_t table = NODE_TABLE(node);
    uint8_t opcode = NODE_OPCODE(node);
    
    if (table == TABLE_OPCODE) {
        switch (opcode) {
            case OP_AND:    return eval_and(mod, node);
            case OP_OR:     return eval_or(mod, node);
            case OP_NOT:    return eval_not(mod, node);
            default:        return false;
        }
    } else if (table == TABLE_BOOLEAN) {
        return eval_boolean(mod, node);
    }
    
    return false;
}

// ============================================================================
// S-EXPRESSION EVALUATION (v2.2)
// ============================================================================

cfl_code_t eval_sexpr(
    module_runtime_t* mod,
    const node_t* node,
    node_state_t* state,
    const param_t* params,
    uint16_t open_idx
) {
    // Verify it's a callable S-expr
    if (params[open_idx].type != PARAM_OPEN_CALL) {
        return CFL_TERMINATE;
    }
    
    // Get function reference (first element after open brace)
    const param_t* func_param = &params[open_idx + 1];
    
    // Get args (everything between function and close brace)
    uint16_t close_idx = params[open_idx].brace_idx;
    const param_t* args = &params[open_idx + 2];
    uint8_t arg_count = (close_idx > open_idx + 2) ? (close_idx - open_idx - 2) : 0;
    
    // Dispatch based on function type
    switch (func_param->type) {
        case PARAM_MAIN: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->main_count || !mod->main_fns[fn_idx]) {
                return CFL_TERMINATE;
            }
            return mod->main_fns[fn_idx](
                mod, node, state, 
                mod->current_event, mod->event_data,
                args, arg_count
            );
        }
        
        case PARAM_ONESHOT: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->oneshot_count || !mod->oneshot_fns[fn_idx]) {
                return CFL_TERMINATE;
            }
            mod->oneshot_fns[fn_idx](
                mod, node, state,
                mod->current_event, mod->event_data,
                args, arg_count
            );
            return CFL_CONTINUE;
        }
        
        case PARAM_PRED: {
            uint16_t fn_idx = func_param->func_idx;
            if (fn_idx >= mod->def->boolean_count || !mod->boolean_fns[fn_idx]) {
                return CFL_TERMINATE;
            }
            bool result = mod->boolean_fns[fn_idx](
                mod, node, state,
                mod->current_event, mod->event_data,
                args, arg_count
            );
            return result ? CFL_CONTINUE : CFL_HALT;
        }
        
        default:
            return CFL_TERMINATE;
    }
}

// ============================================================================
// PIPELINE
// ============================================================================

static cfl_code_t eval_pipeline(module_runtime_t* mod, const node_t* node) {
    uint16_t child_idx = node->first_child;
    cfl_code_t result = CFL_CONTINUE;
    
    while (child_idx != NO_CHILD) {
        result = eval_node(mod, child_idx);
        
        // Any non-CONTINUE stops pipeline
        if (result != CFL_CONTINUE) {
            return result;
        }
        
        // Move to next sibling
        const node_t* child = get_node(mod, child_idx);
        child_idx = child->next_sibling;
    }
    
    return result;
}

// ============================================================================
// IF (condition + then)
// ============================================================================

static cfl_code_t eval_if(module_runtime_t* mod, const node_t* node) {
    if (node->child_count < 2) {
        return CFL_HALT;
    }
    
    // First child is condition
    uint16_t cond_idx = node->first_child;
    const node_t* cond_node = get_node(mod, cond_idx);
    
    // Second child is then-action
    uint16_t then_idx = cond_node->next_sibling;
    
    if (eval_bool(mod, cond_idx)) {
        return eval_node(mod, then_idx);
    }
    
    // No else - return CONTINUE
    return CFL_CONTINUE;
}

// ============================================================================
// IF-ELSE (condition + then + else)
// ============================================================================

static cfl_code_t eval_if_else(module_runtime_t* mod, const node_t* node) {
    if (node->child_count < 3) {
        return CFL_HALT;
    }
    
    // First child is condition
    uint16_t cond_idx = node->first_child;
    const node_t* cond_node = get_node(mod, cond_idx);
    
    // Second child is then-action
    uint16_t then_idx = cond_node->next_sibling;
    const node_t* then_node = get_node(mod, then_idx);
    
    // Third child is else-action
    uint16_t else_idx = then_node->next_sibling;
    
    if (eval_bool(mod, cond_idx)) {
        return eval_node(mod, then_idx);
    } else {
        return eval_node(mod, else_idx);
    }
}

// ============================================================================
// COND (multi-way conditional)
// ============================================================================

static cfl_code_t eval_cond(module_runtime_t* mod, const node_t* node) {
    uint16_t clause_idx = node->first_child;
    
    while (clause_idx != NO_CHILD) {
        const node_t* clause = get_node(mod, clause_idx);
        
        // Clause has: condition (first child), action (second child)
        // Default clause has: just action (first child), is_default flag set
        
        if (IS_DEFAULT(clause)) {
            // Default clause - execute action
            return eval_node(mod, clause->first_child);
        }
        
        // Normal clause - check condition
        uint16_t cond_idx = clause->first_child;
        const node_t* cond_node = get_node(mod, cond_idx);
        uint16_t action_idx = cond_node->next_sibling;
        
        if (eval_bool(mod, cond_idx)) {
            return eval_node(mod, action_idx);
        }
        
        clause_idx = clause->next_sibling;
    }
    
    // No clause matched and no default
    return CFL_HALT;
}

// ============================================================================
// DISPATCH (event-based switching)
// ============================================================================

static cfl_code_t eval_dispatch(module_runtime_t* mod, const node_t* node) {
    // First param is the dispatch key (string index)
    const param_t* params = get_params(mod, node);
    uint16_t key_str_idx = params[0].str_index;
    const char* key_name = mod->def->strings[key_str_idx];
    
    uint16_t event_id = mod->current_event;
    
    uint16_t case_idx = node->first_child;
    
    while (case_idx != NO_CHILD) {
        const node_t* case_node = get_node(mod, case_idx);
        
        if (IS_DEFAULT(case_node)) {
            // Default case - execute action
            return eval_node(mod, case_node->first_child);
        }
        
        // Check patterns (stored as params of case node)
        const param_t* case_params = get_params(mod, case_node);
        
        for (uint8_t i = 0; i < case_node->param_count; i++) {
            uint16_t pattern_str_idx = case_params[i].str_index;
            const char* pattern = mod->def->strings[pattern_str_idx];
            
            // Simple string comparison matching
            // Application can extend this for more complex matching
            (void)pattern;
            (void)event_id;
            (void)key_name;
            
            // TODO: Implement application-specific matching logic
            // For example:
            // if (match_event(key_name, pattern, event_id, mod->event_data)) {
            //     return eval_node(mod, case_node->first_child);
            // }
        }
        
        case_idx = case_node->next_sibling;
    }
    
    // No case matched
    return CFL_HALT;
}

// ============================================================================
// DEBUG
// ============================================================================

static cfl_code_t eval_debug(module_runtime_t* mod, const node_t* node) {
    if (mod->debug_fn) {
        const param_t* params = get_params(mod, node);
        uint16_t msg_idx = params[0].str_index;
        const char* message = mod->def->strings[msg_idx];
        mod->debug_fn(mod, message);
    }
    
    // Evaluate child and return its result (transparent)
    return eval_node(mod, node->first_child);
}

// ============================================================================
// BOOLEAN OPERATORS
// ============================================================================

static bool eval_and(module_runtime_t* mod, const node_t* node) {
    uint16_t child_idx = node->first_child;
    
    while (child_idx != NO_CHILD) {
        if (!eval_bool(mod, child_idx)) {
            return false;  // short-circuit
        }
        
        const node_t* child = get_node(mod, child_idx);
        child_idx = child->next_sibling;
    }
    
    return true;
}

static bool eval_or(module_runtime_t* mod, const node_t* node) {
    uint16_t child_idx = node->first_child;
    
    while (child_idx != NO_CHILD) {
        if (eval_bool(mod, child_idx)) {
            return true;  // short-circuit
        }
        
        const node_t* child = get_node(mod, child_idx);
        child_idx = child->next_sibling;
    }
    
    return false;
}

static bool eval_not(module_runtime_t* mod, const node_t* node) {
    return !eval_bool(mod, node->first_child);
}

// ============================================================================
// FUNCTION CALLS (v2.2 - with param_count)
// ============================================================================

static cfl_code_t eval_oneshot(module_runtime_t* mod, const node_t* node) {
    node_state_t* state = get_state(mod, node->node_index);
    
    // Check if disabled
    if (!(state->flags & NODE_FLAG_ACTIVE)) {
        return CFL_CONTINUE;
    }
    
    // Check if already fired
    if (state->flags & NODE_FLAG_ONESHOT_FIRED) {
        return CFL_CONTINUE;
    }
    
    // Mark as fired
    state->flags |= NODE_FLAG_ONESHOT_FIRED;
    
    // Get function and params
    oneshot_fn_t fn = mod->oneshot_fns[node->fn_index];
    const param_t* params = get_params(mod, node);
    
    // Call function with param_count (v2.2)
    fn(mod, node, state, mod->current_event, mod->event_data, 
       params, node->param_count);
    
    return CFL_CONTINUE;
}

static bool eval_boolean(module_runtime_t* mod, const node_t* node) {
    node_state_t* state = get_state(mod, node->node_index);
    
    // Check if disabled - disabled returns false
    if (!(state->flags & NODE_FLAG_ACTIVE)) {
        return false;
    }
    
    // Get function and params
    boolean_fn_t fn = mod->boolean_fns[node->fn_index];
    const param_t* params = get_params(mod, node);
    
    // Call function with param_count (v2.2)
    return fn(mod, node, state, mod->current_event, mod->event_data,
              params, node->param_count);
}

static cfl_code_t eval_main(module_runtime_t* mod, const node_t* node) {
    node_state_t* state = get_state(mod, node->node_index);
    
    // Check if disabled
    if (!(state->flags & NODE_FLAG_ACTIVE)) {
        return CFL_CONTINUE;
    }
    
    // Initialize on first call
    if (!(state->flags & NODE_FLAG_INITIALIZED)) {
        state->flags |= NODE_FLAG_INITIALIZED;
        // Could call init here if needed
    }
    
    // Get function and params
    main_fn_t fn = mod->main_fns[node->fn_index];
    const param_t* params = get_params(mod, node);
    
    // Call function with param_count (v2.2)
    cfl_code_t result = fn(mod, node, state, mod->current_event, mod->event_data,
                           params, node->param_count);
    
    // Handle CFL_DISABLE
    if (result == CFL_DISABLE) {
        state->flags &= ~NODE_FLAG_ACTIVE;
        return CFL_CONTINUE;
    }
    
    return result;
}