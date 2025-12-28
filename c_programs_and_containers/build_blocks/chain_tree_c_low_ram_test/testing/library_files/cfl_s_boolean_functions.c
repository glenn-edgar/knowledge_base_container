#include "cfl_s_boolean_functions.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"

// ============================================================================
// BOOLEAN FUNCTION IMPLEMENTATIONS
// ============================================================================

static bool cfl_read_bit(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params;  // Add this line
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)inst->handle;
    if (param_count < 1) {
        printf("  [?] READ_BIT: missing parameters\n");
        EXCEPTION("cfl_read_bit: Invalid parameter count");
    return false;
    }
    
    if (params[0].type != S_EXPR_PARAM_INT && params[0].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("cfl_read_bit: Expected integer parameter");
        return false;
    }
    
    uint32_t bit_index = (uint32_t)s_expr_param_get_uint(&params[0]);
    
    if (bit_index >= 32) {
        EXCEPTION("cfl_read_bit: Bit index out of range");
        return false;
    }
    uint32_t bit_mask = 1U << bit_index;
   
    bool result = (runtime_handle->bitmask & bit_mask) != 0; 
    
    return result;
}

static bool cfl_true(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    return true;
}

static bool cfl_false(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    return false;
}

// ============================================================================
// Bit evaluation helpers
// ============================================================================

typedef enum {
    BIT_OP_OR,   // Short-circuit on first true, default false
    BIT_OP_AND   // Short-circuit on first false, default true
} bit_op_mode_t;

// Evaluate a single parameter - returns true/false
static inline bool cfl_s_bit_eval_param(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint16_t idx,
    uint32_t bitmask
) {
    uint8_t type = params[idx].type;
    
    if (type == S_EXPR_PARAM_INT || type == S_EXPR_PARAM_UINT) {
        unsigned bit_index = (unsigned)s_expr_param_get_uint(&params[idx]);
        return (bitmask & (1U << bit_index)) != 0;
    }
    
    if (type == S_EXPR_PARAM_PRED || type == S_EXPR_PARAM_OPEN_CALL) {
        return s_expr_invoke_any(inst, node, state, params, idx) == SE_CONTINUE;
    }
    
    EXCEPTION("cfl_s_bit: Invalid parameter type");
    return false;
}

// Validate all parameters during INIT
static inline bool cfl_s_bit_validate(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count
) {
    uint16_t idx = 0;
    
    while (idx < param_count) {
        uint8_t type = params[idx].type;
        
        switch (type) {
            case S_EXPR_PARAM_INT:
            case S_EXPR_PARAM_UINT:
                // Valid - integer bit index
                break;
                
            case S_EXPR_PARAM_PRED:
            case S_EXPR_PARAM_OPEN_CALL:
                // Valid - invoke to validate nested params
                s_expr_invoke_any(inst, node, state, params, idx);
                break;
                
            default:
                EXCEPTION("cfl_s_bit: Invalid parameter type");
                return false;
        }
        idx = s_expr_skip_param(params, idx);
    }
    return true;
}

// Generic bit operation evaluator
static inline bool cfl_s_bit_eval(
    s_expr_tree_instance_t* inst,
    const s_expr_node_t* node,
    s_expr_node_state_t* state,
    const s_expr_param_t* params,
    uint8_t param_count,
    uint32_t bitmask,
    bit_op_mode_t mode
) {
    uint16_t idx = 0;
    
    while (idx < param_count) {
        bool result = cfl_s_bit_eval_param(inst, node, state, params, idx, bitmask);
        
        if (mode == BIT_OP_OR && result) {
            return true;   // OR short-circuit
        }
        if (mode == BIT_OP_AND && !result) {
            return false;  // AND short-circuit
        }
        
        idx = s_expr_skip_param(params, idx);
    }
    
    return (mode == BIT_OP_AND);  // AND defaults true, OR defaults false
}

// ============================================================================
// Public functions
// ============================================================================

static bool cfl_s_bit_or(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        cfl_s_bit_validate(inst, node, state, params, param_count);
        return true;
    }
    
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)inst->handle;
    return cfl_s_bit_eval(inst, node, state, params, param_count, runtime->bitmask, BIT_OP_OR);
}

static bool cfl_s_bit_and(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        cfl_s_bit_validate(inst, node, state, params, param_count);
        return true;
    }
    
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return true;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)inst->handle;
    return cfl_s_bit_eval(inst, node, state, params, param_count, runtime->bitmask, BIT_OP_AND);
}

// ============================================================================
// FUNCTION TABLE
// ============================================================================

static const s_expr_fn_entry_t system_boolean_entries[] = {
    { "CFL_READ_BIT", (void*)cfl_read_bit },
    { "CFL_TRUE",     (void*)cfl_true },
    { "CFL_FALSE",    (void*)cfl_false },
    { "CFL_S_BIT_OR", (void*)cfl_s_bit_or },
    { "CFL_S_BIT_AND", (void*)cfl_s_bit_and },
    // Add more system boolean functions here
};

static const s_expr_fn_table_t system_boolean = {
    .entries = system_boolean_entries,
    .count = sizeof(system_boolean_entries) / sizeof(system_boolean_entries[0])
};

void cfl_load_boolean_s_functions(cfl_runtime_handle_t* handle) {
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_modules;
    
    if (!mod) {
        printf("ERROR: load_boolean_s_functions called before module init\n");
        return;
    }
    
    s_expr_module_load_boolean(mod, &system_boolean);
    
}