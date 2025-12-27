#include "cfl_s_boolean_functions.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>

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
// FUNCTION TABLE
// ============================================================================

static const s_expr_fn_entry_t system_boolean_entries[] = {
    { "CFL_READ_BIT", (void*)cfl_read_bit },
    { "CFL_TRUE",     (void*)cfl_true },
    { "CFL_FALSE",    (void*)cfl_false },
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