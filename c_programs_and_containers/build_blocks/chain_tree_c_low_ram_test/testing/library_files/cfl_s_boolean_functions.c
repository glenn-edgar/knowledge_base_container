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
#if 0
#define S_EXPR_PARAM_INT       0x00
#define S_EXPR_PARAM_UINT      0x01
#define S_EXPR_PARAM_FLOAT     0x02
#define S_EXPR_PARAM_STRING    0x03
#define S_EXPR_PARAM_MAIN      0x04
#define S_EXPR_PARAM_ONESHOT   0x05
#define S_EXPR_PARAM_PRED      0x06
#define S_EXPR_PARAM_OPEN      0x07
#define S_EXPR_PARAM_CLOSE     0x08
#define S_EXPR_PARAM_OPEN_CALL 0x09
#define S_EXPR_PARAM_SLOT      0x0A
#endif

static void cfl_s_bit_validate_parameters(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    const s_expr_param_t* params, uint8_t param_count)
 {
    
    unsigned logical_parameters = s_expr_count_logical_params(params, param_count);
    unsigned first_parameter= 0;
    for(unsigned i = 0; i < logical_parameters; i++){
        switch(params[first_parameter].type){
            case S_EXPR_PARAM_INT:
            case S_EXPR_PARAM_UINT:
                break;

            case S_EXPR_PARAM_PRED:
            case S_EXPR_PARAM_OPEN_CALL:
                s_expr_invoke_any(inst, node, state, params, first_parameter);
                break;
            default:
                EXCEPTION("cfl_s_bit_validate_parameters: Invalid parameter type");
                return;
        }
        first_parameter = s_expr_skip_param(params, first_parameter);
    }
}



static bool cfl_s_bit_or(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)inst->handle;
    if(event_id ==  S_EXPR_EVENT_INIT){
        cfl_s_bit_validate_parameters(inst, node, state, params, param_count);
        
       
        return SE_CONTINUE;
    }
    if(event_id == S_EXPR_EVENT_TERMINATE){
        printf("cfl_s_bit_or: TERMINATE\n");
        exit(0);
        return true;
    }
    bool result = false;
    
    unsigned logical_parameters = s_expr_count_logical_params(params, param_count);
    unsigned first_parameter= 0;
    for(unsigned i = 0; i < logical_parameters; i++){
        switch(params[first_parameter].type){
            case S_EXPR_PARAM_INT:
            case S_EXPR_PARAM_UINT:
                unsigned bit_index = (unsigned)s_expr_param_get_uint(&params[first_parameter]);
                bool bit_result = (runtime_handle->bitmask & (1U << bit_index)) != 0;
        
                
                if(bit_result == true){
                        return true;
                }
                break;

            case S_EXPR_PARAM_PRED:
            case S_EXPR_PARAM_OPEN_CALL:
                 if(s_expr_invoke_any(inst, node, state, params, first_parameter) == SE_CONTINUE){
                    
                    return true;
                    
                 }
                break;
            default:
                EXCEPTION("cfl_s_bit_validate_parameters: Invalid parameter type");
                return false;
        }
        first_parameter = s_expr_skip_param(params, first_parameter);
    }
    return false;
}

static bool cfl_s_bit_and(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;

    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)inst->handle;
    if(event_id ==  S_EXPR_EVENT_INIT){
        cfl_s_bit_validate_parameters(inst, node, state, params, param_count);
        
        
        return SE_CONTINUE;
    }
    if(event_id == S_EXPR_EVENT_TERMINATE){
        printf("cfl_s_bit_or: TERMINATE\n");
        exit(0);
        return true;
    }
    bool result = true;
    
    unsigned logical_parameters = s_expr_count_logical_params(params, param_count);
    unsigned first_parameter= 0;
    for(unsigned i = 0; i < logical_parameters; i++){
        switch(params[first_parameter].type){
            case S_EXPR_PARAM_INT:
            case S_EXPR_PARAM_UINT:
                unsigned bit_index = (unsigned)s_expr_param_get_uint(&params[first_parameter]);
                bool bit_result = (runtime_handle->bitmask & (1U << bit_index)) != 0;
                
                if(bit_result == false){
                    return false;
                }
                break;

            case S_EXPR_PARAM_PRED:
            case S_EXPR_PARAM_OPEN_CALL:
                                
                 if(s_expr_invoke_any(inst, node, state, params, first_parameter) == SE_HALT){
                    return false;
                 }
                break;
            default:
                EXCEPTION("cfl_s_bit_validate_parameters: Invalid parameter type");
                return false;
        }
        first_parameter = s_expr_skip_param(params, first_parameter);
    }
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