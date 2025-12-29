#include "cfl_s_one_shot_functions.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>

// ============================================================================
// ONESHOT FUNCTION IMPLEMENTATIONS
// ============================================================================

#include "cfl_s_one_shot_functions.h"
#include "cfl_common_function_headers.h"
#include "s_engine_module.h"  // 
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include <stdio.h>
#include <stdlib.h>

// ============================================================================
// ONESHOT FUNCTION IMPLEMENTATIONS
// ============================================================================

static void s_log(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    const char* msg = "";
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_STRING) {
        msg = s_expr_inst_get_string(inst, params[0].str_index);  // Defined in s_engine_module.h
    }
    else{
        EXCEPTION("Invalid parameters for CFL_LOG");
        return;
    }
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    double timestamp = cfl_timer_get_timestamp(runtime_handle->timer_handle);
    printf("Timestamp----------------->: %f, Node Index: %d, Message: %s\n", timestamp, inst->ct_node_id, msg);
    

}

static void s_enable_children(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    (void)params; (void)param_count;
    
    // TODO: Implement enable children logic
    
    cfl_enable_all_children(runtime_handle,inst->ct_node_id);
}

static void s_enable_child(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    if ((param_count != 1) && ((params[0].type == S_EXPR_PARAM_INT)|| (params[0].type == S_EXPR_PARAM_UINT))) {
        EXCEPTION("Invalid parameters for CFL_ENABLE_CHILD");
        return;
    }
    
    cfl_enable_child(runtime_handle,inst->ct_node_id, (unsigned)params[0].i);

    
}

static void s_disable_children(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    
    
    cfl_disable_all_children(runtime_handle,inst->ct_node_id);
}

static void s_internal_event(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    if(param_count != 2){
        EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
        return;
    }
    if((params[0].type != S_EXPR_PARAM_INT) && (params[0].type != S_EXPR_PARAM_UINT)){
        EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
        return;
    }
    if((params[1].type != S_EXPR_PARAM_INT) && (params[1].type != S_EXPR_PARAM_UINT)){
        EXCEPTION("Invalid parameters for CFL_INTERNAL_EVENT");
        return;
    }
    
    cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, (unsigned)params[0].i, (cfl_int_t)params[1].i);
}

static void s_exception_handler(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    //cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    if(param_count != 1){
        EXCEPTION("Invalid parameters for CFL_EXCEPTION_HANDLER");
        return;
    }
    if((params[0].type != S_EXPR_PARAM_STRING)){
        EXCEPTION("Invalid parameters for CFL_EXCEPTION_HANDLER");
        return;
    }
    EXCEPTION(s_expr_inst_get_string(inst, params[0].str_index));
}
// ============================================================================
// FUNCTION TABLE
// ============================================================================

static const s_expr_fn_entry_t system_oneshot_entries[] = {
    { "CFL_LOG",              (void*)s_log },
    { "CFL_ENABLE_CHILDREN",  (void*)s_enable_children },
    { "CFL_DISABLE_CHILDREN", (void*)s_disable_children },
    { "CFL_ENABLE_CHILD", (void*)s_enable_child },
    {"CFL_INTERNAL_EVENT",(void*)s_internal_event },
    {"CFL_EXCEPTION",(void*)s_exception_handler },

    // Add more system oneshot functions here
};

static const s_expr_fn_table_t system_oneshot = {
    .entries = system_oneshot_entries,
    .count = sizeof(system_oneshot_entries) / sizeof(system_oneshot_entries[0])
};

void cfl_load_oneshot_s_functions(cfl_runtime_handle_t* handle) {
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_modules;
    
    if (!mod) {
        printf("ERROR: load_oneshot_s_functions called before module init\n");
        return;
    }
    
    s_expr_module_load_oneshot(mod, &system_oneshot);
    
}