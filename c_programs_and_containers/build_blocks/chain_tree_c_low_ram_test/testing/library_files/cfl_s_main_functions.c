#include "cfl_s_main_functions.h"
#include "cfl_common_functions.h"
#include <stdio.h>

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


static const s_expr_fn_entry_t user_main_entries[] = {
    { "CFL_ENABLE_CHILDREN", (void*)cfl_enable_children_main },
    { "CFL_DISABLE_CHILDREN", (void*)cfl_disable_children_main },
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