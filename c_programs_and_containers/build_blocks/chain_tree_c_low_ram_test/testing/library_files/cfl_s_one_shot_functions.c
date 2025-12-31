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

// ============================================================================
// CFL_LOG: Print timestamp, node ID, and message hash
// Params: [0] = string hash (str_hash)
// ============================================================================

static void cfl_log_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_LOG: requires 1 parameter");
        return; 
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_STR_HASH) {
        EXCEPTION("CFL_LOG: param[0] must be STR_HASH");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_LOG: no runtime handle");
        return;
    }
    
    double timestamp = cfl_timer_get_timestamp(runtime_handle->timer_handle);
    printf("Timestamp: %f, Node ID: %u, Message: 0x%08X\n", 
           timestamp, inst->ct_node_id, params[0].str_hash);
}

static void cfl_enable_children_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_ENABLE_CHILDREN: no runtime handle");
        return;
    }
    
    cfl_enable_all_children(runtime_handle, inst->ct_node_id);  // <-- WORKS NOW
}

static void cfl_enable_child_oneshot(
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

static void cfl_disable_child_oneshot(
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
    
    cfl_terminate_node_tree(runtime_handle,inst->ct_node_id, (unsigned)params[0].i);

    
}


static void cfl_disable_children_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        return;
    }
    
    cfl_disable_all_children(runtime_handle, inst->ct_node_id);  // <-- WORKS NOW
}

// ============================================================================
// CFL_INTERNAL_EVENT: Send an internal event to the event queue
// Params: [0] = event_type (int/uint)
//         [1] = event_data (int/uint)
// ============================================================================

static s_expr_result_t cfl_internal_event_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Skip on INIT and TERMINATE
    if (event_type != SE_EVENT_TICK) {
        return SE_CONTINUE;
    }
    
    // Validate param count
    if (param_count != 2) {
        EXCEPTION("CFL_INTERNAL_EVENT requires 2 parameters: event_type, event_data");
        return SE_TERMINATE;
    }
    
    // Validate param types
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_INTERNAL_EVENT param[0] must be INT or UINT");
        return SE_TERMINATE;
    }
    
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_INTERNAL_EVENT param[1] must be INT or UINT");
        return SE_TERMINATE;
    }
    
    // Get runtime handle from user context
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_INTERNAL_EVENT: no runtime handle");
        return SE_TERMINATE;
    }
    
    // Send the event
    cfl_send_integer_event(
        runtime_handle->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        inst->ct_node_id,  // or use tree hash: s_expr_tree_name_hash(inst)
        (unsigned)s_expr_param_int(&params[0]),
        (cfl_int_t)s_expr_param_int(&params[1])
    );
    
    return SE_CONTINUE;
}

// ============================================================================
// CFL_EXCEPTION_HANDLER: Raise an exception with a string hash
// Params: [0] = string hash (str_hash)
// ============================================================================

static void cfl_exception_handler_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count != 1) {
        EXCEPTION("CFL_EXCEPTION_HANDLER: requires 1 parameter");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_STR_HASH) {
        EXCEPTION("CFL_EXCEPTION_HANDLER: param[0] must be STR_HASH");
        return;
    }
    
    EXCEPTION("Exception: 0x%08X", params[0].str_hash);
}
// ============================================================================
// FUNCTION TABLE
// ============================================================================

static const s_expr_fn_entry_t system_oneshot_entries[] = {
    { "CFL_LOG",              (void*)cfl_log_oneshot },
    { "CFL_ENABLE_CHILDREN",  (void*)cfl_enable_children_oneshot },
    { "CFL_DISABLE_CHILDREN", (void*)cfl_disable_children_oneshot },
    { "CFL_ENABLE_CHILD", (void*)cfl_enable_child_oneshot },
    {"CFL_INTERNAL_EVENT",(void*)cfl_internal_event_oneshot },
    {"CFL_EXCEPTION",(void*)cfl_exception_handler_oneshot },

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