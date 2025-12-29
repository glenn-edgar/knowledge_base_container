#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_common_function_headers.h"

#include "chain_flow_dsl_tests_pools.h"
// ============================================================================
// USER ONESHOT FUNCTIONS (@)
// ============================================================================

static void test_29_set_state_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
   
    if (param_count < 3 || 
        params[1].type != S_EXPR_PARAM_SLOT || 
        (params[2].type != S_EXPR_PARAM_INT && params[2].type != S_EXPR_PARAM_UINT)) {
        EXCEPTION("Invalid parameters for TEST_29_SET_STATE_ONESHOT");
        return;
    }
    
    // Use helper macro (defined in s_engine_module.h)a
    node_state_t* data = S_EXPR_TREE_GET_SLOT(inst, &params[1], node_state_t);

    
    data->children_active = params[1].i;
}


static void test_30_set_state_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
   
    if (param_count < 2) {
        EXCEPTION("CFL_SET_STATE: requires slot and value");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        EXCEPTION("CFL_SET_STATE: param[0] must be SLOT");
        return;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_SET_STATE: param[1] must be INT or UINT");
        return;
    }
    
    // Use int32_t* to match main functions
    int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!slot_ptr) return;
    
    *slot_ptr = (uint32_t)s_expr_param_get_int(&params[1]);
   
}
// ============================================================================
// USER BOOLEAN FUNCTIONS (?)
// ============================================================================

static bool test_29_read_state_boolean(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 2 || params[1].type != S_EXPR_PARAM_SLOT ) {
        EXCEPTION("Invalid parameters for TEST_29_READ_STATE_BOOLEAN");
        return false;
    }
    
    // Use helper macro (defined in s_engine_module.h)
    node_state_t* data = S_EXPR_TREE_GET_SLOT(inst, &params[1], node_state_t);
    
    return data->children_active;
   
}
 
static bool test_30_check_state_boolean(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
   
    if (param_count < 2) {
        EXCEPTION("CFL_CHECK_STATE: requires slot and value");
        return false;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        EXCEPTION("CFL_CHECK_STATE: param[0] must be SLOT");
        return false;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_CHECK_STATE: param[1] must be INT or UINT");
        return false;
    }
    
    state_machine_state_t* data = S_EXPR_TREE_GET_SLOT(inst, &params[0], state_machine_state_t);
    
    return data->state == (uint32_t)s_expr_param_get_int(&params[1]);
}
// ============================================================================
// USER MAIN FUNCTIONS (!)
// ============================================================================

static s_expr_result_t test_29_set_state_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    

    if (param_count < 3 || params[1].type != S_EXPR_PARAM_SLOT || params[2].type != S_EXPR_PARAM_INT) {
        EXCEPTION("Invalid parameters for TEST_29_SET_STATE_MAIN");
        return SE_CONTINUE;
    }
    
    if (event_id == S_EXPR_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    
    // Use helper macro (defined in s_engine_module.h)
    node_state_t* data = S_EXPR_TREE_GET_SLOT(inst, &params[1], node_state_t);
    
    
    data->children_active = params[2].i;
    
    return SE_CONTINUE;
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

// ============================================================================
// Parameter type validation helpers (add to s_engine_eval.h or local)
// ============================================================================

static inline bool s_expr_param_is_predicate(const s_expr_param_t* p) {
    return p->type == S_EXPR_PARAM_PRED || p->type == S_EXPR_PARAM_OPEN_CALL;
}

static inline bool s_expr_param_is_action(const s_expr_param_t* p) {
    return p->type == S_EXPR_PARAM_MAIN || 
           p->type == S_EXPR_PARAM_ONESHOT || 
           p->type == S_EXPR_PARAM_OPEN_CALL;
}

// ============================================================================
// Named flag for state tracking
// ============================================================================

#define DF_CONTROL_FLAG_ACTIVE  0x80  // true branch was activated

// ============================================================================
// DF_CONTROL: if (pred) then_action else else_action
// Params: [0] = predicate, [1] = then_action, [2] = else_action
// ============================================================================

static s_expr_result_t test_29_df_control_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)event_data;
    
    // Calculate parameter positions once
    const uint16_t pred_idx = 0;
    const uint16_t then_idx = s_expr_skip_param(params, pred_idx);
    const uint16_t else_idx = s_expr_skip_param(params, then_idx);
    
    // -------------------------------------------------------------------------
    // INIT: Validate parameters
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_INIT) {
        // Validate param count
        if (s_expr_count_logical_params(params, param_count) < 3) {
            EXCEPTION("DF_CONTROL requires 3 parameters: pred, then, else");
            state->flags |= S_EXPR_NODE_FLAG_ERROR;
            return SE_CONTINUE;
        }
        
        // Validate types
        if (!s_expr_param_is_predicate(&params[pred_idx])) {
            EXCEPTION("DF_CONTROL param[0] must be predicate");
            state->flags |= S_EXPR_NODE_FLAG_ERROR;
            return SE_CONTINUE;
        }
        if (!s_expr_param_is_action(&params[then_idx])) {
            EXCEPTION("DF_CONTROL param[1] must be action");
            state->flags |= S_EXPR_NODE_FLAG_ERROR;
            return SE_CONTINUE;
        }
        if (!s_expr_param_is_action(&params[else_idx])) {
            EXCEPTION("DF_CONTROL param[2] must be action");
            state->flags |= S_EXPR_NODE_FLAG_ERROR;
            return SE_CONTINUE;
        }
        
        // Clear active flag
        state->flags &= ~DF_CONTROL_FLAG_ACTIVE;
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TERMINATE: Cleanup
    // -------------------------------------------------------------------------
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // -------------------------------------------------------------------------
    // TICK: Evaluate and dispatch
    // -------------------------------------------------------------------------
    bool pred_result = (s_expr_invoke_any(inst, node, state, params, pred_idx) == SE_CONTINUE);
    bool was_active = (state->flags & DF_CONTROL_FLAG_ACTIVE) != 0;
    
    if (pred_result) {
        // Condition true - activate if not already active
        if (!was_active) {
            s_expr_invoke_any(inst, node, state, params, then_idx);
            state->flags |= DF_CONTROL_FLAG_ACTIVE;
        }
    } else {
        // Condition false - deactivate if was active
        if (was_active) {
            s_expr_invoke_any(inst, node, state, params, else_idx);
            state->flags &= ~DF_CONTROL_FLAG_ACTIVE;
        }
    }
    
    return SE_CONTINUE;
}
static s_expr_result_t test_30_set_state_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 2) return SE_CONTINUE;
    
    int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
        
    
    *slot_ptr = (int32_t)s_expr_param_get_int(&params[1]);
    
    return SE_DISABLE;
}

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static const s_expr_fn_entry_t user_oneshot_entries[] = {
    { "TEST_29_SET_STATE", (void*)test_29_set_state_oneshot },
    { "TEST_30_SET_STATE", (void*)test_30_set_state_oneshot },
    // Add more user oneshot functions here
};

static const s_expr_fn_entry_t user_boolean_entries[] = {
    { "TEST_29_READ_STATE", (void*)test_29_read_state_boolean },
    { "TEST_30_CHECK_STATE", (void*)test_30_check_state_boolean },
    // Add more user boolean functions here
};

static const s_expr_fn_entry_t user_main_entries[] = {
    { "TEST_29_SET_STATE", (void*)test_29_set_state_main },
    {"TEST_29_DF_CONTROL",(void*)test_29_df_control_main },
    {"TEST_30_SET_STATE",(void*)test_30_set_state_main },
    // Add more user main functions here
};

static const s_expr_fn_table_t user_oneshot_table = {
    .entries = user_oneshot_entries,
    .count = sizeof(user_oneshot_entries) / sizeof(user_oneshot_entries[0])
};

static const s_expr_fn_table_t user_boolean_table = {
    .entries = user_boolean_entries,
    .count = sizeof(user_boolean_entries) / sizeof(user_boolean_entries[0])
};

static const s_expr_fn_table_t user_main_table = {
    .entries = user_main_entries,
    .count = sizeof(user_main_entries) / sizeof(user_main_entries[0])
};

// ============================================================================
// LOAD FUNCTION
// ============================================================================

void load_user_s_functions(cfl_runtime_handle_t* handle) {
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_modules;
    
    if (!mod) {
        printf("ERROR: load_user_s_functions called before module init\n");
        return;
    }
    
    uint16_t loaded_oneshot = s_expr_module_load_oneshot(mod, &user_oneshot_table);
    uint16_t loaded_boolean = s_expr_module_load_boolean(mod, &user_boolean_table);
    uint16_t loaded_main = s_expr_module_load_main(mod, &user_main_table);
    
    printf("load_user_s_functions: %u oneshot, %u boolean, %u main\n",
           loaded_oneshot, loaded_boolean, loaded_main);
}

#if 0
// future reference
uint16_t content_count;
const s_expr_param_t* contents = s_expr_param_brace_contents(params, idx, &content_count);

// Iterate contents
uint16_t i = 0;
while (i < content_count) {
    // Process contents[i]
    printf("type: %d\n", contents[i].type);
    i = s_expr_skip_param(contents, i);
}
#endif