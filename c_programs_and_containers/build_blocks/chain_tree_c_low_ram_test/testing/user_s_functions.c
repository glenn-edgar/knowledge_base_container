#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"

#include "s_engine_module.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>
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

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static const s_expr_fn_entry_t user_oneshot_entries[] = {
    { "TEST_29_SET_STATE", (void*)test_29_set_state_oneshot },
    // Add more user oneshot functions here
};

static const s_expr_fn_entry_t user_boolean_entries[] = {
    { "TEST_29_READ_STATE", (void*)test_29_read_state_boolean },
    // Add more user boolean functions here
};

static const s_expr_fn_entry_t user_main_entries[] = {
    { "TEST_29_SET_STATE", (void*)test_29_set_state_main },
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