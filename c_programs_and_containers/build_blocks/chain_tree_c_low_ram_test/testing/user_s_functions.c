#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"

#include "s_engine_module.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>

// ============================================================================
// USER ONESHOT FUNCTIONS (@)
// ============================================================================

static void user_fn_example_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] USER_EXAMPLE_ONESHOT\n");
}

// ============================================================================
// USER BOOLEAN FUNCTIONS (?)
// ============================================================================

static bool user_fn_example_boolean(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] USER_EXAMPLE_BOOLEAN -> true\n");
    return true;
}

// ============================================================================
// USER MAIN FUNCTIONS (!)
// ============================================================================

static s_expr_result_t user_fn_example_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("  [!] USER_EXAMPLE_MAIN: init event\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        printf("  [!] USER_EXAMPLE_MAIN: terminate event\n");
        return SE_CONTINUE;
    }
    
    printf("  [!] USER_EXAMPLE_MAIN\n");
    return SE_CONTINUE;
}

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static const s_expr_fn_entry_t user_oneshot_entries[] = {
    { "TEST_29_SET_STATE", (void*)user_fn_example_oneshot },
    // Add more user oneshot functions here
};

static const s_expr_fn_entry_t user_boolean_entries[] = {
    { "TEST_29_READ_STATE", (void*)user_fn_example_boolean },
    // Add more user boolean functions here
};

static const s_expr_fn_entry_t user_main_entries[] = {
    { "USER_EXAMPLE_MAIN", (void*)user_fn_example_main },
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
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_module_ptr;
    
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