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






// ============================================================================
// V3 TRANSLATIONS
// ============================================================================

// ----------------------------------------------------------------------------
// ONESHOT: TEST_30_SET_STATE
// DSL: io_call("TEST_30_SET_STATE") field_ref("state_b") int(0) end_call(...)
// ----------------------------------------------------------------------------
static void test_30_set_state_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        // EXCEPTION("TEST_30_SET_STATE: requires field and value");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        // EXCEPTION("TEST_30_SET_STATE: param[0] must be FIELD");
        return;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        // EXCEPTION("TEST_30_SET_STATE: param[1] must be INT or UINT");
        return;
    }
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return;
    
    *field_ptr = (int32_t)s_expr_param_int(&params[1]);
}

// ----------------------------------------------------------------------------
// PREDICATE: TEST_29_READ_STATE
// DSL: p_call("TEST_29_READ_STATE") field_ref("children_active") end_call(...)
// ----------------------------------------------------------------------------
static bool test_29_read_state_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        // EXCEPTION("TEST_29_READ_STATE: requires field");
        return false;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        // EXCEPTION("TEST_29_READ_STATE: param[0] must be FIELD");
        return false;
    }
    
    // Assuming field points to an int32 that holds bool-like value
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    return *field_ptr != 0;
}

// ----------------------------------------------------------------------------
// PREDICATE: TEST_30_CHECK_STATE
// DSL: p_call("TEST_30_CHECK_STATE") field_ref("state") int(1) end_call(...)
// ----------------------------------------------------------------------------
static bool test_30_check_state_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        // EXCEPTION("TEST_30_CHECK_STATE: requires field and value");
        return false;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        // EXCEPTION("TEST_30_CHECK_STATE: param[0] must be FIELD");
        return false;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        // EXCEPTION("TEST_30_CHECK_STATE: param[1] must be INT or UINT");
        return false;
    }
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    return *field_ptr == (int32_t)s_expr_param_int(&params[1]);
}

// ----------------------------------------------------------------------------
// MAIN: TEST_29_SET_STATE_ONESHOT
// DSL: m_call("TEST_29_SET_STATE_ONESHOT") field_ref("children_active") int(1) end_call(...)
// ----------------------------------------------------------------------------
static s_expr_result_t test_29_set_state_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Handle lifecycle events
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Normal tick
    if (param_count < 2) {
        // EXCEPTION("TEST_29_SET_STATE_MAIN: requires field and value");
        return SE_CONTINUE;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        // EXCEPTION("TEST_29_SET_STATE_MAIN: param[0] must be FIELD");
        return SE_CONTINUE;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        // EXCEPTION("TEST_29_SET_STATE_MAIN: param[1] must be INT or UINT");
        return SE_CONTINUE;
    }
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return SE_CONTINUE;
    
    *field_ptr = (int32_t)s_expr_param_int(&params[1]);
    
    return SE_CONTINUE;
}











// ============================================================================
// TEST_30_SET_STATE_MAIN: Set a field value and disable
// Params: [0] = field_ref (target field in blackboard)
//         [1] = value (int/uint)
// Returns: SE_DISABLE after setting value
// ============================================================================



 #include "user_s_test_31.h"
 #include "user_s_test_32.h"
// ============================================================================
// FUNCTION TABLES
// ============================================================================

// ============================================================================
// USER FUNCTION ENTRIES (named for readability)
// ============================================================================

static const s_expr_fn_entry_named_t user_oneshot_entries_named[] = {
    { "TEST_29_SET_STATE",          (void*)test_29_set_state_oneshot },
    { "TEST_30_SET_STATE",          (void*)test_30_set_state_oneshot },
    { "TEST_31_SET_MOTOR",          (void*)test_31_set_motor_oneshot },
    { "TEST_31_SET_STATE",          (void*)test_31_set_state_oneshot },
    { "TEST_32_TOGGLE_LED",         (void*)test_32_toggle_led_oneshot },
    { "TEST_32_ENABLE_BUZZER",      (void*)test_32_enable_buzzer_oneshot },
    { "TEST_32_SET_LED",            (void*)test_32_set_led_oneshot },   
    { "TEST_32_DISABLE_ALL_OUTPUTS",(void*)test_32_disable_all_outputs_oneshot },
    { "TEST_32_SAVE_STATE",         (void*)test_32_save_state_oneshot },
    { "TEST_32_NOTIFY_SYSTEM",      (void*)test_32_notify_system_oneshot },
    // Add more user oneshot functions here
};

static const s_expr_fn_entry_named_t user_pred_entries_named[] = {
    { "TEST_29_READ_STATE",         (void*)test_29_read_state_boolean },
    { "TEST_30_CHECK_STATE",        (void*)test_30_check_state_boolean },
    // Add more user predicate functions here
};

static const s_expr_fn_entry_named_t user_main_entries_named[] = {

    { "TEST_32_RUN_BACKGROUND_TASKS",       (void*)test_32_run_background_tasks_main },
    { "TEST_32_DEBOUNCE",                   (void*)test_32_debounce_main },
    { "TEST_32_CHECK_THRESHOLD",            (void*)test_32_check_threshold_main },
    { "TEST_32_GENERATE_INTERNAL_EVENTS",   (void*)test_32_generate_internal_events_main },
    { "TEST_32_PROCESS_SCHEDULED_TASKS",    (void*)test_32_process_scheduled_tasks_main },
    // Add more user main functions here
};

// ============================================================================
// HASH TABLES (populated at runtime)
// ============================================================================

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

static s_expr_fn_entry_t user_oneshot_entries[ARRAY_COUNT(user_oneshot_entries_named)];
static s_expr_fn_entry_t user_pred_entries[ARRAY_COUNT(user_pred_entries_named)];
static s_expr_fn_entry_t user_main_entries[ARRAY_COUNT(user_main_entries_named)];

static s_expr_fn_table_t user_oneshot_table;
static s_expr_fn_table_t user_pred_table;
static s_expr_fn_table_t user_main_table;


// ============================================================================
// INITIALIZE FUNCTION TABLES (call once at startup)
// ============================================================================

static void init_user_function_tables(void) {
    // Build hash tables from named tables
    s_expr_build_fn_table(
        user_oneshot_entries_named,
        user_oneshot_entries,
        ARRAY_COUNT(user_oneshot_entries_named)
    );
    s_expr_build_fn_table(
        user_pred_entries_named,
        user_pred_entries,
        ARRAY_COUNT(user_pred_entries_named)
    );
    s_expr_build_fn_table(
        user_main_entries_named,
        user_main_entries,
        ARRAY_COUNT(user_main_entries_named)
    );
    
    // Setup table structs
    user_oneshot_table.entries = user_oneshot_entries;
    user_oneshot_table.count = ARRAY_COUNT(user_oneshot_entries);
    
    user_pred_table.entries = user_pred_entries;
    user_pred_table.count = ARRAY_COUNT(user_pred_entries);
    
    user_main_table.entries = user_main_entries;
    user_main_table.count = ARRAY_COUNT(user_main_entries);
}

// ============================================================================
// LOAD FUNCTION (v3)
// ============================================================================



void load_user_s_functions(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        printf("ERROR: load_user_s_functions called with NULL\n");
        return;
    }
    
    // Initialize hash tables once

    init_user_function_tables();
        
    
    // Register to all modules
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_register_oneshot(modules[i], &user_oneshot_table);
        s_expr_module_register_pred(modules[i], &user_pred_table);
        s_expr_module_register_main(modules[i], &user_main_table);
    }
    
    
}