#ifndef USER_TEST_32_H
#define USER_TEST_32_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"



// ============================================================================
// EVENT IDS
// ============================================================================

#define EVT_TIMER     0xEE01
#define EVT_BUTTON    0xEE02
#define EVT_SENSOR    0xEE03
#define EVT_ALARM     0xEE04
#define EVT_SHUTDOWN  0xEE05

// ============================================================================
// ONESHOT FUNCTIONS
// ============================================================================

// ============================================================================
// TEST_32_ENABLE_BUZZER: Enable buzzer
// Params: none
// ============================================================================

static void test_32_enable_buzzer_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("BUZZER: ENABLED\n");
}

// ============================================================================
// TEST_32_DISABLE_ALL_OUTPUTS: Disable all outputs
// Params: none
// ============================================================================

static void test_32_disable_all_outputs_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("OUTPUTS: ALL DISABLED\n");
}

// ============================================================================
// TEST_32_TOGGLE_LED: Toggle LED by ID
// Params: [0] = led_id (int/uint)
// ============================================================================

static void test_32_toggle_led_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_32_TOGGLE_LED: requires 1 param (led_id)");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_32_TOGGLE_LED: param[0] must be INT or UINT");
        return;
    }
    
    int32_t led_id = (int32_t)s_expr_param_int(&params[0]);
    printf("LED[%d]: TOGGLED\n", led_id);
}

// ============================================================================
// TEST_32_SET_LED: Set LED state
// Params: [0] = led_id (int/uint)
//         [1] = state (int/uint: 0=off, non-zero=on)
// ============================================================================

static void test_32_set_led_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_32_SET_LED: requires 2 params (led_id, state)");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_32_SET_LED: param[0] must be INT or UINT");
        return;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_32_SET_LED: param[1] must be INT or UINT");
        return;
    }
    
    int32_t led_id = (int32_t)s_expr_param_int(&params[0]);
    int32_t led_state = (int32_t)s_expr_param_int(&params[1]);
    printf("LED[%d]: %s\n", led_id, led_state ? "ON" : "OFF");
}

// ============================================================================
// TEST_32_SAVE_STATE: Save state
// Params: none
// ============================================================================

static void test_32_save_state_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    printf("STATE: SAVED\n");
}

// ============================================================================
// TEST_32_NOTIFY_SYSTEM: Print notification message (hash)
// Params: [0] = message (str_hash)
// ============================================================================

static void test_32_notify_system_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_32_NOTIFY_SYSTEM: requires 1 param (message)");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_STR_HASH) {
        EXCEPTION("TEST_32_NOTIFY_SYSTEM: param[0] must be STR_HASH");
        return;
    }
    
    printf("NOTIFY: 0x%08X\n", params[0].str_hash);
}

// ============================================================================
// TEST_32_PROCESS_SCHEDULED_TASKS: Process tasks on timer event
// Params: none
// ============================================================================

static s_expr_result_t test_32_process_scheduled_tasks_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        printf("SCHEDULED_TASKS: INIT\n");
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Normal tick
    if (event_id == EVT_TIMER) {
        printf("SCHEDULED_TASKS: PROCESSING\n");
    }
    return SE_CONTINUE;
}

// ============================================================================
// TEST_32_RUN_BACKGROUND_TASKS: Run background tasks on second event
// Params: none
// ============================================================================

static s_expr_result_t test_32_run_background_tasks_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_id == CFL_SECOND_EVENT) {
        printf("BACKGROUND_TASKS: RUNNING\n");
    }
    return SE_CONTINUE;
}

// ============================================================================
// TEST_32_DEBOUNCE: Debounce button presses
// Params: [0] = field_ref (counter in blackboard)
//         [1] = threshold (int/uint)
// ============================================================================

static s_expr_result_t test_32_debounce_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_32_DEBOUNCE: requires 2 params (field, threshold)");
        return SE_TERMINATE;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        EXCEPTION("TEST_32_DEBOUNCE: param[0] must be FIELD");
        return SE_TERMINATE;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_32_DEBOUNCE: param[1] must be INT or UINT");
        return SE_TERMINATE;
    }
    
    int32_t* counter = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!counter) {
        EXCEPTION("TEST_32_DEBOUNCE: invalid field");
        return SE_TERMINATE;
    }
    
    int32_t threshold = (int32_t)s_expr_param_int(&params[1]);
    
    if (event_type == SE_EVENT_INIT) {
        *counter = 0;
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Normal tick
    if (event_id == EVT_BUTTON) {
        (*counter)++;
        if (*counter >= threshold) {
            printf("DEBOUNCE: COMPLETE (count=%d)\n", *counter);
            return SE_DISABLE;
        }
    }
    return SE_CONTINUE;
}

// ============================================================================
// TEST_32_CHECK_THRESHOLD: Check sensor against threshold
// Params: [0] = field_ref (unused, for compatibility)
//         [1] = threshold (int/uint)
// ============================================================================

static s_expr_result_t test_32_check_threshold_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst;
    
    if (param_count < 2) {
        EXCEPTION("TEST_32_CHECK_THRESHOLD: requires 2 params (field, threshold)");
        return SE_TERMINATE;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        EXCEPTION("TEST_32_CHECK_THRESHOLD: param[0] must be FIELD");
        return SE_TERMINATE;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_32_CHECK_THRESHOLD: param[1] must be INT or UINT");
        return SE_TERMINATE;
    }
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Normal tick
    if (event_id == EVT_SENSOR) {
        int32_t threshold = (int32_t)s_expr_param_int(&params[1]);
        int32_t reading = (int32_t)(size_t)event_data;
        if (reading > threshold) {
            printf("SENSOR: ABOVE THRESHOLD (%d > %d)\n", reading, threshold);
        }
    }
    return SE_CONTINUE;
}

// ============================================================================
// TEST_32_GENERATE_INTERNAL_EVENTS: Generate test events on timer
// Params: [0] = field_ref (counter in blackboard)
// ============================================================================

static s_expr_result_t test_32_generate_internal_events_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_32_GENERATE_INTERNAL_EVENTS: requires 1 param (counter field)");
        return SE_TERMINATE;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_FIELD) {
        EXCEPTION("TEST_32_GENERATE_INTERNAL_EVENTS: param[0] must be FIELD");
        return SE_TERMINATE;
    }
    
    int32_t* counter = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!counter) {
        EXCEPTION("TEST_32_GENERATE_INTERNAL_EVENTS: invalid field");
        return SE_TERMINATE;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime) {
        EXCEPTION("TEST_32_GENERATE_INTERNAL_EVENTS: no runtime handle");
        return SE_TERMINATE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        *counter = 0;
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    // Normal tick
    if (event_id == CFL_TIMER_EVENT) {
        (*counter)++;
        
        if (*counter % 100 == 0) {
            cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                                   inst->ct_node_id, EVT_TIMER, 0);
        }
        if (*counter % 10 == 0) {
            cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                                   inst->ct_node_id, EVT_BUTTON, 0);
        }
        if (*counter == 200) {
            cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                                   inst->ct_node_id, EVT_ALARM, 0);
            printf("EVENT_GEN: ALARM TRIGGERED\n");
        }
        cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                               inst->ct_node_id, EVT_SENSOR, (*counter % 60));
    }
    return SE_CONTINUE;
}
#ifdef __cplusplus
}
#endif

#endif // USER_TEST_32_H