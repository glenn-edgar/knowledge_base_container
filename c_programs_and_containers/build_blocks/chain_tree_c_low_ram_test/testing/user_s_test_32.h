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
#include "chain_flow_dsl_tests_pools.h"

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

static void test_32_enable_buzzer_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("BUZZER: ENABLED\n");
}

static void test_32_disable_all_outputs_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("OUTPUTS: ALL DISABLED\n");
}

static void test_32_toggle_led_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        printf("ERROR: test_32_toggle_led requires 1 param (led_id)\n");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_INT && params[0].type != S_EXPR_PARAM_UINT) {
        printf("ERROR: test_32_toggle_led param[0] must be INT/UINT\n");
        return;
    }
    
    int32_t led_id = (int32_t)s_expr_param_get_int(&params[0]);
    printf("LED[%d]: TOGGLED\n", led_id);
}

static void test_32_set_led_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        printf("ERROR: test_32_set_led requires 2 params (led_id, state)\n");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_INT && params[0].type != S_EXPR_PARAM_UINT) {
        printf("ERROR: test_32_set_led param[0] must be INT/UINT\n");
        return;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        printf("ERROR: test_32_set_led param[1] must be INT/UINT\n");
        return;
    }
    
    int32_t led_id = (int32_t)s_expr_param_get_int(&params[0]);
    int32_t led_state = (int32_t)s_expr_param_get_int(&params[1]);
    printf("LED[%d]: %s\n", led_id, led_state ? "ON" : "OFF");
}

static void test_32_save_state_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("STATE: SAVED\n");
}

static void test_32_notify_system_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        printf("ERROR: test_32_notify_system requires 1 param (message)\n");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_STRING) {
        printf("ERROR: test_32_notify_system param[0] must be STRING\n");
        return;
    }
    
    const char* msg = s_expr_module_get_string(inst->module, params[0].str_index);
    printf("NOTIFY: %s\n", msg ? msg : "(null)");
}

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

static s_expr_result_t test_32_process_scheduled_tasks_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("SCHEDULED_TASKS: INIT\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    if (event_id == EVT_TIMER) {
        printf("SCHEDULED_TASKS: PROCESSING\n");
        return SE_CONTINUE;
    }
    return SE_CONTINUE;
}

static s_expr_result_t test_32_run_background_tasks_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    
    if (event_id == CFL_SECOND_EVENT) {
        printf("BACKGROUND_TASKS: RUNNING\n");
    }
    return SE_CONTINUE;
}

static s_expr_result_t test_32_debounce_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (param_count < 2) {
        printf("ERROR: test_32_debounce requires 2 params (slot, threshold)\n");
        return SE_TERMINATE;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        printf("ERROR: test_32_debounce param[0] must be SLOT\n");
        return SE_TERMINATE;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        printf("ERROR: test_32_debounce param[1] must be INT/UINT\n");
        return SE_TERMINATE;
    }
    
    int32_t* counter = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!counter) {
        printf("ERROR: test_32_debounce invalid slot\n");
        return SE_TERMINATE;
    }
    int32_t threshold = (int32_t)s_expr_param_get_int(&params[1]);
    
    if (event_id == S_EXPR_EVENT_INIT) {
        *counter = 0;
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    if (event_id == EVT_BUTTON) {
        (*counter)++;
        if (*counter >= threshold) {
            printf("DEBOUNCE: COMPLETE (count=%d)\n", *counter);
            return SE_DISABLE;
        }
        return SE_CONTINUE;
    }
    return SE_CONTINUE;
}

static s_expr_result_t test_32_check_threshold_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state;
    
    if (param_count < 2) {
        printf("ERROR: test_32_check_threshold requires 2 params (slot, threshold)\n");
        return SE_TERMINATE;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        printf("ERROR: test_32_check_threshold param[0] must be SLOT\n");
        return SE_TERMINATE;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        printf("ERROR: test_32_check_threshold param[1] must be INT/UINT\n");
        return SE_TERMINATE;
    }
    
    if (event_id == EVT_SENSOR) {
        int32_t threshold = (int32_t)s_expr_param_get_int(&params[1]);
        int32_t reading = (int32_t)(size_t)event_data;
        if (reading > threshold) {
            printf("SENSOR: ABOVE THRESHOLD (%d > %d)\n", reading, threshold);
        }
    }
    return SE_CONTINUE;
}

// ============================================================================
// EVENT GENERATOR (for testing)
// ============================================================================

static s_expr_result_t test_32_generate_internal_events_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (param_count < 1) {
        printf("ERROR: test_32_generate_internal_events requires 1 param (counter_slot)\n");
        return SE_TERMINATE;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        printf("ERROR: test_32_generate_internal_events param[0] must be SLOT\n");
        return SE_TERMINATE;
    }
    
    int32_t* counter = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!counter) {
        printf("ERROR: test_32_generate_internal_events invalid slot\n");
        return SE_TERMINATE;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)inst->handle;
    if (!runtime) {
        printf("ERROR: test_32_generate_internal_events no runtime handle\n");
        return SE_TERMINATE;
    }
    
    if (event_id == S_EXPR_EVENT_INIT) {
        *counter = 0;
        return SE_CONTINUE;
    }
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