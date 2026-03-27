/* User functions for s_engine_test_2 — TEST_31 and TEST_32 */

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include <stdio.h>

/* ====================================================================
 * TEST_31_SET_MOTOR
 * Params: [0] = motor_id (int/uint), [1] = speed (int/uint)
 * ==================================================================== */

void test_31_set_motor_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    int motor_id = (int)s_expr_param_int(&params[0]);
    int speed = (int)s_expr_param_int(&params[1]);
    printf("TEST_31_SET_MOTOR: MOTOR[%d] = %d\n", motor_id, speed);
}

/* ====================================================================
 * TEST_31_SET_STATE
 * Params: [0] = field_ref, [1] = value (int/uint)
 * ==================================================================== */

void test_31_set_state_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    int32_t *field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return;
    int32_t value = (int32_t)s_expr_param_int(&params[1]);
    *field_ptr = value;
    printf("TEST_31_SET_STATE: set %d\n", value);
}

/* ====================================================================
 * TEST_32 functions
 * ==================================================================== */

#define EVT_TIMER     0xEE01
#define EVT_BUTTON    0xEE02
#define EVT_SENSOR    0xEE03
#define EVT_ALARM     0xEE04
#define EVT_SHUTDOWN  0xEE05

void test_32_enable_buzzer_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("BUZZER: ENABLED\n");
}

void test_32_disable_all_outputs_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("OUTPUTS: ALL DISABLED\n");
}

void test_32_toggle_led_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) return;
    int32_t led_id = (int32_t)s_expr_param_int(&params[0]);
    printf("LED[%d]: TOGGLED\n", led_id);
}

void test_32_set_led_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    int32_t led_id = (int32_t)s_expr_param_int(&params[0]);
    int32_t led_state = (int32_t)s_expr_param_int(&params[1]);
    printf("LED[%d]: %s\n", led_id, led_state ? "ON" : "OFF");
}

void test_32_save_state_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    printf("STATE: SAVED\n");
}

void test_32_notify_system_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) return;
    printf("NOTIFY: 0x%08X\n", (unsigned)s_expr_param_str_hash(&params[0]));
}

s_expr_result_t test_32_process_scheduled_tasks_main(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)params; (void)param_count; (void)event_data;
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE)
        return SE_CONTINUE;
    if (event_id == EVT_TIMER) printf("SCHEDULED_TASKS: PROCESSING\n");
    return SE_CONTINUE;
}

s_expr_result_t test_32_run_background_tasks_main(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst; (void)params; (void)param_count; (void)event_data;
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE)
        return SE_CONTINUE;
    if (event_id == CFL_SECOND_EVENT) printf("BACKGROUND_TASKS: RUNNING\n");
    return SE_CONTINUE;
}

s_expr_result_t test_32_check_threshold_main(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)inst;
    if (param_count < 2) return SE_TERMINATE;
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE)
        return SE_CONTINUE;
    if (event_id == EVT_SENSOR) {
        int32_t threshold = (int32_t)s_expr_param_int(&params[1]);
        int32_t reading = (int32_t)(size_t)event_data;
        if (reading > threshold)
            printf("SENSOR: ABOVE THRESHOLD (%d > %d)\n", reading, threshold);
    }
    return SE_CONTINUE;
}

s_expr_result_t test_32_generate_internal_events_main(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)params; (void)param_count; (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)s_expr_tree_get_user_ctx(inst);
    if (!runtime) return SE_TERMINATE;

    if (event_type == SE_EVENT_INIT) {
        s_expr_set_u64(inst, 0);
        return SE_CONTINUE;
    }
    if (event_type == SE_EVENT_TERMINATE) return SE_CONTINUE;
    if (event_type != SE_EVENT_TICK) return SE_CONTINUE;
    if (event_id != CFL_TIMER_EVENT) return SE_CONTINUE;

    uint64_t counter = s_expr_get_u64(inst);
    counter++;
    s_expr_set_u64(inst, counter);

    if (counter % 100 == 0) {
        cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                               inst->ct_node_id, EVT_TIMER, 0);
    }
    if (counter % 10 == 0) {
        cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                               inst->ct_node_id, EVT_BUTTON, 0);
    }
    if (counter == 200) {
        cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                               inst->ct_node_id, EVT_ALARM, 0);
        printf("EVENT_GEN: ALARM TRIGGERED\n");
    }
    cfl_send_integer_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
                           inst->ct_node_id, EVT_SENSOR, (uint16_t)(counter % 60));

    return SE_CONTINUE;
}
