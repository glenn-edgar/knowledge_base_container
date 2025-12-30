#ifndef USER_TEST_32_H
#define USER_TEST_32_H
#ifdef __cplusplus
extern "C" {
#endif
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_function_headers.h"
#include "s_engine_types.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_common_function_headers.h"

#include "chain_flow_dsl_tests_pools.h"

#define EVT_TIMER     0xEE01
#define EVT_BUTTON    0xEE02
#define EVT_SENSOR    0xEE03
#define EVT_ALARM     0xEE04
#define EVT_SHUTDOWN  0xEE05



static void test_32_enable_buzzer_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("TEST_32_ENABLE_BUZZER: ENABLE BUZZER--->  buzzer is active now\n");
    
}

static void test_32_disable_all_outputs_oneshot (
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;(void)inst;
    printf("TEST_32_DISABLE_ALL_OUTPUTS: DISABLE ALL OUTPUTS---> ALL OUTPUTS ARE DISABLED\n");
    
}






static void test_32_toggle_led_oneshot(
s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
uint16_t event_id, void* event_data,
const s_expr_param_t* params, uint8_t param_count
) {
(void)node; (void)state; (void)event_id; (void)event_data;(void)inst;(void)param_count;
int32_t led_status = (int32_t)s_expr_param_get_int(&params[0]);
if(led_status == 0){
    printf("TEST_32_TOGGLE_LED: LED IS OFF\n");
}
else{
    printf("TEST_32_TOGGLE_LED: LED IS ON\n");
}

}
static void test_32_save_state_oneshot(
s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
uint16_t event_id, void* event_data,
const s_expr_param_t* params, uint8_t param_count
) {
(void)node; (void)state; (void)event_id; (void)event_data;(void)inst;
(void)params; (void)param_count;
printf("TEST_32_SAVE_STATE: SAVE STATE---> STATE SAVED\n");

}

static void test_32_set_led_oneshot(
s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
uint16_t event_id, void* event_data,
const s_expr_param_t* params, uint8_t param_count
) {
(void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
(void)params; (void)param_count;(void)inst;
printf("TEST_32_SET_LED: SET LED---> LED is active now\n");
 
}

static s_expr_result_t test_32_process_scheduled_tasks_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;(void)inst;(void)param_count;(void)params;
    if(event_id == S_EXPR_EVENT_INIT){
        printf("TEST_32_PROCESS_SCHEDULED_TASKS: INITIALIZING\n");
        return SE_CONTINUE;
    }
    if(event_id == S_EXPR_EVENT_TERMINATE){
        
        return SE_CONTINUE;
    }
    if(event_id == EVT_TIMER){
        printf("processing scheduled tasks\n");
        return SE_CONTINUE;
    }
    printf("TEST_32_PROCESS_SCHEDULED_TASKS: UNKNOWN EVENT\n");
    return SE_TERMINATE;
}
static s_expr_result_t test_32_run_background_tasks_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {

    (void)node; (void)state; (void)event_data;(void)inst;(void)param_count;(void)params;
    if(event_id == CFL_SECOND_EVENT){
        printf("TEST_32_RUN_BACKGROUND_TASKS: ARE RUNNING\n");
        return SE_CONTINUE;
    }
    return SE_CONTINUE;
}
static s_expr_result_t test_32_debounce_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    int32_t *debounce_counter = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    int32_t debounce_count = (int32_t)s_expr_param_get_int(&params[1]);
    if(event_id == S_EXPR_EVENT_INIT){
        *debounce_counter = 0;
        return SE_CONTINUE;
    }
    if(event_id == S_EXPR_EVENT_TERMINATE){
        
        return SE_CONTINUE;
    }
    if(event_id == EVT_BUTTON){
        *debounce_counter += 1;
        if(*debounce_counter >= debounce_count){
        
            return SE_DISABLE;
        }
        return SE_CONTINUE;
    }
    printf("TEST_32_DEBOUNCE: UNKNOWN EVENT\n");
    return SE_TERMINATE;
}

static s_expr_result_t test_32_check_threshold_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;(void)inst;
    if(event_id == EVT_SENSOR){
        int32_t sensor_reference = (int32_t)s_expr_param_get_int(&params[1]);
        int32_t sensor_reading = (int32_t)(size_t)event_data;
        if(sensor_reading > sensor_reference){
            printf("SENSOR READING IS ABOVE THE REFERENCE %d > %d\n", sensor_reading, sensor_reference);
            
        }
        else{
            ;//printf("SENSOR READING IS BELOW THE REFERENCE %d < %d\n", sensor_reading, sensor_reference);
            
        }
    }

    return SE_CONTINUE;
}
static void test_32_notify_system_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;(void)inst;
    printf("TEST_32_NOTIFY_SYSTEM: NOTIFY SYSTEM---> SYSTEM NOTIFIED\n");
}




static s_expr_result_t test_32_generate_internal_events_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;(void)param_count;
    int32_t *internal_counter = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)inst->handle;
    if(event_id == S_EXPR_EVENT_INIT){
        *internal_counter = 0;
        return SE_CONTINUE;
    }
    if(event_id == CFL_TIMER_EVENT){
        *internal_counter += 1;
        if(*internal_counter % 100 == 0){
            cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, EVT_TIMER, 0);
            
        }
        if(*internal_counter % 10 == 0){
            cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, EVT_BUTTON, 0);
        }
        
        if(*internal_counter == 200){
         
            cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, EVT_ALARM, 0);
            printf("ALARM TRIGGERED\n");
    
        }
        cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, EVT_SENSOR, (*internal_counter%60));
        if(*internal_counter % 10 == 0){
            cfl_send_integer_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW,inst->ct_node_id, EVT_BUTTON, 0);
        }
    }

    return SE_CONTINUE;
}
#ifdef __cplusplus
}
#endif

#endif