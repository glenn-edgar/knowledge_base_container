// user_s_test_38.c

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_runtime.h"
#include "chain_flow_dsl_tests_records.h"
#include <string.h>
#include <stdio.h>
#include <math.h>

#define FLOAT_EQ(a, b) (fabsf((a) - (b)) < 0.0001f)

void test_38_verify_defaults_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data; (void)param_count;
    
    void* bb = s_expr_tree_get_blackboard(inst);
    pid_gains_t* gains = (pid_gains_t*)((uint8_t*)bb + params[0].field_offset);
    
    bool pass = FLOAT_EQ(gains->kp, 1.0f) &&
                FLOAT_EQ(gains->ki, 0.2f) &&
                FLOAT_EQ(gains->kd, 0.05f);
    
    printf("VERIFY_DEFAULTS: kp=%.2f ki=%.2f kd=%.2f -> %s\n",
           gains->kp, gains->ki, gains->kd, pass ? "PASS" : "FAIL");
}

void test_38_verify_test_pid_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    (void)param_count;
    
    void* bb = s_expr_tree_get_blackboard(inst);
    pid_gains_t* gains = (pid_gains_t*)((uint8_t*)bb + params[0].field_offset);
    
    bool pass = FLOAT_EQ(gains->kp, 2.5f) &&
                FLOAT_EQ(gains->ki, 0.5f) &&
                FLOAT_EQ(gains->kd, 0.1f);
    
    printf("VERIFY_TEST_PID: kp=%.2f ki=%.2f kd=%.2f -> %s\n",
           gains->kp, gains->ki, gains->kd, pass ? "PASS" : "FAIL");
}