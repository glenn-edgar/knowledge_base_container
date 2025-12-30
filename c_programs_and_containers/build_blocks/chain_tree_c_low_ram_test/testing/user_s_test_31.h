#ifndef USER_TEST_31_H
#define USER_TEST_31_H
#ifdef __cplusplus
extern "C" {
#endif

#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "user_s_functions.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_common_function_headers.h"

#include "chain_flow_dsl_tests_pools.h"

static void test_31_set_motor_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_31_SET_MOTOR: requires motor_id and speed");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_INT && params[0].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_MOTOR: param[0] must be INT (motor_id)");
        return;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_MOTOR: param[1] must be INT (speed)");
        return;
    }
    
    int motor_id = (int)s_expr_param_get_int(&params[0]);
    int speed = (int)s_expr_param_get_int(&params[1]);
    
    printf("TEST_31_SET_MOTOR_ONESHOT: MOTOR[%d] = %d\n", motor_id, speed);
    
    // TODO: Actual motor control
    // set_motor_speed(motor_id, speed);
}

static void test_31_set_state_oneshot(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_31_SET_STATE: requires slot and value");
        return;
    }
    if (params[0].type != S_EXPR_PARAM_SLOT) {
        EXCEPTION("TEST_31_SET_STATE: param[0] must be SLOT");
        return;
    }
    if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_STATE: param[1] must be INT or UINT");
        return;
    }
    
    int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
    if (!slot_ptr) {
        EXCEPTION("TEST_31_SET_STATE: invalid slot");
        return;
    }
    *slot_ptr = (int32_t)s_expr_param_get_int(&params[1]);
    printf("TEST_31_SET_STATE: slot_ptr: %p, value: %d\n", slot_ptr, (int32_t)s_expr_param_get_int(&params[1]));
    
}

static s_expr_result_t test_31_set_state_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;

    if (event_id == S_EXPR_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("TEST_31_SET_STATE: requires slot and value");
            return SE_CONTINUE;
        }
        if (params[0].type != S_EXPR_PARAM_SLOT) {
            EXCEPTION("TEST_31_SET_STATE: param[0] must be SLOT");
            return SE_CONTINUE;
        }
        if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
            EXCEPTION("TEST_31_SET_STATE: param[1] must be INT or UINT");
            return SE_CONTINUE;
        }
        
        int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
        if (!slot_ptr) {
            EXCEPTION("TEST_31_SET_STATE: invalid slot");
            return SE_CONTINUE;
        }
        
        *slot_ptr = (int32_t)s_expr_param_get_int(&params[1]);
        printf("TEST_31_SET_STATE_MAIN: slot_ptr: %p, value: %d\n", slot_ptr, (int32_t)s_expr_param_get_int(&params[1]));
        
        return SE_CONTINUE;
    }
    //printf("TEST_31_SET_STATE: invalid event_id: %d\n", event_id);
    return SE_CONTINUE;  // Done, continue to next

}

static s_expr_result_t test_31_set_motor_main(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;

    if (event_id == S_EXPR_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("TEST_31_SET_STATE: requires slot and value");
            return SE_CONTINUE;
        }
        if (params[0].type != S_EXPR_PARAM_SLOT) {
            EXCEPTION("TEST_31_SET_STATE: param[0] must be SLOT");
            return SE_CONTINUE;
        }
        if (params[1].type != S_EXPR_PARAM_INT && params[1].type != S_EXPR_PARAM_UINT) {
            EXCEPTION("TEST_31_SET_STATE: param[1] must be INT or UINT");
            return SE_CONTINUE;
        }
        
        int32_t* slot_ptr = (int32_t*)s_expr_tree_get_pool_slot(inst, &params[0], sizeof(int32_t));
        if (!slot_ptr) {
            EXCEPTION("TEST_31_SET_STATE: invalid slot");
            return SE_CONTINUE;
        }
        
        int motor_id = (int)s_expr_param_get_int(&params[0]);
        int speed = (int)s_expr_param_get_int(&params[1]);
        
        printf("TEST_31_SET_MOTOR_MAIN: MOTOR[%d] = %d\n", motor_id, speed);
        
        return SE_CONTINUE;
    }
    //printf("TEST_31_SET_MOTOR: invalid event_id: %d\n", event_id);
    return SE_CONTINUE;  // Done, continue to next

}
#ifdef __cplusplus
}
#endif
#endif


