// ============================================================================
// test_31_user_functions.c
// Test User Functions for S-Expression Engine
// Updated for new API
// ============================================================================

#include "cfl_runtime.h"
#include "cfl_engine.h"

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_common_function_headers.h"
#include <stdio.h>

// ============================================================================
// TEST_31_SET_MOTOR: Set motor speed
// Params: [0] = motor_id (int/uint)
//         [1] = speed (int/uint)
// ============================================================================

void test_31_set_motor_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_31_SET_MOTOR: requires motor_id and speed");
        return;
    }
    
    uint8_t type0 = s_expr_param_opcode(&params[0]);
    uint8_t type1 = s_expr_param_opcode(&params[1]);
    
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_MOTOR: param[0] must be INT or UINT (motor_id)");
        return;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_MOTOR: param[1] must be INT or UINT (speed)");
        return;
    }
    
    int motor_id = (int)s_expr_param_int(&params[0]);
    int speed = (int)s_expr_param_int(&params[1]);
    
    printf("TEST_31_SET_MOTOR: MOTOR[%d] = %d\n", motor_id, speed);
    
    // TODO: Actual motor control
    // set_motor_speed(motor_id, speed);
}

// ============================================================================
// TEST_31_SET_STATE: Set field value in blackboard
// Params: [0] = field_ref (target field)
//         [1] = value (int/uint)
// ============================================================================

void test_31_set_state_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_31_SET_STATE: requires field and value");
        return;
    }
    
    uint8_t type0 = s_expr_param_opcode(&params[0]);
    uint8_t type1 = s_expr_param_opcode(&params[1]);
    
    if (type0 != S_EXPR_PARAM_FIELD) {
        EXCEPTION("TEST_31_SET_STATE: param[0] must be FIELD");
        return;
    }
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("TEST_31_SET_STATE: param[1] must be INT or UINT");
        return;
    }
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) {
        EXCEPTION("TEST_31_SET_STATE: invalid field");
        return;
    }
    
    int32_t value = (int32_t)s_expr_param_int(&params[1]);
    printf("TEST_31_SET_STATE: value = %p, %d\n", (void*)field_ptr, value);
    *field_ptr = value;
    
    printf("TEST_31_SET_STATE: field_ptr: %p, value: %d\n", (void*)field_ptr, value);
}