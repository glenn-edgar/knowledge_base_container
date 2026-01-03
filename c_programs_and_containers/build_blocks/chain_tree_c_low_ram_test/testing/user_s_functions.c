#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include "cfl_runtime.h"
#include "cfl_engine.h"

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
void test_30_set_state_oneshot(
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
    
    uint8_t type0 = s_expr_param_opcode(&params[0]);
    uint8_t type1 = s_expr_param_opcode(&params[1]);
    
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
// TEST_29_SET_STATE_ONESHOT
// DSL: m_call("TEST_29_SET_STATE_ONESHOT") field_ref("children_active") int(1) end_call(...)
// ----------------------------------------------------------------------------
void test_29_set_state_oneshot(
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
    
    uint8_t type0 = s_expr_param_opcode(&params[0]);
    uint8_t type1 = s_expr_param_opcode(&params[1]);
    
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