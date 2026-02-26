// ============================================================================
// chain_flow_dsl_tests_user_functions.h
// User function prototypes for chain_flow_dsl_tests
// DO NOT EDIT
// ============================================================================

#ifndef CHAIN_FLOW_DSL_TESTS_USER_FUNCTIONS_H
#define CHAIN_FLOW_DSL_TESTS_USER_FUNCTIONS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "s_engine_types.h"

// Oneshot functions
void CFL_DISABLE_CHILDREN(s_engine_ctx_t* ctx);
void CFL_ENABLE_CHILDREN(s_engine_ctx_t* ctx);
void TEST_30_SET_STATE(s_engine_ctx_t* ctx);
void CFL_ENABLE_CHILD(s_engine_ctx_t* ctx);
void TEST_31_SET_STATE(s_engine_ctx_t* ctx);
void CFL_LOG(s_engine_ctx_t* ctx);
void TEST_31_SET_MOTOR(s_engine_ctx_t* ctx);
void TEST_32_TOGGLE_LED(s_engine_ctx_t* ctx);
void TEST_32_ENABLE_BUZZER(s_engine_ctx_t* ctx);
void TEST_32_SET_LED(s_engine_ctx_t* ctx);
void TEST_32_NOTIFY_SYSTEM(s_engine_ctx_t* ctx);
void CFL_INTERNAL_EVENT(s_engine_ctx_t* ctx);
void TEST_32_DISABLE_ALL_OUTPUTS(s_engine_ctx_t* ctx);
void TEST_32_SAVE_STATE(s_engine_ctx_t* ctx);
void TEST_33_SET_VECTOR(s_engine_ctx_t* ctx);
void TEST_33_SET_PID(s_engine_ctx_t* ctx);
void TEST_33_SET_SYSTEM(s_engine_ctx_t* ctx);
void TEST_33_READ_VECTOR(s_engine_ctx_t* ctx);
void TEST_33_READ_PID(s_engine_ctx_t* ctx);
void TEST_33_READ_SYSTEM(s_engine_ctx_t* ctx);
void TEST_34_SET_UINT32(s_engine_ctx_t* ctx);
void TEST_34_ALLOC_NODE(s_engine_ctx_t* ctx);
void TEST_34_ALLOC_SENSOR(s_engine_ctx_t* ctx);
void TEST_34_SET_UINT16(s_engine_ctx_t* ctx);
void TEST_34_READ_NODE(s_engine_ctx_t* ctx);
void TEST_34_READ_SENSOR(s_engine_ctx_t* ctx);
void TEST_34_READ_UINT32(s_engine_ctx_t* ctx);
void TEST_34_READ_UINT16(s_engine_ctx_t* ctx);
void TEST_34_CHECK_NULL(s_engine_ctx_t* ctx);
void TEST_34_FREE_PTR(s_engine_ctx_t* ctx);
void TEST_35_BUILD_LIST(s_engine_ctx_t* ctx);
void TEST_35_TRAVERSE_LIST(s_engine_ctx_t* ctx);
void TEST_35_FREE_LIST(s_engine_ctx_t* ctx);
void TEST_36_COPY_PTR(s_engine_ctx_t* ctx);
void TEST_36_VERIFY_SAME_PTR(s_engine_ctx_t* ctx);
void TEST_36_MODIFY_NODE_VALUE(s_engine_ctx_t* ctx);
void TEST_36_CLEAR_PTR(s_engine_ctx_t* ctx);
void TEST_37_COPY_STATIC_NETWORK(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_NETWORK(s_engine_ctx_t* ctx);
void CFL_JSON_READ_FLOAT(s_engine_ctx_t* ctx);
void CFL_JSON_READ_UINT(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_SENSORS(s_engine_ctx_t* ctx);
void CFL_JSON_READ_STRING_BUF(s_engine_ctx_t* ctx);
void CFL_JSON_READ_BOOL(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_DEVICE_NAME(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_DEVICE_SERIAL(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_DEVICE_INFO(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_TOP_LEVEL(s_engine_ctx_t* ctx);
void TEST_37_DUMP_STATE(s_engine_ctx_t* ctx);
void CFL_JSON_READ_STRING_PTR(s_engine_ctx_t* ctx);
void TEST_37_VERIFY_STRING_PTR(s_engine_ctx_t* ctx);
void CFL_COPY_CONST_FULL(s_engine_ctx_t* ctx);
void TEST_38_VERIFY_DEFAULTS(s_engine_ctx_t* ctx);
void CFL_COPY_CONST(s_engine_ctx_t* ctx);
void TEST_38_VERIFY_TEST_PID(s_engine_ctx_t* ctx);
void TEST_39_VERIFY_GAINS(s_engine_ctx_t* ctx);
void TEST_39_VERIFY_POINTER(s_engine_ctx_t* ctx);

// Main functions
s_result_t CFL_TRIGGER_ON_CHANGE(s_engine_ctx_t* ctx);
s_result_t CFL_STATE_MACHINE(s_engine_ctx_t* ctx);
s_result_t CFL_STATE_ACTIONS(s_engine_ctx_t* ctx);
s_result_t CFL_TICK_DELAY(s_engine_ctx_t* ctx);
s_result_t CFL_FIELD_DISPATCH(s_engine_ctx_t* ctx);
s_result_t CFL_EVENT_DISPATCH(s_engine_ctx_t* ctx);
s_result_t CFL_PIPELINE(s_engine_ctx_t* ctx);
s_result_t TEST_32_PROCESS_SCHEDULED_TASKS(s_engine_ctx_t* ctx);
s_result_t CFL_WAIT_EVENT(s_engine_ctx_t* ctx);
s_result_t TEST_32_CHECK_THRESHOLD(s_engine_ctx_t* ctx);
s_result_t TEST_32_GENERATE_INTERNAL_EVENTS(s_engine_ctx_t* ctx);
s_result_t TEST_32_RUN_BACKGROUND_TASKS(s_engine_ctx_t* ctx);

// Predicate functions
bool CFL_S_BIT_OR(s_engine_ctx_t* ctx);
bool CFL_S_BIT_AND(s_engine_ctx_t* ctx);

#ifdef __cplusplus
}
#endif

#endif // CHAIN_FLOW_DSL_TESTS_USER_FUNCTIONS_H
