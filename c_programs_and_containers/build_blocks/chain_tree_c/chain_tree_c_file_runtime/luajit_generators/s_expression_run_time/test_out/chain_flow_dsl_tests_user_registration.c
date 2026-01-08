// ============================================================================
// chain_flow_dsl_tests_user_registration.c
// User function registration for chain_flow_dsl_tests
// ============================================================================

#include "chain_flow_dsl_tests.h"
#include "chain_flow_dsl_tests_user_functions.h"

void chain_flow_dsl_tests_register_functions(s_engine_t* engine) {
    s_engine_register_oneshot(engine, 0x4E007ACA, CFL_DISABLE_CHILDREN);
    s_engine_register_oneshot(engine, 0x00000000, CFL_ENABLE_CHILDREN);
    s_engine_register_oneshot(engine, 0x00000000, TEST_30_SET_STATE);
    s_engine_register_oneshot(engine, 0x44006B0C, CFL_ENABLE_CHILD);
    s_engine_register_oneshot(engine, 0x00000000, TEST_31_SET_STATE);
    s_engine_register_oneshot(engine, 0x00000000, CFL_LOG);
    s_engine_register_oneshot(engine, 0x00000000, TEST_31_SET_MOTOR);
    s_engine_register_oneshot(engine, 0x44006B0C, TEST_32_TOGGLE_LED);
    s_engine_register_oneshot(engine, 0x00000000, TEST_32_ENABLE_BUZZER);
    s_engine_register_oneshot(engine, 0x00000000, TEST_32_SET_LED);
    s_engine_register_oneshot(engine, 0x00000000, TEST_32_NOTIFY_SYSTEM);
    s_engine_register_oneshot(engine, 0x5400843C, CFL_INTERNAL_EVENT);
    s_engine_register_oneshot(engine, 0x00000000, TEST_32_DISABLE_ALL_OUTPUTS);
    s_engine_register_oneshot(engine, 0x45006C9F, TEST_32_SAVE_STATE);
    s_engine_register_oneshot(engine, 0x00000000, TEST_33_SET_VECTOR);
    s_engine_register_oneshot(engine, 0x44006B0C, TEST_33_SET_PID);
    s_engine_register_oneshot(engine, 0x00000000, TEST_33_SET_SYSTEM);
    s_engine_register_oneshot(engine, 0x52008116, TEST_33_READ_VECTOR);
    s_engine_register_oneshot(engine, 0x00000000, TEST_33_READ_PID);
    s_engine_register_oneshot(engine, 0x4D007937, TEST_33_READ_SYSTEM);
    s_engine_register_oneshot(engine, 0xC47EB1A0, TEST_34_SET_UINT32);
    s_engine_register_oneshot(engine, 0x45006C9F, TEST_34_ALLOC_NODE);
    s_engine_register_oneshot(engine, 0x52008116, TEST_34_ALLOC_SENSOR);
    s_engine_register_oneshot(engine, 0x38795810, TEST_34_SET_UINT16);
    s_engine_register_oneshot(engine, 0x45006C9F, TEST_34_READ_NODE);
    s_engine_register_oneshot(engine, 0x52008116, TEST_34_READ_SENSOR);
    s_engine_register_oneshot(engine, 0x32004EB6, TEST_34_READ_UINT32);
    s_engine_register_oneshot(engine, 0x36005502, TEST_34_READ_UINT16);
    s_engine_register_oneshot(engine, 0x00000000, TEST_34_CHECK_NULL);
    s_engine_register_oneshot(engine, 0x52008116, TEST_34_FREE_PTR);
    s_engine_register_oneshot(engine, 0x5400843C, TEST_35_BUILD_LIST);
    s_engine_register_oneshot(engine, 0x00000000, TEST_35_TRAVERSE_LIST);
    s_engine_register_oneshot(engine, 0x00000000, TEST_35_FREE_LIST);
    s_engine_register_oneshot(engine, 0x52008116, TEST_36_COPY_PTR);
    s_engine_register_oneshot(engine, 0x00000000, TEST_36_VERIFY_SAME_PTR);
    s_engine_register_oneshot(engine, 0x00000000, TEST_36_MODIFY_NODE_VALUE);
    s_engine_register_oneshot(engine, 0x00000000, TEST_36_CLEAR_PTR);
    s_engine_register_oneshot(engine, 0x4B007611, TEST_37_COPY_STATIC_NETWORK);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_NETWORK);
    s_engine_register_oneshot(engine, 0x00000000, CFL_JSON_READ_FLOAT);
    s_engine_register_oneshot(engine, 0x5400843C, CFL_JSON_READ_UINT);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_SENSORS);
    s_engine_register_oneshot(engine, 0x46006E32, CFL_JSON_READ_STRING_BUF);
    s_engine_register_oneshot(engine, 0x4C0077A4, CFL_JSON_READ_BOOL);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_DEVICE_NAME);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_DEVICE_SERIAL);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_DEVICE_INFO);
    s_engine_register_oneshot(engine, 0x00000000, TEST_37_VERIFY_TOP_LEVEL);
    s_engine_register_oneshot(engine, 0x45006C9F, TEST_37_DUMP_STATE);
    s_engine_register_oneshot(engine, 0x52008116, CFL_JSON_READ_STRING_PTR);
    s_engine_register_oneshot(engine, 0x52008116, TEST_37_VERIFY_STRING_PTR);
    s_engine_register_oneshot(engine, 0x00000000, CFL_COPY_CONST_FULL);
    s_engine_register_oneshot(engine, 0x530082A9, TEST_38_VERIFY_DEFAULTS);
    s_engine_register_oneshot(engine, 0x5400843C, CFL_COPY_CONST);
    s_engine_register_oneshot(engine, 0x44006B0C, TEST_38_VERIFY_TEST_PID);
    s_engine_register_oneshot(engine, 0x00000000, TEST_39_VERIFY_GAINS);
    s_engine_register_oneshot(engine, 0x00000000, TEST_39_VERIFY_POINTER);
    s_engine_register_main(engine, 0x00000000, CFL_TRIGGER_ON_CHANGE);
    s_engine_register_main(engine, 0x00000000, CFL_STATE_MACHINE);
    s_engine_register_main(engine, 0x00000000, CFL_STATE_ACTIONS);
    s_engine_register_main(engine, 0x59008C1B, CFL_TICK_DELAY);
    s_engine_register_main(engine, 0x48007158, CFL_FIELD_DISPATCH);
    s_engine_register_main(engine, 0x48007158, CFL_EVENT_DISPATCH);
    s_engine_register_main(engine, 0x45006C9F, CFL_PIPELINE);
    s_engine_register_main(engine, 0x00000000, TEST_32_PROCESS_SCHEDULED_TASKS);
    s_engine_register_main(engine, 0x5400843C, CFL_WAIT_EVENT);
    s_engine_register_main(engine, 0x00000000, TEST_32_CHECK_THRESHOLD);
    s_engine_register_main(engine, 0x530082A9, TEST_32_GENERATE_INTERNAL_EVENTS);
    s_engine_register_main(engine, 0x530082A9, TEST_32_RUN_BACKGROUND_TASKS);
    s_engine_register_pred(engine, 0x52008116, CFL_S_BIT_OR);
    s_engine_register_pred(engine, 0x00000000, CFL_S_BIT_AND);
}
