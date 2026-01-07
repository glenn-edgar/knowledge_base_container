// ============================================================================
// chain_flow_dsl_tests.h
// Generated S-expression module for chain_flow_dsl_tests
// DO NOT EDIT
// ============================================================================

#ifndef CHAIN_FLOW_DSL_TESTS_H
#define CHAIN_FLOW_DSL_TESTS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "s_engine_types.h"
#include "chain_flow_dsl_tests_records.h"

// Module: chain_flow_dsl_tests
#define CHAIN_FLOW_DSL_TESTS_NAME_HASH 0x518C2FF8
#define CHAIN_FLOW_DSL_TESTS_TREE_COUNT 11
#define CHAIN_FLOW_DSL_TESTS_RECORD_COUNT 20

// String table
static const char* const chain_flow_dsl_tests_strings[] = {
    "Moving forward",
    "Moving backward",
    "Turning left",
    "Turning right",
    "Stopping",
    "Idle - SHOULD NOT HAPPEN",
    "Timer expired",
    "Button pressed",
    "Sensor reading",
    "ALARM TRIGGERED",
    "ALARM",
    "ALARM SETTING",
    "Shutdown requested",
    "Setting motor position",
    "Reading motor position",
    "Test 34: Pointer field test starting",
    "Verifying pointer fields...",
    "Testing NULL pointer handling...",
    "Freeing allocated nodes...",
    "Test 34: PASSED",
    "Test 35: Linked list test starting",
    "Test 35: PASSED",
    "Test 36: Pointer sharing test starting",
    "Test 36: PASSED",
    "Test 37: Static buffer copy starting",
    "Static buffer copy verified",
    "Test 37: JSON sensor reads starting",
    "node_dict.column_data.user_data.sensors.temperature",
    "node_dict.column_data.user_data.sensors.pressure",
    "node_dict.column_data.user_data.sensors.humidity",
    "node_dict.column_data.user_data.sensors.timestamp",
    "JSON sensor reads verified",
    "Test 37: JSON device reads starting",
    "node_dict.column_data.user_data.device.name",
    "node_dict.column_data.user_data.device.serial",
    "node_dict.column_data.user_data.device.version",
    "node_dict.column_data.user_data.device.enabled",
    "TestDevice",
    "SN12345",
    "JSON device reads verified",
    "Test 37: JSON top-level reads starting",
    "node_dict.column_data.user_data.error_code",
    "node_dict.column_data.user_data.run_count",
    "JSON top-level reads verified",
    "Test 37: Final state dump",
    "Test 37: PASSED",
    "Test 37: String pointer read starting",
    "Test 38: PASS"
};
#define CHAIN_FLOW_DSL_TESTS_STRING_COUNT 48

// Function hashes
#define CFL_DISABLE_CHILDREN_HASH 0x96B35B00
#define CFL_ENABLE_CHILDREN_HASH 0x58FF45AE
#define TEST_30_SET_STATE_HASH 0xD7C5DCB8
#define CFL_ENABLE_CHILD_HASH 0x53B44B58
#define TEST_31_SET_STATE_HASH 0x0DE5642C
#define CFL_LOG_HASH 0x73F012F8
#define TEST_31_SET_MOTOR_HASH 0xF0C3831F
#define TEST_32_TOGGLE_LED_HASH 0xCA453200
#define TEST_32_ENABLE_BUZZER_HASH 0xD13797E0
#define TEST_32_SET_LED_HASH 0x6469118C
#define TEST_32_NOTIFY_SYSTEM_HASH 0xD526B22C
#define CFL_INTERNAL_EVENT_HASH 0x68B0B8EA
#define TEST_32_DISABLE_ALL_OUTPUTS_HASH 0xB44FA327
#define TEST_32_SAVE_STATE_HASH 0x7BAF1574
#define TEST_33_SET_VECTOR_HASH 0xAD7EE846
#define TEST_33_SET_PID_HASH 0xFFFA6650
#define TEST_33_SET_SYSTEM_HASH 0xE771B849
#define TEST_33_READ_VECTOR_HASH 0x09C2C184
#define TEST_33_READ_PID_HASH 0xD84FAC65
#define TEST_33_READ_SYSTEM_HASH 0xD6ED3B0B
#define TEST_34_SET_UINT32_HASH 0xA0C3577A
#define TEST_34_ALLOC_NODE_HASH 0xA516BD95
#define TEST_34_ALLOC_SENSOR_HASH 0x0655CD1A
#define TEST_34_SET_UINT16_HASH 0x4523168A
#define TEST_34_READ_NODE_HASH 0x8E6A6DA0
#define TEST_34_READ_SENSOR_HASH 0x6A9E1DB8
#define TEST_34_READ_UINT32_HASH 0x184BB6F2
#define TEST_34_READ_UINT16_HASH 0x73EBF7E2
#define TEST_34_CHECK_NULL_HASH 0xAF1C843B
#define TEST_34_FREE_PTR_HASH 0xBEA669B8
#define TEST_35_BUILD_LIST_HASH 0x329C95FC
#define TEST_35_TRAVERSE_LIST_HASH 0xEAC93218
#define TEST_35_FREE_LIST_HASH 0x56AA5289
#define TEST_36_COPY_PTR_HASH 0xF2C0FCAD
#define TEST_36_VERIFY_SAME_PTR_HASH 0xB51814DB
#define TEST_36_MODIFY_NODE_VALUE_HASH 0x7838A3E0
#define TEST_36_CLEAR_PTR_HASH 0x6019CAD8
#define TEST_37_COPY_STATIC_NETWORK_HASH 0x09200508
#define TEST_37_VERIFY_NETWORK_HASH 0xDB49DD96
#define CFL_JSON_READ_FLOAT_HASH 0xB4F112F8
#define CFL_JSON_READ_UINT_HASH 0xA8C85E66
#define TEST_37_VERIFY_SENSORS_HASH 0xBD841C85
#define CFL_JSON_READ_STRING_BUF_HASH 0x717E5F77
#define CFL_JSON_READ_BOOL_HASH 0x7ED0DC50
#define TEST_37_VERIFY_DEVICE_NAME_HASH 0x71353D27
#define TEST_37_VERIFY_DEVICE_SERIAL_HASH 0x92FD253C
#define TEST_37_VERIFY_DEVICE_INFO_HASH 0xBA892338
#define TEST_37_VERIFY_TOP_LEVEL_HASH 0x7BB221C8
#define TEST_37_DUMP_STATE_HASH 0xA531D13D
#define CFL_JSON_READ_STRING_PTR_HASH 0x6A663D59
#define TEST_37_VERIFY_STRING_PTR_HASH 0x098AD0C8
#define CFL_COPY_CONST_FULL_HASH 0xF2E7B527
#define TEST_38_VERIFY_DEFAULTS_HASH 0xB6F4D06F
#define CFL_COPY_CONST_HASH 0x713F7B20
#define TEST_38_VERIFY_TEST_PID_HASH 0x5DA0BC52
#define TEST_39_VERIFY_GAINS_HASH 0xD22E5D7D
#define TEST_39_VERIFY_POINTER_HASH 0xC7569358
#define CFL_TRIGGER_ON_CHANGE_HASH 0x62D8C8B8
#define CFL_STATE_MACHINE_HASH 0x45E8FA90
#define CFL_STATE_ACTIONS_HASH 0xC9B2C065
#define CFL_TICK_DELAY_HASH 0xB6A5415F
#define CFL_FIELD_DISPATCH_HASH 0x5F0EC2C0
#define CFL_EVENT_DISPATCH_HASH 0xFCA1669E
#define CFL_PIPELINE_HASH 0x59A9051E
#define TEST_32_PROCESS_SCHEDULED_TASKS_HASH 0x9F3DA300
#define CFL_WAIT_EVENT_HASH 0xBBAADC0D
#define TEST_32_CHECK_THRESHOLD_HASH 0x24D11DA8
#define TEST_32_GENERATE_INTERNAL_EVENTS_HASH 0x852EC798
#define TEST_32_RUN_BACKGROUND_TASKS_HASH 0xCCAE740A
#define CFL_S_BIT_OR_HASH 0x3AABCECC
#define CFL_S_BIT_AND_HASH 0x4A2A30C0

// Tree hashes
#define S_EXPRESSION_TEST_2_HASH 0xA2DA4748
#define S_EXPRESSION_TEST_4_HASH 0x9EDA40FC
#define S_EXPRESSION_TEST_7_HASH 0x9CDA3DD6
#define S_EXPRESSION_TEST_8_HASH 0x96DA3464
#define S_EXPRESSION_TEST_10_HASH 0x53299B60
#define S_EXPRESSION_TEST_11_HASH 0x52A99A98
#define S_EXPRESSION_TEST_12_HASH 0x54299CF4
#define S_EXPRESSION_TEST_13_HASH 0x53A99C2A
#define S_EXPRESSION_TEST_14_HASH 0x5129983C
#define S_EXPRESSION_TEST_15_HASH 0x50A99772
#define S_EXPRESSION_TEST_16_HASH 0x522999CE

// Record hashes
#define TEST2_BLACKBOARD_HASH 0x330FEDDE
#define STATE_MACHINE_BLACKBOARD_HASH 0x2CC9216E
#define ROBOT_BLACKBOARD_HASH 0x2C80411F
#define EVENT_BLACKBOARD_HASH 0xF490C889
#define VECTOR3D_HASH 0xEE4485D0
#define PID_GAINS_HASH 0xAAB42556
#define MOTOR_STATE_HASH 0xDD680D86
#define SYSTEM_STATE_HASH 0x8D9A7288
#define NODE_DATA_HASH 0xF7E9D986
#define LIST_NODE_HASH 0x95679C0E
#define SENSOR_READING_HASH 0xC808EDEA
#define SYSTEM_CONTEXT_HASH 0x27358698
#define NETWORK_CONFIG_A_HASH 0x072BF9F0
#define SENSOR_DATA_A_HASH 0xF065D91C
#define DEVICE_INFO_A_HASH 0x8B070F95
#define SYSTEM_STATE_A_HASH 0xBFE3C0EA
#define PID_GAINS_A_HASH 0x079394EC
#define TEST38_BB_HASH 0xB8369AAC
#define PID_GAINS_C_HASH 0x081395B5
#define TEST39_BB_HASH 0x7E1A6D26

// Field hashes
#define FIELD_PLACEHOLDER_HASH 0x876567EA
#define FIELD_STATE_HASH 0x4F339CC2
#define FIELD_STATE_B_HASH 0x4150739E
#define FIELD_COMMAND_HASH 0x648A9C46
#define FIELD_TIMER_COUNT_HASH 0xE4DA7557
#define FIELD_SENSOR_VALUE_HASH 0x1712128C
#define FIELD_EVENT_ID_HASH 0xDD069C70
#define FIELD_X_HASH 0x80BCEBDE
#define FIELD_Y_HASH 0x80FCEC43
#define FIELD_Z_HASH 0x803CEB15
#define FIELD_KP_HASH 0x2E53DFA0
#define FIELD_KI_HASH 0x3013E261
#define FIELD_KD_HASH 0x3353E77E
#define FIELD_POSITION_HASH 0x54A50DB1
#define FIELD_VELOCITY_HASH 0x4B293318
#define FIELD_TORQUE_HASH 0xF26BC9EE
#define FIELD_ENABLED_HASH 0x81F054FF
#define FIELD_MOTOR_HASH 0x981ADD68
#define FIELD_PID_HASH 0xEEC46943
#define FIELD_SYSTEM_TIME_HASH 0xFF812DC8
#define FIELD_ERROR_CODE_HASH 0x114B9480
#define FIELD_ID_HASH 0xAEB36604
#define FIELD_VALUE_HASH 0x8E10B40E
#define FIELD_FLAGS_HASH 0x37B48012
#define FIELD_DATA_HASH 0x3E1C3930
#define FIELD_NEXT_HASH 0x2BFEE6EA
#define FIELD_TIMESTAMP_HASH 0xEAD480D9
#define FIELD_TEMPERATURE_HASH 0x52076A11
#define FIELD_PRESSURE_HASH 0x70D9869A
#define FIELD_HUMIDITY_HASH 0x88085588
#define FIELD_SYSTEM_ID_HASH 0x9E0E473E
#define FIELD_PRIMARY_NODE_HASH 0x0F813270
#define FIELD_BACKUP_NODE_HASH 0x259F62B4
#define FIELD_SENSOR_HASH 0xEAF53574
#define FIELD_TASK_LIST_HASH 0x9ECD7014
#define FIELD_NODE_COUNT_HASH 0x8170D6BA
#define FIELD_IP_ADDR_HASH 0x217E84AC
#define FIELD_PORT_HASH 0xF1C607BE
#define FIELD_TIMEOUT_MS_HASH 0x92AE8024
#define FIELD_NAME_HASH 0xC4205608
#define FIELD_SERIAL_HASH 0x7F80A836
#define FIELD_VERSION_HASH 0x8D29BAD4
#define FIELD_NETWORK_HASH 0x9D0BAFE0
#define FIELD_SENSORS_HASH 0x3813D028
#define FIELD_DEVICE_HASH 0x65ECEE22
#define FIELD_RUN_COUNT_HASH 0xFE7705AE
#define FIELD_DEVICE_PTR_HASH 0x968B47BF
#define FIELD_GAINS_HASH 0x155407AC
#define FIELD_VERIFIED_HASH 0xFB1E6007
#define FIELD_GAINS_PTR_HASH 0xF6E20F50

// Constants
// Constant: test_pid (type=pid_gains_a)
static const pid_gains_a_t test_pid = {
    {0}  // Use binary data for actual initialization
};
#define TEST_PID_HASH 0xA9D9F984

// Constant: test38_defaults (type=test38_bb)
static const test38_bb_t test38_defaults = {
    {0}  // Use binary data for actual initialization
};
#define TEST38_DEFAULTS_HASH 0xC97BD98C

#ifdef __cplusplus
}
#endif

#endif // CHAIN_FLOW_DSL_TESTS_H
