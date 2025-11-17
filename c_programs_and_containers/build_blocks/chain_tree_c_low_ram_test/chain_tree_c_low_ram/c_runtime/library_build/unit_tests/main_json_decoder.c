/* test_json_decoder.c */

#include "json_node_decoder.h"
#include <stdio.h>
#include <string.h>
#include <math.h>

/* ============================================================================
 * Test Statistics
 * ============================================================================ */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_START(name) \
    do { \
        printf("\n=== Testing: %s ===\n", name); \
        tests_run++; \
    } while(0)

#define ASSERT(condition, message) \
    do { \
        if (condition) { \
            printf("  ✓ PASS: %s\n", message); \
            tests_passed++; \
        } else { \
            printf("  ✗ FAIL: %s\n", message); \
            tests_failed++; \
        } \
    } while(0)

#define ASSERT_EQ(actual, expected, message) \
    ASSERT((actual) == (expected), message)

#define ASSERT_STREQ(actual, expected, message) \
    ASSERT(strcmp((actual), (expected)) == 0, message)

#define ASSERT_DOUBLE_EQ(actual, expected, message) \
    ASSERT(fabs((actual) - (expected)) < 0.0001, message)

/* ============================================================================
 * Mock Preprocessed JSON Data
 * Simulates output from yaml_to_c translator
 * 
 * Represents this JSON structure:
 * {
 *   "name": "motor_controller",
 *   "enabled": true,
 *   "speed": 1500,
 *   "temperature": 45.5,
 *   "settings": {
 *     "pid": {
 *       "kp": 1.2,
 *       "ki": 0.5,
 *       "kd": 0.1
 *     },
 *     "limits": {
 *       "min_speed": 100,
 *       "max_speed": 3000
 *     }
 *   },
 *   "sensors": [
 *     {"id": 1, "type": "temp", "value": 45.5},
 *     {"id": 2, "type": "current", "value": 2.3},
 *     {"id": 3, "type": "voltage", "value": 12.1}
 *   ],
 *   "status": null,
 *   "empty_array": [],
 *   "empty_object": {}
 * }
 * ============================================================================ */

/* String table - all strings packed together */
static const char test_strings[] = 
    /* 0 */ "name\0"
    /* 5 */ "motor_controller\0"
    /* 22 */ "enabled\0"
    /* 30 */ "speed\0"
    /* 36 */ "temperature\0"
    /* 48 */ "settings\0"
    /* 57 */ "pid\0"
    /* 61 */ "kp\0"
    /* 64 */ "ki\0"
    /* 67 */ "kd\0"
    /* 70 */ "limits\0"
    /* 77 */ "min_speed\0"
    /* 87 */ "max_speed\0"
    /* 97 */ "sensors\0"
    /* 105 */ "id\0"
    /* 108 */ "type\0"
    /* 113 */ "value\0"
    /* 119 */ "temp\0"
    /* 124 */ "current\0"
    /* 132 */ "voltage\0"
    /* 140 */ "status\0"
    /* 147 */ "empty_array\0"
    /* 159 */ "empty_object\0";

#define STR_OFFSET_NAME              0
#define STR_OFFSET_MOTOR_CONTROLLER  5
#define STR_OFFSET_ENABLED           22
#define STR_OFFSET_SPEED             30
#define STR_OFFSET_TEMPERATURE       36
#define STR_OFFSET_SETTINGS          48
#define STR_OFFSET_PID               57
#define STR_OFFSET_KP                61
#define STR_OFFSET_KI                64
#define STR_OFFSET_KD                67
#define STR_OFFSET_LIMITS            70
#define STR_OFFSET_MIN_SPEED         77
#define STR_OFFSET_MAX_SPEED         87
#define STR_OFFSET_SENSORS           97
#define STR_OFFSET_ID                105
#define STR_OFFSET_TYPE              108
#define STR_OFFSET_VALUE             113
#define STR_OFFSET_TEMP              119
#define STR_OFFSET_CURRENT           124
#define STR_OFFSET_VOLTAGE           132
#define STR_OFFSET_STATUS            140
#define STR_OFFSET_EMPTY_ARRAY       147
#define STR_OFFSET_EMPTY_OBJECT      159

/* Preprocessed records array */
static const json_record_t test_records[] = {
    /* Record 0: Root object */
    {
        .key_offset = 0,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 1, .child_count = 9 }
    },
    
    /* Record 1: "name": "motor_controller" */
    {
        .key_offset = STR_OFFSET_NAME,
        .type = JSON_TYPE_STRING,
        .flags = 0,
        .value.string_offset = STR_OFFSET_MOTOR_CONTROLLER
    },
    
    /* Record 2: "enabled": true */
    {
        .key_offset = STR_OFFSET_ENABLED,
        .type = JSON_TYPE_BOOL,
        .flags = 0,
        .value.bool_val = true
    },
    
    /* Record 3: "speed": 1500 */
    {
        .key_offset = STR_OFFSET_SPEED,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 1500
    },
    
    /* Record 4: "temperature": 45.5 */
    {
        .key_offset = STR_OFFSET_TEMPERATURE,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 45.5
    },
    
    /* Record 5: "settings": {...} */
    {
        .key_offset = STR_OFFSET_SETTINGS,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 10, .child_count = 2 }
    },
    
    /* Record 6: "sensors": [...] */
    {
        .key_offset = STR_OFFSET_SENSORS,
        .type = JSON_TYPE_ARRAY,
        .flags = 0,
        .value.container = { .first_child = 19, .child_count = 3 }
    },
    
    /* Record 7: "status": null */
    {
        .key_offset = STR_OFFSET_STATUS,
        .type = JSON_TYPE_NULL,
        .flags = 0,
        .value.int_val = 0
    },
    
    /* Record 8: "empty_array": [] */
    {
        .key_offset = STR_OFFSET_EMPTY_ARRAY,
        .type = JSON_TYPE_ARRAY,
        .flags = 0,
        .value.container = { .first_child = 0, .child_count = 0 }
    },
    
    /* Record 9: "empty_object": {} */
    {
        .key_offset = STR_OFFSET_EMPTY_OBJECT,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 0, .child_count = 0 }
    },
    
    /* Record 10: settings.pid */
    {
        .key_offset = STR_OFFSET_PID,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 12, .child_count = 3 }
    },
    
    /* Record 11: settings.limits */
    {
        .key_offset = STR_OFFSET_LIMITS,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 15, .child_count = 2 }
    },
    
    /* Record 12: settings.pid.kp */
    {
        .key_offset = STR_OFFSET_KP,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 1.2
    },
    
    /* Record 13: settings.pid.ki */
    {
        .key_offset = STR_OFFSET_KI,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 0.5
    },
    
    /* Record 14: settings.pid.kd */
    {
        .key_offset = STR_OFFSET_KD,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 0.1
    },
    
    /* Record 15: settings.limits.min_speed */
    {
        .key_offset = STR_OFFSET_MIN_SPEED,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 100
    },
    
    /* Record 16: settings.limits.max_speed */
    {
        .key_offset = STR_OFFSET_MAX_SPEED,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 3000
    },
    
    /* Record 17: Unused (padding for alignment) */
    {
        .key_offset = 0,
        .type = JSON_TYPE_NULL,
        .flags = 0,
        .value.int_val = 0
    },
    
    /* Record 18: Unused (padding for alignment) */
    {
        .key_offset = 0,
        .type = JSON_TYPE_NULL,
        .flags = 0,
        .value.int_val = 0
    },
    
    /* Record 19: sensors[0] - temperature sensor */
    {
        .key_offset = 0,  /* Array elements have no key */
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 22, .child_count = 3 }
    },
    
    /* Record 20: sensors[1] - current sensor */
    {
        .key_offset = 0,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 25, .child_count = 3 }
    },
    
    /* Record 21: sensors[2] - voltage sensor */
    {
        .key_offset = 0,
        .type = JSON_TYPE_OBJECT,
        .flags = 0,
        .value.container = { .first_child = 28, .child_count = 3 }
    },
    
    /* Record 22-24: sensors[0] fields */
    {
        .key_offset = STR_OFFSET_ID,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 1
    },
    {
        .key_offset = STR_OFFSET_TYPE,
        .type = JSON_TYPE_STRING,
        .flags = 0,
        .value.string_offset = STR_OFFSET_TEMP
    },
    {
        .key_offset = STR_OFFSET_VALUE,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 45.5
    },
    
    /* Record 25-27: sensors[1] fields */
    {
        .key_offset = STR_OFFSET_ID,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 2
    },
    {
        .key_offset = STR_OFFSET_TYPE,
        .type = JSON_TYPE_STRING,
        .flags = 0,
        .value.string_offset = STR_OFFSET_CURRENT
    },
    {
        .key_offset = STR_OFFSET_VALUE,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 2.3
    },
    
    /* Record 28-30: sensors[2] fields */
    {
        .key_offset = STR_OFFSET_ID,
        .type = JSON_TYPE_INT,
        .flags = 0,
        .value.int_val = 3
    },
    {
        .key_offset = STR_OFFSET_TYPE,
        .type = JSON_TYPE_STRING,
        .flags = 0,
        .value.string_offset = STR_OFFSET_VOLTAGE
    },
    {
        .key_offset = STR_OFFSET_VALUE,
        .type = JSON_TYPE_DOUBLE,
        .flags = 0,
        .value.double_val = 12.1
    }
};

static const record_control_t test_control = {
    .record_count = sizeof(test_records) / sizeof(test_records[0]),
    .string_table_size = sizeof(test_strings),
    .root_record = 0,
    .reserved = 0
};

#define TEST_ROOT_RECORD 0

/* ============================================================================
 * Test Functions
 * ============================================================================ */

void test_decoder_init(void) {
    TEST_START("json_decoder_init");
    
    json_decoder_ctx_t ctx;
    int result;
    
    /* Valid initialization */
    result = json_decoder_init(&ctx, test_records, &test_control, 
                               test_strings, TEST_ROOT_RECORD);
    ASSERT_EQ(result, JSON_OK, "Valid initialization");
    ASSERT_EQ(ctx.current_record, TEST_ROOT_RECORD, "Current record set correctly");
    ASSERT(ctx.records == test_records, "Records pointer set");
    ASSERT(ctx.control == &test_control, "Control pointer set");
    ASSERT(ctx.strings == test_strings, "Strings pointer set");
    
    /* NULL pointer tests */
    result = json_decoder_init(NULL, test_records, &test_control, 
                               test_strings, TEST_ROOT_RECORD);
    ASSERT_EQ(result, JSON_ERR_NULL_PTR, "NULL context rejected");
    
    result = json_decoder_init(&ctx, NULL, &test_control, 
                               test_strings, TEST_ROOT_RECORD);
    ASSERT_EQ(result, JSON_ERR_NULL_PTR, "NULL records rejected");
    
    /* Invalid record number */
    result = json_decoder_init(&ctx, test_records, &test_control, 
                               test_strings, 999);
    ASSERT_EQ(result, JSON_ERR_INVALID_RECORD, "Invalid record number rejected");
}

void test_get_record_and_string(void) {
    TEST_START("json_get_record and json_get_string");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Valid record access */
    const json_record_t *record = json_get_record(&ctx, 0);
    ASSERT(record != NULL, "Get valid record 0");
    ASSERT_EQ(record->type, JSON_TYPE_OBJECT, "Record 0 is object type");
    
    record = json_get_record(&ctx, 1);
    ASSERT(record != NULL, "Get valid record 1");
    
    /* Out of bounds */
    record = json_get_record(&ctx, test_control.record_count);
    ASSERT(record == NULL, "Out of bounds record returns NULL");
    
    /* Valid string access */
    const char *str = json_get_string(&ctx, STR_OFFSET_NAME);
    ASSERT(str != NULL, "Get valid string");
    ASSERT_STREQ(str, "name", "String content correct");
    
    str = json_get_string(&ctx, STR_OFFSET_MOTOR_CONTROLLER);
    ASSERT_STREQ(str, "motor_controller", "String 'motor_controller' correct");
    
    /* Out of bounds string */
    str = json_get_string(&ctx, test_control.string_table_size);
    ASSERT(str == NULL, "Out of bounds string returns NULL");
}

void test_type_extraction(void) {
    TEST_START("Type-safe value extraction");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Boolean extraction */
    bool bool_val;
    int result = json_get_bool(&ctx, 2, &bool_val);  /* "enabled": true */
    ASSERT_EQ(result, JSON_OK, "Extract boolean success");
    ASSERT(bool_val == true, "Boolean value correct");
    
    /* Integer extraction */
    int64_t int_val;
    result = json_get_int(&ctx, 3, &int_val);  /* "speed": 1500 */
    ASSERT_EQ(result, JSON_OK, "Extract integer success");
    ASSERT_EQ(int_val, 1500, "Integer value correct");
    
    /* Double extraction */
    double double_val;
    result = json_get_double(&ctx, 4, &double_val);  /* "temperature": 45.5 */
    ASSERT_EQ(result, JSON_OK, "Extract double success");
    ASSERT_DOUBLE_EQ(double_val, 45.5, "Double value correct");
    
    /* String extraction */
    const char *str_val;
    result = json_get_string_value(&ctx, 1, &str_val);  /* "name": "motor_controller" */
    ASSERT_EQ(result, JSON_OK, "Extract string success");
    ASSERT_STREQ(str_val, "motor_controller", "String value correct");
    
    /* Null check */
    bool is_null = json_is_null(&ctx, 7);  /* "status": null */
    ASSERT(is_null, "Null detection works");
    
    is_null = json_is_null(&ctx, 2);  /* "enabled": true */
    ASSERT(!is_null, "Non-null not detected as null");
    
    /* Type mismatch errors */
    result = json_get_bool(&ctx, 3, &bool_val);  /* Try to get int as bool */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "Type mismatch for bool");
    
    result = json_get_int(&ctx, 4, &int_val);  /* Try to get double as int */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "Type mismatch for int");
    
    result = json_get_double(&ctx, 1, &double_val);  /* Try to get string as double */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "Type mismatch for double");
    
    result = json_get_string_value(&ctx, 2, &str_val);  /* Try to get bool as string */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "Type mismatch for string");
}

void test_object_navigation(void) {
    TEST_START("Object navigation");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Find direct child */
    uint16_t child_record;
    int result = json_find_child(&ctx, 0, "name", &child_record);
    ASSERT_EQ(result, JSON_OK, "Find 'name' field");
    ASSERT_EQ(child_record, 1, "Correct record number for 'name'");
    
    result = json_find_child(&ctx, 0, "speed", &child_record);
    ASSERT_EQ(result, JSON_OK, "Find 'speed' field");
    ASSERT_EQ(child_record, 3, "Correct record number for 'speed'");
    
    result = json_find_child(&ctx, 0, "settings", &child_record);
    ASSERT_EQ(result, JSON_OK, "Find 'settings' object");
    ASSERT_EQ(child_record, 5, "Correct record number for 'settings'");
    
    /* Find non-existent key */
    result = json_find_child(&ctx, 0, "nonexistent", &child_record);
    ASSERT_EQ(result, JSON_ERR_NOT_FOUND, "Non-existent key not found");
    
    /* Navigate nested object */
    uint16_t settings_rec;
    result = json_find_child(&ctx, 0, "settings", &settings_rec);
    ASSERT_EQ(result, JSON_OK, "Find settings object");
    
    uint16_t pid_rec;
    result = json_find_child(&ctx, settings_rec, "pid", &pid_rec);
    ASSERT_EQ(result, JSON_OK, "Find nested pid object");
    
    uint16_t kp_rec;
    result = json_find_child(&ctx, pid_rec, "kp", &kp_rec);
    ASSERT_EQ(result, JSON_OK, "Find kp value");
    
    double kp_val;
    result = json_get_double(&ctx, kp_rec, &kp_val);
    ASSERT_EQ(result, JSON_OK, "Extract kp double value");
    ASSERT_DOUBLE_EQ(kp_val, 1.2, "Kp value correct");
}

void test_array_navigation(void) {
    TEST_START("Array navigation");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Find array */
    uint16_t sensors_rec;
    int result = json_find_child(&ctx, 0, "sensors", &sensors_rec);
    ASSERT_EQ(result, JSON_OK, "Find sensors array");
    
    /* Get child count */
    uint16_t count;
    result = json_get_child_count(&ctx, sensors_rec, &count);
    ASSERT_EQ(result, JSON_OK, "Get array child count");
    ASSERT_EQ(count, 3, "Array has 3 elements");
    
    /* Access by index */
    uint16_t sensor_rec;
    result = json_get_child_at(&ctx, sensors_rec, 0, &sensor_rec);
    ASSERT_EQ(result, JSON_OK, "Get array element 0");
    
    result = json_get_child_at(&ctx, sensors_rec, 1, &sensor_rec);
    ASSERT_EQ(result, JSON_OK, "Get array element 1");
    
    result = json_get_child_at(&ctx, sensors_rec, 2, &sensor_rec);
    ASSERT_EQ(result, JSON_OK, "Get array element 2");
    
    /* Out of bounds */
    result = json_get_child_at(&ctx, sensors_rec, 3, &sensor_rec);
    ASSERT_EQ(result, JSON_ERR_OUT_OF_BOUNDS, "Out of bounds array access rejected");
    
    /* Iterate array */
    uint16_t iterator = 0;
    int element_count = 0;
    while (json_iterate_children(&ctx, sensors_rec, &iterator, &sensor_rec) == JSON_OK) {
        element_count++;
        
        /* Access fields in each sensor object */
        int64_t id;
        const char *type;
        double value;
        
        json_object_get_int(&ctx, sensor_rec, "id", &id);
        json_object_get_string(&ctx, sensor_rec, "type", &type);
        json_object_get_double(&ctx, sensor_rec, "value", &value);
        
        ASSERT(id >= 1 && id <= 3, "Sensor ID in valid range");
        ASSERT(type != NULL, "Sensor type not NULL");
        ASSERT(value > 0, "Sensor value positive");
    }
    ASSERT_EQ(element_count, 3, "Iteration counted all 3 elements");
}

void test_convenience_functions(void) {
    TEST_START("Convenience helper functions");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Object get helpers */
    const char *name;
    int result = json_object_get_string(&ctx, 0, "name", &name);
    ASSERT_EQ(result, JSON_OK, "json_object_get_string works");
    ASSERT_STREQ(name, "motor_controller", "Retrieved string correct");
    
    bool enabled;
    result = json_object_get_bool(&ctx, 0, "enabled", &enabled);
    ASSERT_EQ(result, JSON_OK, "json_object_get_bool works");
    ASSERT(enabled, "Retrieved bool correct");
    
    int64_t speed;
    result = json_object_get_int(&ctx, 0, "speed", &speed);
    ASSERT_EQ(result, JSON_OK, "json_object_get_int works");
    ASSERT_EQ(speed, 1500, "Retrieved int correct");
    
    double temp;
    result = json_object_get_double(&ctx, 0, "temperature", &temp);
    ASSERT_EQ(result, JSON_OK, "json_object_get_double works");
    ASSERT_DOUBLE_EQ(temp, 45.5, "Retrieved double correct");
    
    /* Test with non-existent key */
    result = json_object_get_string(&ctx, 0, "nonexistent", &name);
    ASSERT_EQ(result, JSON_ERR_NOT_FOUND, "Non-existent key returns NOT_FOUND");
}

void test_nested_value_access(void) {
    TEST_START("Nested value access with paths");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Single level path */
    uint16_t record;
    int result = json_get_nested_value(&ctx, 0, "name", &record);
    ASSERT_EQ(result, JSON_OK, "Single level path works");
    
    const char *name;
    json_get_string_value(&ctx, record, &name);
    ASSERT_STREQ(name, "motor_controller", "Single level value correct");
    
    /* Two level path */
    result = json_get_nested_value(&ctx, 0, "settings.pid", &record);
    ASSERT_EQ(result, JSON_OK, "Two level path works");
    ASSERT_EQ(json_get_type(&ctx, record), JSON_TYPE_OBJECT, "Found object at path");
    
    /* Three level path */
    result = json_get_nested_value(&ctx, 0, "settings.pid.kp", &record);
    ASSERT_EQ(result, JSON_OK, "Three level path works");
    
    double kp;
    json_get_double(&ctx, record, &kp);
    ASSERT_DOUBLE_EQ(kp, 1.2, "Three level value correct");
    
    /* Test another nested path */
    result = json_get_nested_value(&ctx, 0, "settings.limits.max_speed", &record);
    ASSERT_EQ(result, JSON_OK, "Alternative nested path works");
    
    int64_t max_speed;
    json_get_int(&ctx, record, &max_speed);
    ASSERT_EQ(max_speed, 3000, "Nested int value correct");
    
    /* Invalid path */
    result = json_get_nested_value(&ctx, 0, "settings.nonexistent.field", &record);
    ASSERT_EQ(result, JSON_ERR_NOT_FOUND, "Invalid path returns NOT_FOUND");
}

void test_type_checking(void) {
    TEST_START("Type checking functions");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Get type */
    json_type_t type = json_get_type(&ctx, 0);
    ASSERT_EQ(type, JSON_TYPE_OBJECT, "Root is object type");
    
    type = json_get_type(&ctx, 1);
    ASSERT_EQ(type, JSON_TYPE_STRING, "Record 1 is string type");
    
    type = json_get_type(&ctx, 2);
    ASSERT_EQ(type, JSON_TYPE_BOOL, "Record 2 is bool type");
    
    type = json_get_type(&ctx, 3);
    ASSERT_EQ(type, JSON_TYPE_INT, "Record 3 is int type");
    
    type = json_get_type(&ctx, 4);
    ASSERT_EQ(type, JSON_TYPE_DOUBLE, "Record 4 is double type");
    
    type = json_get_type(&ctx, 6);
    ASSERT_EQ(type, JSON_TYPE_ARRAY, "Record 6 is array type");
    
    type = json_get_type(&ctx, 7);
    ASSERT_EQ(type, JSON_TYPE_NULL, "Record 7 is null type");
    
    /* Check type */
    bool correct_type = json_check_type(&ctx, 0, JSON_TYPE_OBJECT);
    ASSERT(correct_type, "Check object type works");
    
    correct_type = json_check_type(&ctx, 3, JSON_TYPE_INT);
    ASSERT(correct_type, "Check int type works");
    
    correct_type = json_check_type(&ctx, 3, JSON_TYPE_DOUBLE);
    ASSERT(!correct_type, "Incorrect type check returns false");
}

void test_empty_containers(void) {
    TEST_START("Empty containers handling");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Empty array */
    uint16_t empty_array_rec;
    int result = json_find_child(&ctx, 0, "empty_array", &empty_array_rec);
    ASSERT_EQ(result, JSON_OK, "Find empty_array");
    
    uint16_t count;
    result = json_get_child_count(&ctx, empty_array_rec, &count);
    ASSERT_EQ(result, JSON_OK, "Get empty array count");
    ASSERT_EQ(count, 0, "Empty array has 0 children");
    
    /* Try to iterate empty array */
    uint16_t iterator = 0;
    uint16_t child;
    result = json_iterate_children(&ctx, empty_array_rec, &iterator, &child);
    ASSERT_EQ(result, JSON_ERR_NOT_FOUND, "Empty array iteration returns NOT_FOUND");
    
    /* Empty object */
    uint16_t empty_obj_rec;
    result = json_find_child(&ctx, 0, "empty_object", &empty_obj_rec);
    ASSERT_EQ(result, JSON_OK, "Find empty_object");
    
    result = json_get_child_count(&ctx, empty_obj_rec, &count);
    ASSERT_EQ(result, JSON_OK, "Get empty object count");
    ASSERT_EQ(count, 0, "Empty object has 0 children");
    
    /* Try to find child in empty object */
    result = json_find_child(&ctx, empty_obj_rec, "anything", &child);
    ASSERT_EQ(result, JSON_ERR_NOT_FOUND, "Empty object find returns NOT_FOUND");
}

void test_validation(void) {
    TEST_START("Record validation");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Validate our test data */
    int result = json_validate_records(&ctx);
    ASSERT_EQ(result, JSON_OK, "Test data validates successfully");
}

void test_edge_cases(void) {
    TEST_START("Edge cases and error handling");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* NULL pointer checks */
    bool bool_val;
    int result = json_get_bool(&ctx, 2, NULL);
    ASSERT_EQ(result, JSON_ERR_NULL_PTR, "NULL output pointer rejected");
    
    result = json_get_bool(NULL, 2, &bool_val);
    ASSERT_EQ(result, JSON_ERR_NULL_PTR, "NULL context rejected");
    
    /* Invalid record access */
    result = json_get_bool(&ctx, 999, &bool_val);
    ASSERT_EQ(result, JSON_ERR_INVALID_RECORD, "Invalid record number rejected");
    
    /* Type mismatch on container */
    uint16_t child;
    result = json_find_child(&ctx, 1, "key", &child);  /* Record 1 is string, not object */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "find_child on non-object rejected");
    
    result = json_get_child_at(&ctx, 2, 0, &child);  /* Record 2 is bool, not array */
    ASSERT_EQ(result, JSON_ERR_TYPE_MISMATCH, "get_child_at on non-array rejected");
}

void test_real_world_scenario(void) {
    TEST_START("Real-world usage scenario");
    
    json_decoder_ctx_t ctx;
    json_decoder_init(&ctx, test_records, &test_control, 
                     test_strings, TEST_ROOT_RECORD);
    
    /* Scenario: Read motor controller configuration */
    
    /* 1. Check if motor is enabled */
    bool enabled;
    if (json_object_get_bool(&ctx, 0, "enabled", &enabled) == JSON_OK) {
        ASSERT(enabled, "Motor is enabled");
    }
    
    /* 2. Get operating speed */
    int64_t speed;
    if (json_object_get_int(&ctx, 0, "speed", &speed) == JSON_OK) {
        ASSERT_EQ(speed, 1500, "Speed is 1500 RPM");
    }
    
    /* 3. Read PID controller settings */
    uint16_t pid_rec;
    if (json_get_nested_value(&ctx, 0, "settings.pid", &pid_rec) == JSON_OK) {
        double kp, ki, kd;
        json_object_get_double(&ctx, pid_rec, "kp", &kp);
        json_object_get_double(&ctx, pid_rec, "ki", &ki);
        json_object_get_double(&ctx, pid_rec, "kd", &kd);
        
        ASSERT_DOUBLE_EQ(kp, 1.2, "PID kp correct");
        ASSERT_DOUBLE_EQ(ki, 0.5, "PID ki correct");
        ASSERT_DOUBLE_EQ(kd, 0.1, "PID kd correct");
    }
    
    /* 4. Check speed limits */
    uint16_t limits_rec;
    if (json_get_nested_value(&ctx, 0, "settings.limits", &limits_rec) == JSON_OK) {
        int64_t min_speed, max_speed;
        json_object_get_int(&ctx, limits_rec, "min_speed", &min_speed);
        json_object_get_int(&ctx, limits_rec, "max_speed", &max_speed);
        
        ASSERT_EQ(min_speed, 100, "Min speed correct");
        ASSERT_EQ(max_speed, 3000, "Max speed correct");
        ASSERT(speed >= min_speed && speed <= max_speed, "Speed within limits");
    }
    
    /* 5. Process all sensor readings */
    uint16_t sensors_rec;
    if (json_find_child(&ctx, 0, "sensors", &sensors_rec) == JSON_OK) {
        uint16_t iterator = 0;
        uint16_t sensor_rec;
        int sensor_count = 0;
        
        while (json_iterate_children(&ctx, sensors_rec, &iterator, &sensor_rec) == JSON_OK) {
            int64_t id;
            const char *type;
            double value;
            
            json_object_get_int(&ctx, sensor_rec, "id", &id);
            json_object_get_string(&ctx, sensor_rec, "type", &type);
            json_object_get_double(&ctx, sensor_rec, "value", &value);
            
            sensor_count++;
            ASSERT(id > 0, "Valid sensor ID");
            ASSERT(type != NULL, "Sensor has type");
            ASSERT(value >= 0, "Sensor value non-negative");
        }
        
        ASSERT_EQ(sensor_count, 3, "Processed all 3 sensors");
    }
    
    printf("  ✓ Real-world scenario completed successfully\n");
}

/* ============================================================================
 * Main Test Runner
 * ============================================================================ */

int main(void) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           JSON Node Decoder Unit Test Suite                   ║\n");
    printf("╚════════════════════════════════════════════════════════════════╝\n");
    
    /* Run all tests */
    test_decoder_init();
    test_get_record_and_string();
    test_type_extraction();
    test_object_navigation();
    test_array_navigation();
    test_convenience_functions();
    test_nested_value_access();
    test_type_checking();
    test_empty_containers();
    test_validation();
    test_edge_cases();
    test_real_world_scenario();
    
    /* Print summary */
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║                        Test Summary                            ║\n");
    printf("╠════════════════════════════════════════════════════════════════╣\n");
    printf("║  Total Tests:   %3d                                           ║\n", tests_run);
    printf("║  Passed:        %3d                                           ║\n", tests_passed);
    printf("║  Failed:        %3d                                           ║\n", tests_failed);
    printf("╚════════════════════════════════════════════════════════════════╝\n");
    
    if (tests_failed == 0) {
        printf("\n✓ ALL TESTS PASSED!\n\n");
        return 0;
    } else {
        printf("\n✗ SOME TESTS FAILED!\n\n");
        return 1;
    }
}