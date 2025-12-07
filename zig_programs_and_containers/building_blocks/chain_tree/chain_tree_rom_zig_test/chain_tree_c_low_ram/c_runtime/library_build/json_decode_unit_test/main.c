/* main.c - Unit tests for JSON decoder and query system */
#include "json_decoder.h"
#include "json_query.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

/* Test result tracking */
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_START(name) \
    printf("\n=== TEST: %s ===\n", name)

#define TEST_ASSERT(condition, message) \
    do { \
        if (condition) { \
            printf("  ✓ %s\n", message); \
            tests_passed++; \
        } else { \
            printf("  ✗ FAILED: %s\n", message); \
            tests_failed++; \
        } \
    } while(0)

#define TEST_SUMMARY() \
    printf("\n========================================\n"); \
    printf("Tests passed: %d\n", tests_passed); \
    printf("Tests failed: %d\n", tests_failed); \
    printf("========================================\n")

/* String table for all test data */
static const char string_table[] =
    "name\0"              /* offset 0 */
    "John Doe\0"          /* offset 5 */
    "age\0"               /* offset 14 */
    "email\0"             /* offset 18 */
    "john@example.com\0"  /* offset 24 */
    "active\0"            /* offset 41 */
    "temperature\0"       /* offset 48 */
    "humidity\0"          /* offset 61 */
    "sensors\0"           /* offset 70 */
    "type\0"              /* offset 78 */
    "temp\0"              /* offset 83 */
    "value\0"             /* offset 88 */
    "pressure\0"          /* offset 94 */
    "config\0"            /* offset 103 */
    "timeout\0"           /* offset 110 */
    "retries\0"           /* offset 119 */
    "endpoints\0"         /* offset 127 */
    "api.example.com\0"   /* offset 137 */
    "backup.example.com\0" /* offset 153 */
    "data\0"              /* offset 172 */
    "items\0"             /* offset 177 */
    "id\0"                /* offset 183 */
    "status\0"            /* offset 186 */
    "pending\0"           /* offset 193 */
    "completed\0"         /* offset 201 */
    "city\0"              /* offset 211 */
    "New York\0";         /* offset 216 */

/* Test Object 1: Simple object with primitives
 * {
 *   "name": "John Doe",
 *   "age": 30,
 *   "email": "john@example.com",
 *   "active": true,
 *   "temperature": 98.6,
 *   "humidity": null
 * }
 */
static const json_record_t test1_records[] = {
    {JSON_TYPE_OBJECT, .value.container_count = 6},  /* 0: root object */
    {JSON_TYPE_STRING, .value.string_offset = 0},    /* 1: key "name" */
    {JSON_TYPE_STRING, .value.string_offset = 5},    /* 2: value "John Doe" */
    {JSON_TYPE_STRING, .value.string_offset = 14},   /* 3: key "age" */
    {JSON_TYPE_INT32, .value.i32_value = 30},        /* 4: value 30 */
    {JSON_TYPE_STRING, .value.string_offset = 18},   /* 5: key "email" */
    {JSON_TYPE_STRING, .value.string_offset = 24},   /* 6: value "john@example.com" */
    {JSON_TYPE_STRING, .value.string_offset = 41},   /* 7: key "active" */
    {JSON_TYPE_BOOL, .value.bool_value = 1},         /* 8: value true */
    {JSON_TYPE_STRING, .value.string_offset = 48},   /* 9: key "temperature" */
    {JSON_TYPE_FLOAT32, .value.f32_value = 98.6f},   /* 10: value 98.6 */
    {JSON_TYPE_STRING, .value.string_offset = 61},   /* 11: key "humidity" */
    {JSON_TYPE_NULL}                                 /* 12: value null */
};

/* Test Object 2: Nested object with array
 * {
 *   "sensors": [
 *     {"type": "temp", "value": 22.5},
 *     {"type": "pressure", "value": 1013}
 *   ]
 * }
 */
static const json_record_t test2_records[] = {
    {JSON_TYPE_OBJECT, .value.container_count = 1},  /* 0: root object */
    {JSON_TYPE_STRING, .value.string_offset = 70},   /* 1: key "sensors" */
    {JSON_TYPE_ARRAY, .value.container_count = 2},   /* 2: array */
    {JSON_TYPE_OBJECT, .value.container_count = 2},  /* 3: first sensor object */
    {JSON_TYPE_STRING, .value.string_offset = 78},   /* 4: key "type" */
    {JSON_TYPE_STRING, .value.string_offset = 83},   /* 5: value "temp" */
    {JSON_TYPE_STRING, .value.string_offset = 88},   /* 6: key "value" */
    {JSON_TYPE_FLOAT32, .value.f32_value = 22.5f},   /* 7: value 22.5 */
    {JSON_TYPE_OBJECT, .value.container_count = 2},  /* 8: second sensor object */
    {JSON_TYPE_STRING, .value.string_offset = 78},   /* 9: key "type" */
    {JSON_TYPE_STRING, .value.string_offset = 94},   /* 10: value "pressure" */
    {JSON_TYPE_STRING, .value.string_offset = 88},   /* 11: key "value" */
    {JSON_TYPE_INT32, .value.i32_value = 1013}       /* 12: value 1013 */
};

/* Test Object 3: Deep nesting with config
 * {
 *   "config": {
 *     "timeout": 5000,
 *     "retries": 3,
 *     "endpoints": ["api.example.com", "backup.example.com"]
 *   }
 * }
 */
static const json_record_t test3_records[] = {
    {JSON_TYPE_OBJECT, .value.container_count = 1},    /* 0: root object */
    {JSON_TYPE_STRING, .value.string_offset = 103},    /* 1: key "config" */
    {JSON_TYPE_OBJECT, .value.container_count = 3},    /* 2: config object */
    {JSON_TYPE_STRING, .value.string_offset = 110},    /* 3: key "timeout" */
    {JSON_TYPE_INT32, .value.i32_value = 5000},        /* 4: value 5000 */
    {JSON_TYPE_STRING, .value.string_offset = 119},    /* 5: key "retries" */
    {JSON_TYPE_INT32, .value.i32_value = 3},           /* 6: value 3 */
    {JSON_TYPE_STRING, .value.string_offset = 127},    /* 7: key "endpoints" */
    {JSON_TYPE_ARRAY, .value.container_count = 2},     /* 8: endpoints array */
    {JSON_TYPE_STRING, .value.string_offset = 137},    /* 9: "api.example.com" */
    {JSON_TYPE_STRING, .value.string_offset = 153}     /* 10: "backup.example.com" */
};

/* Test Object 4: Array of mixed types
 * {
 *   "data": [42, "test", true, null, 3.14]
 * }
 */
static const json_record_t test4_records[] = {
    {JSON_TYPE_OBJECT, .value.container_count = 1},    /* 0: root object */
    {JSON_TYPE_STRING, .value.string_offset = 172},    /* 1: key "data" */
    {JSON_TYPE_ARRAY, .value.container_count = 5},     /* 2: mixed array */
    {JSON_TYPE_INT32, .value.i32_value = 42},          /* 3: 42 */
    {JSON_TYPE_STRING, .value.string_offset = 83},     /* 4: "temp" (reusing) */
    {JSON_TYPE_BOOL, .value.bool_value = 1},           /* 5: true */
    {JSON_TYPE_NULL},                                  /* 6: null */
    {JSON_TYPE_FLOAT32, .value.f32_value = 3.14f}      /* 7: 3.14 */
};

/* Test Object 5: Complex nested structure
 * {
 *   "items": [
 *     {"id": 1, "status": "pending"},
 *     {"id": 2, "status": "completed"}
 *   ]
 * }
 */
static const json_record_t test5_records[] = {
    {JSON_TYPE_OBJECT, .value.container_count = 1},    /* 0: root object */
    {JSON_TYPE_STRING, .value.string_offset = 177},    /* 1: key "items" */
    {JSON_TYPE_ARRAY, .value.container_count = 2},     /* 2: items array */
    {JSON_TYPE_OBJECT, .value.container_count = 2},    /* 3: first item */
    {JSON_TYPE_STRING, .value.string_offset = 183},    /* 4: key "id" */
    {JSON_TYPE_INT32, .value.i32_value = 1},           /* 5: value 1 */
    {JSON_TYPE_STRING, .value.string_offset = 186},    /* 6: key "status" */
    {JSON_TYPE_STRING, .value.string_offset = 193},    /* 7: value "pending" */
    {JSON_TYPE_OBJECT, .value.container_count = 2},    /* 8: second item */
    {JSON_TYPE_STRING, .value.string_offset = 183},    /* 9: key "id" */
    {JSON_TYPE_INT32, .value.i32_value = 2},           /* 10: value 2 */
    {JSON_TYPE_STRING, .value.string_offset = 186},    /* 11: key "status" */
    {JSON_TYPE_STRING, .value.string_offset = 201}     /* 12: value "completed" */
};

/* Combine all records into single array with control records */
static json_record_t all_records[100];
static record_control_t controls[5];

void setup_test_data(void)
{
    uint32_t offset = 0;
    
    /* Object 1 */
    memcpy(&all_records[offset], test1_records, sizeof(test1_records));
    controls[0].start_position = offset;
    controls[0].num_records = sizeof(test1_records) / sizeof(json_record_t);
    offset += controls[0].num_records;
    
    /* Object 2 */
    memcpy(&all_records[offset], test2_records, sizeof(test2_records));
    controls[1].start_position = offset;
    controls[1].num_records = sizeof(test2_records) / sizeof(json_record_t);
    offset += controls[1].num_records;
    
    /* Object 3 */
    memcpy(&all_records[offset], test3_records, sizeof(test3_records));
    controls[2].start_position = offset;
    controls[2].num_records = sizeof(test3_records) / sizeof(json_record_t);
    offset += controls[2].num_records;
    
    /* Object 4 */
    memcpy(&all_records[offset], test4_records, sizeof(test4_records));
    controls[3].start_position = offset;
    controls[3].num_records = sizeof(test4_records) / sizeof(json_record_t);
    offset += controls[3].num_records;
    
    /* Object 5 */
    memcpy(&all_records[offset], test5_records, sizeof(test5_records));
    controls[4].start_position = offset;
    controls[4].num_records = sizeof(test5_records) / sizeof(json_record_t);
    offset += controls[4].num_records;
}

void test_basic_decoding(json_decoder_t *decoder)
{
    TEST_START("Basic JSON Decoding");
    
    printf("\nDecoding Object 1 (Simple Primitives):\n");
    int result = json_decode_object(decoder, 0);
    TEST_ASSERT(result == 0, "Object 1 decoded successfully");
    
    printf("\nDecoding Object 2 (Nested with Array):\n");
    result = json_decode_object(decoder, 1);
    TEST_ASSERT(result == 0, "Object 2 decoded successfully");
    
    printf("\nDecoding Object 3 (Deep Nesting):\n");
    result = json_decode_object(decoder, 2);
    TEST_ASSERT(result == 0, "Object 3 decoded successfully");
}

void test_simple_queries(json_decoder_t *decoder)
{
    TEST_START("Simple Path Queries");
    
    json_query_result_t result;
    
    /* Test string query */
    json_query_path(decoder, 0, "name", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING, 
                "Query 'name' found");
    TEST_ASSERT(strcmp(result.value.string_value, "John Doe") == 0,
                "Query 'name' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test integer query */
    json_query_path(decoder, 0, "age", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'age' found");
    TEST_ASSERT(result.value.i32_value == 30,
                "Query 'age' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test float query */
    json_query_path(decoder, 0, "temperature", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_FLOAT32,
                "Query 'temperature' found");
    TEST_ASSERT(result.value.f32_value > 98.5f && result.value.f32_value < 98.7f,
                "Query 'temperature' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test boolean query */
    json_query_path(decoder, 0, "active", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_BOOL,
                "Query 'active' found");
    TEST_ASSERT(result.value.bool_value == 1,
                "Query 'active' returns true");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test null query */
    json_query_path(decoder, 0, "humidity", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_NULL,
                "Query 'humidity' found as null");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
}

void test_nested_queries(json_decoder_t *decoder)
{
    TEST_START("Nested Path Queries");
    
    json_query_result_t result;
    
    /* Test nested object query */
    json_query_path(decoder, 2, "config.timeout", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'config.timeout' found");
    TEST_ASSERT(result.value.i32_value == 5000,
                "Query 'config.timeout' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 2, "config.retries", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'config.retries' found");
    TEST_ASSERT(result.value.i32_value == 3,
                "Query 'config.retries' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
}

void test_array_queries(json_decoder_t *decoder)
{
    TEST_START("Array Index Queries");
    
    json_query_result_t result;
    
    /* Test array index in nested object */
    json_query_path(decoder, 1, "sensors[0].type", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'sensors[0].type' found");
    TEST_ASSERT(strcmp(result.value.string_value, "temp") == 0,
                "Query 'sensors[0].type' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 1, "sensors[0].value", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_FLOAT32,
                "Query 'sensors[0].value' found");
    TEST_ASSERT(result.value.f32_value > 22.4f && result.value.f32_value < 22.6f,
                "Query 'sensors[0].value' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 1, "sensors[1].type", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'sensors[1].type' found");
    TEST_ASSERT(strcmp(result.value.string_value, "pressure") == 0,
                "Query 'sensors[1].type' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 1, "sensors[1].value", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'sensors[1].value' found");
    TEST_ASSERT(result.value.i32_value == 1013,
                "Query 'sensors[1].value' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test array of strings */
    json_query_path(decoder, 2, "config.endpoints[0]", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'config.endpoints[0]' found");
    TEST_ASSERT(strcmp(result.value.string_value, "api.example.com") == 0,
                "Query 'config.endpoints[0]' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 2, "config.endpoints[1]", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'config.endpoints[1]' found");
    TEST_ASSERT(strcmp(result.value.string_value, "backup.example.com") == 0,
                "Query 'config.endpoints[1]' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test mixed array elements */
    json_query_path(decoder, 3, "data[0]", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'data[0]' found");
    TEST_ASSERT(result.value.i32_value == 42,
                "Query 'data[0]' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 3, "data[2]", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_BOOL,
                "Query 'data[2]' found");
    TEST_ASSERT(result.value.bool_value == 1,
                "Query 'data[2]' returns true");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 3, "data[3]", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_NULL,
                "Query 'data[3]' found as null");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
}

void test_complex_paths(json_decoder_t *decoder)
{
    TEST_START("Complex Path Queries");
    
    json_query_result_t result;
    
    /* Test array of objects */
    json_query_path(decoder, 4, "items[0].id", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'items[0].id' found");
    TEST_ASSERT(result.value.i32_value == 1,
                "Query 'items[0].id' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 4, "items[0].status", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'items[0].status' found");
    TEST_ASSERT(strcmp(result.value.string_value, "pending") == 0,
                "Query 'items[0].status' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 4, "items[1].id", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_INT32,
                "Query 'items[1].id' found");
    TEST_ASSERT(result.value.i32_value == 2,
                "Query 'items[1].id' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    json_query_path(decoder, 4, "items[1].status", &result);
    TEST_ASSERT(result.found && result.type == JSON_TYPE_STRING,
                "Query 'items[1].status' found");
    TEST_ASSERT(strcmp(result.value.string_value, "completed") == 0,
                "Query 'items[1].status' returns correct value");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
}

void test_error_cases(json_decoder_t *decoder)
{
    TEST_START("Error Cases");
    
    json_query_result_t result;
    
    /* Test non-existent field */
    json_query_path(decoder, 0, "nonexistent", &result);
    TEST_ASSERT(!result.found, "Query 'nonexistent' not found");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test out of bounds array index */
    json_query_path(decoder, 2, "config.endpoints[5]", &result);
    TEST_ASSERT(!result.found, "Query 'config.endpoints[5]' not found (out of bounds)");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test type mismatch - treating object as array */
    json_query_path(decoder, 0, "name[0]", &result);
    TEST_ASSERT(!result.found, "Query 'name[0]' not found (type mismatch)");
    printf("  Result: ");
    json_print_query_result(&result);
    printf("\n");
    
    /* Test invalid control index */
    int ret = json_query_path(decoder, 10, "name", &result);
    TEST_ASSERT(ret < 0, "Invalid control index returns error");
}

void test_helper_macros(json_decoder_t *decoder)
{
    TEST_START("Helper Macros");
    
    /* Test JSON_QUERY_INT */
    int age = JSON_QUERY_INT(decoder, 0, "age");
    TEST_ASSERT(age == 30, "JSON_QUERY_INT macro works correctly");
    printf("  age = %d\n", age);
    
    /* Test JSON_QUERY_FLOAT */
    float temp = JSON_QUERY_FLOAT(decoder, 0, "temperature");
    TEST_ASSERT(temp > 98.5f && temp < 98.7f, 
                "JSON_QUERY_FLOAT macro works correctly");
    printf("  temperature = %g\n", temp);
    
    /* Test JSON_QUERY_STRING */
    const char *name = JSON_QUERY_STRING(decoder, 0, "name");
    TEST_ASSERT(name != NULL && strcmp(name, "John Doe") == 0,
                "JSON_QUERY_STRING macro works correctly");
    printf("  name = \"%s\"\n", name);
    
    /* Test with nested path */
    int timeout = JSON_QUERY_INT(decoder, 2, "config.timeout");
    TEST_ASSERT(timeout == 5000, "JSON_QUERY_INT works with nested path");
    printf("  timeout = %d\n", timeout);
    
    /* Test with array index */
    const char *endpoint = JSON_QUERY_STRING(decoder, 2, "config.endpoints[0]");
    TEST_ASSERT(endpoint != NULL && strcmp(endpoint, "api.example.com") == 0,
                "JSON_QUERY_STRING works with array index");
    printf("  endpoint = \"%s\"\n", endpoint);
}

void test_compact_decoder(json_decoder_t *decoder)
{
    TEST_START("Compact Decoder");
    
    printf("\nCompact format output for Object 1:\n");
    uint32_t index = controls[0].start_position;
    json_decode_value_compact(decoder, &index);
    printf("\n");
    TEST_ASSERT(1, "Compact decoder executed");
    
    printf("\nCompact format output for Object 2:\n");
    index = controls[1].start_position;
    json_decode_value_compact(decoder, &index);
    printf("\n");
    TEST_ASSERT(1, "Compact decoder executed for nested structure");
}

int main(void)
{
    printf("========================================\n");
    printf("JSON Decoder & Query Unit Tests\n");
    printf("========================================\n");
    
    /* Setup test data */
    setup_test_data();
    
    /* Initialize decoder */
    json_decoder_t decoder;
    json_decoder_init(&decoder,
                     all_records,
                     sizeof(all_records) / sizeof(json_record_t),
                     string_table,
                     sizeof(string_table),
                     controls,
                     5);
    
    /* Run test suites */
    test_basic_decoding(&decoder);
    test_simple_queries(&decoder);
    test_nested_queries(&decoder);
    test_array_queries(&decoder);
    test_complex_paths(&decoder);
    test_error_cases(&decoder);
    test_helper_macros(&decoder);
    test_compact_decoder(&decoder);
    
    /* Summary */
    TEST_SUMMARY();
    
    return (tests_failed == 0) ? 0 : 1;
}
