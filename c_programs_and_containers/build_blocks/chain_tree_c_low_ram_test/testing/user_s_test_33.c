// ============================================================================
// TEST_33 ONESHOT FUNCTIONS - Nested Structure Tests
// ============================================================================

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_runtime.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "chain_flow_dsl_tests_records.h"

// Helper macro for float comparison
#define FLOAT_EQ(a, b) (fabsf((a) - (b)) < 0.0001f)

// ============================================================================
// TEST_33_SET_VECTOR
// Params: field_ref(x), field_ref(y), field_ref(z), flt(x_val), flt(y_val), flt(z_val)
// ============================================================================

void test_33_set_vector_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 6) {
        EXCEPTION("TEST_33_SET_VECTOR: requires 6 parameters (3 fields + 3 floats)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 3; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_SET_VECTOR: params 0-2 must be FIELD");
            return;
        }
    }
    for (int i = 3; i < 6; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FLOAT) {
            EXCEPTION("TEST_33_SET_VECTOR: params 3-5 must be FLOAT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_SET_VECTOR: no blackboard");
        return;
    }
    
    // Set x, y, z
    float* px = (float*)((uint8_t*)bb + params[0].field_offset);
    float* py = (float*)((uint8_t*)bb + params[1].field_offset);
    float* pz = (float*)((uint8_t*)bb + params[2].field_offset);
    
    *px = s_expr_param_float(&params[3]);
    *py = s_expr_param_float(&params[4]);
    *pz = s_expr_param_float(&params[5]);
    
    printf("TEST_33_SET_VECTOR: set (%.2f, %.2f, %.2f)\n", *px, *py, *pz);
}

// ============================================================================
// TEST_33_READ_VECTOR
// Params: field_ref(x), field_ref(y), field_ref(z), flt(expected_x), flt(expected_y), flt(expected_z)
// ============================================================================

void test_33_read_vector_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 6) {
        EXCEPTION("TEST_33_READ_VECTOR: requires 6 parameters (3 fields + 3 floats)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 3; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_READ_VECTOR: params 0-2 must be FIELD");
            return;
        }
    }
    for (int i = 3; i < 6; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FLOAT) {
            EXCEPTION("TEST_33_READ_VECTOR: params 3-5 must be FLOAT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_READ_VECTOR: no blackboard");
        return;
    }
    
    // Read x, y, z
    float x = *(float*)((uint8_t*)bb + params[0].field_offset);
    float y = *(float*)((uint8_t*)bb + params[1].field_offset);
    float z = *(float*)((uint8_t*)bb + params[2].field_offset);
    
    float expected_x = s_expr_param_float(&params[3]);
    float expected_y = s_expr_param_float(&params[4]);
    float expected_z = s_expr_param_float(&params[5]);
    
    printf("TEST_33_READ_VECTOR: read (%.2f, %.2f, %.2f), expected (%.2f, %.2f, %.2f)\n",
           x, y, z, expected_x, expected_y, expected_z);
    
    if (!FLOAT_EQ(x, expected_x) || !FLOAT_EQ(y, expected_y) || !FLOAT_EQ(z, expected_z)) {
        EXCEPTION("TEST_33_READ_VECTOR: MISMATCH!");
    } else {
        printf("TEST_33_READ_VECTOR: PASS\n");
    }
}

// ============================================================================
// TEST_33_SET_PID
// Params: field_ref(kp), field_ref(ki), field_ref(kd), flt(kp_val), flt(ki_val), flt(kd_val)
// ============================================================================

void test_33_set_pid_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 6) {
        EXCEPTION("TEST_33_SET_PID: requires 6 parameters (3 fields + 3 floats)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 3; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_SET_PID: params 0-2 must be FIELD");
            return;
        }
    }
    for (int i = 3; i < 6; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FLOAT) {
            EXCEPTION("TEST_33_SET_PID: params 3-5 must be FLOAT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_SET_PID: no blackboard");
        return;
    }
    
    // Set kp, ki, kd
    float* pkp = (float*)((uint8_t*)bb + params[0].field_offset);
    float* pki = (float*)((uint8_t*)bb + params[1].field_offset);
    float* pkd = (float*)((uint8_t*)bb + params[2].field_offset);
    
    *pkp = s_expr_param_float(&params[3]);
    *pki = s_expr_param_float(&params[4]);
    *pkd = s_expr_param_float(&params[5]);
    
    printf("TEST_33_SET_PID: set kp=%.4f, ki=%.4f, kd=%.4f\n", *pkp, *pki, *pkd);
}

// ============================================================================
// TEST_33_READ_PID
// Params: field_ref(kp), field_ref(ki), field_ref(kd), flt(expected_kp), flt(expected_ki), flt(expected_kd)
// ============================================================================

void test_33_read_pid_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 6) {
        EXCEPTION("TEST_33_READ_PID: requires 6 parameters (3 fields + 3 floats)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 3; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_READ_PID: params 0-2 must be FIELD");
            return;
        }
    }
    for (int i = 3; i < 6; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FLOAT) {
            EXCEPTION("TEST_33_READ_PID: params 3-5 must be FLOAT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_READ_PID: no blackboard");
        return;
    }
    
    // Read kp, ki, kd
    float kp = *(float*)((uint8_t*)bb + params[0].field_offset);
    float ki = *(float*)((uint8_t*)bb + params[1].field_offset);
    float kd = *(float*)((uint8_t*)bb + params[2].field_offset);
    
    float expected_kp = s_expr_param_float(&params[3]);
    float expected_ki = s_expr_param_float(&params[4]);
    float expected_kd = s_expr_param_float(&params[5]);
    
    printf("TEST_33_READ_PID: read kp=%.4f, ki=%.4f, kd=%.4f, expected kp=%.4f, ki=%.4f, kd=%.4f\n",
           kp, ki, kd, expected_kp, expected_ki, expected_kd);
    
    if (!FLOAT_EQ(kp, expected_kp) || !FLOAT_EQ(ki, expected_ki) || !FLOAT_EQ(kd, expected_kd)) {
        EXCEPTION("TEST_33_READ_PID: MISMATCH!");
    } else {
        printf("TEST_33_READ_PID: PASS\n");
    }
}

// ============================================================================
// TEST_33_SET_SYSTEM
// Params: field_ref(system_time), field_ref(error_code), uint(time_val), uint(error_val)
// ============================================================================

void test_33_set_system_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_33_SET_SYSTEM: requires 4 parameters (2 fields + 2 uints)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 2; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_SET_SYSTEM: params 0-1 must be FIELD");
            return;
        }
    }
    for (int i = 2; i < 4; i++) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_UINT && opcode != S_EXPR_PARAM_INT) {
            EXCEPTION("TEST_33_SET_SYSTEM: params 2-3 must be UINT or INT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_SET_SYSTEM: no blackboard");
        return;
    }
    
    // Set system_time (uint32) and error_code (uint16)
    uint32_t* p_time = (uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint16_t* p_error = (uint16_t*)((uint8_t*)bb + params[1].field_offset);
    
    *p_time = (uint32_t)s_expr_param_uint(&params[2]);
    *p_error = (uint16_t)s_expr_param_uint(&params[3]);
    
    printf("TEST_33_SET_SYSTEM: set system_time=%u, error_code=%u\n", *p_time, *p_error);
}

// ============================================================================
// TEST_33_READ_SYSTEM
// Params: field_ref(system_time), field_ref(error_code), uint(expected_time), uint(expected_error)
// ============================================================================

void test_33_read_system_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_33_READ_SYSTEM: requires 4 parameters (2 fields + 2 uints)");
        return;
    }
    
    // Validate param types
    for (int i = 0; i < 2; i++) {
        if ((params[i].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
            EXCEPTION("TEST_33_READ_SYSTEM: params 0-1 must be FIELD");
            return;
        }
    }
    for (int i = 2; i < 4; i++) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_UINT && opcode != S_EXPR_PARAM_INT) {
            EXCEPTION("TEST_33_READ_SYSTEM: params 2-3 must be UINT or INT");
            return;
        }
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_33_READ_SYSTEM: no blackboard");
        return;
    }
    
    // Read system_time (uint32) and error_code (uint16)
    uint32_t time_val = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint16_t error_val = *(uint16_t*)((uint8_t*)bb + params[1].field_offset);
    
    uint32_t expected_time = (uint32_t)s_expr_param_uint(&params[2]);
    uint16_t expected_error = (uint16_t)s_expr_param_uint(&params[3]);
    
    printf("TEST_33_READ_SYSTEM: read time=%u, error=%u, expected time=%u, error=%u\n",
           time_val, error_val, expected_time, expected_error);
    
    if (time_val != expected_time || error_val != expected_error) {
        EXCEPTION("TEST_33_READ_SYSTEM: MISMATCH!");
    } else {
        printf("TEST_33_READ_SYSTEM: PASS\n");
    }
}


// ============================================================================
// TEST_34_SET_UINT32
// Params: field_ref, uint(value)
// ============================================================================

void test_34_set_uint32_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_34_SET_UINT32: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_SET_UINT32: no blackboard");
        return;
    }
    
    uint32_t* ptr = (uint32_t*)((uint8_t*)bb + params[0].field_offset);
    *ptr = (uint32_t)s_expr_param_uint(&params[1]);
    
    printf("TEST_34_SET_UINT32: set 0x%08X\n", *ptr);
}

// ============================================================================
// TEST_34_READ_UINT32
// Params: field_ref, uint(expected)
// ============================================================================

void test_34_read_uint32_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_34_READ_UINT32: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_READ_UINT32: no blackboard");
        return;
    }
    
    uint32_t val = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t expected = (uint32_t)s_expr_param_uint(&params[1]);
    
    printf("TEST_34_READ_UINT32: read 0x%08X, expected 0x%08X\n", val, expected);
    
    if (val != expected) {
        EXCEPTION("TEST_34_READ_UINT32: MISMATCH!");
    } else {
        printf("TEST_34_READ_UINT32: PASS\n");
    }
}

// ============================================================================
// TEST_34_SET_UINT16
// Params: field_ref, uint(value)
// ============================================================================

void test_34_set_uint16_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_34_SET_UINT16: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_SET_UINT16: no blackboard");
        return;
    }
    
    uint16_t* ptr = (uint16_t*)((uint8_t*)bb + params[0].field_offset);
    *ptr = (uint16_t)s_expr_param_uint(&params[1]);
    
    printf("TEST_34_SET_UINT16: set %u\n", *ptr);
}

// ============================================================================
// TEST_34_READ_UINT16
// Params: field_ref, uint(expected)
// ============================================================================

void test_34_read_uint16_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_34_READ_UINT16: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_READ_UINT16: no blackboard");
        return;
    }
    
    uint16_t val = *(uint16_t*)((uint8_t*)bb + params[0].field_offset);
    uint16_t expected = (uint16_t)s_expr_param_uint(&params[1]);
    
    printf("TEST_34_READ_UINT16: read %u, expected %u\n", val, expected);
    
    if (val != expected) {
        EXCEPTION("TEST_34_READ_UINT16: MISMATCH!");
    } else {
        printf("TEST_34_READ_UINT16: PASS\n");
    }
}

// ============================================================================
// TEST_34_ALLOC_NODE
// Params: field_ref(ptr_field), uint(id), flt(value), uint(flags)
// Allocates a node_data_t and stores pointer in the field
// ============================================================================

void test_34_alloc_node_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_34_ALLOC_NODE: requires 4 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_ALLOC_NODE: no blackboard");
        return;
    }
    
    // Allocate node
    node_data_t* node = (node_data_t*)malloc(sizeof(node_data_t));
    if (!node) {
        EXCEPTION("TEST_34_ALLOC_NODE: malloc failed");
        return;
    }
    
    // Fill in data
    node->id = (uint32_t)s_expr_param_uint(&params[1]);
    node->value = s_expr_param_float(&params[2]);
    node->flags = (uint8_t)s_expr_param_uint(&params[3]);
    
    // Store pointer in blackboard field
    node_data_t** ptr_field = (node_data_t**)((uint8_t*)bb + params[0].field_offset);
    *ptr_field = node;
    
    printf("TEST_34_ALLOC_NODE: allocated node at %p (id=%u, value=%.5f, flags=0x%02X)\n",
           (void*)node, node->id, node->value, node->flags);
}

// ============================================================================
// TEST_34_READ_NODE
// Params: field_ref(ptr_field), uint(expected_id), flt(expected_value), uint(expected_flags)
// ============================================================================

void test_34_read_node_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_34_READ_NODE: requires 4 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_READ_NODE: no blackboard");
        return;
    }
    
    // Get pointer from blackboard field
    node_data_t** ptr_field = (node_data_t**)((uint8_t*)bb + params[0].field_offset);
    node_data_t* node = *ptr_field;
    
    if (!node) {
        EXCEPTION("TEST_34_READ_NODE: NULL pointer");
        return;
    }
    
    uint32_t expected_id = (uint32_t)s_expr_param_uint(&params[1]);
    float expected_value = s_expr_param_float(&params[2]);
    uint8_t expected_flags = (uint8_t)s_expr_param_uint(&params[3]);
    
    printf("TEST_34_READ_NODE: node at %p - id=%u (exp %u), value=%.5f (exp %.5f), flags=0x%02X (exp 0x%02X)\n",
           (void*)node, node->id, expected_id, node->value, expected_value, node->flags, expected_flags);
    
    if (node->id != expected_id || !FLOAT_EQ(node->value, expected_value) || node->flags != expected_flags) {
        EXCEPTION("TEST_34_READ_NODE: MISMATCH!");
    } else {
        printf("TEST_34_READ_NODE: PASS\n");
    }
}

// ============================================================================
// TEST_34_ALLOC_SENSOR
// Params: field_ref(ptr_field), uint(timestamp), flt(temp), flt(pressure), flt(humidity)
// ============================================================================

void test_34_alloc_sensor_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 5) {
        EXCEPTION("TEST_34_ALLOC_SENSOR: requires 5 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_ALLOC_SENSOR: no blackboard");
        return;
    }
    
    sensor_reading_t* sensor = (sensor_reading_t*)malloc(sizeof(sensor_reading_t));
    if (!sensor) {
        EXCEPTION("TEST_34_ALLOC_SENSOR: malloc failed");
        return;
    }
    
    sensor->timestamp = (uint32_t)s_expr_param_uint(&params[1]);
    sensor->temperature = s_expr_param_float(&params[2]);
    sensor->pressure = s_expr_param_float(&params[3]);
    sensor->humidity = s_expr_param_float(&params[4]);
    
    sensor_reading_t** ptr_field = (sensor_reading_t**)((uint8_t*)bb + params[0].field_offset);
    *ptr_field = sensor;
    
    printf("TEST_34_ALLOC_SENSOR: allocated sensor at %p (ts=%u, temp=%.2f, pres=%.2f, hum=%.2f)\n",
           (void*)sensor, sensor->timestamp, sensor->temperature, sensor->pressure, sensor->humidity);
}

// ============================================================================
// TEST_34_READ_SENSOR
// Params: field_ref(ptr_field), uint(ts), flt(temp), flt(pressure), flt(humidity)
// ============================================================================

void test_34_read_sensor_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 5) {
        EXCEPTION("TEST_34_READ_SENSOR: requires 5 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_READ_SENSOR: no blackboard");
        return;
    }
    
    sensor_reading_t** ptr_field = (sensor_reading_t**)((uint8_t*)bb + params[0].field_offset);
    sensor_reading_t* sensor = *ptr_field;
    
    if (!sensor) {
        EXCEPTION("TEST_34_READ_SENSOR: NULL pointer");
        return;
    }
    
    uint32_t exp_ts = (uint32_t)s_expr_param_uint(&params[1]);
    float exp_temp = s_expr_param_float(&params[2]);
    float exp_pres = s_expr_param_float(&params[3]);
    float exp_hum = s_expr_param_float(&params[4]);
    
    printf("TEST_34_READ_SENSOR: ts=%u (exp %u), temp=%.2f (exp %.2f), pres=%.2f (exp %.2f), hum=%.2f (exp %.2f)\n",
           sensor->timestamp, exp_ts, sensor->temperature, exp_temp,
           sensor->pressure, exp_pres, sensor->humidity, exp_hum);
    
    if (sensor->timestamp != exp_ts ||
        !FLOAT_EQ(sensor->temperature, exp_temp) ||
        !FLOAT_EQ(sensor->pressure, exp_pres) ||
        !FLOAT_EQ(sensor->humidity, exp_hum)) {
        EXCEPTION("TEST_34_READ_SENSOR: MISMATCH!");
    } else {
        printf("TEST_34_READ_SENSOR: PASS\n");
    }
}

// ============================================================================
// TEST_34_CHECK_NULL
// Params: field_ref(ptr_field), uint(expect_null: 1=NULL, 0=not NULL)
// ============================================================================

void test_34_check_null_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_34_CHECK_NULL: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_CHECK_NULL: no blackboard");
        return;
    }
    
    void** ptr_field = (void**)((uint8_t*)bb + params[0].field_offset);
    void* ptr = *ptr_field;
    bool expect_null = (s_expr_param_uint(&params[1]) != 0);
    
    printf("TEST_34_CHECK_NULL: ptr=%p, expect_null=%d\n", ptr, expect_null);
    
    if ((ptr == NULL) != expect_null) {
        EXCEPTION("TEST_34_CHECK_NULL: MISMATCH!");
    } else {
        printf("TEST_34_CHECK_NULL: PASS\n");
    }
}

// ============================================================================
// TEST_34_FREE_PTR
// Params: field_ref(ptr_field)
// Frees the pointer and sets field to NULL
// ============================================================================

void test_34_free_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_34_FREE_PTR: requires 1 parameter");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_34_FREE_PTR: no blackboard");
        return;
    }
    
    void** ptr_field = (void**)((uint8_t*)bb + params[0].field_offset);
    void* ptr = *ptr_field;
    
    if (ptr) {
        printf("TEST_34_FREE_PTR: freeing %p\n", ptr);
        free(ptr);
        *ptr_field = NULL;
    } else {
        printf("TEST_34_FREE_PTR: already NULL\n");
    }
}

// ============================================================================
// TEST_35_BUILD_LIST
// Params: field_ref(head_ptr), uint(count)
// Builds a linked list with 'count' nodes
// ============================================================================

void test_35_build_list_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_35_BUILD_LIST: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_35_BUILD_LIST: no blackboard");
        return;
    }
    
    uint32_t count = (uint32_t)s_expr_param_uint(&params[1]);
    list_node_t** head_ptr = (list_node_t**)((uint8_t*)bb + params[0].field_offset);
    
    *head_ptr = NULL;
    list_node_t* tail = NULL;
    
    for (uint32_t i = 0; i < count; i++) {
        list_node_t* node = (list_node_t*)malloc(sizeof(list_node_t));
        if (!node) {
            EXCEPTION("TEST_35_BUILD_LIST: malloc failed");
            return;
        }
        node->data = (int32_t)(i + 1) * 100;  // 100, 200, 300, ...
        node->next = NULL;
        
        if (!*head_ptr) {
            *head_ptr = node;
        } else {
            tail->next = node;
        }
        tail = node;
        
        printf("TEST_35_BUILD_LIST: created node %u at %p (data=%d)\n", i, (void*)node, node->data);
    }
    
    printf("TEST_35_BUILD_LIST: built list with %u nodes, head=%p\n", count, (void*)*head_ptr);
}

// ============================================================================
// TEST_35_TRAVERSE_LIST
// Params: field_ref(head_ptr), uint(expected_count)
// ============================================================================

void test_35_traverse_list_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_35_TRAVERSE_LIST: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_35_TRAVERSE_LIST: no blackboard");
        return;
    }
    
    list_node_t** head_ptr = (list_node_t**)((uint8_t*)bb + params[0].field_offset);
    uint32_t expected_count = (uint32_t)s_expr_param_uint(&params[1]);
    
    uint32_t actual_count = 0;
    list_node_t* current = *head_ptr;
    
    printf("TEST_35_TRAVERSE_LIST: traversing from head=%p\n", (void*)*head_ptr);
    
    while (current) {
        printf("  node %u: %p -> data=%d, next=%p\n", 
               actual_count, (void*)current, current->data, (void*)current->next);
        actual_count++;
        current = current->next;
        
        if (actual_count > 1000) {
            EXCEPTION("TEST_35_TRAVERSE_LIST: infinite loop detected!");
            return;
        }
    }
    
    printf("TEST_35_TRAVERSE_LIST: found %u nodes, expected %u\n", actual_count, expected_count);
    
    if (actual_count != expected_count) {
        EXCEPTION("TEST_35_TRAVERSE_LIST: count MISMATCH!");
    } else {
        printf("TEST_35_TRAVERSE_LIST: PASS\n");
    }
}

// ============================================================================
// TEST_35_FREE_LIST
// Params: field_ref(head_ptr)
// Frees entire linked list
// ============================================================================

void test_35_free_list_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_35_FREE_LIST: requires 1 parameter");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_35_FREE_LIST: no blackboard");
        return;
    }
    
    list_node_t** head_ptr = (list_node_t**)((uint8_t*)bb + params[0].field_offset);
    list_node_t* current = *head_ptr;
    uint32_t freed = 0;
    
    while (current) {
        list_node_t* next = current->next;
        printf("TEST_35_FREE_LIST: freeing node %p\n", (void*)current);
        free(current);
        current = next;
        freed++;
    }
    
    *head_ptr = NULL;
    printf("TEST_35_FREE_LIST: freed %u nodes\n", freed);
}

// ============================================================================
// TEST_36_COPY_PTR
// Params: field_ref(dest), field_ref(src)
// Copies pointer from src field to dest field (sharing)
// ============================================================================

void test_36_copy_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_36_COPY_PTR: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_36_COPY_PTR: no blackboard");
        return;
    }
    
    void** dest_field = (void**)((uint8_t*)bb + params[0].field_offset);
    void** src_field = (void**)((uint8_t*)bb + params[1].field_offset);
    
    *dest_field = *src_field;
    
    printf("TEST_36_COPY_PTR: copied %p from src to dest (now sharing)\n", *dest_field);
}

// ============================================================================
// TEST_36_VERIFY_SAME_PTR
// Params: field_ref(ptr1), field_ref(ptr2)
// Verifies both fields point to the same address
// ============================================================================

void test_36_verify_same_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_36_VERIFY_SAME_PTR: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_36_VERIFY_SAME_PTR: no blackboard");
        return;
    }
    
    void** ptr1_field = (void**)((uint8_t*)bb + params[0].field_offset);
    void** ptr2_field = (void**)((uint8_t*)bb + params[1].field_offset);
    
    printf("TEST_36_VERIFY_SAME_PTR: ptr1=%p, ptr2=%p\n", *ptr1_field, *ptr2_field);
    
    if (*ptr1_field != *ptr2_field) {
        EXCEPTION("TEST_36_VERIFY_SAME_PTR: pointers differ!");
    } else {
        printf("TEST_36_VERIFY_SAME_PTR: PASS (same address)\n");
    }
}

// ============================================================================
// TEST_36_MODIFY_NODE_VALUE
// Params: field_ref(ptr_field), flt(new_value)
// Modifies the value field of a node_data_t through pointer
// ============================================================================

void test_36_modify_node_value_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_36_MODIFY_NODE_VALUE: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_36_MODIFY_NODE_VALUE: no blackboard");
        return;
    }
    
    node_data_t** ptr_field = (node_data_t**)((uint8_t*)bb + params[0].field_offset);
    node_data_t* node = *ptr_field;
    
    if (!node) {
        EXCEPTION("TEST_36_MODIFY_NODE_VALUE: NULL pointer");
        return;
    }
    
    float old_value = node->value;
    node->value = s_expr_param_float(&params[1]);
    
    printf("TEST_36_MODIFY_NODE_VALUE: changed value from %.5f to %.5f\n", old_value, node->value);
}

// ============================================================================
// TEST_36_CLEAR_PTR
// Params: field_ref(ptr_field)
// Sets pointer to NULL (does NOT free - for use with shared pointers)
// ============================================================================

void test_36_clear_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("TEST_36_CLEAR_PTR: requires 1 parameter");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_36_CLEAR_PTR: no blackboard");
        return;
    }
    
    void** ptr_field = (void**)((uint8_t*)bb + params[0].field_offset);
    
    printf("TEST_36_CLEAR_PTR: clearing %p (NOT freeing)\n", *ptr_field);
    *ptr_field = NULL;
}