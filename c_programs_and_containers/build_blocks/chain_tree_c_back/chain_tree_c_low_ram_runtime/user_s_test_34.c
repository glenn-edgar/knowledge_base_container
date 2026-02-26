// ============================================================================
// TEST_37 ONESHOT FUNCTIONS
// ============================================================================

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_runtime.h"
#include "chain_flow_dsl_tests_records.h"
#include <string.h>
#include <stdio.h>
#include <math.h>

#define FLOAT_EQ(a, b) (fabsf((a) - (b)) < 0.0001f)

// ============================================================================
// Static data for TEST_37_COPY_STATIC_NETWORK
// ============================================================================

static const uint32_t STATIC_IP_ADDR     = 0xC0A80001;  // 192.168.0.1
static const uint16_t STATIC_PORT        = 8080;
static const uint16_t STATIC_TIMEOUT_MS  = 5000;

// ============================================================================
// TEST_37_COPY_STATIC_NETWORK
// Params: field_ref(ip), field_ref(port), field_ref(timeout)
// ============================================================================

void test_37_copy_static_network_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 3) {
        EXCEPTION("TEST_37_COPY_STATIC_NETWORK: requires 3 field_refs");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_COPY_STATIC_NETWORK: no blackboard");
        return;
    }
    
    // Copy static values to fields
    uint32_t* ip_ptr = (uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint16_t* port_ptr = (uint16_t*)((uint8_t*)bb + params[1].field_offset);
    uint16_t* timeout_ptr = (uint16_t*)((uint8_t*)bb + params[2].field_offset);
    
    *ip_ptr = STATIC_IP_ADDR;
    *port_ptr = STATIC_PORT;
    *timeout_ptr = STATIC_TIMEOUT_MS;
    
    printf("TEST_37_COPY_STATIC_NETWORK: ip=0x%08X, port=%u, timeout=%u\n",
           *ip_ptr, *port_ptr, *timeout_ptr);
}

// ============================================================================
// TEST_37_VERIFY_NETWORK
// Params: field_ref(ip), field_ref(port), field_ref(timeout),
//         uint(exp_ip), uint(exp_port), uint(exp_timeout)
// ============================================================================

void test_37_verify_network_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 6) {
        EXCEPTION("TEST_37_VERIFY_NETWORK: requires 6 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_NETWORK: no blackboard");
        return;
    }
    
    uint32_t ip = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint16_t port = *(uint16_t*)((uint8_t*)bb + params[1].field_offset);
    uint16_t timeout = *(uint16_t*)((uint8_t*)bb + params[2].field_offset);
    
    uint32_t exp_ip = (uint32_t)s_expr_param_uint(&params[3]);
    uint16_t exp_port = (uint16_t)s_expr_param_uint(&params[4]);
    uint16_t exp_timeout = (uint16_t)s_expr_param_uint(&params[5]);
    
    printf("TEST_37_VERIFY_NETWORK: ip=0x%08X (exp 0x%08X), port=%u (exp %u), timeout=%u (exp %u)\n",
           ip, exp_ip, port, exp_port, timeout, exp_timeout);
    
    if (ip != exp_ip || port != exp_port || timeout != exp_timeout) {
        EXCEPTION("TEST_37_VERIFY_NETWORK: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_NETWORK: PASS\n");
    }
}

// ============================================================================
// TEST_37_VERIFY_SENSORS
// Params: field_ref(temp), field_ref(pres), field_ref(hum), field_ref(ts),
//         flt(exp_temp), flt(exp_pres), flt(exp_hum), uint(exp_ts)
// ============================================================================

void test_37_verify_sensors_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 8) {
        EXCEPTION("TEST_37_VERIFY_SENSORS: requires 8 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_SENSORS: no blackboard");
        return;
    }
    
    float temp = *(float*)((uint8_t*)bb + params[0].field_offset);
    float pres = *(float*)((uint8_t*)bb + params[1].field_offset);
    float hum = *(float*)((uint8_t*)bb + params[2].field_offset);
    uint32_t ts = *(uint32_t*)((uint8_t*)bb + params[3].field_offset);
    
    float exp_temp = s_expr_param_float(&params[4]);
    float exp_pres = s_expr_param_float(&params[5]);
    float exp_hum = s_expr_param_float(&params[6]);
    uint32_t exp_ts = (uint32_t)s_expr_param_uint(&params[7]);
    
    printf("TEST_37_VERIFY_SENSORS: temp=%.2f (exp %.2f), pres=%.2f (exp %.2f), hum=%.2f (exp %.2f), ts=%u (exp %u)\n",
           temp, exp_temp, pres, exp_pres, hum, exp_hum, ts, exp_ts);
    
    if (!FLOAT_EQ(temp, exp_temp) || !FLOAT_EQ(pres, exp_pres) || 
        !FLOAT_EQ(hum, exp_hum) || ts != exp_ts) {
        EXCEPTION("TEST_37_VERIFY_SENSORS: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_SENSORS: PASS\n");
    }
}

// ============================================================================
// TEST_37_VERIFY_DEVICE_NAME
// Params: field_ref(name), str_ptr(expected)
// ============================================================================

void test_37_verify_device_name_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_NAME: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_NAME: no blackboard");
        return;
    }
    
    const char* name = (const char*)((uint8_t*)bb + params[0].field_offset);
    const char* expected = s_expr_get_string(inst, &params[1]);
    
    printf("TEST_37_VERIFY_DEVICE_NAME: name=\"%s\" (exp \"%s\")\n", name, expected);
    
    if (strcmp(name, expected) != 0) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_NAME: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_DEVICE_NAME: PASS\n");
    }
}

// ============================================================================
// TEST_37_VERIFY_DEVICE_SERIAL
// Params: field_ref(serial), str_ptr(expected)
// ============================================================================

void test_37_verify_device_serial_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_SERIAL: requires 2 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_SERIAL: no blackboard");
        return;
    }
    
    const char* serial = (const char*)((uint8_t*)bb + params[0].field_offset);
    const char* expected = s_expr_get_string(inst, &params[1]);
    
    printf("TEST_37_VERIFY_DEVICE_SERIAL: serial=\"%s\" (exp \"%s\")\n", serial, expected);
    
    if (strcmp(serial, expected) != 0) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_SERIAL: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_DEVICE_SERIAL: PASS\n");
    }
}

// ============================================================================
// TEST_37_VERIFY_DEVICE_INFO
// Params: field_ref(version), field_ref(enabled), uint(exp_ver), uint(exp_en)
// ============================================================================

void test_37_verify_device_info_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_INFO: requires 4 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_INFO: no blackboard");
        return;
    }
    
    uint16_t version = *(uint16_t*)((uint8_t*)bb + params[0].field_offset);
    bool enabled = *(bool*)((uint8_t*)bb + params[1].field_offset);
    
    uint16_t exp_ver = (uint16_t)s_expr_param_uint(&params[2]);
    bool exp_en = (s_expr_param_uint(&params[3]) != 0);
    
    printf("TEST_37_VERIFY_DEVICE_INFO: version=0x%04X (exp 0x%04X), enabled=%d (exp %d)\n",
           version, exp_ver, enabled, exp_en);
    
    if (version != exp_ver || enabled != exp_en) {
        EXCEPTION("TEST_37_VERIFY_DEVICE_INFO: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_DEVICE_INFO: PASS\n");
    }
}

// ============================================================================
// TEST_37_VERIFY_TOP_LEVEL
// Params: field_ref(error_code), field_ref(run_count),
//         uint(exp_error), uint(exp_count)
// ============================================================================

void test_37_verify_top_level_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 4) {
        EXCEPTION("TEST_37_VERIFY_TOP_LEVEL: requires 4 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_TOP_LEVEL: no blackboard");
        return;
    }
    
    uint32_t error_code = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t run_count = *(uint32_t*)((uint8_t*)bb + params[1].field_offset);
    
    uint32_t exp_error = (uint32_t)s_expr_param_uint(&params[2]);
    uint32_t exp_count = (uint32_t)s_expr_param_uint(&params[3]);
    
    printf("TEST_37_VERIFY_TOP_LEVEL: error_code=%u (exp %u), run_count=%u (exp %u)\n",
           error_code, exp_error, run_count, exp_count);
    
    if (error_code != exp_error || run_count != exp_count) {
        EXCEPTION("TEST_37_VERIFY_TOP_LEVEL: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_TOP_LEVEL: PASS\n");
    }
}

// ============================================================================
// TEST_37_DUMP_STATE
// Params: field_ref(network), field_ref(sensors), field_ref(device),
//         field_ref(error_code), field_ref(run_count)
// ============================================================================

void test_37_dump_state_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 5) {
        EXCEPTION("TEST_37_DUMP_STATE: requires 5 parameters");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_DUMP_STATE: no blackboard");
        return;
    }
    
    // Access embedded structs via offsets
    network_config_a_t* net = (network_config_a_t*)((uint8_t*)bb + params[0].field_offset);
    sensor_data_a_t* sens = (sensor_data_a_t*)((uint8_t*)bb + params[1].field_offset);
    device_info_a_t* dev = (device_info_a_t*)((uint8_t*)bb + params[2].field_offset);
    uint32_t error_code = *(uint32_t*)((uint8_t*)bb + params[3].field_offset);
    uint32_t run_count = *(uint32_t*)((uint8_t*)bb + params[4].field_offset);
    
    printf("========================================\n");
    printf("SYSTEM STATE DUMP\n");
    printf("========================================\n");
    printf("Network:\n");
    printf("  ip_addr:    0x%08X\n", net->ip_addr);
    printf("  port:       %u\n", net->port);
    printf("  timeout_ms: %u\n", net->timeout_ms);
    printf("Sensors:\n");
    printf("  temperature: %.2f\n", sens->temperature);
    printf("  pressure:    %.2f\n", sens->pressure);
    printf("  humidity:    %.2f\n", sens->humidity);
    printf("  timestamp:   %u\n", sens->timestamp);
    printf("Device:\n");
    printf("  name:    \"%s\"\n", dev->name);
    printf("  serial:  \"%s\"\n", dev->serial);
    printf("  version: 0x%04X\n", dev->version);
    printf("  enabled: %s\n", dev->enabled ? "true" : "false");
    printf("Top Level:\n");
    printf("  error_code: %u\n", error_code);
    printf("  run_count:  %u\n", run_count);
    printf("========================================\n");
}


void test_37_verify_string_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: no blackboard");
        return;
    }
    
    // Get char* from field
    char** field_ptr = (char**)((uint8_t*)bb + params[0].field_offset);
    const char* value = *field_ptr;
    
    // Get expected string
    const char* expected = s_expr_get_string(inst, &params[1]);
    
    printf("TEST_37_VERIFY_STRING_PTR: ptr=%p, value=\"%s\" (exp \"%s\")\n",
           (void*)value, value ? value : "(null)", expected ? expected : "(null)");
    
    if (value == NULL && expected == NULL) {
        printf("TEST_37_VERIFY_STRING_PTR: PASS (both NULL)\n");
        return;
    }
    
    if (value == NULL || expected == NULL) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: MISMATCH (one is NULL)");
        return;
    }
    
    if (strcmp(value, expected) != 0) {
        EXCEPTION("TEST_37_VERIFY_STRING_PTR: MISMATCH!");
    } else {
        printf("TEST_37_VERIFY_STRING_PTR: PASS\n");
    }
}
// ============================================================================
// REGISTRATION TABLE
// ============================================================================

/*
{ "TEST_37_COPY_STATIC_NETWORK", (void*)test_37_copy_static_network_oneshot },
{ "TEST_37_VERIFY_NETWORK",      (void*)test_37_verify_network_oneshot },
{ "TEST_37_VERIFY_SENSORS",      (void*)test_37_verify_sensors_oneshot },
{ "TEST_37_VERIFY_DEVICE_NAME",  (void*)test_37_verify_device_name_oneshot },
{ "TEST_37_VERIFY_DEVICE_SERIAL",(void*)test_37_verify_device_serial_oneshot },
{ "TEST_37_VERIFY_DEVICE_INFO",  (void*)test_37_verify_device_info_oneshot },
{ "TEST_37_VERIFY_TOP_LEVEL",    (void*)test_37_verify_top_level_oneshot },
{ "TEST_37_DUMP_STATE",          (void*)test_37_dump_state_oneshot },
*/

