/* User functions TEST_33 through TEST_39 for s_engine_test_2 */

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_se_module_registry.h"
#include "chain_flow_dsl_tests_records.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define FLOAT_EQ(a, b) (fabsf((a) - (b)) < 0.0001f)

/* ====================================================================
 * TEST_33: Read/verify functions for nested field access
 * (SET functions replaced by SE_SET_FIELD / SE_SET_FIELD_FLOAT builtins)
 * ==================================================================== */

void test_33_read_vector_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 6) { printf("TEST_33_READ_VECTOR: not enough params\n"); return; }
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    float x = *(float*)((uint8_t*)bb + params[0].field_offset);
    float y = *(float*)((uint8_t*)bb + params[1].field_offset);
    float z = *(float*)((uint8_t*)bb + params[2].field_offset);
    float ex = s_expr_param_float(&params[3]);
    float ey = s_expr_param_float(&params[4]);
    float ez = s_expr_param_float(&params[5]);
    printf("TEST_33_READ_VECTOR: (%.2f,%.2f,%.2f) exp (%.2f,%.2f,%.2f) %s\n",
           x, y, z, ex, ey, ez,
           (FLOAT_EQ(x,ex) && FLOAT_EQ(y,ey) && FLOAT_EQ(z,ez)) ? "PASS" : "FAIL");
}

void test_33_read_pid_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 6) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    float kp = *(float*)((uint8_t*)bb + params[0].field_offset);
    float ki = *(float*)((uint8_t*)bb + params[1].field_offset);
    float kd = *(float*)((uint8_t*)bb + params[2].field_offset);
    float ekp = s_expr_param_float(&params[3]);
    float eki = s_expr_param_float(&params[4]);
    float ekd = s_expr_param_float(&params[5]);
    printf("TEST_33_READ_PID: kp=%.4f ki=%.4f kd=%.4f %s\n",
           kp, ki, kd,
           (FLOAT_EQ(kp,ekp) && FLOAT_EQ(ki,eki) && FLOAT_EQ(kd,ekd)) ? "PASS" : "FAIL");
}

void test_33_read_system_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 4) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t time_val = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t error_val = *(uint32_t*)((uint8_t*)bb + params[1].field_offset);
    uint32_t exp_time = (uint32_t)s_expr_param_uint(&params[2]);
    uint32_t exp_error = (uint32_t)s_expr_param_uint(&params[3]);
    printf("TEST_33_READ_SYSTEM: time=%u error=%u %s\n",
           time_val, error_val,
           (time_val == exp_time && error_val == exp_error) ? "PASS" : "FAIL");
}

/* ====================================================================
 * TEST_34: Pointer field operations
 * ==================================================================== */

void test_34_alloc_node_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 4) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    node_data_t *node = (node_data_t*)malloc(sizeof(node_data_t));
    if (!node) return;
    node->id = (uint32_t)s_expr_param_uint(&params[1]);
    node->value = s_expr_param_float(&params[2]);
    node->flags = (uint32_t)s_expr_param_uint(&params[3]);
    node_data_t **ptr = (node_data_t**)((uint8_t*)bb + params[0].field_offset);
    *ptr = node;
    printf("TEST_34_ALLOC_NODE: %p id=%u val=%.5f flags=0x%02X\n",
           (void*)node, node->id, node->value, node->flags);
}

void test_34_read_node_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 4) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    node_data_t *node = *(node_data_t**)((uint8_t*)bb + params[0].field_offset);
    if (!node) { printf("TEST_34_READ_NODE: NULL\n"); return; }
    uint32_t eid = (uint32_t)s_expr_param_uint(&params[1]);
    float eval = s_expr_param_float(&params[2]);
    uint32_t eflags = (uint32_t)s_expr_param_uint(&params[3]);
    bool pass = node->id == eid && FLOAT_EQ(node->value, eval) && node->flags == eflags;
    printf("TEST_34_READ_NODE: id=%u val=%.5f flags=0x%02X %s\n",
           node->id, node->value, node->flags, pass ? "PASS" : "FAIL");
}

void test_34_alloc_sensor_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 5) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    sensor_reading_t *s = (sensor_reading_t*)malloc(sizeof(sensor_reading_t));
    if (!s) return;
    s->timestamp = (uint32_t)s_expr_param_uint(&params[1]);
    s->temperature = s_expr_param_float(&params[2]);
    s->pressure = s_expr_param_float(&params[3]);
    s->humidity = s_expr_param_float(&params[4]);
    sensor_reading_t **ptr = (sensor_reading_t**)((uint8_t*)bb + params[0].field_offset);
    *ptr = s;
    printf("TEST_34_ALLOC_SENSOR: %p\n", (void*)s);
}

void test_34_read_sensor_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 5) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    sensor_reading_t *s = *(sensor_reading_t**)((uint8_t*)bb + params[0].field_offset);
    if (!s) { printf("TEST_34_READ_SENSOR: NULL\n"); return; }
    uint32_t ets = (uint32_t)s_expr_param_uint(&params[1]);
    bool pass = s->timestamp == ets &&
                FLOAT_EQ(s->temperature, s_expr_param_float(&params[2])) &&
                FLOAT_EQ(s->pressure, s_expr_param_float(&params[3])) &&
                FLOAT_EQ(s->humidity, s_expr_param_float(&params[4]));
    printf("TEST_34_READ_SENSOR: ts=%u temp=%.2f %s\n", s->timestamp, s->temperature, pass ? "PASS" : "FAIL");
}

void test_34_read_uint32_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t val = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t exp = (uint32_t)s_expr_param_uint(&params[1]);
    printf("TEST_34_READ_UINT32: 0x%08X %s\n", val, val == exp ? "PASS" : "FAIL");
}

void test_34_read_uint16_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    /* Field is now uint32 in DSL but test still reads as uint32 */
    uint32_t val = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t exp = (uint32_t)s_expr_param_uint(&params[1]);
    printf("TEST_34_READ_UINT16: %u %s\n", val, val == exp ? "PASS" : "FAIL");
}

void test_34_check_null_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    void *ptr = *(void**)((uint8_t*)bb + params[0].field_offset);
    bool expect_null = (s_expr_param_uint(&params[1]) != 0);
    printf("TEST_34_CHECK_NULL: ptr=%p expect_null=%d %s\n",
           ptr, expect_null, ((ptr == NULL) == expect_null) ? "PASS" : "FAIL");
}

void test_34_free_ptr_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    void **field = (void**)((uint8_t*)bb + params[0].field_offset);
    if (*field) { free(*field); *field = NULL; printf("TEST_34_FREE_PTR: freed\n"); }
}

/* ====================================================================
 * TEST_35: Linked list operations
 * ==================================================================== */

void test_35_build_list_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t count = (uint32_t)s_expr_param_uint(&params[1]);
    list_node_t **head = (list_node_t**)((uint8_t*)bb + params[0].field_offset);
    *head = NULL;
    list_node_t *tail = NULL;
    for (uint32_t i = 0; i < count; i++) {
        list_node_t *n = (list_node_t*)malloc(sizeof(list_node_t));
        if (!n) return;
        n->data = (int32_t)(i + 1) * 100;
        n->next = 0;
        if (!*head) *head = n; else tail->next = (uint64_t)(uintptr_t)n;
        tail = n;
    }
    printf("TEST_35_BUILD_LIST: %u nodes\n", count);
}

void test_35_traverse_list_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    list_node_t *cur = *(list_node_t**)((uint8_t*)bb + params[0].field_offset);
    uint32_t exp = (uint32_t)s_expr_param_uint(&params[1]);
    uint32_t count = 0;
    while (cur && count < 1000) { count++; cur = (list_node_t*)(uintptr_t)cur->next; }
    printf("TEST_35_TRAVERSE_LIST: %u nodes %s\n", count, count == exp ? "PASS" : "FAIL");
}

void test_35_free_list_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    list_node_t **head = (list_node_t**)((uint8_t*)bb + params[0].field_offset);
    list_node_t *cur = *head;
    uint32_t freed = 0;
    while (cur) { list_node_t *next = (list_node_t*)(uintptr_t)cur->next; free(cur); cur = next; freed++; }
    *head = NULL;
    printf("TEST_35_FREE_LIST: freed %u\n", freed);
}

/* ====================================================================
 * TEST_36: Pointer sharing operations
 * ==================================================================== */

void test_36_copy_ptr_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    void **dst = (void**)((uint8_t*)bb + params[0].field_offset);
    void **src = (void**)((uint8_t*)bb + params[1].field_offset);
    *dst = *src;
    printf("TEST_36_COPY_PTR: %p\n", *dst);
}

void test_36_verify_same_ptr_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    void *p1 = *(void**)((uint8_t*)bb + params[0].field_offset);
    void *p2 = *(void**)((uint8_t*)bb + params[1].field_offset);
    printf("TEST_36_VERIFY_SAME_PTR: %s\n", p1 == p2 ? "PASS" : "FAIL");
}

void test_36_modify_node_value_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    node_data_t *node = *(node_data_t**)((uint8_t*)bb + params[0].field_offset);
    if (!node) return;
    node->value = s_expr_param_float(&params[1]);
    printf("TEST_36_MODIFY_NODE_VALUE: new=%.5f\n", node->value);
}

void test_36_clear_ptr_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    void **field = (void**)((uint8_t*)bb + params[0].field_offset);
    *field = NULL;
    printf("TEST_36_CLEAR_PTR: cleared\n");
}

/* ====================================================================
 * TEST_37: Static buffer copy + JSON read verification
 * ==================================================================== */

static const uint32_t STATIC_IP_ADDR = 0xC0A80001;
static const uint32_t STATIC_PORT = 8080;
static const uint32_t STATIC_TIMEOUT_MS = 5000;

void test_37_copy_static_network_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 3) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    *(uint32_t*)((uint8_t*)bb + params[0].field_offset) = STATIC_IP_ADDR;
    *(uint32_t*)((uint8_t*)bb + params[1].field_offset) = STATIC_PORT;
    *(uint32_t*)((uint8_t*)bb + params[2].field_offset) = STATIC_TIMEOUT_MS;
    printf("TEST_37_COPY_STATIC_NETWORK: done\n");
}

void test_37_verify_network_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 6) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t ip = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t port = *(uint32_t*)((uint8_t*)bb + params[1].field_offset);
    uint32_t timeout = *(uint32_t*)((uint8_t*)bb + params[2].field_offset);
    uint32_t eip = (uint32_t)s_expr_param_uint(&params[3]);
    uint32_t eport = (uint32_t)s_expr_param_uint(&params[4]);
    uint32_t etimeout = (uint32_t)s_expr_param_uint(&params[5]);
    bool pass = ip == eip && port == eport && timeout == etimeout;
    printf("TEST_37_VERIFY_NETWORK: ip=0x%08X port=%u timeout=%u %s\n", ip, port, timeout, pass ? "PASS" : "FAIL");
}

void test_37_verify_sensors_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 8) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    float temp = *(float*)((uint8_t*)bb + params[0].field_offset);
    float pres = *(float*)((uint8_t*)bb + params[1].field_offset);
    float hum = *(float*)((uint8_t*)bb + params[2].field_offset);
    uint32_t ts = *(uint32_t*)((uint8_t*)bb + params[3].field_offset);
    bool pass = FLOAT_EQ(temp, s_expr_param_float(&params[4])) &&
                FLOAT_EQ(pres, s_expr_param_float(&params[5])) &&
                FLOAT_EQ(hum, s_expr_param_float(&params[6])) &&
                ts == (uint32_t)s_expr_param_uint(&params[7]);
    printf("TEST_37_VERIFY_SENSORS: temp=%.2f pres=%.2f hum=%.2f ts=%u %s\n",
           temp, pres, hum, ts, pass ? "PASS" : "FAIL");
}

void test_37_verify_device_name_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    const char *name = (const char*)((uint8_t*)bb + params[0].field_offset);
    const char *expected = s_expr_get_string(inst, &params[1]);
    printf("TEST_37_VERIFY_DEVICE_NAME: \"%s\" %s\n", name,
           (strcmp(name, expected) == 0) ? "PASS" : "FAIL");
}

void test_37_verify_device_serial_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    const char *serial = (const char*)((uint8_t*)bb + params[0].field_offset);
    const char *expected = s_expr_get_string(inst, &params[1]);
    printf("TEST_37_VERIFY_DEVICE_SERIAL: \"%s\" %s\n", serial,
           (strcmp(serial, expected) == 0) ? "PASS" : "FAIL");
}

void test_37_verify_device_info_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 4) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t version = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    int32_t enabled = *(int32_t*)((uint8_t*)bb + params[1].field_offset);
    uint32_t ever = (uint32_t)s_expr_param_uint(&params[2]);
    int32_t een = (int32_t)s_expr_param_uint(&params[3]);
    printf("TEST_37_VERIFY_DEVICE_INFO: ver=0x%04X en=%d %s\n",
           version, enabled, (version == ever && enabled == een) ? "PASS" : "FAIL");
}

void test_37_verify_top_level_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 4) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    uint32_t ec = *(uint32_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t rc = *(uint32_t*)((uint8_t*)bb + params[1].field_offset);
    uint32_t eec = (uint32_t)s_expr_param_uint(&params[2]);
    uint32_t erc = (uint32_t)s_expr_param_uint(&params[3]);
    printf("TEST_37_VERIFY_TOP_LEVEL: error=%u run=%u %s\n",
           ec, rc, (ec == eec && rc == erc) ? "PASS" : "FAIL");
}

void test_37_dump_state_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 5) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    network_config_a_t *net = (network_config_a_t*)((uint8_t*)bb + params[0].field_offset);
    uint32_t ec = *(uint32_t*)((uint8_t*)bb + params[3].field_offset);
    uint32_t rc = *(uint32_t*)((uint8_t*)bb + params[4].field_offset);
    printf("=== STATE DUMP ===\n");
    printf("  net: ip=0x%08X port=%u timeout=%u\n", net->ip_addr, net->port, net->timeout_ms);
    printf("  error=%u run=%u\n", ec, rc);
}

void test_37_verify_string_ptr_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) return;
    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) return;
    char *value = *(char**)((uint8_t*)bb + params[0].field_offset);
    const char *expected = s_expr_get_string(inst, &params[1]);
    printf("TEST_37_VERIFY_STRING_PTR: \"%s\" %s\n",
           value ? value : "(null)",
           (value && expected && strcmp(value, expected) == 0) ? "PASS" : "FAIL");
}

/* ====================================================================
 * TEST_38: Constant record copy verification
 * ==================================================================== */

void test_38_verify_defaults_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data; (void)param_count;
    void *bb = s_expr_tree_get_blackboard(inst);
    pid_gains_a_t *gains = (pid_gains_a_t*)((uint8_t*)bb + params[0].field_offset);
    bool pass = FLOAT_EQ(gains->kp, 1.0f) && FLOAT_EQ(gains->ki, 0.2f) && FLOAT_EQ(gains->kd, 0.05f);
    printf("TEST_38_VERIFY_DEFAULTS: kp=%.2f ki=%.2f kd=%.2f %s\n",
           gains->kp, gains->ki, gains->kd, pass ? "PASS" : "FAIL");
}

void test_38_verify_test_pid_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data; (void)param_count;
    void *bb = s_expr_tree_get_blackboard(inst);
    pid_gains_a_t *gains = (pid_gains_a_t*)((uint8_t*)bb + params[0].field_offset);
    bool pass = FLOAT_EQ(gains->kp, 2.5f) && FLOAT_EQ(gains->ki, 0.5f) && FLOAT_EQ(gains->kd, 0.1f);
    printf("TEST_38_VERIFY_TEST_PID: kp=%.2f ki=%.2f kd=%.2f %s\n",
           gains->kp, gains->ki, gains->kd, pass ? "PASS" : "FAIL");
}

/* ====================================================================
 * TEST_39: Pointer field with external init
 * ==================================================================== */

#define EXPECTED_KP 2.5f
#define EXPECTED_KI 0.5f
#define EXPECTED_KD 0.1f

void test_39_verify_gains_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data; (void)param_count;
    pid_gains_c_t *gains = S_EXPR_GET_FIELD(inst, &params[0], pid_gains_c_t);
    if (!gains) { printf("TEST_39_VERIFY_GAINS: NULL\n"); return; }
    bool pass = gains->kp == EXPECTED_KP && gains->ki == EXPECTED_KI && gains->kd == EXPECTED_KD;
    printf("TEST_39_VERIFY_GAINS: kp=%.1f ki=%.1f kd=%.1f %s\n",
           gains->kp, gains->ki, gains->kd, pass ? "PASS" : "FAIL");
}

void test_39_verify_pointer_oneshot(
    s_expr_tree_instance_t *inst, const s_expr_param_t *params,
    uint16_t param_count, s_expr_event_type_t event_type,
    uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data; (void)param_count;
    pid_gains_c_t *field_gains = S_EXPR_GET_FIELD(inst, &params[0], pid_gains_c_t);
    pid_gains_c_t **gains_ptr = s_expr_blackboard_get_field_by_string(inst, "gains_ptr");
    if (!gains_ptr || !*gains_ptr) { printf("TEST_39_VERIFY_POINTER: NULL\n"); return; }
    pid_gains_c_t *ptr_gains = *gains_ptr;
    bool pass = ptr_gains == field_gains &&
                ptr_gains->kp == EXPECTED_KP && ptr_gains->ki == EXPECTED_KI && ptr_gains->kd == EXPECTED_KD;
    printf("TEST_39_VERIFY_POINTER: ptr=%p %s\n", (void*)ptr_gains, pass ? "PASS" : "FAIL");
}

/* TEST_39 boolean init — called from ChainTree aux_function */
static void test39_init(s_expr_tree_instance_t *inst) {
    pid_gains_c_t *gains = s_expr_blackboard_get_field_by_string(inst, "gains");
    if (!gains) return;
    gains->kp = EXPECTED_KP;
    gains->ki = EXPECTED_KI;
    gains->kd = EXPECTED_KD;

    pid_gains_c_t **gains_ptr = s_expr_blackboard_get_field_by_string(inst, "gains_ptr");
    if (!gains_ptr) return;
    *gains_ptr = gains;
    printf("test39_init: gains set, ptr -> %p\n", (void*)gains);
}

bool test_39_set_init_data_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)event_type; (void)event_data;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    if (event_id == CFL_INIT_EVENT) {
        /* Get the se_engine node data to find the tree instance */
        cfl_se_engine_node_data_t *nd = (cfl_se_engine_node_data_t *)
            cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
        if (nd && nd->inst) {
            test39_init(nd->inst);
        }
        return true;
    }
    return true;
}
