/* ========================================================================
 * main.c — JSON Packet Library Standalone Test
 *
 * Tests the cfl_json_packets library directly with cfl_heap as allocator.
 * Does NOT require a compiled .ctb or ChainTree runtime.
 *
 * Tests:
 *   1. Parse + extract fields from static JSON packets
 *   2. Build packets programmatically
 *   3. Roundtrip: build → serialize → parse → compare
 *   4. Dispatch by "type" field with default column
 *   5. Path navigation: nested objects, arrays
 *   6. has_field probe (soft query)
 *   7. to_buffer serialization into fixed buffer
 * ======================================================================== */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "cfl_perm.h"
#include "cfl_heap.h"
#include "cfl_json_packets.h"
#include "test_packets.h"

/* ── Heap interface bridge ── */

static void *heap_alloc(void *ctx, uint16_t size) {
    return cfl_heap_malloc_pointer((cfl_heap_t *)ctx, size);
}

static void heap_free(void *ctx, void *ptr) {
    cfl_heap_free_pointer((cfl_heap_t *)ctx, ptr);
}

static cfl_json_heap_interface_t make_heap_iface(cfl_heap_t *heap) {
    cfl_json_heap_interface_t iface;
    iface.alloc = heap_alloc;
    iface.free  = heap_free;
    iface.ctx   = heap;
    return iface;
}

/* ── Test helpers ── */

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT_TRUE(cond, msg) do { \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else { tests_failed++; printf("  FAIL: %s\n", msg); } \
} while(0)

#define ASSERT_INT_EQ(a, b, msg) ASSERT_TRUE((a) == (b), msg)
#define ASSERT_FLOAT_NEAR(a, b, eps, msg) ASSERT_TRUE(fabs((a) - (b)) < (eps), msg)
#define ASSERT_STR_EQ(a, b, msg) ASSERT_TRUE(strcmp((a), (b)) == 0, msg)

/* ======================================================================== */

static void test_parse_extract(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 1: Parse + Extract ===\n");

    cfl_json_packet_t *pkt = cfl_json_parse(heap, test_telemetry_packet, -1);

    const char *type_str = NULL;
    cfl_json_get_string(pkt, "type", &type_str);
    ASSERT_STR_EQ(type_str, "telemetry", "type == telemetry");

    const char *topic = NULL;
    cfl_json_get_string(pkt, "topic", &topic);
    ASSERT_STR_EQ(topic, "sensors/accel", "topic == sensors/accel");

    int32_t seq = 0;
    cfl_json_get_int(pkt, "seq", &seq);
    ASSERT_INT_EQ(seq, 1, "seq == 1");

    double x = 0;
    cfl_json_get_float(pkt, "payload.x", &x);
    ASSERT_FLOAT_NEAR(x, 1.5, 0.001, "payload.x == 1.5");

    double y = 0;
    cfl_json_get_float(pkt, "payload.y", &y);
    ASSERT_FLOAT_NEAR(y, -0.3, 0.001, "payload.y == -0.3");

    double z = 0;
    cfl_json_get_float(pkt, "payload.z", &z);
    ASSERT_FLOAT_NEAR(z, 9.81, 0.001, "payload.z == 9.81");

    cfl_json_free(heap, pkt);
}

static void test_parse_command(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 2: Parse Command Packet ===\n");

    cfl_json_packet_t *pkt = cfl_json_parse(heap, test_command_packet, -1);

    const char *action = NULL;
    cfl_json_get_string(pkt, "payload.action", &action);
    ASSERT_STR_EQ(action, "calibrate", "payload.action == calibrate");

    int32_t axis = 0;
    cfl_json_get_int(pkt, "payload.axis", &axis);
    ASSERT_INT_EQ(axis, 2, "payload.axis == 2");

    bool force = false;
    cfl_json_get_bool(pkt, "payload.force", &force);
    ASSERT_TRUE(force == true, "payload.force == true");

    cfl_json_free(heap, pkt);
}

static void test_build_packet(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 3: Build Packet ===\n");

    cfl_json_packet_t *pkt = cfl_json_create(heap);

    cfl_json_set_string(pkt, "type", "status");
    cfl_json_set_int(pkt, "seq", 42);
    cfl_json_set_object(pkt, "payload");
    cfl_json_set_float(pkt, "payload.battery", 87.5);
    cfl_json_set_bool(pkt, "payload.charging", false);

    /* Verify what we built */
    const char *type_str = NULL;
    cfl_json_get_string(pkt, "type", &type_str);
    ASSERT_STR_EQ(type_str, "status", "built type == status");

    int32_t seq = 0;
    cfl_json_get_int(pkt, "seq", &seq);
    ASSERT_INT_EQ(seq, 42, "built seq == 42");

    double bat = 0;
    cfl_json_get_float(pkt, "payload.battery", &bat);
    ASSERT_FLOAT_NEAR(bat, 87.5, 0.001, "built payload.battery == 87.5");

    bool charging = true;
    cfl_json_get_bool(pkt, "payload.charging", &charging);
    ASSERT_TRUE(charging == false, "built payload.charging == false");

    cfl_json_free(heap, pkt);
}

static void test_roundtrip(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 4: Roundtrip (build → serialize → parse → compare) ===\n");

    /* Build */
    cfl_json_packet_t *pkt1 = cfl_json_create(heap);
    cfl_json_set_string(pkt1, "type", "telemetry");
    cfl_json_set_int(pkt1, "seq", 99);
    cfl_json_set_object(pkt1, "payload");
    cfl_json_set_float(pkt1, "payload.x", 3.14);

    /* Serialize */
    char buf[CFL_JSON_MAX_PACKET_SIZE];
    int len = cfl_json_to_buffer(pkt1, buf, (int)sizeof(buf));
    ASSERT_TRUE(len > 0, "serialized length > 0");
    printf("  Serialized: %s\n", buf);

    /* Parse back */
    cfl_json_packet_t *pkt2 = cfl_json_parse(heap, buf, -1);

    /* Compare */
    const char *type2 = NULL;
    cfl_json_get_string(pkt2, "type", &type2);
    ASSERT_STR_EQ(type2, "telemetry", "roundtrip type == telemetry");

    int32_t seq2 = 0;
    cfl_json_get_int(pkt2, "seq", &seq2);
    ASSERT_INT_EQ(seq2, 99, "roundtrip seq == 99");

    double x2 = 0;
    cfl_json_get_float(pkt2, "payload.x", &x2);
    ASSERT_FLOAT_NEAR(x2, 3.14, 0.001, "roundtrip payload.x == 3.14");

    cfl_json_free(heap, pkt1);
    cfl_json_free(heap, pkt2);
}

static void test_dispatch(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 5: Dispatch by type field ===\n");

    /* Define routes */
    cfl_json_route_t routes[] = {
        { .type_name = "telemetry", .column_id = 10 },
        { .type_name = "command",   .column_id = 20 },
        { .type_name = "config",    .column_id = 30 },
    };
    unsigned default_column = 99;

    cfl_json_dispatcher_t *d = cfl_json_dispatcher_create(heap, routes, 3, default_column);

    /* Test telemetry → column 10 */
    cfl_json_packet_t *pkt1 = cfl_json_parse(heap, test_telemetry_packet, -1);
    unsigned col1 = cfl_json_dispatch(d, pkt1);
    ASSERT_INT_EQ((int)col1, 10, "telemetry → column 10");
    cfl_json_free(heap, pkt1);

    /* Test command → column 20 */
    cfl_json_packet_t *pkt2 = cfl_json_parse(heap, test_command_packet, -1);
    unsigned col2 = cfl_json_dispatch(d, pkt2);
    ASSERT_INT_EQ((int)col2, 20, "command → column 20");
    cfl_json_free(heap, pkt2);

    /* Test config → column 30 */
    cfl_json_packet_t *pkt3 = cfl_json_parse(heap, test_config_packet, -1);
    unsigned col3 = cfl_json_dispatch(d, pkt3);
    ASSERT_INT_EQ((int)col3, 30, "config → column 30");
    cfl_json_free(heap, pkt3);

    /* Test unknown → default column 99 */
    cfl_json_packet_t *pkt4 = cfl_json_parse(heap, test_unknown_packet, -1);
    unsigned col4 = cfl_json_dispatch(d, pkt4);
    ASSERT_INT_EQ((int)col4, 99, "diagnostics → default column 99");
    cfl_json_free(heap, pkt4);

    cfl_json_dispatcher_free(heap, d);
}

static void test_nested_paths(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 6: Nested Object + Array Paths ===\n");

    cfl_json_packet_t *pkt = cfl_json_parse(heap, test_nested_packet, -1);

    /* Deep object path */
    int32_t device_id = 0;
    cfl_json_get_int(pkt, "payload.device.id", &device_id);
    ASSERT_INT_EQ(device_id, 7, "payload.device.id == 7");

    const char *name = NULL;
    cfl_json_get_string(pkt, "payload.device.name", &name);
    ASSERT_STR_EQ(name, "imu_1", "payload.device.name == imu_1");

    /* Array access */
    int arr_size = cfl_json_array_size(pkt, "payload.sensors");
    ASSERT_INT_EQ(arr_size, 3, "payload.sensors array size == 3");

    double s0_x = 0, s1_x = 0, s2_x = 0;
    cfl_json_get_float(pkt, "payload.sensors[0].x", &s0_x);
    cfl_json_get_float(pkt, "payload.sensors[1].x", &s1_x);
    cfl_json_get_float(pkt, "payload.sensors[2].x", &s2_x);

    ASSERT_FLOAT_NEAR(s0_x, 1.0, 0.001, "sensors[0].x == 1.0");
    ASSERT_FLOAT_NEAR(s1_x, 2.0, 0.001, "sensors[1].x == 2.0");
    ASSERT_FLOAT_NEAR(s2_x, 3.0, 0.001, "sensors[2].x == 3.0");

    cfl_json_free(heap, pkt);
}

static void test_has_field(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 7: has_field probe ===\n");

    cfl_json_packet_t *pkt = cfl_json_parse(heap, test_telemetry_packet, -1);

    ASSERT_TRUE(cfl_json_has_field(pkt, "type"), "has type");
    ASSERT_TRUE(cfl_json_has_field(pkt, "payload"), "has payload");
    ASSERT_TRUE(cfl_json_has_field(pkt, "payload.x"), "has payload.x");
    ASSERT_TRUE(!cfl_json_has_field(pkt, "payload.w"), "!has payload.w");
    ASSERT_TRUE(!cfl_json_has_field(pkt, "nonexistent"), "!has nonexistent");
    ASSERT_TRUE(!cfl_json_has_field(pkt, "payload.x.deeper"), "!has payload.x.deeper");

    cfl_json_free(heap, pkt);
}

static void test_to_buffer(cfl_json_heap_interface_t *heap) {
    printf("\n=== Test 8: to_buffer fixed-size serialization ===\n");

    cfl_json_packet_t *pkt = cfl_json_create(heap);
    cfl_json_set_string(pkt, "msg", "hello");
    cfl_json_set_int(pkt, "val", 7);

    char buf[256];
    int len = cfl_json_to_buffer(pkt, buf, (int)sizeof(buf));
    ASSERT_TRUE(len > 0, "buffer length > 0");
    ASSERT_TRUE(len < 256, "buffer length < 256");

    /* Verify buffer content is valid JSON */
    cfl_json_packet_t *pkt2 = cfl_json_parse(heap, buf, -1);
    const char *msg = NULL;
    cfl_json_get_string(pkt2, "msg", &msg);
    ASSERT_STR_EQ(msg, "hello", "buffer roundtrip msg == hello");

    cfl_json_free(heap, pkt);
    cfl_json_free(heap, pkt2);
}

/* ======================================================================== */

int main(void) {
    printf("========================================\n");
    printf("JSON Packet Library Test Suite\n");
    printf("========================================\n");

    /* Set up perm allocator and heap */
    static uint8_t perm_buffer[8192];
    cfl_perm_t perm;
    cfl_perm_init(&perm, perm_buffer, sizeof(perm_buffer));

    cfl_heap_t *heap = cfl_heap_init(&perm, 4096);
    cfl_heap_reset(heap);

    cfl_json_heap_interface_t heap_iface = make_heap_iface(heap);

    /* Run tests */
    test_parse_extract(&heap_iface);
    test_parse_command(&heap_iface);
    test_build_packet(&heap_iface);
    test_roundtrip(&heap_iface);
    test_dispatch(&heap_iface);
    test_nested_paths(&heap_iface);
    test_has_field(&heap_iface);
    test_to_buffer(&heap_iface);

    /* Summary */
    printf("\n========================================\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("========================================\n");

    return tests_failed > 0 ? 1 : 0;
}
