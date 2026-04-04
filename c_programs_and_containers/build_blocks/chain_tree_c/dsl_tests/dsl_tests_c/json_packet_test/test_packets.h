/* ========================================================================
 * test_packets.h — Static JSON test packet data
 *
 * C string literals for testing parse, build, roundtrip, dispatch.
 * ======================================================================== */

#ifndef TEST_PACKETS_H
#define TEST_PACKETS_H

/* Telemetry packet — sensors/accel topic */
static const char *test_telemetry_packet =
    "{\"type\":\"telemetry\","
    "\"topic\":\"sensors/accel\","
    "\"seq\":1,"
    "\"payload\":{"
        "\"x\":1.5,"
        "\"y\":-0.3,"
        "\"z\":9.81"
    "}}";

/* Command packet — calibrate axis */
static const char *test_command_packet =
    "{\"type\":\"command\","
    "\"topic\":\"control/cmd\","
    "\"seq\":2,"
    "\"payload\":{"
        "\"action\":\"calibrate\","
        "\"axis\":2,"
        "\"force\":true"
    "}}";

/* Config packet — update sample rate */
static const char *test_config_packet =
    "{\"type\":\"config\","
    "\"topic\":\"system/config\","
    "\"seq\":3,"
    "\"payload\":{"
        "\"sample_rate\":100,"
        "\"filter_window\":5,"
        "\"enabled\":false"
    "}}";

/* Unknown type — should route to default */
static const char *test_unknown_packet =
    "{\"type\":\"diagnostics\","
    "\"topic\":\"system/diag\","
    "\"seq\":4,"
    "\"payload\":{"
        "\"uptime\":3600,"
        "\"cpu_temp\":42.5"
    "}}";

/* Nested payload — tests deep path extraction */
static const char *test_nested_packet =
    "{\"type\":\"telemetry\","
    "\"payload\":{"
        "\"device\":{\"id\":7,\"name\":\"imu_1\"},"
        "\"sensors\":[{\"x\":1.0},{\"x\":2.0},{\"x\":3.0}]"
    "}}";

/* Malformed JSON — tests EXCEPTION on parse */
static const char *test_bad_json = "{\"type\":\"telemetry\",\"payload\":{";

#endif /* TEST_PACKETS_H */
