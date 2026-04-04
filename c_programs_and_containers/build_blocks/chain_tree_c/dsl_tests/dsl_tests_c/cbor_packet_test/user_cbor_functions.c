/* ========================================================================
 * user_cbor_functions.c — User boolean functions for CBOR packet test
 *
 * These callbacks receive parsed cfl_json_packet_t* as event_data
 * from the CBOR sink (which decodes CBOR→JSON internally).
 * Identical interface to the JSON test user functions.
 * ======================================================================== */

#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_cbor_functions.h"
#include "cfl_json_packets.h"
#include "cfl_common_functions.h"
#include "user_cbor_functions.h"

/* ========================================================================
 * TELEMETRY SINK
 * ======================================================================== */

bool cbor_telem_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle; (void)node_index; (void)event_type; (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    const char *topic = NULL;
    int32_t seq = 0;
    double x = 0, y = 0, z = 0;

    cfl_json_get_string(pkt, "topic", &topic);
    cfl_json_get_int(pkt, "seq", &seq);
    cfl_json_get_float(pkt, "payload.x", &x);
    cfl_json_get_float(pkt, "payload.y", &y);
    cfl_json_get_float(pkt, "payload.z", &z);

    printf("CBOR_TELEM_SINK [%s] seq=%d: x=%.3f y=%.3f z=%.3f\n",
           topic ? topic : "?", seq, x, y, z);

    return true;
}

/* ========================================================================
 * VERIFY X RANGE
 * ======================================================================== */

bool cbor_verify_x_range_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle; (void)node_index; (void)event_type; (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    double x = 0;
    cfl_json_get_float(pkt, "payload.x", &x);

    int32_t seq = 0;
    cfl_json_get_int(pkt, "seq", &seq);

    if (x >= 0.0 && x <= 0.5) {
        printf("CBOR_VERIFY: seq=%d x=%.3f PASS\n", seq, x);
        return true;
    } else {
        printf("CBOR_VERIFY: seq=%d x=%.3f REJECT\n", seq, x);
        return false;
    }
}

/* ========================================================================
 * VERIFIED SINK
 * ======================================================================== */

bool cbor_verified_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle; (void)node_index; (void)event_type; (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    const char *topic = NULL;
    int32_t seq = 0;
    double x = 0, y = 0, z = 0;

    cfl_json_get_string(pkt, "topic", &topic);
    cfl_json_get_int(pkt, "seq", &seq);
    cfl_json_get_float(pkt, "payload.x", &x);
    cfl_json_get_float(pkt, "payload.y", &y);
    cfl_json_get_float(pkt, "payload.z", &z);

    printf("CBOR_VERIFIED_SINK [%s] seq=%d: x=%.3f y=%.3f z=%.3f\n",
           topic ? topic : "?", seq, x, y, z);

    return true;
}
