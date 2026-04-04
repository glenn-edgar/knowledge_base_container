/* ========================================================================
 * user_json_functions.c — User boolean functions for JSON packet test
 *
 * No emit callback — emit is a universal CFL one-shot that reads
 * packet_text from node_dict and queues it directly.
 *
 * These callbacks receive parsed cfl_json_packet_t* as event_data
 * from the built-in CFL_JSON_SINK_MAIN / CFL_JSON_TRANSFORM_MAIN.
 * ======================================================================== */

#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_json_functions.h"
#include "cfl_json_packets.h"
#include "cfl_common_functions.h"
#include "user_json_functions.h"

/* ========================================================================
 * TELEMETRY SINK — extracts x/y/z from telemetry packets
 * ======================================================================== */

bool json_telem_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

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

    printf("TELEM_SINK [%s] seq=%d: x=%.3f y=%.3f z=%.3f\n",
           topic ? topic : "?", seq, x, y, z);

    return true;
}

/* ========================================================================
 * COMMAND/CONFIG SINK — prints command and config packets
 * ======================================================================== */

bool json_cmd_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    const char *type_str = NULL;
    const char *topic = NULL;
    int32_t seq = 0;

    cfl_json_get_string(pkt, "type", &type_str);
    cfl_json_get_string(pkt, "topic", &topic);
    cfl_json_get_int(pkt, "seq", &seq);

    printf("CMD_SINK [%s] type='%s' seq=%d\n",
           topic ? topic : "?", type_str ? type_str : "?", seq);

    if (type_str && strcmp(type_str, "command") == 0) {
        const char *action = NULL;
        cfl_json_get_string(pkt, "payload.action", &action);
        int32_t axis = 0;
        cfl_json_get_int(pkt, "payload.axis", &axis);
        bool force = false;
        cfl_json_get_bool(pkt, "payload.force", &force);
        printf("  action='%s' axis=%d force=%s\n",
               action ? action : "?", axis, force ? "true" : "false");
    } else if (type_str && strcmp(type_str, "config") == 0) {
        int32_t rate = 0, window = 0;
        cfl_json_get_int(pkt, "payload.sample_rate", &rate);
        cfl_json_get_int(pkt, "payload.filter_window", &window);
        printf("  sample_rate=%d filter_window=%d\n", rate, window);
    }

    return true;
}

/* ========================================================================
 * SMOOTHED SINK — receives smoothed telemetry output from transform
 * ======================================================================== */

bool json_smoothed_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    double x = 0, y = 0, z = 0;
    cfl_json_get_float(pkt, "payload.x", &x);
    cfl_json_get_float(pkt, "payload.y", &y);
    cfl_json_get_float(pkt, "payload.z", &z);

    printf("SMOOTHED_SINK: x=%.3f y=%.3f z=%.3f\n", x, y, z);

    return true;
}

/* ========================================================================
 * TRANSFORM — accumulates 3 telemetry packets, emits smoothed output
 * ======================================================================== */

typedef struct {
    double sum_x, sum_y, sum_z;
    unsigned count;
    unsigned window;
} smoother_state_t;

bool json_smoother_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_id;

    if (!event_data) return false;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_transform_node_t *node = cfl_get_json_transform_node(rt, node_index);

    /* Lazy init user state */
    if (!node->user_data) {
        node->user_data = cfl_additional_arena_alloc(rt, node_index,
                                                      sizeof(smoother_state_t));
        smoother_state_t *st = (smoother_state_t *)node->user_data;
        st->sum_x = 0;
        st->sum_y = 0;
        st->sum_z = 0;
        st->count = 0;
        st->window = 3;
    }

    smoother_state_t *st = (smoother_state_t *)node->user_data;
    cfl_json_packet_t *in_pkt = (cfl_json_packet_t *)event_data;

    double x, y, z;
    cfl_json_get_float(in_pkt, "payload.x", &x);
    cfl_json_get_float(in_pkt, "payload.y", &y);
    cfl_json_get_float(in_pkt, "payload.z", &z);

    st->sum_x += x;
    st->sum_y += y;
    st->sum_z += z;
    st->count++;

    printf("TRANSFORM: accumulating %u/%u\n", st->count, st->window);

    if (st->count >= st->window) {
        cfl_json_set_string(node->out_packet, "type", "smoothed_telemetry");
        cfl_json_set_object(node->out_packet, "payload");
        cfl_json_set_float(node->out_packet, "payload.x", st->sum_x / st->window);
        cfl_json_set_float(node->out_packet, "payload.y", st->sum_y / st->window);
        cfl_json_set_float(node->out_packet, "payload.z", st->sum_z / st->window);

        printf("TRANSFORM: emitting smoothed packet\n");

        st->sum_x = 0;
        st->sum_y = 0;
        st->sum_z = 0;
        st->count = 0;

        return true;  /* emit output */
    }

    return false;  /* not ready yet */
}

/* ========================================================================
 * VERIFY X RANGE — returns true if payload.x is in [0.0, 0.5]
 * Used as a filter: prints PASS/REJECT, returns result
 * ======================================================================== */

bool json_verify_x_range_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    double x = 0;
    cfl_json_get_float(pkt, "payload.x", &x);

    int32_t seq = 0;
    cfl_json_get_int(pkt, "seq", &seq);

    if (x >= 0.0 && x <= 0.5) {
        printf("VERIFY: seq=%d x=%.3f PASS\n", seq, x);
        return true;
    } else {
        printf("VERIFY: seq=%d x=%.3f REJECT\n", seq, x);
        return false;
    }
}

/* ========================================================================
 * VERIFIED SINK — prints packets that passed verification
 * ======================================================================== */

bool json_verified_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

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

    printf("VERIFIED_SINK [%s] seq=%d: x=%.3f y=%.3f z=%.3f\n",
           topic ? topic : "?", seq, x, y, z);

    return true;
}

/* ========================================================================
 * DEFAULT SINK — catches unrouted packets (e.g. "diagnostics")
 * ======================================================================== */

bool json_default_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;

    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    const char *type_str = NULL;
    int32_t seq = 0;

    if (cfl_json_has_field(pkt, "type")) {
        cfl_json_get_string(pkt, "type", &type_str);
    }
    if (cfl_json_has_field(pkt, "seq")) {
        cfl_json_get_int(pkt, "seq", &seq);
    }

    printf("DEFAULT_SINK: unrouted type='%s' seq=%d\n",
           type_str ? type_str : "unknown", seq);

    return true;
}
