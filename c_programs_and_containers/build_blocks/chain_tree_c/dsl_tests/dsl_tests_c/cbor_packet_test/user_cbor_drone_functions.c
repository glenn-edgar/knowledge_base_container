/* ========================================================================
 * user_cbor_drone_functions.c — User functions for CBOR drone control tests
 *
 * Server monitors: event_data is char* JSON text (decoded from CBOR).
 * Client completions: event_data is char* JSON text (decoded from CBOR).
 * Finalizers: set JSON response text via cfl_cbor_server_set_response_text
 *             (the CBOR term function encodes it to CBOR before sending).
 * ======================================================================== */

#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_cbor_functions.h"
#include "cfl_cbor_node_control.h"
#include "cfl_json_packets.h"
#include "cfl_common_functions.h"
#include "user_cbor_drone_functions.h"

/* ========================================================================
 * SERVER MONITORS
 * event_data is char* JSON text (decoded from CBOR by the server main)
 * ======================================================================== */

bool cbor_fly_straight_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)node_index; (void)event_type; (void)event_id;
    if (!event_data) return false;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *server = cfl_get_cbor_server_node(rt, node_index);
    cfl_json_packet_t *pkt = cfl_json_parse(&server->heap_iface, (const char *)event_data, -1);

    double distance = 0, altitude = 0, speed = 0, heading = 0;
    cfl_json_get_float(pkt, "distance", &distance);
    cfl_json_get_float(pkt, "final_altitude", &altitude);
    cfl_json_get_float(pkt, "final_speed", &speed);
    cfl_json_get_float(pkt, "heading", &heading);

    printf("CBOR_FLY_STRAIGHT_MONITOR: distance=%.1f altitude=%.1f speed=%.1f heading=%.1f\n",
           distance, altitude, speed, heading);

    cfl_json_free(&server->heap_iface, pkt);
    return true;
}

bool cbor_fly_arc_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)node_index; (void)event_type; (void)event_id;
    if (!event_data) return false;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *server = cfl_get_cbor_server_node(rt, node_index);
    cfl_json_packet_t *pkt = cfl_json_parse(&server->heap_iface, (const char *)event_data, -1);

    double distance = 0, altitude = 0, speed = 0, heading = 0;
    cfl_json_get_float(pkt, "distance", &distance);
    cfl_json_get_float(pkt, "final_altitude", &altitude);
    cfl_json_get_float(pkt, "final_speed", &speed);
    cfl_json_get_float(pkt, "heading", &heading);

    printf("CBOR_FLY_ARC_MONITOR: distance=%.1f altitude=%.1f speed=%.1f heading=%.1f\n",
           distance, altitude, speed, heading);

    cfl_json_free(&server->heap_iface, pkt);
    return true;
}

bool cbor_fly_up_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)node_index; (void)event_type; (void)event_id;
    if (!event_data) return false;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *server = cfl_get_cbor_server_node(rt, node_index);
    cfl_json_packet_t *pkt = cfl_json_parse(&server->heap_iface, (const char *)event_data, -1);

    double altitude = 0, speed = 0;
    cfl_json_get_float(pkt, "final_altitude", &altitude);
    cfl_json_get_float(pkt, "final_speed", &speed);

    printf("CBOR_FLY_UP_MONITOR: altitude=%.1f speed=%.1f\n", altitude, speed);

    cfl_json_free(&server->heap_iface, pkt);
    return true;
}

bool cbor_fly_down_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)node_index; (void)event_type; (void)event_id;
    if (!event_data) return false;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *server = cfl_get_cbor_server_node(rt, node_index);
    cfl_json_packet_t *pkt = cfl_json_parse(&server->heap_iface, (const char *)event_data, -1);

    double altitude = 0, speed = 0;
    cfl_json_get_float(pkt, "final_altitude", &altitude);
    cfl_json_get_float(pkt, "final_speed", &speed);

    printf("CBOR_FLY_DOWN_MONITOR: altitude=%.1f speed=%.1f\n", altitude, speed);

    cfl_json_free(&server->heap_iface, pkt);
    return true;
}

/* ========================================================================
 * CLIENT COMPLETION HANDLERS
 * event_data is char* JSON text (decoded from CBOR response)
 * ======================================================================== */

bool cbor_on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle; (void)node_index; (void)event_type; (void)event_id;
    printf("CBOR_ON_FLY_STRAIGHT_COMPLETE: response=%s\n",
           event_data ? (const char *)event_data : "(null)");
    return true;
}

bool cbor_on_fly_arc_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle; (void)node_index; (void)event_type; (void)event_id;
    printf("CBOR_ON_FLY_ARC_COMPLETE: response=%s\n",
           event_data ? (const char *)event_data : "(null)");
    return true;
}

bool cbor_on_fly_up_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle; (void)node_index; (void)event_type; (void)event_id;
    printf("CBOR_ON_FLY_UP_COMPLETE: response=%s\n",
           event_data ? (const char *)event_data : "(null)");
    return true;
}

bool cbor_on_fly_down_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle; (void)node_index; (void)event_type; (void)event_id;
    printf("CBOR_ON_FLY_DOWN_COMPLETE: response=%s\n",
           event_data ? (const char *)event_data : "(null)");
    return true;
}

/* ========================================================================
 * SERVER FINALIZERS — set JSON response text (encoded to CBOR by term)
 * ======================================================================== */

void cbor_fly_straight_final_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    printf("CBOR_FLY_STRAIGHT_FINAL: setting response\n");
    cfl_cbor_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_straight\"}");
}

void cbor_fly_arc_final_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    printf("CBOR_FLY_ARC_FINAL: setting response\n");
    cfl_cbor_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_arc\"}");
}

void cbor_fly_up_final_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    printf("CBOR_FLY_UP_FINAL: setting response\n");
    cfl_cbor_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_up\"}");
}

void cbor_fly_down_final_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    printf("CBOR_FLY_DOWN_FINAL: setting response\n");
    cfl_cbor_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_down\"}");
}

/* ========================================================================
 * EXCEPTION HANDLER
 * ======================================================================== */

bool cbor_drone_exception_catch_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle; (void)node_index; (void)event_type;

    if (event_id == CFL_RAISE_EXCEPTION_EVENT) {
        uint16_t original_node_id = (uint16_t)((size_t)event_data);
        printf("CBOR_DRONE_EXCEPTION_CATCH: exception from node %u\n", original_node_id);
        return false;
    }

    return false;
}
