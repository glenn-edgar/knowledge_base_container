/* ========================================================================
 * user_json_functions.h — User boolean functions for JSON packet test
 *
 * No emit callback needed — emit is a universal one-shot.
 * ======================================================================== */

#ifndef USER_JSON_FUNCTIONS_H
#define USER_JSON_FUNCTIONS_H

#include <stdbool.h>

/* Telemetry sink — extracts x/y/z from telemetry packets */
bool json_telem_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Command/config sink — prints command and config packets */
bool json_cmd_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Smoothed sink — receives transform output */
bool json_smoothed_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Transform callback — accumulates 3 packets, emits smoothed */
bool json_smoother_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Verify x range — returns true if payload.x in [0.0, 0.5] */
bool json_verify_x_range_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Verified sink — prints packets that passed verification */
bool json_verified_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Default sink — catches unrouted packets */
bool json_default_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

#endif /* USER_JSON_FUNCTIONS_H */
