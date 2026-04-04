#ifndef USER_CBOR_FUNCTIONS_H
#define USER_CBOR_FUNCTIONS_H

#include <stdbool.h>

/* Telemetry sink — extracts x/y/z from telemetry packets */
bool cbor_telem_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Verify x range — returns true if payload.x in [0.0, 0.5] */
bool cbor_verify_x_range_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Verified sink — prints packets that passed verification */
bool cbor_verified_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

#endif /* USER_CBOR_FUNCTIONS_H */
