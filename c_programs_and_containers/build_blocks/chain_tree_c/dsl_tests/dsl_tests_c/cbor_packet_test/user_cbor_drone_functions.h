#ifndef USER_CBOR_DRONE_FUNCTIONS_H
#define USER_CBOR_DRONE_FUNCTIONS_H

#include <stdbool.h>

/* Server monitors — parse JSON text (decoded from CBOR request) */
bool cbor_fly_straight_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_fly_arc_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_fly_up_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_fly_down_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Client completions — parse JSON text (decoded from CBOR response) */
bool cbor_on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_on_fly_arc_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_on_fly_up_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool cbor_on_fly_down_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Server finalizers — build JSON response (encoded to CBOR by term) */
void cbor_fly_straight_final_one_shot_fn(void *handle, unsigned node_index);
void cbor_fly_arc_final_one_shot_fn(void *handle, unsigned node_index);
void cbor_fly_up_final_one_shot_fn(void *handle, unsigned node_index);
void cbor_fly_down_final_one_shot_fn(void *handle, unsigned node_index);

/* Exception handler */
bool cbor_drone_exception_catch_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

#endif /* USER_CBOR_DRONE_FUNCTIONS_H */
