/* ========================================================================
 * user_json_drone_functions.h — User boolean functions for JSON drone tests
 * ======================================================================== */

#ifndef USER_JSON_DRONE_FUNCTIONS_H
#define USER_JSON_DRONE_FUNCTIONS_H

#include <stdbool.h>

/* Server monitors — called on request event, print flight params from JSON */
bool json_fly_straight_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_fly_arc_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_fly_up_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_fly_down_monitor_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Client completion handlers — called on response event */
bool json_on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_on_fly_arc_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_on_fly_up_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);
bool json_on_fly_down_complete_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Exception handler */
bool json_drone_exception_catch_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Server finalizer one-shots — build response JSON */
void json_fly_straight_final_one_shot_fn(void *handle, unsigned node_index);
void json_fly_arc_final_one_shot_fn(void *handle, unsigned node_index);
void json_fly_up_final_one_shot_fn(void *handle, unsigned node_index);
void json_fly_down_final_one_shot_fn(void *handle, unsigned node_index);

#endif /* USER_JSON_DRONE_FUNCTIONS_H */
