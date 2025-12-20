

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>


#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "cfl_engine.h"
#include "cfl_node_control_support.h"
#include "json_node_decoder.h"





// Boolean functions (aux functions)
bool on_fly_arc_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("on_fly_arc_complete_boolean_fn\n");
    exit(0);
    return true;
}

bool on_fly_down_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("on_fly_down_complete_boolean_fn\n");
    exit(0);
    return true;
}
typedef struct {
    const char *waypoint;
} fly_straight_finalize_data_t;

typedef struct {
    float distance;
    float final_altitude;
    float final_speed;
    float heading;
    fly_straight_finalize_data_t finalize_data;
} fly_straight_aux_data_t;

bool on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    
    (void)event_type;
    
    (void)event_data;
    fly_straight_aux_data_t *aux_data = NULL;
    if( event_id = CFL_INIT_EVENT_ID)
    {
        cfl_
        aux_data = (fly_straight_aux_data_t *)cfl_additional_arena_alloc(handle, node_index, sizeof(fly_straight_aux_data_t));
        aux_data->distance = 0.0;
        aux_data->final_altitude = 0.0;
        aux_data->final_speed = 0.0;
        aux_data->heading = 0.0;
    }
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    json_print_node_data_runtime(runtime, node_index);
    
    printf("on_fly_straight_complete_boolean_fn\n");
    exit(0);
    return true;
}

bool on_fly_up_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    json_print_node_data_runtime(handle, node_index);
    printf("on_fly_up_complete_boolean_fn\n");
    exit(0);
    return true;
}

bool fly_arc_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("fly_arc_monitor_boolean_fn\n");
    exit(0);
    return true;
}

bool fly_down_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("fly_down_monitor_boolean_fn\n");
    exit(0);
    return true;
}

bool fly_straight_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("fly_straight_monitor_boolean_fn\n");
    exit(0);
    return true;
}

bool fly_up_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("fly_up_monitor_boolean_fn\n");
    exit(0);
    return true;
}

// One-shot functions
void update_fly_arc_final_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    printf("update_fly_arc_final_one_shot_fn\n");
    exit(0);
}

void update_fly_down_final_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    printf("update_fly_down_final_one_shot_fn\n");
    exit(0);
}

void update_fly_straight_final_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    printf("update_fly_straight_final_one_shot_fn\n");
    exit(0);
}

void update_fly_up_final_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    printf("update_fly_up_final_one_shot_fn\n");
    exit(0);
}