

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
#include "drone_control.h"





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
    (void)event_type;
    (void)event_data;
    (void)event_id;
    (void)handle;
    (void)node_index;
    printf("on_fly_down_complete_boolean_fn\n");
    exit(0);
    return true;
}




bool on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    
    (void)event_type;
    
    (void)event_data;
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if( event_id == CFL_INIT_EVENT)
    {
        if(ptr->aux_data != NULL)
        {
            return false; // alreday allocatoed
        }
        
    
        ptr->request_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_straight_request_packet_t));  // <-- Full packet size
        ptr->request_port.data_pointer =  fly_straight_request_packet_encode((void *)ptr->request_port.packet_pointer, node_index);
        if( cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false)
        {
            EXCEPTION("on_fly_straight_complete_boolean_fn: failed to match port");
            return false;
        }
        
        fly_straight_request_t *req = (fly_straight_request_t *)ptr->request_port.data_pointer;
        json_decoder_init_from_runtime(runtime, node_index);
        //json_print_node_data_runtime(runtime, node_index);
        const char *api_name = NULL;
        json_extract_string_runtime(runtime, "node_dict.api_name", &api_name);
        //printf("API Name: %s\n", api_name); 
        if (strcmp(api_name, "drone_control_fly_straight") != 0) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: API name does not match");
            return true;
        }
        json_extract_float32_runtime(runtime, "node_dict.aux_data.distance", &req->distance);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_altitude", &req->final_altitude);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_speed", &req->final_speed);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.heading", &req->heading);
    
    // Extract finalize_data.waypoint as string pointer
        const char *waypoint = NULL;
        json_extract_string_runtime(runtime, "node_dict.aux_data.finalize_data.waypoint", &waypoint);
        req->finalize.ptr = (void *)waypoint;
        cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port);
        
        
        return false;
    }
    if( event_id == CFL_TERMINATE_EVENT)
    {
        printf("on_fly_straight_complete_boolean_fn: TERMINATE_EVENT\n");
        exit(0);
        return false;
    }
    
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
    (void)event_type;
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if( event_id == CFL_INIT_EVENT)
    {
    
        if(ptr->response_port.packet_pointer != NULL)
        {
            return false; // alreday allocatoed
        }
        ptr->response_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_straight_response_packet_t));
        ptr->response_port.data_pointer =  fly_straight_response_packet_encode((void *)ptr->response_port.packet_pointer, node_index);
        fly_straight_response_t *resp = (fly_straight_response_t *)ptr->response_port.data_pointer;
        resp->success = false;
        resp->error_code = 0;
      
    
    }
    if( event_id == CFL_TERMINATE_EVENT)
    {
        printf("on_fly_down_complete_boolean_fn: TERMINATE_EVENT\n");
        exit(0);
        return false;
    }
    if( event_id == ptr->request_port.event_id)
    {
        uint16_t source_node = 0;
        const fly_straight_request_t *req = fly_straight_request_packet_verify(event_data, &source_node);
        
        ptr->request_port.packet_pointer = (void *)event_data;
        ptr->request_port.data_pointer = (void *)req;
        return true;
    }

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