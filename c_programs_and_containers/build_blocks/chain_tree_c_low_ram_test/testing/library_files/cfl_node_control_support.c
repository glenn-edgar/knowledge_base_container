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


static void decode_port(
    const cfl_runtime_handle_t *runtime,
    const char *port_path,
    cfl_port_t *port);


void cfl_controlled_node_container_init_one_shot_fn(void *handle,unsigned node_index){
    (void)handle;
    (void)node_index;
}

void cfl_controlled_node_container_term_one_shot_fn(void *handle,unsigned node_index){
    (void)handle;
    (void)node_index;
}







unsigned cfl_controlled_node_container_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_CONTINUE;
}





void cfl_controlled_node_init_one_shot_fn(void *handle,unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_controlled_node_init_one_shot_fn\n");
    exit(0);
}

void cfl_controlled_node_term_one_shot_fn(void *handle,unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_controlled_node_term_one_shot_fn\n");
    exit(0);
}


unsigned cfl_controlled_node_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("cfl_controlled_node_main_main_fn\n");
    exit(0);
    return CFL_CONTINUE;
}


/**
 * Initialize client controlled node from JSON
 * Decodes generic fields: request_port, response_port, server_node_index
 * 
 * @param runtime Runtime handle
 * @param node_index Node index
 * @param node_data Output node data structure (must be pre-allocated)
 */


void cfl_client_controlled_node_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_client_controlled_node_t *node_data)
{
    int32_t temp_int;
    
    // Initialize decoder for this node
    json_decoder_init_from_runtime(runtime, node_index);
    json_print_node_data_runtime(runtime, node_index);
    
    // Decode request port
    decode_port(runtime, "node_dict.request_port", &node_data->request_port);
    
    // Decode response port
    decode_port(runtime, "node_dict.response_port", &node_data->response_port);
    
    // Extract server_node_index
    json_extract_int32_runtime(runtime, "node_dict.server_node_index", &temp_int);
    node_data->server_node_index = (unsigned)temp_int;
    
    // Initialize runtime fields
    node_data->aux_data = NULL;
    node_data->node_is_active = false;
}

void cfl_client_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = NULL;
    
    if (cfl_allocate_state(handle, node_index) == false)
    {
        ptr = (cfl_client_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_client_controlled_node_t));
        cfl_client_controlled_node_decode(runtime, node_index, ptr);
    }
    else
    {
        ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(
            runtime->arena_system, node_index);
        ptr->node_is_active = false;
    }
    printf("ptr->request_port.header.schema_file: %s\n", ptr->request_port.header.schema_file);
    printf("ptr->request_port.header.index: %d\n", ptr->request_port.header.index);
    printf("ptr->request_port.event_id: %u\n", ptr->request_port.event_id);
    printf("ptr->server_node_index: %u\n", ptr->server_node_index);
    printf("ptr->response_port.header.schema_file: %s\n", ptr->response_port.header.schema_file);
    printf("ptr->response_port.header.index: %d\n", ptr->response_port.header.index);
    printf("ptr->response_port.event_id: %u\n", ptr->response_port.event_id);
    printf("ptr->aux_data: %p\n", ptr->aux_data);
    
}



void cfl_client_controlled_node_term_one_shot_fn(void *handle,unsigned node_index){
    
    (void)handle;
    (void)node_index;
    printf("cfl_client_controlled_node_term_one_shot_fn\n");
    exit(0);
}
unsigned cfl_client_controlled_node_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("cfl_client_controlled_node_main_main_fn\n");
    exit(0);
    return CFL_CONTINUE;
}




static void decode_port(
    const cfl_runtime_handle_t *runtime,
    const char *port_path,
    cfl_port_t *port)
{
    char path_buf[128];
    int32_t temp_int;
    
    // Extract file_name -> header.schema_file
    snprintf(path_buf, sizeof(path_buf), "%s.file_name", port_path);
    json_extract_string_runtime(runtime, path_buf, &port->header.schema_file);
    
    // Extract handler_id -> header.index
    snprintf(path_buf, sizeof(path_buf), "%s.handler_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->header.index = (uint8_t)temp_int;
    
    // Extract event_id
    snprintf(path_buf, sizeof(path_buf), "%s.event_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->event_id = (unsigned)temp_int;
    
    // Initialize runtime fields
    port->header.source_node = 0;
    port->header.length = 0;
    port->packet_pointer = NULL;
    port->data_pointer = NULL;
}