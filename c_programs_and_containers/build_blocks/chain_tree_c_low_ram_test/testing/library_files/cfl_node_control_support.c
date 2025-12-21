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
#include "cfl_exception_support.h"
#include "avro_common.h"





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



void cfl_server_controlled_node_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_server_controlled_node_t *node_data)
{
    
    
    // Initialize decoder for this node
    json_decoder_init_from_runtime(runtime, node_index);
    //_node_data_runtime(runtime, node_index);
    
    // Decode request port
    cfl_avro_decode_port(runtime, "node_dict.column_data.request_port", &node_data->request_port);
    
    // Decode response port
    cfl_avro_decode_port(runtime, "node_dict.column_data.response_port", &node_data->response_port);
    
    
}

void cfl_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = NULL;
    printf("cfl_controlled_node_init_one_shot_fn: node_index: %d\n", node_index);
    if (cfl_allocate_state(handle, node_index) == false)
    {
        ptr = (cfl_server_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_server_controlled_node_t));
        printf("server node allocated at: %p - %p\n", ptr, (uint8_t*)ptr + sizeof(cfl_server_controlled_node_t));
        cfl_server_controlled_node_decode(runtime, node_index, ptr);
    }
    else
    {
        ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(
            runtime->arena_system, node_index);
    }
}
void cfl_controlled_node_term_one_shot_fn(void *handle,unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_controlled_node_term_one_shot_fn\n");
    exit(0);
}


unsigned cfl_controlled_node_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    
    if(event_id == ptr->request_port.event_id)
    {
        if(event_type != CFL_EVENT_TYPE_STREAMING_DATA)
        {
            EXCEPTION("cfl_controlled_node_main_main_fn: event type is not CFL_EVENT_TYPE_STREAMING_DATA");
        }
        if(cfl_packet_matches_port(event_data, &ptr->request_port) == false)
        {
            EXCEPTION("cfl_controlled_node_main_main_fn: packet does not match request port");
        }
        ptr->client_node_index = cfl_avro_get_source_node(event_data);
        
        boolean_function_t boolean_function = runtime->flash_handle->boolean_functions[bool_function_index];
        boolean_function(runtime, node_index, event_type, event_id, event_data);
        
        cfl_enable_all_nodes(runtime, node_index);
        return CFL_HALT;
    }
    if(event_id == CFL_RAISE_EXCEPTION_EVENT){
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: event_type is not CFL_EVENT_TYPE_JSON_RECORD");
        }
        uint16_t original_node_id = (uint32_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, ptr->client_node_index, original_node_id);
    
        return CFL_DISABLE;

    }

    unsigned return_value = cfl_verify_active_children(runtime, node_index);
    
    
    return return_value;
    

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
    //json_print_node_data_runtime(runtime, node_index);
    
    // Decode request port
    cfl_avro_decode_port(runtime, "node_dict.request_port", &node_data->request_port);
    
    // Decode response port
    cfl_avro_decode_port(runtime, "node_dict.response_port", &node_data->response_port);
    
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
    
    
}



void cfl_client_controlled_node_term_one_shot_fn(void *handle,unsigned node_index){
    
    (void)handle;
    (void)node_index;
    printf("cfl_client_controlled_node_term_one_shot_fn\n");
    exit(0);
}
unsigned cfl_client_controlled_node_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    
    if(ptr->node_is_active == false)
    {
        
        uint16_t server_node_id = ptr->server_node_index;
        if(cfl_engine_node_is_enabled(runtime, server_node_id) == true)
        {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: server node is already enabled");
        }
        if(cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false)
        {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: packet does not match request port");
        }

    

        runtime->flags[server_node_id] &= ~CT_FLAG_USER_MASK;
        runtime->flags[server_node_id] |= (CT_FLAG_USER3 | CT_FLAG_USER2);
        
        const chaintree_node_t server_node = runtime->flash_handle->nodes[server_node_id];
        one_shot_function_t one_shot_function = runtime->flash_handle->one_shot_functions[server_node.init_function_index];

        one_shot_function(runtime, server_node_id);
        

        boolean_function_t boolean_function = runtime->flash_handle->boolean_functions[server_node.aux_function_index];
        boolean_function(runtime, server_node_id, CFL_EVENT_TYPE_NULL, CFL_INIT_EVENT, NULL);
        main_function_t main_function = runtime->flash_handle->main_functions[server_node.main_function_index];
    
        main_function(runtime,server_node.aux_function_index, server_node_id, CFL_EVENT_TYPE_STREAMING_DATA,ptr->request_port.event_id,
            ptr->request_port.packet_pointer);
        ptr->node_is_active = true;
        return CFL_HALT;
    }

    if(event_id ==ptr->response_port.event_id ){
        if(event_type != CFL_EVENT_TYPE_STREAMING_DATA)
        {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: event type is not CFL_EVENT_TYPE_STREAMING_DATA");
        }
        if(cfl_packet_matches_port(event_data, &ptr->response_port) == false)
        {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: packet does not match response port");
        }
        boolean_function_t boolean_function = runtime->flash_handle->boolean_functions[bool_function_index];
        boolean_function(runtime, node_index, event_type, event_id, event_data);
        return CFL_DISABLE;

    }
    if(event_id == CFL_RAISE_EXCEPTION_EVENT){
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_client_controlled_node_main_main_fn: event_type is not CFL_EVENT_TYPE_JSON_RECORD");
        }
        uint16_t original_node_id = (uint32_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, 0xffff, original_node_id);
    
        return CFL_DISABLE;

    }
   
    return CFL_HALT;
}




