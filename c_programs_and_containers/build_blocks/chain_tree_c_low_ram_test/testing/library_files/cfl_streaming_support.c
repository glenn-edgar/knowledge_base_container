#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cfl_runtime.h"
#include "cfl_exception.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"
#include "cfl_streaming_support.h"
#include "avro_common.h"


static void dispatch_streaming_data( cfl_one_port_monitor_data_t *ptr, cfl_runtime_handle_t *runtime_handle, 
        unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);




void cfl_streaming_tap_packet_init_one_shot_fn(void *handle, unsigned node_index){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = cfl_one_port_monitor_data_init(runtime_handle, node_index);

    ptr->aux_data = NULL;
    
}

void cfl_streaming_tap_packet_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;

}


unsigned cfl_streaming_tap_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (ptr == NULL) {
        EXCEPTION("cfl_streaming_tap_packet_main_fn: Failed to allocate memory");
    }
    
    dispatch_streaming_data(ptr, runtime_handle, bool_function_index, node_index, event_type, event_id, event_data);
    return CFL_CONTINUE;
}

void cfl_streaming_filter_packet_init_one_shot_fn(void *handle, unsigned node_index){
    cfl_streaming_tap_packet_init_one_shot_fn(handle, node_index);
}

void cfl_streaming_filter_packet_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_filter_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (ptr == NULL) {
        EXCEPTION("cfl_streaming_tap_packet_main_fn: Failed to allocate memory");
    }

    if (event_type == CFL_EVENT_TYPE_STREAMING_DATA) {
        if (ptr->event_id == event_id) {
            if (cfl_verify_avro_packet(event_data, &ptr->port_data)) {

               const boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
               if (boolean_function(runtime_handle, node_index, event_type, event_id, event_data)==false) {
                   return CFL_HALT;
               }
        ;
            }
        }
    }
    
    return CFL_CONTINUE;
}


void cfl_streaming_sink_packet_init_one_shot_fn(void *handle, unsigned node_index){
    cfl_streaming_tap_packet_init_one_shot_fn(handle, node_index);
}

void cfl_streaming_sink_packet_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    
}

unsigned cfl_streaming_sink_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    
    return cfl_streaming_tap_packet_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
}


cfl_one_port_monitor_data_t * cfl_one_port_monitor_data_init(cfl_runtime_handle_t *runtime_handle, uint16_t node_index){
    
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_one_port_monitor_data_t));
    if (ptr == NULL) {
        EXCEPTION("cfl_streaming_tap_packet_init_one_shot_fn: Failed to allocate memory");
    }
    json_decoder_init_from_runtime(runtime_handle, node_index);

    json_extract_string_runtime(runtime_handle, "node_dict.inport.file_name", &ptr->port_data.h_file_name);
    int32_t temp;
    json_extract_int32_runtime(runtime_handle, "node_dict.inport.handler_id", &temp);
    ptr->port_data.handler_id = (unsigned)temp;
    json_extract_int32_runtime(runtime_handle, "node_dict.event_id", &temp);
    ptr->event_id = (unsigned)temp;
    
    return ptr;
}

bool cfl_verify_avro_packet(void *data, cfl_streaming_port_data_t *port_data)
{
    const char *schema_file;
    uint16_t source_node;
    uint8_t index;
    uint16_t length;
    
    get_packet_header((const void*)data, &schema_file, &source_node, &index, &length);
    if (strcmp(schema_file, port_data->h_file_name) != 0) {
    
        return false;
    }
    if (index != port_data->handler_id) {
        
        return false;
    }

    return true;
}

void cfl_emit_setup_data_init(cfl_emit_setup_data_t *emit_setup_data, cfl_runtime_handle_t *runtime_handle, uint16_t node_index){
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_string_runtime(runtime_handle, "node_dict.outport.file_name", &emit_setup_data->port_data.h_file_name);
    int32_t temp;
    json_extract_int32_runtime(runtime_handle, "node_dict.outport.handler_id", &temp);
    emit_setup_data->port_data.handler_id = (unsigned)temp;
    json_extract_int32_runtime(runtime_handle, "node_dict.event_id", &temp);
    emit_setup_data->event_id = (unsigned)temp;
    json_extract_int32_runtime(runtime_handle, "node_dict.event_column", &temp);
    emit_setup_data->event_column_id = (unsigned)temp;
    
 
}

void cfl_emit_packet_verify(void *data, cfl_emit_setup_data_t *emit_setup_data){
    if (cfl_verify_avro_packet(data, &emit_setup_data->port_data)) {
        return;
    }
    EXCEPTION("cfl_emit_packet_verify: Failed to verify avro packet");
}


static void dispatch_streaming_data( cfl_one_port_monitor_data_t *ptr, cfl_runtime_handle_t *runtime_handle, 
        unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    if (event_type == CFL_EVENT_TYPE_STREAMING_DATA) {
        if (ptr->event_id == event_id) {
            if (cfl_verify_avro_packet(event_data, &ptr->port_data)) {

            const boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
            boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
            }
        }
    }
}