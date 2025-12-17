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
#include "cfl_timer_system.h"

#include "stream_test_1.h"

typedef struct{
    unsigned device_id;
    unsigned seq;
}packet_generator_aux_data_t;

void packet_generator_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    bool allocator_state = cfl_allocate_state(runtime_handle, node_index);
    cfl_emit_setup_data_t *emit_setup_data = NULL;
    if (!allocator_state) {
        emit_setup_data = (cfl_emit_setup_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_emit_setup_data_t));
        emit_setup_data->aux_data = (packet_generator_aux_data_t *)cfl_additional_arena_alloc(runtime_handle, node_index, sizeof(packet_generator_aux_data_t));
        emit_setup_data->packet_data = (accelerometer_reading_packet_t *)cfl_additional_arena_alloc(runtime_handle, node_index, sizeof(accelerometer_reading_packet_t));
        json_decoder_init_from_runtime(runtime_handle, node_index);
        
        int32_t temp;
         json_extract_int32_runtime(runtime_handle, "node_dict.aux_data.device_id", &temp);
        ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->device_id = (unsigned)temp;
        ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->seq = 0;
      
       
        cfl_emit_setup_data_init(emit_setup_data, runtime_handle, node_index);
        
        
    }else{
        
        emit_setup_data = (cfl_emit_setup_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
        ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->seq = ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->seq + 1;
    }
    
    
    
  
    accelerometer_reading_packet_t *pkt = (accelerometer_reading_packet_t *)emit_setup_data->packet_data;
    accelerometer_reading_t *pkt_data = accelerometer_reading_packet_encode(pkt,node_index);
    pkt_data->x = (float)rand() / ((float)RAND_MAX)*1.0f;
    pkt_data->y = (float)rand() / ((float)RAND_MAX)*2.0f;
    pkt_data->z = (float)rand() / ((float)RAND_MAX)*3.0f;
    pkt_data->header.device_id = ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->device_id;
    pkt_data->header.seq = ((packet_generator_aux_data_t *)emit_setup_data->aux_data)->seq;
    pkt_data->header.timestamp = cfl_timer_get_timestamp(runtime_handle->timer_handle);
 
    cfl_emit_packet_verify(pkt, emit_setup_data);
    cfl_send_streaming_data_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW, emit_setup_data->event_column_id, emit_setup_data->event_id, pkt);
    
}

bool packet_sink_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (event_id == CFL_INIT_EVENT) {

        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_string_runtime(runtime_handle, "node_dict.aux_data.sink_message", (const char **)&ptr->aux_data);

        return true;
    }
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    uint16_t source_node=0;
    const accelerometer_reading_t* pkt_data = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt_data == NULL) {
        EXCEPTION("accelerometer_reading_packet_verify failed");
    }
    printf("sink event received\n");
    printf("sink event received: %f, %f, %f from node %d\n", pkt_data->x, pkt_data->y, pkt_data->z, source_node);
    printf("sink header: %d, %d, %f\n", pkt_data->header.device_id, pkt_data->header.seq, pkt_data->header.timestamp);
    return true;
}

bool packet_tap_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (event_id == CFL_INIT_EVENT) {

        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_string_runtime(runtime_handle, "node_dict.aux_data.log_message", (const char **)&ptr->aux_data);
        
        return true;
    }
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    uint16_t source_node=0;
    const accelerometer_reading_t* pkt_data = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt_data == NULL) {
        EXCEPTION("accelerometer_reading_packet_verify failed");
    }
    printf("tap event received\n");
    printf("tap event received: %f, %f, %f from node %d\n", pkt_data->x, pkt_data->y, pkt_data->z, source_node);
    printf("tap header: %d, %d, %f\n", pkt_data->header.device_id, pkt_data->header.seq, pkt_data->header.timestamp);
    return true;   return true;
}

bool packet_filter_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_one_port_monitor_data_t *ptr = (cfl_one_port_monitor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (event_id == CFL_INIT_EVENT) {
        if (ptr->aux_data == NULL) {
            ptr->aux_data = (void *)cfl_additional_arena_alloc(runtime_handle, node_index, sizeof(float));
            float temp;
            json_decoder_init_from_runtime(runtime_handle, node_index);
            json_extract_float32_runtime(runtime_handle, "node_dict.aux_data.x", &temp);
            *((float *)ptr->aux_data) = temp;
        
        }
        return true;
        
    }
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    uint16_t source_node=0;
    
    const accelerometer_reading_t* pkt_data = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt_data == NULL) {
        EXCEPTION("accelerometer_reading_packet_verify failed");
    }
    printf("filter event received\n");
    printf("filter event received: %f, %f, %f from node %d\n", pkt_data->x, pkt_data->y, pkt_data->z, source_node);
    printf("filter header: %d, %d, %f\n", pkt_data->header.device_id, pkt_data->header.seq, pkt_data->header.timestamp);
    if (pkt_data->x > *((float *)ptr->aux_data)) {
        return false;
    }
    return true;
    
}
