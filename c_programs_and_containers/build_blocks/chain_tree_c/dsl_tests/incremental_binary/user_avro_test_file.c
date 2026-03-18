#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "cfl_runtime.h"
#include "cfl_common_functions.h"

#include "avro_packets/stream_test_1.h"
#include "avro_packets/stream_test_1_bin.h"
#include "json_node_decoder.h"


void generate_const_avro_packet_one_shot(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    
    int32_t event_id;
    int32_t event_node_index;
    json_extract_int32_runtime(runtime, "node_dict.event_id", &event_id);
    json_extract_int32_runtime(runtime, "node_dict.node_index", &event_node_index);
    
    // Allocate and copy from const template
    accelerometer_reading_packet_t *pkt = cfl_smart_arena_alloc(runtime, node_index, sizeof(accelerometer_reading_packet_t));
    memcpy(pkt, default_accel_reading_bin, DEFAULT_ACCEL_READING_SIZE);
    
    // Stamp header with runtime timestamp/seq
    cfl_avro_update_packet_header(runtime, pkt);
    cfl_avro_update_packet_header_source_node(runtime, pkt, node_index);
    
    cfl_send_data_event(
        runtime->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        event_node_index,
        false,
        event_id,
        pkt);
}
void generate_avro_packet_one_shot(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    
    int32_t event_id;
    int32_t event_node_index;
    json_extract_int32_runtime(runtime, "node_dict.event_id", &event_id);
    json_extract_int32_runtime(runtime, "node_dict.node_index", &event_node_index);
    accelerometer_reading_packet_t *pkt = cfl_smart_arena_alloc(runtime, node_index, sizeof(accelerometer_reading_packet_t));
    accelerometer_reading_wire_t* data = accelerometer_reading_packet_init(pkt, node_index);
    data->x = 1.0f;
    data->y = 2.0f;
    data->z = 3.0f;
    cfl_avro_update_packet_header(runtime, pkt);
    printf("event_node_index: %d\n", event_node_index);
    printf("event_id: %d\n", event_id);
    printf("node_index: %d\n", node_index);
    cfl_send_data_event(
        runtime->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        event_node_index,
        false,
        event_id,
        pkt);
}

typedef struct {
    int32_t event_id;
} avro_verify_packet_init_data_t;

void avro_verify_packet_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    avro_verify_packet_init_data_t *data = cfl_smart_arena_alloc(runtime, node_index, sizeof(avro_verify_packet_init_data_t));
    json_extract_int32_runtime(runtime, "node_dict.event_id", &data->event_id);
}

unsigned int avro_verify_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    (void)bool_function_index;
    (void)event_type;
    
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    avro_verify_packet_init_data_t *init_data = cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if (event_id == (unsigned)init_data->event_id)
    {
        const accelerometer_reading_packet_t *packet = (const accelerometer_reading_packet_t *)event_data;
        
        // Print header fields directly from wire header
        printf("schema_hash: 0x%08X\n", packet->header.schema_hash);
        printf("timestamp: %f\n", packet->header.timestamp);
        printf("seq: %d\n", packet->header.seq);
        printf("source_node: %d\n", packet->header.source_node);
    
        
        // Verify and access payload
        const accelerometer_reading_wire_t *reading = accelerometer_reading_packet_verify(packet);
        if (reading == NULL)
        {
            printf("packet verification failed\n");
            EXCEPTION("packet verification failed");
        }
        
        printf("packet received from node %d\n", packet->header.source_node);
        printf("x: %f, y: %f, z: %f\n", reading->x, reading->y, reading->z);
        return CFL_DISABLE;
    }
    return CFL_CONTINUE;
}


void avro_verify_const_packet_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    avro_verify_packet_init_data_t *data = cfl_smart_arena_alloc(runtime, node_index, sizeof(avro_verify_packet_init_data_t));
    json_extract_int32_runtime(runtime, "node_dict.event_id", &data->event_id);
}

unsigned int avro_verify_const_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    (void)bool_function_index;
    (void)event_type;
    
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    avro_verify_packet_init_data_t *init_data = cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if (event_id == (unsigned)init_data->event_id)
    {
        const accelerometer_reading_packet_t *packet = (const accelerometer_reading_packet_t *)event_data;
        
        // Print header fields directly from wire header
        printf("schema_hash: 0x%08X\n", packet->header.schema_hash);
        printf("timestamp: %f\n", packet->header.timestamp);
        printf("seq: %d\n", packet->header.seq);
        printf("source_node: %d\n", packet->header.source_node);
    
        
        // Verify and access payload
        const accelerometer_reading_wire_t *reading = accelerometer_reading_packet_verify(packet);
        if (reading == NULL)
        {
            printf("packet verification failed\n");
            EXCEPTION("packet verification failed");
        }
        
        printf("packet received from node %d\n", packet->header.source_node);
        printf("x: %f, y: %f, z: %f\n", reading->x, reading->y, reading->z);
        return CFL_TERMINATE;
    }
    return CFL_CONTINUE;
}