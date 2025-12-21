#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>


#include "cfl_runtime.h"
#include "cfl_common_functions.h"
#include "avro_common.h"
#include "streaming_test_1.h"
#include "json_node_decoder.h"

void generate_avro_packet_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    
    int32_t event_id;
    int32_t event_node_index;
    json_extract_int32_runtime(runtime, "node_dict.event_id", &event_id);
    json_extract_int32_runtime(runtime, "node_dict.node_index", &event_node_index);
    accelerometer_reading_packet_t *pkt = cfl_smart_arena_alloc(runtime, node_index, sizeof(accelerometer_reading_packet_t));
    accelerometer_reading_t* data = accelerometer_reading_packet_encode(pkt, node_index);
    data->x = 1.0;
    data->y = 2.0;
    data->z = 3.0;
    cfl_avro_update_packet_header(runtime, pkt);
    cfl_send_data_event(
        runtime->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        event_node_index,
        false,
        event_id,
        pkt);
}

typedef struct{

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
    
    const char* schema_file;
    double timestamp;
    uint32_t seq;
    uint16_t source_node;
    uint8_t index;
    uint16_t length;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    avro_verify_packet_init_data_t *init_data = cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if( event_id == (unsigned)init_data->event_id)
    {
        const void* payload = get_packet_header(event_data, &schema_file, &timestamp, &seq, &source_node, &index, &length);
        if( payload == NULL)        
        {
            printf("packet header verification failed\n");
            EXCEPTION("packet header verification failed");
        }
        printf("schema file: %s\n", schema_file);
        printf("timestamp: %f\n", timestamp);
        printf("seq: %d\n", seq);
        printf("source node: %d\n", source_node);
        printf("index: %d\n", index);
        printf("length: %d\n", length);
      
        const accelerometer_reading_t* reading = accelerometer_reading_packet_verify(event_data, &source_node);
        if( reading == NULL)
        {
            printf("packet verification failed\n");
            EXCEPTION("packet verification failed");
        }
       
        printf("packet received from node %d\n", source_node);
        printf("x: %f, y: %f, z: %f\n", reading->x, reading->y, reading->z);
        return CFL_TERMINATE;
    }
    return CFL_CONTINUE;
}