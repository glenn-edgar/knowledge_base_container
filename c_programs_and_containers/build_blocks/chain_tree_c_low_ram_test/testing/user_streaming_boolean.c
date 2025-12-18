#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cfl_runtime.h"
#include "cfl_exception.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"
#include "cfl_streaming_support.h"
#include "avro_common.h"
#include "cfl_timer_system.h"

#include "stream_test_1.h"

// ============ EMIT (ONE-SHOT) ============

typedef struct {
    unsigned device_id;
    unsigned seq;
} packet_generator_user_data_t;

void packet_generator_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    bool already_allocated = cfl_allocate_state(rt, node_index);
    cfl_emit_node_t *node;
    
    if (!already_allocated) {
        // First call - allocate and initialize
        node = (cfl_emit_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_emit_node_t));
        if (node == NULL) {
            EXCEPTION("packet_generator_one_shot_fn: Failed to allocate node");
        }
        
        // Allocate user data
        node->user_data = cfl_additional_arena_alloc(rt, node_index, sizeof(packet_generator_user_data_t));
        if (node->user_data == NULL) {
            EXCEPTION("packet_generator_one_shot_fn: Failed to allocate user_data");
        }
        
        // Allocate packet buffer
        node->packet_buffer = cfl_additional_arena_alloc(rt, node_index, sizeof(accelerometer_reading_packet_t));
        if (node->packet_buffer == NULL) {
            EXCEPTION("packet_generator_one_shot_fn: Failed to allocate packet_buffer");
        }
        
        // Parse JSON and initialize
        json_decoder_init_from_runtime(rt, node_index);
        
        // Initialize outport from JSON
        outport_init(&node->outport, rt, "node_dict.outport", "node_dict.event_column");
        
        // Initialize user-specific data
        int32_t temp;
        json_extract_int32_runtime(rt, "node_dict.aux_data.device_id", &temp);
        
        packet_generator_user_data_t *user = (packet_generator_user_data_t *)node->user_data;
        user->device_id = (unsigned)temp;
        user->seq = 0;
        
    } else {
        // Subsequent calls - retrieve state and increment seq
        node = cfl_get_emit_node(rt, node_index);
        packet_generator_user_data_t *user = (packet_generator_user_data_t *)node->user_data;
        user->seq++;
    }
    
    // Generate packet
    packet_generator_user_data_t *user = (packet_generator_user_data_t *)node->user_data;
    accelerometer_reading_packet_t *pkt = (accelerometer_reading_packet_t *)node->packet_buffer;
    accelerometer_reading_t *data = accelerometer_reading_packet_encode(pkt, node_index);
    
    data->x = (float)rand() / (float)RAND_MAX * 1.0f;
    data->y = (float)rand() / (float)RAND_MAX * 2.0f;
    data->z = (float)rand() / (float)RAND_MAX * 3.0f;
    data->header.device_id = user->device_id;
    data->header.seq = user->seq;
    data->header.timestamp = cfl_timer_get_timestamp(rt->timer_handle);
    
    // Emit packet
    cfl_emit_packet(rt, &node->outport, pkt);
}

// ============ SINK A (raw accelerometer) ============

bool packet_sink_a_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            json_decoder_init_from_runtime(rt, node_index);
            json_extract_string_runtime(rt, "node_dict.aux_data.sink_message", (const char **)&node->user_data);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Process packet
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_sink_a_boolean_fn: packet verify failed");
    }
    
    const char *msg = (const char *)node->user_data;
    printf("sink_a [%s]: x=%.3f, y=%.3f, z=%.3f from node %d\n", 
           msg, pkt->x, pkt->y, pkt->z, source_node);
    printf("  header: device=%d, seq=%d, time=%.3f\n", 
           pkt->header.device_id, pkt->header.seq, pkt->header.timestamp);
    
    return true;
}

// ============ SINK B (filtered accelerometer) ============

bool packet_sink_b_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            json_decoder_init_from_runtime(rt, node_index);
            json_extract_string_runtime(rt, "node_dict.aux_data.sink_message", (const char **)&node->user_data);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Process packet
    uint16_t source_node = 0;
    const accelerometer_reading_filtered_t *pkt = accelerometer_reading_filtered_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_sink_b_boolean_fn: packet verify failed");
    }
    
    const char *msg = (const char *)node->user_data;
    printf("sink_b [%s]: x=%.3f, y=%.3f, z=%.3f from node %d\n", 
           msg, pkt->x, pkt->y, pkt->z, source_node);
    printf("  header: device=%d, seq=%d, time=%.3f\n", 
           pkt->header.device_id, pkt->header.seq, pkt->header.timestamp);
    
    return true;
}

// ============ TAP ============

bool packet_tap_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            json_decoder_init_from_runtime(rt, node_index);
            json_extract_string_runtime(rt, "node_dict.aux_data.log_message", (const char **)&node->user_data);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Process packet
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_tap_boolean_fn: packet verify failed");
    }
    
    const char *msg = (const char *)node->user_data;
    printf("tap [%s]: x=%.3f, y=%.3f, z=%.3f from node %d\n", 
           msg, pkt->x, pkt->y, pkt->z, source_node);
    
    return true;
}

// ============ FILTER ============

typedef struct {
    float threshold;
} filter_user_data_t;

bool packet_filter_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            node->user_data = cfl_additional_arena_alloc(rt, node_index, sizeof(filter_user_data_t));
            if (node->user_data == NULL) {
                EXCEPTION("packet_filter_boolean_fn: Failed to allocate user_data");
            }
            
            json_decoder_init_from_runtime(rt, node_index);
            filter_user_data_t *user = (filter_user_data_t *)node->user_data;
            json_extract_float32_runtime(rt, "node_dict.aux_data.x", &user->threshold);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Process packet
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_filter_boolean_fn: packet verify failed");
    }
    
    filter_user_data_t *user = (filter_user_data_t *)node->user_data;
    
    printf("filter: x=%.3f (threshold=%.3f)\n", pkt->x, user->threshold);
    
    // Return false to halt column if x exceeds threshold
    if (pkt->x > user->threshold) {
        printf("filter: BLOCKED (x > threshold)\n");
        return false;
    }
    
    return true;
}

// ============ TRANSFORM ============

typedef struct {
    unsigned sum_count;
    unsigned sum_index;
    accelerometer_reading_filtered_packet_t *out_pkt;
    accelerometer_reading_filtered_t *out_data;
} transform_user_data_t;

bool packet_transform_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_transform_node_t *node = cfl_get_transform_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            // Allocate user data
            node->user_data = cfl_additional_arena_alloc(rt, node_index, sizeof(transform_user_data_t));
            if (node->user_data == NULL) {
                EXCEPTION("packet_transform_boolean_fn: Failed to allocate user_data");
            }
            
            transform_user_data_t *user = (transform_user_data_t *)node->user_data;
            
            // Allocate output packet buffer
            user->out_pkt = (accelerometer_reading_filtered_packet_t *)cfl_additional_arena_alloc(
                rt, node_index, sizeof(accelerometer_reading_filtered_packet_t));
            if (user->out_pkt == NULL) {
                EXCEPTION("packet_transform_boolean_fn: Failed to allocate out_pkt");
            }
            
            // Encode packet header and get data pointer
            user->out_data = accelerometer_reading_filtered_packet_encode(user->out_pkt, node_index);
            
            // Verify packet matches outport
            if (!cfl_packet_matches_port(user->out_pkt, &node->outport.port)) {
                EXCEPTION("packet_transform_boolean_fn: output packet doesn't match outport");
            }
            
            // Initialize output data
            user->out_data->x = 0;
            user->out_data->y = 0;
            user->out_data->z = 0;
            user->out_data->header.device_id = 0;
            user->out_data->header.seq = 0;
            user->out_data->header.timestamp = 0;
            
            // Parse averaging count from JSON
            json_decoder_init_from_runtime(rt, node_index);
            int32_t temp;
            json_extract_int32_runtime(rt, "node_dict.aux_data.average", &temp);
            user->sum_count = (unsigned)temp;
            user->sum_index = 0;
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Process input packet
    uint16_t source_node = 0;
    const accelerometer_reading_t *in_pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (in_pkt == NULL) {
        EXCEPTION("packet_transform_boolean_fn: input packet verify failed");
    }
    
    transform_user_data_t *user = (transform_user_data_t *)node->user_data;
    
    // Reset accumulator at start of new averaging window
    if (user->sum_index == 0) {
        user->out_data->header.seq++;
        user->out_data->x = 0;
        user->out_data->y = 0;
        user->out_data->z = 0;
    }
    
    // Accumulate
    user->out_data->x += in_pkt->x;
    user->out_data->y += in_pkt->y;
    user->out_data->z += in_pkt->z;
    user->sum_index++;
    
    printf("transform: accumulating %u/%u\n", user->sum_index, user->sum_count);
    
    // Emit averaged packet when window is complete
    if (user->sum_index >= user->sum_count) {
        user->sum_index = 0;
        user->out_data->x /= user->sum_count;
        user->out_data->y /= user->sum_count;
        user->out_data->z /= user->sum_count;
        user->out_data->header.timestamp = cfl_timer_get_timestamp(rt->timer_handle);
        
        printf("transform: emitting averaged packet\n");
        cfl_emit_packet(rt, &node->outport, user->out_pkt);
    }
    
    return true;
}

// ============ COLLECTOR ============

// ============ COLLECTOR AUX FUNCTION ============
// Returns true to accept packet, false to reject

bool packet_collector_boolean_fn(void *handle, unsigned node_index, unsigned port_index, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_collect_node_t *node = cfl_get_collect_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        // User-specific init if needed
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // Verify packet
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_collector_boolean_fn: packet verify failed");
    }
    
    printf("collector: evaluating packet from port %u, device %u (x=%.3f)\n", 
           port_index, pkt->header.device_id, pkt->x);
    
    // Example filter: reject packets with x < 0.1
    if (pkt->x < 0.1f) {
        printf("collector: REJECTED (x too low)\n");
        return false;
    }
    
    printf("collector: ACCEPTED (count will be %u/%u)\n", 
           node->container.count + 1, node->container.capacity);
    return true;
}

// ============ COLLECTOR SINK ============
// Receives container with collected packet pointers

bool packet_collector_sink_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_collected_sink_node_t *node = cfl_get_collected_sink_node(rt, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            json_decoder_init_from_runtime(rt, node_index);
            json_extract_string_runtime(rt, "node_dict.aux_data.sink_message", (const char **)&node->user_data);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    // event_data is pointer to cfl_packet_container_t
    const cfl_packet_container_t *container = (const cfl_packet_container_t *)event_data;
    const char *msg = (const char *)node->user_data;
    
    printf("collector_sink [%s]: received %u packets\n", msg, container->count);
    
    // Iterate through collected packets
    for (unsigned i = 0; i < container->count; i++) {
        uint16_t source_node = 0;
        const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(
            container->packets[i], &source_node);
        
        if (pkt != NULL) {
            printf("  [%u] port=%u, x=%.3f, y=%.3f, z=%.3f, device=%u\n",
                   i, container->port_indices[i],
                   pkt->x, pkt->y, pkt->z, pkt->header.device_id);
        }
    }
    
    return true;
}


// ============ VERIFY X RANGE ============

bool packet_verify_x_range_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_verify_packet_node_t *node = cfl_get_streaming_verify_node(rt, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
            node->user_data = cfl_additional_arena_alloc(rt, node_index, sizeof(float) * 2);
            if (node->user_data == NULL) {
                EXCEPTION("packet_verify_x_range_boolean_fn: Failed to allocate user_data");
            }
            
            json_decoder_init_from_runtime(rt, node_index);
            float *limits = (float *)node->user_data;
            // Note: paths are under fn_data.aux_data
            json_extract_float32_runtime(rt, "node_dict.fn_data.aux_data.min_x", &limits[0]);
            json_extract_float32_runtime(rt, "node_dict.fn_data.aux_data.max_x", &limits[1]);
            
            printf("verify_x_range: initialized with range [%.3f, %.3f]\n", limits[0], limits[1]);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        printf("verify_x_range: packet decode failed\n");
        return false;
    }
    
    float *limits = (float *)node->user_data;
    
    if (pkt->x < limits[0] || pkt->x > limits[1]) {
        printf("verify_x_range: FAILED - x=%.3f not in range [%.3f, %.3f] -> RESET\n", 
               pkt->x, limits[0], limits[1]);
        return false;
    }
    
    printf("verify_x_range: PASSED - x=%.3f in range [%.3f, %.3f]\n",
           pkt->x, limits[0], limits[1]);
    return true;
}

// ============ VERIFIED SINK ============

bool packet_verified_sink_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    printf("packet_verified_sink_boolean_fn: event_id = %u\n", event_id);
    if (event_id == CFL_INIT_EVENT) {
        if (node->user_data == NULL) {
        
            json_decoder_init_from_runtime(rt, node_index);
            
            json_extract_string_runtime(rt, "node_dict.aux_data.sink_message", (const char **)&node->user_data);
        }
        return true;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
        return true;
    }
    
    uint16_t source_node = 0;
    const accelerometer_reading_t *pkt = accelerometer_reading_packet_verify(event_data, &source_node);
    if (pkt == NULL) {
        EXCEPTION("packet_verified_sink_boolean_fn: packet verify failed");
    }
    
    const char *msg = (const char *)node->user_data;
    printf("verified_sink [%s]: x=%.3f, y=%.3f, z=%.3f from node %d\n",
           msg, pkt->x, pkt->y, pkt->z, source_node);
    printf("  header: device=%d, seq=%d, time=%.3f\n",
           pkt->header.device_id, pkt->header.seq, pkt->header.timestamp);
    
    return true;
}