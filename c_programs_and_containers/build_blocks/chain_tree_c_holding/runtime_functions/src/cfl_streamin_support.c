#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "cfl_streaming_support.h"
#include "cfl_exception.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"
#include "avro_common.h"

// ============ INTERNAL HELPERS ============

static void port_init(cfl_port_t *port, cfl_runtime_handle_t *rt, const char *json_path) {
    char path_buffer[128];
    int32_t temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "%s.schema_hash", json_path);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    port->schema_hash = (uint32_t)temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "%s.handler_id", json_path);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    port->handler_id = (unsigned)temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "%s.event_id", json_path);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    port->event_id = (unsigned)temp;
}

static void inport_init(cfl_inport_t *inport, cfl_runtime_handle_t *rt, const char *port_path) {
    port_init(&inport->port, rt, port_path);
}

void outport_init(cfl_outport_t *outport, cfl_runtime_handle_t *rt,
                         const char *port_path, const char *event_column_path) {
    int32_t temp;
    port_init(&outport->port, rt, port_path);
    json_extract_int32_runtime(rt, event_column_path, &temp);
    outport->event_column_id = (unsigned)temp;
}
#if 0
static cfl_inport_node_t* inport_node_init(cfl_runtime_handle_t *rt, unsigned node_index) {
    cfl_inport_node_t *node = (cfl_inport_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_inport_node_t));
    if (node == NULL) {
        EXCEPTION("inport_node_init: Failed to allocate memory");
    }
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    inport_init(&node->inport, rt, "node_dict.inport");
    
    return node;
}
#endif
// ============ SINK ============

void cfl_streaming_sink_packet_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_inport_node_t *node = (cfl_inport_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_inport_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_sink_packet_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    inport_init(&node->inport, rt, "node_dict.inport");
}

void cfl_streaming_sink_packet_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_sink_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                            unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (cfl_streaming_event_matches(event_type, event_id, event_data, &node->inport)) {
        const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
        bool_fn(rt, node_index, event_type, event_id, event_data);
    }
    
    return CFL_CONTINUE;
}

// ============ TAP ============

void cfl_streaming_tap_packet_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_inport_node_t *node = (cfl_inport_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_inport_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_tap_packet_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    inport_init(&node->inport, rt, "node_dict.inport");
}

void cfl_streaming_tap_packet_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_tap_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                           unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (cfl_streaming_event_matches(event_type, event_id, event_data, &node->inport)) {
        const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
        bool_fn(rt, node_index, event_type, event_id, event_data);
    }
    
    return CFL_CONTINUE;
}

// ============ FILTER ============

void cfl_streaming_filter_packet_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_inport_node_t *node = (cfl_inport_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_inport_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_filter_packet_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    inport_init(&node->inport, rt, "node_dict.inport");
}


void cfl_streaming_filter_packet_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_filter_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                              unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_inport_node_t *node = cfl_get_inport_node(rt, node_index);
    
    if (cfl_streaming_event_matches(event_type, event_id, event_data, &node->inport)) {
        const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
        if (bool_fn(rt, node_index, event_type, event_id, event_data) == false) {
            return CFL_HALT;
        }
    }
    
    return CFL_CONTINUE;
}

// ============ TRANSFORM ============

void cfl_streaming_transform_packet_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_transform_node_t *node = (cfl_transform_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_transform_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_transform_packet_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    node->packet_buffer = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    inport_init(&node->inport, rt, "node_dict.inport");
    outport_init(&node->outport, rt, "node_dict.outport", "node_dict.output_event_column_id");
}

void cfl_streaming_transform_packet_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_transform_packet_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                                 unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_transform_node_t *node = cfl_get_transform_node(rt, node_index);
    
    if (cfl_streaming_event_matches(event_type, event_id, event_data, &node->inport)) {
        const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
        bool_fn(rt, node_index, event_type, event_id, event_data);
    }
    
    return CFL_CONTINUE;
}



// ============ COLLECT PACKETS ============

static void inport_init_from_array(cfl_inport_t *inport, cfl_runtime_handle_t *rt, unsigned array_index) {
    char path_buffer[128];
    int32_t temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "node_dict.inports[%u].schema_hash", array_index);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    inport->port.schema_hash = (uint32_t)temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "node_dict.inports[%u].handler_id", array_index);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    inport->port.handler_id = (unsigned)temp;
    
    snprintf(path_buffer, sizeof(path_buffer), "node_dict.inports[%u].event_id", array_index);
    json_extract_int32_runtime(rt, path_buffer, &temp);
    inport->port.event_id = (unsigned)temp;
}

void cfl_streaming_collect_packets_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_collect_node_t *node = (cfl_collect_node_t *)cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_collect_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_collect_packets_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    
    // Inports array
    uint32_t count;
    json_extract_array_length_runtime(rt, "node_dict.inports", &count);
    node->inport_count = (unsigned)count;
    
    node->inports = (cfl_inport_t *)cfl_additional_arena_alloc(rt, node_index, sizeof(cfl_inport_t) * count);
    if (node->inports == NULL) {
        EXCEPTION("cfl_streaming_collect_packets_init_one_shot_fn: Failed to allocate inports array");
    }
    
    for (unsigned i = 0; i < node->inport_count; i++) {
        inport_init_from_array(&node->inports[i], rt, i);
    }
    
    // Output event
    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.output_event_id", &temp);
    node->output_event_id = (unsigned)temp;
    json_extract_int32_runtime(rt, "node_dict.output_event_column_id", &temp);
    node->output_event_column_id = (unsigned)temp;
    
    // Container setup
    json_extract_int32_runtime(rt, "node_dict.aux_data.expected_count", &temp);
    node->container.capacity = (unsigned)temp;
    node->container.count = 0;
    node->container.pending = false;
    
    node->container.packets = (void **)cfl_additional_arena_alloc(
        rt, node_index, sizeof(void *) * node->container.capacity);
    if (node->container.packets == NULL) {
        EXCEPTION("cfl_streaming_collect_packets_init_one_shot_fn: Failed to allocate packets array");
    }
    
    node->container.port_indices = (unsigned *)cfl_additional_arena_alloc(
        rt, node_index, sizeof(unsigned) * node->container.capacity);
    if (node->container.port_indices == NULL) {
        EXCEPTION("cfl_streaming_collect_packets_init_one_shot_fn: Failed to allocate port_indices array");
    }
    
    for (unsigned i = 0; i < node->container.capacity; i++) {
        node->container.packets[i] = NULL;
        node->container.port_indices[i] = 0;
    }
}


void cfl_streaming_collect_packets_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_collect_packets_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                                unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_collect_node_t *node = cfl_get_collect_node(rt, node_index);
    
    // Don't accept new packets while container is pending consumption
    if (node->container.pending) {
        return CFL_CONTINUE;
    }
    
    for (unsigned i = 0; i < node->inport_count; i++) {
        if (cfl_streaming_event_matches(event_type, event_id, event_data, &node->inports[i])) {
            
            const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
            bool accept = bool_fn(rt, node_index, i, event_id, event_data);
            
            if (accept && node->container.count < node->container.capacity) {
                node->container.packets[node->container.count] = event_data;
                node->container.port_indices[node->container.count] = i;
                node->container.count++;
                
                if (node->container.count >= node->container.capacity) {
                    // Mark as pending, don't reset yet
                    node->container.pending = true;
                    
                    cfl_send_streaming_collected_packets_event(
                        rt->event_queue,
                        CFL_EVENT_PRIORITY_LOW,
                        node->output_event_column_id,
                        node->output_event_id,
                        &node->container
                    );
                    // DO NOT reset here - sink needs to read the data first
                }
            }
            break;
        }
    }
    
    return CFL_CONTINUE;
}
// ============ COLLECTED PACKETS SINK ============
// In cfl_streaming_support.h
static inline void cfl_packet_container_reset(cfl_packet_container_t *container) {
    container->count = 0;
    container->pending = false;
    for (unsigned i = 0; i < container->capacity; i++) {
        container->packets[i] = NULL;
        container->port_indices[i] = 0;
    }
}
void cfl_streaming_sink_collected_packets_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    bool already_allocated = cfl_allocate_state(rt, node_index);
    if (already_allocated) {
        return;
    }
    
    cfl_collected_sink_node_t *node = (cfl_collected_sink_node_t *)cfl_smart_arena_alloc(
        rt, node_index, sizeof(cfl_collected_sink_node_t));
    if (node == NULL) {
        EXCEPTION("cfl_streaming_sink_collected_packets_init_one_shot_fn: Failed to allocate memory");
    }
    
    node->user_data = NULL;
    
    json_decoder_init_from_runtime(rt, node_index);
    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
    node->event_id = (unsigned)temp;
}

void cfl_streaming_sink_collected_packets_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_streaming_sink_collected_packets_main_fn(void *handle, unsigned bool_function_index, unsigned node_index,
                                                       unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_collected_sink_node_t *node = cfl_get_collected_sink_node(rt, node_index);
    
    if (event_type == CFL_EVENT_TYPE_STREAMING_COLLECTED_PACKETS && event_id == node->event_id) {
        const boolean_function_t bool_fn = rt->flash_handle->boolean_functions[bool_function_index];
        bool_fn(rt, node_index, event_type, event_id, event_data);
        
        // Reset container after user function processes it
        cfl_packet_container_t *container = (cfl_packet_container_t *)event_data;
        container->count = 0;
        container->pending = false;
    }
    
    return CFL_CONTINUE;
}


bool cfl_streaming_verify_packet_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    // Get the verify node data allocated by cfl_verify_init_one_shot_fn
    cfl_verify_fn_data_t *verify_data = (cfl_verify_fn_data_t *)cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    
    if (event_id == CFL_INIT_EVENT) {
        if (verify_data->auxiliary_data == NULL) {
            // Allocate our streaming-specific data in additional arena
            cfl_verify_packet_node_t *node = (cfl_verify_packet_node_t *)cfl_additional_arena_alloc(
                rt, node_index, sizeof(cfl_verify_packet_node_t));
            if (node == NULL) {
                EXCEPTION("cfl_streaming_verify_packet_boolean_fn: Failed to allocate memory");
            }
            
            // Store pointer in auxiliary_data
            verify_data->auxiliary_data = node;
            
            node->user_data = NULL;
            
            json_decoder_init_from_runtime(rt, node_index);
            inport_init(&node->inport, rt, "node_dict.fn_data.inport");
            
            int32_t temp=0;
            json_extract_int32_runtime(rt, "node_dict.fn_data.user_aux_function_id", &temp);
            node->user_aux_fn_index = (unsigned)temp;
        }
        
        // Forward init to user function
        cfl_verify_packet_node_t *node = (cfl_verify_packet_node_t *)verify_data->auxiliary_data;
        const boolean_function_t user_fn = rt->flash_handle->boolean_functions[node->user_aux_fn_index];
        return user_fn(rt, node_index, event_type, event_id, event_data);
        
    }
    
    // Retrieve our streaming-specific data from auxiliary_data
    cfl_verify_packet_node_t *node = (cfl_verify_packet_node_t *)verify_data->auxiliary_data;
    
    if (event_id == CFL_TERMINATE_EVENT) {
        const boolean_function_t user_fn = rt->flash_handle->boolean_functions[node->user_aux_fn_index];
        return user_fn(rt, node_index, event_type, event_id, event_data);
    }
    
    if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
        return true;
    }
    
    if (!cfl_streaming_event_matches(event_type, event_id, event_data, &node->inport)) {
        return true;
    }
    
    const boolean_function_t user_fn = rt->flash_handle->boolean_functions[node->user_aux_fn_index];
    return user_fn(rt, node_index, event_type, event_id, event_data);
    
}
// ============ PACKET OPERATIONS ============



bool cfl_streaming_event_matches(unsigned event_type, unsigned event_id,
                                  void *event_data, const cfl_inport_t *inport) {
    if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
        return false;
    }
    if (event_id != inport->port.event_id) {
        return false;
    }
    return cfl_packet_matches_port(event_data, &inport->port);
}

void cfl_emit_packet(cfl_runtime_handle_t *rt, const cfl_outport_t *outport, void *packet) {
    if (!cfl_packet_matches_port(packet, &outport->port)) {
        EXCEPTION("cfl_emit_packet: Packet does not match outport");
    }
    
    cfl_send_streaming_data_event(
        rt->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        outport->event_column_id,
        outport->port.event_id,
        packet
    );
}