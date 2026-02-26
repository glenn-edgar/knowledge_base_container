#include <stdio.h>
#include "avro_common.h"
#include "json_node_decoder.h"




const void* get_packet_header(
    const void* packet_buffer, 
    const char** schema_file,
    double* timestamp,
    uint32_t* seq,
    uint16_t* source_node,
    uint8_t* index,          
    uint16_t* length)
{
    const avro_packet_header_t* hdr = (const avro_packet_header_t*)packet_buffer;
    
    if (schema_file) *schema_file = hdr->schema_file;
    if (timestamp)   *timestamp   = hdr->timestamp;
    if (seq)         *seq         = hdr->seq;
    if (source_node) *source_node = hdr->source_node;
    if (index)       *index       = hdr->index;
    if (length)      *length      = hdr->length;
    
    // Return pointer to payload (after header)
    return (const uint8_t*)packet_buffer + sizeof(avro_packet_header_t);
}

void cfl_avro_update_packet_header(cfl_runtime_handle_t *runtime, void *packet) {
    avro_packet_header_t *header = (avro_packet_header_t*)packet;
    header->seq += 1;
    header->timestamp = cfl_timer_get_timestamp(runtime->timer_handle);
}

bool cfl_packet_matches_port(const void *packet, const cfl_port_t *port) {
    const char *schema_file;
    double timestamp;
    uint32_t seq;
    uint16_t source_node;
    uint8_t index;
    uint16_t length;
    
    get_packet_header(packet, &schema_file,&timestamp, &seq, &source_node, &index, &length);
    
    if (strcmp(schema_file, port->schema_file) != 0) {
        printf("schema file does not match %s and %s\n", schema_file, port->schema_file);
        return false;
    }
    if (index != port->handler_id) {
        printf("index does not match %d and %d\n", index, port->handler_id);
        return false;
    }
    
    return true;
}

void cfl_avro_decode_port(
    const cfl_runtime_handle_t *runtime,
    const char *port_path,
    cfl_port_t *port)
{
    char path_buf[128];
    int32_t temp_int;
    
    // Extract file_name -> header.schema_file
    snprintf(path_buf, sizeof(path_buf), "%s.file_name", port_path);
    json_extract_string_runtime(runtime, path_buf, &port->schema_file);
    
    // Extract handler_id -> header.index
    snprintf(path_buf, sizeof(path_buf), "%s.handler_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->handler_id = (uint8_t)temp_int;
    
    // Extract event_id
    snprintf(path_buf, sizeof(path_buf), "%s.event_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->event_id = (unsigned)temp_int;
    
    // Initialize runtime fields

}