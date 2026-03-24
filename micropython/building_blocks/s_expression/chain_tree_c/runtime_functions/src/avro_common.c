#include <stdio.h>
#include "avro_common.h"
#include "json_node_decoder.h"

void cfl_avro_update_packet_header(cfl_runtime_handle_t *runtime, void *packet) {
    avro_packet_header_t *header = (avro_packet_header_t *)packet;
    // Only touch timestamp and seq — schema_hash and source_node
    // are already set by _packet_init and must not be overwritten
    header->seq += 1;
    header->timestamp = cfl_timer_get_timestamp(runtime->timer_handle);
}

void cfl_avro_update_packet_header_source_node(cfl_runtime_handle_t *runtime, void *packet, unsigned source_node) {
    (void)runtime;
    avro_packet_header_t *header = (avro_packet_header_t *)packet;
    // Only touch timestamp and seq — schema_hash and source_node
    // are already set by _packet_init and must not be overwritten

    header->source_node = source_node;
}

bool cfl_packet_matches_port(const void *packet, const cfl_port_t *port) {
    const avro_packet_header_t *hdr = (const avro_packet_header_t *)packet;
    
    // Per-record hash is sufficient — each record type has a unique hash
    if (hdr->schema_hash != port->schema_hash) {
        printf("schema_hash does not match: 0x%08X vs 0x%08X\n", hdr->schema_hash, port->schema_hash);
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
    printf("cfl_avro_decode_port: port_path: %s\n", port_path);
    // Extract schema_hash (per-record)
    snprintf(path_buf, sizeof(path_buf), "%s.schema_hash", port_path);
    
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->schema_hash = (uint32_t)temp_int;
    printf("cfl_avro_decode_port: schema_hash: %d\n", port->schema_hash);
    // Extract handler_id (record index — for dispatch tables)
    snprintf(path_buf, sizeof(path_buf), "%s.handler_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->handler_id = (unsigned)temp_int;
    printf("cfl_avro_decode_port: handler_id: %d\n", port->handler_id);
    // Extract event_id
    snprintf(path_buf, sizeof(path_buf), "%s.event_id", port_path);
    json_extract_int32_runtime(runtime, path_buf, &temp_int);
    port->event_id = (unsigned)temp_int;
}