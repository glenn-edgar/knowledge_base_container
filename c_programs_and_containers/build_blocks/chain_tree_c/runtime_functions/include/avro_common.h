#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "cfl_runtime.h"

typedef struct {
    const char *schema_file;
    unsigned    handler_id;
    unsigned    event_id;
    void        *packet_pointer;
    void        *data_pointer;
} cfl_port_t;

// Common header for all packet types (packed largest to smallest)
// Generic packet header structure (matches all generated packets)
typedef struct {
    const char* schema_file;   // 8 bytes (offset 0)
    double      timestamp;     // 8 bytes (offset 8)
    uint32_t    seq;           // 4 bytes (offset 16)
    uint16_t    source_node;   // 2 bytes (offset 20)
    uint16_t    length;        // 2 bytes (offset 22)
    uint8_t     index;         // 1 byte  (offset 24)
} avro_packet_header_t;

const void* get_packet_header(
    const void* packet_buffer, 
    const char** schema_file,
    double* timestamp,
    uint32_t* seq,
    uint16_t* source_node,
    uint8_t* index,          
    uint16_t* length);

static inline uint16_t cfl_avro_get_source_node(const void* packet_buffer)
{
    const avro_packet_header_t* hdr = (const avro_packet_header_t*)packet_buffer;
    return hdr->source_node;
}

bool cfl_packet_matches_port(const void *packet, const cfl_port_t *port); 

void cfl_avro_decode_port(const cfl_runtime_handle_t *runtime, const char *port_path, cfl_port_t *port);

void cfl_avro_update_packet_header(cfl_runtime_handle_t *runtime, void *packet);

#ifdef __cplusplus
}
#endif

