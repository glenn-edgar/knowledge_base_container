#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>


// Common header for all packet types
// Generic packet header structure (matches all generated packets)
typedef struct {
    const char* schema_file;
    uint16_t    source_node;
    uint8_t     index;         // Note: should be uint8_t, not uint16_t
    uint16_t    length;
} avro_packet_header_t;

const void* get_packet_header(const void* packet_buffer, 
    const char** schema_file,
    uint16_t* source_node,
    uint8_t* index,          
    uint16_t* length) ;

#ifdef __cplusplus
}
#endif