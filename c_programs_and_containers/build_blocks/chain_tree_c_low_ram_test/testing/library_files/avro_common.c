#include "avro_common.h"




// Get packet header fields and return data pointer
const void* get_packet_header(const void* packet_buffer, 
                               const char** schema_file,
                               uint16_t* source_node,
                               uint8_t* index,          // Fixed: uint8_t not uint16_t
                               uint16_t* length) 
{
    const avro_packet_header_t* header = (const avro_packet_header_t*)packet_buffer;
    
    if (schema_file) *schema_file = header->schema_file;
    if (source_node) *source_node = header->source_node;
    if (index)       *index = header->index;
    if (length)      *length = header->length;
    
    return header + 1;  // Return pointer to data payload
}
