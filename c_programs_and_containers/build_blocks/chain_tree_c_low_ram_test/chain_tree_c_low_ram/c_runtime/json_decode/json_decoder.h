/* json_decoder.h */
#ifndef JSON_DECODER_H
#define JSON_DECODER_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef enum {
    JSON_TYPE_STRING = 0,
    JSON_TYPE_INT32 = 1,
    JSON_TYPE_FLOAT32 = 2,
    JSON_TYPE_NULL = 3,
    JSON_TYPE_BOOL = 4,
    JSON_TYPE_ARRAY = 5,
    JSON_TYPE_OBJECT = 6
} json_type_t;

typedef struct {
    json_type_t object_type;
    union {
        uint32_t string_offset;
        int32_t i32_value;
        float f32_value;
        uint8_t bool_value;
        uint32_t container_count;
    } value;
} json_record_t;

typedef struct {
    uint32_t start_position;
    uint32_t num_records;
} record_control_t;

typedef struct {
    const json_record_t *records;
    uint32_t num_records;
    const char *string_table;
    uint32_t string_table_size;
    const record_control_t *controls;
    uint32_t num_controls;
} json_decoder_t;

/* Initialize decoder with record arrays */
void json_decoder_init(json_decoder_t *decoder,
                       const json_record_t *records,
                       uint32_t num_records,
                       const char *string_table,
                       uint32_t string_table_size,
                       const record_control_t *controls,
                       uint32_t num_controls);

/* Get string from string table */
const char* json_get_string(const json_decoder_t *decoder, uint32_t offset);

/* Decode and print a specific object by control index */
int json_decode_object(const json_decoder_t *decoder, uint32_t control_index);

/* Internal recursive decoder (advances index) */
int json_decode_value(const json_decoder_t *decoder, uint32_t *index, int indent_level);

#endif /* JSON_DECODER_H */