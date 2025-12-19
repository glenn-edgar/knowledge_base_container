/* json_decoder.c */
#include "json_decoder.h"

void json_decoder_init(json_decoder_t *decoder,
                       const json_record_t *records,
                       uint32_t num_records,
                       const char *string_table,
                       uint32_t string_table_size,
                       const record_control_t *controls,
                       uint32_t num_controls)
{
    decoder->records = records;
    decoder->num_records = num_records;
    decoder->string_table = string_table;
    decoder->string_table_size = string_table_size;
    decoder->controls = controls;
    decoder->num_controls = num_controls;
}

const char* json_get_string(const json_decoder_t *decoder, uint32_t offset)
{
    if (offset >= decoder->string_table_size) {
        return "<invalid_offset>";
    }
    return decoder->string_table + offset;
}

static void print_indent(int indent_level)
{
    for (int i = 0; i < indent_level; i++) {
        printf("  ");
    }
}

int json_decode_value(const json_decoder_t *decoder, uint32_t *index, int indent_level)
{
    if (*index >= decoder->num_records) {
        fprintf(stderr, "Error: index %u out of bounds\n", *index);
        return -1;
    }

    const json_record_t *rec = &decoder->records[*index];
    (*index)++;

    switch (rec->object_type) {
        case JSON_TYPE_STRING: {
            const char *str = json_get_string(decoder, rec->value.string_offset);
            printf("\"%s\"", str);
            break;
        }

        case JSON_TYPE_INT32: {
            printf("%d", rec->value.i32_value);
            break;
        }

        case JSON_TYPE_FLOAT32: {
            printf("%g", rec->value.f32_value);
            break;
        }

        case JSON_TYPE_NULL: {
            printf("null");
            break;
        }

        case JSON_TYPE_BOOL: {
            printf("%s", rec->value.bool_value ? "true" : "false");
            break;
        }

        case JSON_TYPE_ARRAY: {
            uint32_t count = rec->value.container_count;
            printf("[\n");
            
            for (uint32_t i = 0; i < count; i++) {
                print_indent(indent_level + 1);
                if (json_decode_value(decoder, index, indent_level + 1) < 0) {
                    return -1;
                }
                if (i < count - 1) {
                    printf(",");
                }
                printf("\n");
            }
            
            print_indent(indent_level);
            printf("]");
            break;
        }

        case JSON_TYPE_OBJECT: {
            uint32_t count = rec->value.container_count;
            printf("{\n");
            
            for (uint32_t i = 0; i < count; i++) {
                // Decode key (must be STRING)
                if (*index >= decoder->num_records) {
                    fprintf(stderr, "Error: unexpected end of records\n");
                    return -1;
                }
                
                const json_record_t *key_rec = &decoder->records[*index];
                if (key_rec->object_type != JSON_TYPE_STRING) {
                    fprintf(stderr, "Error: object key is not a string\n");
                    return -1;
                }
                (*index)++;
                
                const char *key = json_get_string(decoder, key_rec->value.string_offset);
                print_indent(indent_level + 1);
                printf("\"%s\": ", key);
                
                // Decode value (recursive)
                if (json_decode_value(decoder, index, indent_level + 1) < 0) {
                    return -1;
                }
                
                if (i < count - 1) {
                    printf(",");
                }
                printf("\n");
            }
            
            print_indent(indent_level);
            printf("}");
            break;
        }

        default: {
            fprintf(stderr, "Error: unknown type %d\n", rec->object_type);
            return -1;
        }
    }

    return 0;
}

int json_decode_object(const json_decoder_t *decoder, uint32_t control_index)
{
    if (control_index >= decoder->num_controls) {
        fprintf(stderr, "Error: control index %u out of bounds\n", control_index);
        return -1;
    }

    const record_control_t *ctrl = &decoder->controls[control_index];
    uint32_t index = ctrl->start_position;
    uint32_t end_index = ctrl->start_position + ctrl->num_records;

    // Validate range
    if (end_index > decoder->num_records) {
        fprintf(stderr, "Error: control range exceeds record array\n");
        return -1;
    }

    printf("Object #%u (records %u to %u):\n", 
           control_index, ctrl->start_position, end_index - 1);

    int result = json_decode_value(decoder, &index, 0);
    printf("\n");

    // Verify we consumed the expected number of records
    if (index != end_index) {
        fprintf(stderr, "Warning: expected to end at %u but ended at %u\n", 
                end_index, index);
    }

    return result;
}

/* Decode all objects */
void json_decode_all_objects(const json_decoder_t *decoder)
{
    printf("Decoding %u objects:\n", decoder->num_controls);
    printf("========================================\n\n");

    for (uint32_t i = 0; i < decoder->num_controls; i++) {
        json_decode_object(decoder, i);
        printf("\n");
    }
}

/* Compact single-line decoder (no pretty printing) */
int json_decode_value_compact(const json_decoder_t *decoder, uint32_t *index)
{
    if (*index >= decoder->num_records) {
        return -1;
    }

    const json_record_t *rec = &decoder->records[*index];
    (*index)++;

    switch (rec->object_type) {
        case JSON_TYPE_STRING:
            printf("\"%s\"", json_get_string(decoder, rec->value.string_offset));
            break;
        case JSON_TYPE_INT32:
            printf("%d", rec->value.i32_value);
            break;
        case JSON_TYPE_FLOAT32:
            printf("%g", rec->value.f32_value);
            break;
        case JSON_TYPE_NULL:
            printf("null");
            break;
        case JSON_TYPE_BOOL:
            printf("%s", rec->value.bool_value ? "true" : "false");
            break;
        case JSON_TYPE_ARRAY: {
            uint32_t count = rec->value.container_count;
            printf("[");
            for (uint32_t i = 0; i < count; i++) {
                if (i > 0) printf(", ");
                json_decode_value_compact(decoder, index);
            }
            printf("]");
            break;
        }
        case JSON_TYPE_OBJECT: {
            uint32_t count = rec->value.container_count;
            printf("{");
            for (uint32_t i = 0; i < count; i++) {
                if (i > 0) printf(", ");
                const json_record_t *key_rec = &decoder->records[*index];
                (*index)++;
                printf("\"%s\": ", json_get_string(decoder, key_rec->value.string_offset));
                json_decode_value_compact(decoder, index);
            }
            printf("}");
            break;
        }
    }
    return 0;
}