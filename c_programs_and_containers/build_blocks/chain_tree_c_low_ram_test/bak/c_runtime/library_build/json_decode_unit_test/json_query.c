/* json_query.c */
#include "json_query.h"
#include <string.h>
#include <ctype.h>
#include <stdlib.h>

/* Path token types */
typedef enum {
    TOKEN_FIELD,      /* .field or field */
    TOKEN_INDEX,      /* [n] */
    TOKEN_END
} token_type_t;

typedef struct {
    token_type_t type;
    union {
        char field_name[64];
        uint32_t array_index;
    } data;
} path_token_t;

/* Parse next token from path */
static const char* parse_next_token(const char *path, path_token_t *token)
{
    if (!path || *path == '\0') {
        token->type = TOKEN_END;
        return NULL;
    }

    /* Skip leading dot */
    if (*path == '.') {
        path++;
    }

    /* Check for array index */
    if (*path == '[') {
        path++;
        token->type = TOKEN_INDEX;
        token->data.array_index = 0;
        
        while (isdigit(*path)) {
            token->data.array_index = token->data.array_index * 10 + (*path - '0');
            path++;
        }
        
        if (*path == ']') {
            path++;
        }
        return path;
    }

    /* Parse field name */
    token->type = TOKEN_FIELD;
    size_t len = 0;
    
    while (*path && *path != '.' && *path != '[' && len < 63) {
        token->data.field_name[len++] = *path++;
    }
    token->data.field_name[len] = '\0';
    
    return path;
}

/* Skip over a value in the record stream (returns number of records consumed) */
static int skip_value(const json_decoder_t *decoder, uint32_t *index)
{
    if (*index >= decoder->num_records) {
        return -1;
    }

    const json_record_t *rec = &decoder->records[*index];
    (*index)++;
    int count = 1;

    if (rec->object_type == JSON_TYPE_ARRAY) {
        uint32_t num_elements = rec->value.container_count;
        for (uint32_t i = 0; i < num_elements; i++) {
            int skipped = skip_value(decoder, index);
            if (skipped < 0) return -1;
            count += skipped;
        }
    } else if (rec->object_type == JSON_TYPE_OBJECT) {
        uint32_t num_pairs = rec->value.container_count;
        for (uint32_t i = 0; i < num_pairs; i++) {
            (*index)++;  // Skip key
            int skipped = skip_value(decoder, index);
            if (skipped < 0) return -1;
            count += 1 + skipped;
        }
    }

    return count;
}

/* Find value by following path tokens */
static int find_value_recursive(const json_decoder_t *decoder,
                                uint32_t *index,
                                const char *path,
                                json_query_result_t *result)
{
    if (*index >= decoder->num_records) {
        return -1;
    }

    path_token_t token;
    const char *next_path = parse_next_token(path, &token);

    const json_record_t *rec = &decoder->records[*index];
    
    /* If no more tokens, we've found our value */
    if (token.type == TOKEN_END) {
        result->found = 1;
        result->type = rec->object_type;
        (*index)++;
        
        switch (rec->object_type) {
            case JSON_TYPE_STRING:
                result->value.string_value = json_get_string(decoder, rec->value.string_offset);
                break;
            case JSON_TYPE_INT32:
                result->value.i32_value = rec->value.i32_value;
                break;
            case JSON_TYPE_FLOAT32:
                result->value.f32_value = rec->value.f32_value;
                break;
            case JSON_TYPE_BOOL:
                result->value.bool_value = rec->value.bool_value;
                break;
            case JSON_TYPE_NULL:
                break;
            default:
                /* ARRAY and OBJECT would need special handling */
                result->found = 0;
                return -1;
        }
        return 0;
    }

    /* Handle OBJECT field lookup */
    if (token.type == TOKEN_FIELD && rec->object_type == JSON_TYPE_OBJECT) {
        uint32_t num_pairs = rec->value.container_count;
        (*index)++;
        
        for (uint32_t i = 0; i < num_pairs; i++) {
            /* Get key */
            const json_record_t *key_rec = &decoder->records[*index];
            (*index)++;
            
            const char *key = json_get_string(decoder, key_rec->value.string_offset);
            
            /* Check if this is the field we're looking for */
            if (strcmp(key, token.data.field_name) == 0) {
                return find_value_recursive(decoder, index, next_path, result);
            } else {
                /* Skip this value */
                skip_value(decoder, index);
            }
        }
        return -1;  /* Field not found */
    }

    /* Handle ARRAY index lookup */
    if (token.type == TOKEN_INDEX && rec->object_type == JSON_TYPE_ARRAY) {
        uint32_t num_elements = rec->value.container_count;
        (*index)++;
        
        if (token.data.array_index >= num_elements) {
            return -1;  /* Index out of bounds */
        }
        
        /* Skip to the desired index */
        for (uint32_t i = 0; i < token.data.array_index; i++) {
            skip_value(decoder, index);
        }
        
        /* Now at the desired element */
        return find_value_recursive(decoder, index, next_path, result);
    }

    /* Type mismatch */
    return -1;
}

int json_query_path(const json_decoder_t *decoder,
                    uint32_t control_index,
                    const char *path,
                    json_query_result_t *result)
{
    /* Initialize result */
    memset(result, 0, sizeof(json_query_result_t));
    result->found = 0;

    /* Validate control index */
    if (control_index >= decoder->num_controls) {
        return -1;
    }

    /* Start at the beginning of the object */
    uint32_t index = decoder->controls[control_index].start_position;

    return find_value_recursive(decoder, &index, path, result);
}

void json_print_query_result(const json_query_result_t *result)
{
    if (!result->found) {
        printf("(not found)");
        return;
    }

    switch (result->type) {
        case JSON_TYPE_STRING:
            printf("\"%s\"", result->value.string_value);
            break;
        case JSON_TYPE_INT32:
            printf("%d", result->value.i32_value);
            break;
        case JSON_TYPE_FLOAT32:
            printf("%g", result->value.f32_value);
            break;
        case JSON_TYPE_BOOL:
            printf("%s", result->value.bool_value ? "true" : "false");
            break;
        case JSON_TYPE_NULL:
            printf("null");
            break;
        default:
            printf("(complex type)");
            break;
    }
}

