#include "json_node_decoder.h"
#include <string.h>
#include <stdio.h>

/* ============================================================================
 * Core Decoder Functions Implementation
 * ============================================================================ */

int json_decoder_init(
    json_decoder_ctx_t *ctx,
    const json_record_t *records,
    const record_control_t *control,
    const char *strings,
    uint16_t record_num)
{
    if (!ctx || !records || !control || !strings) {
        return JSON_ERR_NULL_PTR;
    }
    
    if (record_num >= control->record_count) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    ctx->records = records;
    ctx->control = control;
    ctx->strings = strings;
    ctx->current_record = record_num;
    ctx->error_code = JSON_OK;
    
    return JSON_OK;
}

/* ============================================================================
 * Type-Safe Value Extraction Implementation
 * ============================================================================ */

int json_get_bool(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    bool *out_value)
{
    if (!ctx || !out_value) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (record->type != JSON_TYPE_BOOL) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    *out_value = record->value.bool_val;
    return JSON_OK;
}

int json_get_int(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    int64_t *out_value)
{
    if (!ctx || !out_value) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (record->type != JSON_TYPE_INT) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    *out_value = record->value.int_val;
    return JSON_OK;
}

int json_get_double(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    double *out_value)
{
    if (!ctx || !out_value) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (record->type != JSON_TYPE_DOUBLE) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    *out_value = record->value.double_val;
    return JSON_OK;
}

int json_get_string_value(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    const char **out_value)
{
    if (!ctx || !out_value) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (record->type != JSON_TYPE_STRING) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    *out_value = json_get_string(ctx, record->value.string_offset);
    if (!*out_value) {
        return JSON_ERR_OUT_OF_BOUNDS;
    }
    
    return JSON_OK;
}

bool json_is_null(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num)
{
    const json_record_t *record = json_get_record(ctx, record_num);
    return record && (record->type == JSON_TYPE_NULL);
}

/* ============================================================================
 * Object/Array Navigation Implementation
 * ============================================================================ */

int json_find_child(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    const char *key,
    uint16_t *out_record)
{
    if (!ctx || !key || !out_record) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *parent = json_get_record(ctx, parent_record);
    if (!parent) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (parent->type != JSON_TYPE_OBJECT) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    uint16_t first_child = parent->value.container.first_child;
    uint16_t child_count = parent->value.container.child_count;
    
    for (uint16_t i = 0; i < child_count; i++) {
        uint16_t child_idx = first_child + i;
        const json_record_t *child = json_get_record(ctx, child_idx);
        
        if (!child) continue;
        
        const char *child_key = json_get_key(ctx, child);
        if (child_key && strcmp(child_key, key) == 0) {
            *out_record = child_idx;
            return JSON_OK;
        }
    }
    
    return JSON_ERR_NOT_FOUND;
}

int json_get_child_at(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    uint16_t index,
    uint16_t *out_record)
{
    if (!ctx || !out_record) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *parent = json_get_record(ctx, parent_record);
    if (!parent) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (parent->type != JSON_TYPE_ARRAY && parent->type != JSON_TYPE_OBJECT) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    if (index >= parent->value.container.child_count) {
        return JSON_ERR_OUT_OF_BOUNDS;
    }
    
    *out_record = parent->value.container.first_child + index;
    return JSON_OK;
}

int json_get_child_count(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    uint16_t *out_count)
{
    if (!ctx || !out_count) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (record->type != JSON_TYPE_ARRAY && record->type != JSON_TYPE_OBJECT) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    *out_count = record->value.container.child_count;
    return JSON_OK;
}

int json_iterate_children(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    uint16_t *iterator,
    uint16_t *out_record)
{
    if (!ctx || !iterator || !out_record) {
        return JSON_ERR_NULL_PTR;
    }
    
    const json_record_t *parent = json_get_record(ctx, parent_record);
    if (!parent) {
        return JSON_ERR_INVALID_RECORD;
    }
    
    if (parent->type != JSON_TYPE_ARRAY && parent->type != JSON_TYPE_OBJECT) {
        return JSON_ERR_TYPE_MISMATCH;
    }
    
    if (*iterator >= parent->value.container.child_count) {
        return JSON_ERR_NOT_FOUND;
    }
    
    *out_record = parent->value.container.first_child + *iterator;
    (*iterator)++;
    
    return JSON_OK;
}

/* ============================================================================
 * Convenience Helper Functions Implementation
 * ============================================================================ */

int json_object_get_bool(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    bool *out_value)
{
    uint16_t child_record;
    int result = json_find_child(ctx, object_record, key, &child_record);
    if (result != JSON_OK) {
        return result;
    }
    
    return json_get_bool(ctx, child_record, out_value);
}

int json_object_get_int(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    int64_t *out_value)
{
    uint16_t child_record;
    int result = json_find_child(ctx, object_record, key, &child_record);
    if (result != JSON_OK) {
        return result;
    }
    
    return json_get_int(ctx, child_record, out_value);
}

int json_object_get_double(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    double *out_value)
{
    uint16_t child_record;
    int result = json_find_child(ctx, object_record, key, &child_record);
    if (result != JSON_OK) {
        return result;
    }
    
    return json_get_double(ctx, child_record, out_value);
}

int json_object_get_string(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    const char **out_value)
{
    uint16_t child_record;
    int result = json_find_child(ctx, object_record, key, &child_record);
    if (result != JSON_OK) {
        return result;
    }
    
    return json_get_string_value(ctx, child_record, out_value);
}

int json_get_nested_value(
    const json_decoder_ctx_t *ctx,
    uint16_t root_record,
    const char *path,
    uint16_t *out_record)
{
    if (!ctx || !path || !out_record) {
        return JSON_ERR_NULL_PTR;
    }
    
    // Use stack buffer for path parsing (embedded systems friendly)
    char path_buf[256];
    size_t path_len = strlen(path);
    
    if (path_len >= sizeof(path_buf)) {
        return JSON_ERR_OUT_OF_BOUNDS;
    }
    
    memcpy(path_buf, path, path_len + 1);
    
    uint16_t current_record = root_record;
    char *token = strtok(path_buf, ".");
    
    while (token != NULL) {
        int result = json_find_child(ctx, current_record, token, &current_record);
        if (result != JSON_OK) {
            return result;
        }
        token = strtok(NULL, ".");
    }
    
    *out_record = current_record;
    return JSON_OK;
}

/* ============================================================================
 * Type Checking and Validation Implementation
 * ============================================================================ */

bool json_check_type(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    json_type_t expected_type)
{
    const json_record_t *record = json_get_record(ctx, record_num);
    return record && (record->type == expected_type);
}

int json_validate_records(
    const json_decoder_ctx_t *ctx)
{
    if (!ctx || !ctx->records || !ctx->control) {
        return JSON_ERR_NULL_PTR;
    }
    
    for (uint16_t i = 0; i < ctx->control->record_count; i++) {
        const json_record_t *record = &ctx->records[i];
        
        // Validate type
        if (record->type >= JSON_TYPE_INVALID) {
            return JSON_ERR_TYPE_MISMATCH;
        }
        
        // Validate string offsets
        if (record->type == JSON_TYPE_STRING) {
            if (record->value.string_offset >= ctx->control->string_table_size) {
                return JSON_ERR_OUT_OF_BOUNDS;
            }
        }
        
        // Validate container bounds
        if (record->type == JSON_TYPE_OBJECT || record->type == JSON_TYPE_ARRAY) {
            uint16_t first = record->value.container.first_child;
            uint16_t count = record->value.container.child_count;
            
            if (first + count > ctx->control->record_count) {
                return JSON_ERR_OUT_OF_BOUNDS;
            }
        }
    }
    
    return JSON_OK;
}

/* ============================================================================
 * Debug and Diagnostic Functions Implementation
 * ============================================================================ */

#ifdef JSON_DEBUG

const char *json_type_to_string(json_type_t type) {
    switch (type) {
        case JSON_TYPE_NULL:    return "null";
        case JSON_TYPE_BOOL:    return "bool";
        case JSON_TYPE_INT:     return "int";
        case JSON_TYPE_DOUBLE:  return "double";
        case JSON_TYPE_STRING:  return "string";
        case JSON_TYPE_OBJECT:  return "object";
        case JSON_TYPE_ARRAY:   return "array";
        default:                return "invalid";
    }
}

const char *json_error_to_string(int error_code) {
    switch (error_code) {
        case JSON_OK:                return "OK";
        case JSON_ERR_INVALID_RECORD: return "Invalid record";
        case JSON_ERR_TYPE_MISMATCH: return "Type mismatch";
        case JSON_ERR_NOT_FOUND:     return "Not found";
        case JSON_ERR_NULL_PTR:      return "Null pointer";
        case JSON_ERR_OUT_OF_BOUNDS: return "Out of bounds";
        default:                     return "Unknown error";
    }
}

void json_print_record(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    int indent_level)
{
    const json_record_t *record = json_get_record(ctx, record_num);
    if (!record) {
        printf("Invalid record %u\n", record_num);
        return;
    }
    
    for (int i = 0; i < indent_level; i++) {
        printf("  ");
    }
    
    const char *key = json_get_key(ctx, record);
    if (key) {
        printf("\"%s\": ", key);
    }
    
    switch (record->type) {
        case JSON_TYPE_NULL:
            printf("null\n");
            break;
            
        case JSON_TYPE_BOOL:
            printf("%s\n", record->value.bool_val ? "true" : "false");
            break;
            
        case JSON_TYPE_INT:
            printf("%lld\n", (long long)record->value.int_val);
            break;
            
        case JSON_TYPE_DOUBLE:
            printf("%f\n", record->value.double_val);
            break;
            
        case JSON_TYPE_STRING:
            printf("\"%s\"\n", json_get_string(ctx, record->value.string_offset));
            break;
            
        case JSON_TYPE_OBJECT:
            printf("{\n");
            for (uint16_t i = 0; i < record->value.container.child_count; i++) {
                json_print_record(ctx, record->value.container.first_child + i, indent_level + 1);
            }
            for (int i = 0; i < indent_level; i++) printf("  ");
            printf("}\n");
            break;
            
        case JSON_TYPE_ARRAY:
            printf("[\n");
            for (uint16_t i = 0; i < record->value.container.child_count; i++) {
                json_print_record(ctx, record->value.container.first_child + i, indent_level + 1);
            }
            for (int i = 0; i < indent_level; i++) printf("  ");
            printf("]\n");
            break;
    }
}

void json_print_all_records(const json_decoder_ctx_t *ctx) {
    if (!ctx) return;
    
    printf("=== JSON Records Dump ===\n");
    printf("Total records: %u\n", ctx->control->record_count);
    printf("String table size: %u\n", ctx->control->string_table_size);
    printf("Root record: %u\n\n", ctx->control->root_record);
    
    json_print_record(ctx, ctx->control->root_record, 0);
}

#endif /* JSON_DEBUG */
