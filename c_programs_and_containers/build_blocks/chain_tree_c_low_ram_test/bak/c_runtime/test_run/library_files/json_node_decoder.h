#ifndef JSON_NODE_DECODER_H
#define JSON_NODE_DECODER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ============================================================================
 * Core Data Structures
 * ============================================================================ */

/**
 * JSON value types supported in preprocessed records
 */
typedef enum {
    JSON_TYPE_NULL = 0,
    JSON_TYPE_BOOL,
    JSON_TYPE_INT,
    JSON_TYPE_DOUBLE,
    JSON_TYPE_STRING,
    JSON_TYPE_OBJECT,
    JSON_TYPE_ARRAY,
    JSON_TYPE_INVALID = 0xFF
} json_type_t;

/**
 * Preprocessed JSON record entry
 * Memory-efficient structure for embedded systems
 */
typedef struct {
    uint16_t key_offset;        // Offset into string table for key name
    uint8_t  type;              // json_type_t
    uint8_t  flags;             // Optional, required, etc.
    union {
        bool     bool_val;
        int64_t  int_val;
        double   double_val;
        uint16_t string_offset; // Offset into string table
        struct {
            uint16_t first_child;   // Index of first child record
            uint16_t child_count;   // Number of children
        } container;
    } value;
} json_record_t;

/**
 * Control structure for record metadata
 */
typedef struct {
    uint16_t record_count;      // Total number of records
    uint16_t string_table_size; // Size of string table
    uint16_t root_record;       // Index of root record
    uint16_t reserved;          // Alignment/future use
} record_control_t;

/**
 * Decoder context for stateful operations
 */
typedef struct {
    const json_record_t *records;
    const record_control_t *control;
    const char *strings;
    uint16_t current_record;
    int error_code;
} json_decoder_ctx_t;

/* Error codes */
#define JSON_OK                 0
#define JSON_ERR_INVALID_RECORD -1
#define JSON_ERR_TYPE_MISMATCH  -2
#define JSON_ERR_NOT_FOUND      -3
#define JSON_ERR_NULL_PTR       -4
#define JSON_ERR_OUT_OF_BOUNDS  -5

/* ============================================================================
 * Core Decoder Functions
 * ============================================================================ */

/**
 * Initialize decoder context
 * 
 * @param ctx Context to initialize
 * @param records Preprocessed JSON records
 * @param control Record control structure
 * @param strings String table
 * @param record_num Starting record number
 * @return JSON_OK on success, error code otherwise
 */
int json_decoder_init(
    json_decoder_ctx_t *ctx,
    const json_record_t *records,
    const record_control_t *control,
    const char *strings,
    uint16_t record_num
);

/**
 * Get record by number with bounds checking
 */
static inline const json_record_t *json_get_record(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num
)
{
    if (!ctx || !ctx->records || record_num >= ctx->control->record_count) {
        return NULL;
    }
    return &ctx->records[record_num];
}

/**
 * Get string from string table by offset
 */
static inline const char *json_get_string(
    const json_decoder_ctx_t *ctx,
    uint16_t offset
)
{
    if (!ctx || !ctx->strings || offset >= ctx->control->string_table_size) {
        return NULL;
    }
    return &ctx->strings[offset];
}

/**
 * Get key name for a record
 */
static inline const char *json_get_key(
    const json_decoder_ctx_t *ctx,
    const json_record_t *record
)
{
    if (!record) return NULL;
    return json_get_string(ctx, record->key_offset);
}

/* ============================================================================
 * Type-Safe Value Extraction
 * ============================================================================ */

/**
 * Extract boolean value from record
 */
int json_get_bool(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    bool *out_value
);

/**
 * Extract integer value from record
 */
int json_get_int(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    int64_t *out_value
);

/**
 * Extract double value from record
 */
int json_get_double(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    double *out_value
);

/**
 * Extract string value from record
 * Returns pointer into string table (no allocation)
 */
int json_get_string_value(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    const char **out_value
);

/**
 * Check if record is null
 */
bool json_is_null(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num
);

/* ============================================================================
 * Object/Array Navigation
 * ============================================================================ */

/**
 * Find child record by key name (for objects)
 * 
 * @param ctx Decoder context
 * @param parent_record Parent object record number
 * @param key Key to search for
 * @param out_record Output record number if found
 * @return JSON_OK if found, JSON_ERR_NOT_FOUND otherwise
 */
int json_find_child(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    const char *key,
    uint16_t *out_record
);

/**
 * Get child by index (for arrays)
 */
int json_get_child_at(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    uint16_t index,
    uint16_t *out_record
);

/**
 * Get number of children (for objects/arrays)
 */
int json_get_child_count(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    uint16_t *out_count
);

/**
 * Iterate over object/array children
 * 
 * @param ctx Decoder context
 * @param parent_record Parent record number
 * @param iterator Iterator state (initialize to 0)
 * @param out_record Output child record number
 * @return JSON_OK if child found, JSON_ERR_NOT_FOUND when done
 */
int json_iterate_children(
    const json_decoder_ctx_t *ctx,
    uint16_t parent_record,
    uint16_t *iterator,
    uint16_t *out_record
);

/* ============================================================================
 * Convenience Helper Functions
 * ============================================================================ */

/**
 * Get boolean value from object by key
 */
int json_object_get_bool(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    bool *out_value
);

/**
 * Get integer value from object by key
 */
int json_object_get_int(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    int64_t *out_value
);

/**
 * Get double value from object by key
 */
int json_object_get_double(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    double *out_value
);

/**
 * Get string value from object by key
 */
int json_object_get_string(
    const json_decoder_ctx_t *ctx,
    uint16_t object_record,
    const char *key,
    const char **out_value
);

/**
 * Get nested object by key path (e.g., "settings.network.port")
 * Uses temporary buffer for path parsing
 */
int json_get_nested_value(
    const json_decoder_ctx_t *ctx,
    uint16_t root_record,
    const char *path,
    uint16_t *out_record
);

/* ============================================================================
 * Type Checking and Validation
 * ============================================================================ */

/**
 * Get type of record
 */
static inline json_type_t json_get_type(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num
)
{
    const json_record_t *record = json_get_record(ctx, record_num);
    return record ? (json_type_t)record->type : JSON_TYPE_INVALID;
}

/**
 * Check if record is of expected type
 */
bool json_check_type(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    json_type_t expected_type
);

/**
 * Validate entire record structure (for debugging)
 */
int json_validate_records(
    const json_decoder_ctx_t *ctx
);

/* ============================================================================
 * Debug and Diagnostic Functions
 * ============================================================================ */

/**
 * Print record structure (for debugging)
 */
#ifdef JSON_DEBUG
void json_print_record(
    const json_decoder_ctx_t *ctx,
    uint16_t record_num,
    int indent_level
);

void json_print_all_records(
    const json_decoder_ctx_t *ctx
);

const char *json_type_to_string(json_type_t type);
const char *json_error_to_string(int error_code);
#endif

#endif /* JSON_NODE_DECODER_H */

