/* json_query.h */
#ifndef JSON_QUERY_H
#define JSON_QUERY_H

#include "json_decoder.h"

/* Query result structure */
typedef struct {
    int found;
    json_type_t type;
    union {
        const char *string_value;
        int32_t i32_value;
        float f32_value;
        uint8_t bool_value;
    } value;
} json_query_result_t;

/* Query for a value using a path string
 * Path syntax:
 *   "field"           - object field
 *   "field.subfield"  - nested object
 *   "array[0]"        - array index
 *   "field[0].name"   - combined
 * 
 * Examples:
 *   "temperature"
 *   "config.timeout"
 *   "sensors[1]"
 *   "data[0].name"
 */
int json_query_path(const json_decoder_t *decoder,
                    uint32_t control_index,
                    const char *path,
                    json_query_result_t *result);

/* Helper to print query result */
void json_print_query_result(const json_query_result_t *result);

#endif /* JSON_QUERY_H */

