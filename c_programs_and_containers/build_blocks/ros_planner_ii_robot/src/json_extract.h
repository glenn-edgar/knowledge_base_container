/**
 * @file json_extract.h
 * @brief Extract fields from flat JSON strings without a full parser.
 *
 * Lightweight alternative to cJSON for reading fields from decoded
 * RPC command payloads. Only handles flat objects (no nesting).
 */

#ifndef JSON_EXTRACT_H
#define JSON_EXTRACT_H

#include <stdbool.h>

/**
 * Extract an integer field from a flat JSON string.
 * Returns true if found, stores value in *out.
 */
bool json_get_int(const char *json, const char *key, int *out);

/**
 * Extract a double field from a flat JSON string.
 * Returns true if found, stores value in *out.
 */
bool json_get_double(const char *json, const char *key, double *out);

/**
 * Extract a string field from a flat JSON string.
 * Copies up to max_out-1 chars into out buffer.
 * Returns true if found.
 */
bool json_get_string(const char *json, const char *key,
                     char *out, int max_out);

#endif /* JSON_EXTRACT_H */
