/*
 * kb_stream.h
 * Knowledge Base C Port — Circular buffer stream data
 *
 * Mirrors LuaJIT kb_stream.lua / Python kb_stream.py.
 * Provides push/list/clear operations on a circular buffer
 * stream table with timestamps and JSON payloads.
 */

#ifndef KB_STREAM_H
#define KB_STREAM_H

#include "kb_common.h"
#include "kb_query_support.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_stream kb_stream_t;

kb_stream_t *kb_stream_create(kb_search_t *ks, const char *database);
void         kb_stream_destroy(kb_stream_t *st);

/* Push data to stream at path. Circular buffer wraps using write_index. */
kb_error_t kb_stream_push_data(kb_stream_t *st, const char *path,
                                const char *data_json);

/* List stream data for path.
 * recorded_after / recorded_before are ISO timestamps (NULL to skip).
 * Results returned in result (caller must kb_result_free). */
kb_error_t kb_stream_list_data(kb_stream_t *st, const char *path,
                                const char *recorded_after,
                                const char *recorded_before,
                                kb_result_t *result);

/* Clear all stream data for path (reset write_index, delete entries) */
kb_error_t kb_stream_clear_data(kb_stream_t *st, const char *path);

/* Get current write index for path */
kb_error_t kb_stream_get_write_index(kb_stream_t *st, const char *path,
                                      int *index_out);

#ifdef __cplusplus
}
#endif

#endif /* KB_STREAM_H */
