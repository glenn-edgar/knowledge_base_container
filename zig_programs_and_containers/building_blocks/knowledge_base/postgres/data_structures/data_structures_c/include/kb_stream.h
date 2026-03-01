/*
 * kb_stream.h
 * Knowledge Base C Library (PostgreSQL) — Circular buffer time-series
 *
 * Table: {database}_stream
 * Columns: id, path (ltree), recorded_at (timestamptz), valid (bool), data (jsonb)
 *
 * Concurrent write safety:
 *   - push: FOR UPDATE SKIP LOCKED on oldest row, with retry
 *   - clear: FOR UPDATE on all matching rows, with retry
 *   - reads: FOR SHARE where consistency matters
 */

 #ifndef KB_STREAM_H
 #define KB_STREAM_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* Push stream data (circular buffer: overwrites oldest slot).
  * Uses FOR UPDATE SKIP LOCKED + retry for concurrent safety. */
 kb_error_t kb_stream_push(kb_conn_t *c, const char *database,
                           const char *path, const char *data,
                           int max_retries, int base_delay_ms);
 
 /*
  * List stream data, optionally filtered by time range.
  * after/before: ISO-8601 timestamps or NULL for no filter.
  * Returns result set with columns: id, data, recorded_at.
  */
 kb_error_t kb_stream_list(kb_conn_t *c, const char *database,
                           const char *path,
                           const char *after, const char *before,
                           kb_resultset_t **rs_out);
 
 /* Clear all stream data for a path (reset valid=FALSE, data=NULL).
  * Uses row locking with retry for concurrent safety. */
 kb_error_t kb_stream_clear(kb_conn_t *c, const char *database,
                            const char *path,
                            int max_retries, int base_delay_ms);
 
 /* Count valid stream entries for a path. */
 kb_error_t kb_stream_count(kb_conn_t *c, const char *database,
                            const char *path, int *count_out);
 
 /* Count total (valid + invalid) entries for a path. */
 kb_error_t kb_stream_count_total(kb_conn_t *c, const char *database,
                                  const char *path, int *count_out);
 
 /* Get the most recent valid stream record for a path.
  * Returns result set with 0 or 1 rows: id, data, recorded_at. */
 kb_error_t kb_stream_latest(kb_conn_t *c, const char *database,
                             const char *path, kb_resultset_t **rs_out);
 
 /* Get records within a time range (valid only).
  * start_time/end_time: ISO-8601 timestamps. */
 kb_error_t kb_stream_range(kb_conn_t *c, const char *database,
                            const char *path,
                            const char *start_time, const char *end_time,
                            kb_resultset_t **rs_out);
 
 /* Get a specific record by row ID.
  * Returns result set with 0 or 1 rows. */
 kb_error_t kb_stream_get_by_id(kb_conn_t *c, const char *database,
                                int row_id, kb_resultset_t **rs_out);
 
 /*
  * Stream statistics.
  * Returns result set with 1 row:
  *   total_count, valid_count, invalid_count,
  *   min_recorded_at, max_recorded_at
  */
 kb_error_t kb_stream_statistics(kb_conn_t *c, const char *database,
                                 const char *path,
                                 kb_resultset_t **rs_out);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_STREAM_H */