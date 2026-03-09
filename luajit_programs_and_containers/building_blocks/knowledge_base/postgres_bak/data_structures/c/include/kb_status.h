/*
 * kb_status.h
 * Knowledge Base C Library (PostgreSQL) — Status key-value store
 *
 * Table: {database}_status
 * Columns: id (serial), path (ltree UNIQUE), data (json)
 *
 * Concurrent write safety:
 *   - get: FOR SHARE read lock
 *   - set: atomic UPSERT with retry
 *   - set_multiple: batch UPSERT in single transaction with retry
 */

 #ifndef KB_STATUS_H
 #define KB_STATUS_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /*
  * Get status data for a path.
  * Uses FOR SHARE to prevent reading during concurrent writes.
  * Returns heap-allocated JSON string. Caller must free.
  * Returns KB_ERR_NOT_FOUND if path doesn't exist.
  */
 kb_error_t kb_status_get(kb_conn_t *c, const char *database,
                          const char *path, char **data_out);
 
 /*
  * Set status data for a path (atomic UPSERT with retry).
  * data: JSON string.
  * max_retries: retry attempts on contention (e.g. 3)
  * base_delay_ms: initial backoff delay in ms (e.g. 100)
  */
 kb_error_t kb_status_set(kb_conn_t *c, const char *database,
                          const char *path, const char *data,
                          int max_retries, int base_delay_ms);
 
 /*
  * Set multiple status values in a single transaction.
  * paths[i] / data_values[i] are paired arrays.
  */
 kb_error_t kb_status_set_multiple(kb_conn_t *c, const char *database,
                                   const char **paths,
                                   const char **data_values, int count,
                                   int max_retries, int base_delay_ms);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_STATUS_H */