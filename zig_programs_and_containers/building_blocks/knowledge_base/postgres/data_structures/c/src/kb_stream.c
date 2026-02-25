/*
 * kb_stream.c
 * Knowledge Base C Library (PostgreSQL) — Circular buffer time-series
 *
 * Table: {database}_stream
 * Columns: id, path (ltree), recorded_at (timestamptz), valid (bool), data (jsonb)
 *
 * Circular buffer: push overwrites the oldest row (by recorded_at ASC).
 * Sets valid=TRUE and recorded_at=now() on the updated row.
 *
 * Concurrent write safety:
 *   - push: FOR UPDATE SKIP LOCKED on oldest row, with retry
 *   - clear: batch UPDATE with advisory lock, with retry
 *   - reads: FOR SHARE where consistency matters
 */

 #include "kb_stream.h"
 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ================================================================
  * Push (circular buffer: update oldest slot)
  * FOR UPDATE SKIP LOCKED prevents concurrent writers from
  * grabbing the same slot.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     const char *data;
 } stream_push_ctx_t;
 
 static kb_error_t stream_push_fn(kb_conn_t *c, void *ctx) {
     stream_push_ctx_t *sc = ctx;
 
     char *esc_path = kb_escape_literal(c, sc->path);
     char *esc_data = kb_escape_literal(c, sc->data);
     if (!esc_path || !esc_data) {
         if (esc_path) PQfreemem(esc_path);
         if (esc_data) PQfreemem(esc_data);
         return KB_ERR_PG;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_stream SET data = %s::jsonb, "
         "recorded_at = timezone('UTC', now()), valid = TRUE "
         "WHERE id = ("
         "  SELECT id FROM %s_stream "
         "  WHERE path = %s::ltree "
         "  ORDER BY recorded_at ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ")",
         sc->database, esc_data, sc->database, esc_path);
     PQfreemem(esc_path);
     PQfreemem(esc_data);
     if (!sql) return KB_ERR_NOMEM;
 
     int affected = 0;
     kb_error_t err = kb_exec(c, sql, NULL, 0, &affected);
     free(sql);
     if (err != KB_OK) return err;
     if (affected == 0) return KB_ERR_BUSY;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_stream_push(kb_conn_t *c, const char *database,
                           const char *path, const char *data,
                           int max_retries, int base_delay_ms) {
     if (!c || !database || !path || !data) return KB_ERR_NULL_ARG;
     stream_push_ctx_t ctx = { database, path, data };
     return kb_retry(c, stream_push_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * List with optional time-range filter (read-only, no lock needed
  * for listing since we tolerate slightly stale reads)
  * ================================================================ */
 
 kb_error_t kb_stream_list(kb_conn_t *c, const char *database,
                           const char *path,
                           const char *after, const char *before,
                           kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Build WHERE clause */
     char where_extra[512] = "";
     if (after) {
         char *esc = kb_escape_literal(c, after);
         if (esc) {
             snprintf(where_extra + strlen(where_extra),
                      sizeof(where_extra) - strlen(where_extra),
                      " AND recorded_at > %s::timestamptz", esc);
             PQfreemem(esc);
         }
     }
     if (before) {
         char *esc = kb_escape_literal(c, before);
         if (esc) {
             snprintf(where_extra + strlen(where_extra),
                      sizeof(where_extra) - strlen(where_extra),
                      " AND recorded_at < %s::timestamptz", esc);
             PQfreemem(esc);
         }
     }
 
     char *sql = kb_sprintf(
         "SELECT id, data, recorded_at FROM %s_stream "
         "WHERE path = %s::ltree AND valid = TRUE%s "
         "ORDER BY recorded_at DESC",
         database, esc_path, where_extra);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Clear all stream data for a path
  * Uses row locking with retry for concurrent safety.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
 } stream_clear_ctx_t;
 
 static kb_error_t stream_clear_fn(kb_conn_t *c, void *ctx) {
     stream_clear_ctx_t *sc = ctx;
 
     char *esc_path = kb_escape_literal(c, sc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock all rows for this path, then update */
     char *sql = kb_sprintf(
         "UPDATE %s_stream SET data = NULL, valid = FALSE, "
         "recorded_at = timezone('UTC', now()) "
         "WHERE id IN ("
         "  SELECT id FROM %s_stream "
         "  WHERE path = %s::ltree "
         "  FOR UPDATE"
         ")",
         sc->database, sc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err != KB_OK) return err;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_stream_clear(kb_conn_t *c, const char *database,
                            const char *path,
                            int max_retries, int base_delay_ms) {
     if (!c || !database || !path) return KB_ERR_NULL_ARG;
     stream_clear_ctx_t ctx = { database, path };
     return kb_retry(c, stream_clear_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Count valid stream entries
  * ================================================================ */
 
 kb_error_t kb_stream_count(kb_conn_t *c, const char *database,
                            const char *path, int *count_out) {
     if (!c || !database || !path || !count_out) return KB_ERR_NULL_ARG;
     *count_out = 0;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT COUNT(*) AS cnt FROM %s_stream "
         "WHERE path = %s::ltree AND valid = TRUE",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0)
         *count_out = (int)kb_rs_get_int64(rs, 0, "cnt");
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 /* ================================================================
  * Count total (valid + invalid) entries
  * ================================================================ */
 
 kb_error_t kb_stream_count_total(kb_conn_t *c, const char *database,
                                  const char *path, int *count_out) {
     if (!c || !database || !path || !count_out) return KB_ERR_NULL_ARG;
     *count_out = 0;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT COUNT(*) AS cnt FROM %s_stream "
         "WHERE path = %s::ltree",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0)
         *count_out = (int)kb_rs_get_int64(rs, 0, "cnt");
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 /* ================================================================
  * Get the most recent valid record
  * ================================================================ */
 
 kb_error_t kb_stream_latest(kb_conn_t *c, const char *database,
                             const char *path, kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT id, data, recorded_at FROM %s_stream "
         "WHERE path = %s::ltree AND valid = TRUE "
         "ORDER BY recorded_at DESC LIMIT 1",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Get records within a time range (valid only)
  * ================================================================ */
 
 kb_error_t kb_stream_range(kb_conn_t *c, const char *database,
                            const char *path,
                            const char *start_time, const char *end_time,
                            kb_resultset_t **rs_out) {
     if (!c || !database || !path || !start_time || !end_time || !rs_out)
         return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path  = kb_escape_literal(c, path);
     char *esc_start = kb_escape_literal(c, start_time);
     char *esc_end   = kb_escape_literal(c, end_time);
     if (!esc_path || !esc_start || !esc_end) {
         if (esc_path)  PQfreemem(esc_path);
         if (esc_start) PQfreemem(esc_start);
         if (esc_end)   PQfreemem(esc_end);
         return KB_ERR_PG;
     }
 
     char *sql = kb_sprintf(
         "SELECT id, data, recorded_at FROM %s_stream "
         "WHERE path = %s::ltree AND valid = TRUE "
         "AND recorded_at >= %s::timestamptz "
         "AND recorded_at <= %s::timestamptz "
         "ORDER BY recorded_at DESC",
         database, esc_path, esc_start, esc_end);
     PQfreemem(esc_path);
     PQfreemem(esc_start);
     PQfreemem(esc_end);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Get a specific record by row ID
  * ================================================================ */
 
 kb_error_t kb_stream_get_by_id(kb_conn_t *c, const char *database,
                                int row_id, kb_resultset_t **rs_out) {
     if (!c || !database || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char id_str[32];
     snprintf(id_str, sizeof(id_str), "%d", row_id);
 
     char *sql = kb_sprintf(
         "SELECT id, path, data, recorded_at, valid FROM %s_stream "
         "WHERE id = %s",
         database, id_str);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Stream statistics
  * ================================================================ */
 
 kb_error_t kb_stream_statistics(kb_conn_t *c, const char *database,
                                 const char *path,
                                 kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT "
         "  COUNT(*) AS total_count, "
         "  COUNT(*) FILTER (WHERE valid = TRUE) AS valid_count, "
         "  COUNT(*) FILTER (WHERE valid = FALSE) AS invalid_count, "
         "  MIN(recorded_at) FILTER (WHERE valid = TRUE) AS min_recorded_at, "
         "  MAX(recorded_at) FILTER (WHERE valid = TRUE) AS max_recorded_at "
         "FROM %s_stream WHERE path = %s::ltree",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }