/*
 * kb_job_queue.c
 * Knowledge Base C Library (PostgreSQL) — Job queue operations
 *
 * Table: {database}_job
 * Columns: id, path (ltree), schedule_at, started_at, completed_at,
 *          is_active (bool), valid (bool), data (jsonb)
 *
 * State machine:
 *   Free slot:    valid=FALSE, is_active=FALSE, data=NULL
 *   Queued:       valid=TRUE,  is_active=FALSE  (pushed, waiting)
 *   Active:       valid=TRUE,  is_active=TRUE   (being processed / peeked)
 *   Completed:    valid=FALSE, is_active=FALSE   (recycled back to free)
 *
 * Concurrent write safety:
 *   - push: FOR UPDATE SKIP LOCKED on free slot, with retry
 *   - peek: FOR UPDATE SKIP LOCKED on pending job, with retry
 *   - complete: FOR UPDATE on target row, with retry
 *   - clear: FOR UPDATE on all path rows, with retry
 */

 #include "kb_job_queue.h"
 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ================================================================
  * Slot counting (read-only, no locks needed)
  * ================================================================ */
 
 static kb_error_t job_count_where(kb_conn_t *c, const char *database,
                                   const char *path, const char *where_clause,
                                   int *count_out) {
     if (!c || !database || !path || !count_out) return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT COUNT(*) as cnt FROM %s_job "
         "WHERE path = %s::ltree AND %s",
         database, esc_path, where_clause);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     *count_out = kb_rs_get_int(rs, 0, "cnt");
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 kb_error_t kb_job_free_count(kb_conn_t *c, const char *database,
                              const char *path, int *count_out) {
     return job_count_where(c, database, path,
                            "valid = FALSE", count_out);
 }
 
 kb_error_t kb_job_queued_count(kb_conn_t *c, const char *database,
                                const char *path, int *count_out) {
     return job_count_where(c, database, path,
                            "valid = TRUE AND is_active = FALSE", count_out);
 }
 
 kb_error_t kb_job_active_count(kb_conn_t *c, const char *database,
                                const char *path, int *count_out) {
     return job_count_where(c, database, path,
                            "valid = TRUE AND is_active = TRUE", count_out);
 }
 
 /* ================================================================
  * Push (enqueue a new job into a free slot)
  * FOR UPDATE SKIP LOCKED prevents concurrent writers from
  * grabbing the same free slot.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     const char *data;
 } job_push_ctx_t;
 
 static kb_error_t job_push_fn(kb_conn_t *c, void *ctx) {
     job_push_ctx_t *jc = ctx;
 
     char *esc_path = kb_escape_literal(c, jc->path);
     char *esc_data = kb_escape_literal(c, jc->data);
     if (!esc_path || !esc_data) {
         if (esc_path) PQfreemem(esc_path);
         if (esc_data) PQfreemem(esc_data);
         return KB_ERR_PG;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_job SET "
         "data = %s::jsonb, "
         "schedule_at = timezone('UTC', now()), "
         "started_at = timezone('UTC', now()), "
         "completed_at = timezone('UTC', now()), "
         "valid = TRUE, is_active = FALSE "
         "WHERE id = ("
         "  SELECT id FROM %s_job "
         "  WHERE path = %s::ltree AND valid = FALSE "
         "  ORDER BY completed_at ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ")",
         jc->database, esc_data,
         jc->database, esc_path);
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
 
 kb_error_t kb_job_push(kb_conn_t *c, const char *database,
                        const char *path, const char *data,
                        int max_retries, int base_delay_ms) {
     if (!c || !database || !path || !data) return KB_ERR_NULL_ARG;
     job_push_ctx_t ctx = { database, path, data };
     return kb_retry(c, job_push_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Peek (claim oldest pending job → set is_active=TRUE)
  * FOR UPDATE SKIP LOCKED prevents concurrent consumers from
  * claiming the same job.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     kb_job_info_t *info;
 } job_peek_ctx_t;
 
 static kb_error_t job_peek_fn(kb_conn_t *c, void *ctx) {
     job_peek_ctx_t *jc = ctx;
     jc->info->found = false;
     jc->info->id = 0;
     if (jc->info->data) { free(jc->info->data); jc->info->data = NULL; }
 
     char *esc_path = kb_escape_literal(c, jc->path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "UPDATE %s_job SET is_active = TRUE, "
         "started_at = timezone('UTC', now()) "
         "WHERE id = ("
         "  SELECT id FROM %s_job "
         "  WHERE path = %s::ltree AND valid = TRUE AND is_active = FALSE "
         "  ORDER BY schedule_at ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ") RETURNING id, data",
         jc->database, jc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0) {
         jc->info->found = true;
         jc->info->id = kb_rs_get_int(rs, 0, "id");
         const char *d = kb_rs_get(rs, 0, "data");
         jc->info->data = d ? kb_strdup(d) : NULL;
         kb_resultset_free(rs);
         return kb_commit(c);
     }
 
     kb_resultset_free(rs);
     return KB_ERR_NOT_FOUND;
 }
 
 kb_error_t kb_job_peek(kb_conn_t *c, const char *database,
                        const char *path, kb_job_info_t *info_out,
                        int max_retries, int base_delay_ms) {
     if (!c || !database || !path || !info_out) return KB_ERR_NULL_ARG;
     memset(info_out, 0, sizeof(*info_out));
     job_peek_ctx_t ctx = { database, path, info_out };
     return kb_retry(c, job_peek_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Complete (recycle slot: valid=FALSE, is_active=FALSE, data=NULL)
  * Uses FOR UPDATE to lock the row before modifying.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     int job_id;
 } job_complete_ctx_t;
 
 static kb_error_t job_complete_fn(kb_conn_t *c, void *ctx) {
     job_complete_ctx_t *jc = ctx;
 
     char id_str[32];
     snprintf(id_str, sizeof(id_str), "%d", jc->job_id);
 
     /* Lock row first, then update */
     char *sql = kb_sprintf(
         "UPDATE %s_job SET "
         "completed_at = timezone('UTC', now()), "
         "valid = FALSE, is_active = FALSE, data = NULL "
         "WHERE id = ("
         "  SELECT id FROM %s_job WHERE id = %s "
         "  FOR UPDATE"
         ")",
         jc->database, jc->database, id_str);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err != KB_OK) return err;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_job_complete(kb_conn_t *c, const char *database,
                            int job_id,
                            int max_retries, int base_delay_ms) {
     if (!c || !database) return KB_ERR_NULL_ARG;
     job_complete_ctx_t ctx = { database, job_id };
     return kb_retry(c, job_complete_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Clear all jobs for a path (reset all slots to free)
  * Uses FOR UPDATE to lock all rows before batch reset.
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
 } job_clear_ctx_t;
 
 static kb_error_t job_clear_fn(kb_conn_t *c, void *ctx) {
     job_clear_ctx_t *jc = ctx;
 
     char *esc_path = kb_escape_literal(c, jc->path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "UPDATE %s_job SET "
         "schedule_at = timezone('UTC', now()), "
         "started_at = timezone('UTC', now()), "
         "completed_at = timezone('UTC', now()), "
         "is_active = FALSE, valid = FALSE, data = NULL "
         "WHERE id IN ("
         "  SELECT id FROM %s_job "
         "  WHERE path = %s::ltree "
         "  FOR UPDATE"
         ")",
         jc->database, jc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err != KB_OK) return err;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_job_clear(kb_conn_t *c, const char *database,
                         const char *path,
                         int max_retries, int base_delay_ms) {
     if (!c || !database || !path) return KB_ERR_NULL_ARG;
     job_clear_ctx_t ctx = { database, path };
     return kb_retry(c, job_clear_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * List pending jobs (valid=TRUE, is_active=FALSE)
  * ================================================================ */
 
 kb_error_t kb_job_list_pending(kb_conn_t *c, const char *database,
                                const char *path,
                                kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT id, data, schedule_at FROM %s_job "
         "WHERE path = %s::ltree AND valid = TRUE AND is_active = FALSE "
         "ORDER BY schedule_at ASC",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * List active jobs (valid=TRUE, is_active=TRUE)
  * ================================================================ */
 
 kb_error_t kb_job_list_active(kb_conn_t *c, const char *database,
                               const char *path,
                               kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
     *rs_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT id, data, started_at FROM %s_job "
         "WHERE path = %s::ltree AND valid = TRUE AND is_active = TRUE "
         "ORDER BY started_at ASC",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }