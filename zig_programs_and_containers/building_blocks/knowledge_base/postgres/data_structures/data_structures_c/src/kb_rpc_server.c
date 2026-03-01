/*
 * kb_rpc_server.c
 * Knowledge Base C Library (PostgreSQL) — RPC server request queue
 *
 * Table: {database}_rpc_server
 * Columns: id, server_path (ltree), request_id (uuid), rpc_action (text),
 *          request_payload (jsonb), request_timestamp (timestamptz),
 *          transaction_tag (text), state (text), priority (int),
 *          processing_timestamp (timestamptz), completed_timestamp (timestamptz),
 *          rpc_client_queue (ltree)
 *
 * State machine (CHECK constraint):
 *   'empty'      — free slot, available for new jobs
 *   'new_job'    — filled with request data, waiting to be processed
 *   'processing' — claimed by a worker via peek
 *   (completed → reset back to 'empty')
 *
 * Concurrent write safety:
 *   - push: SERIALIZABLE + advisory lock + FOR UPDATE SKIP LOCKED
 *   - peek: FOR UPDATE SKIP LOCKED (non-blocking claim)
 *   - complete: FOR UPDATE on target row + retry
 *   - clear: FOR UPDATE on all path rows + retry
 */

 #include "kb_rpc_server.h"
 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ================================================================
  * Free resources
  * ================================================================ */
 
 void kb_rpc_server_job_free(kb_rpc_server_job_t *job) {
     if (!job) return;
     free(job->server_path);
     free(job->request_id);
     free(job->rpc_action);
     free(job->request_payload);
     free(job->transaction_tag);
     free(job->state);
     free(job->rpc_client_queue);
     memset(job, 0, sizeof(*job));
 }
 
 /* ================================================================
  * Count by state
  * ================================================================ */
 
 static kb_error_t rpc_server_count_state(kb_conn_t *c, const char *database,
                                          const char *path, const char *state,
                                          int *count_out) {
     if (!c || !database || !path || !state || !count_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     char *esc_state = kb_escape_literal(c, state);
     if (!esc_path || !esc_state) {
         if (esc_path) PQfreemem(esc_path);
         if (esc_state) PQfreemem(esc_state);
         return KB_ERR_PG;
     }
 
     char *sql = kb_sprintf(
         "SELECT COUNT(*) as cnt FROM %s_rpc_server "
         "WHERE server_path = %s::ltree AND state = %s",
         database, esc_path, esc_state);
     PQfreemem(esc_path);
     PQfreemem(esc_state);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     *count_out = kb_rs_get_int(rs, 0, "cnt");
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 kb_error_t kb_rpc_server_count_new(kb_conn_t *c, const char *database,
                                    const char *path, int *count_out) {
     return rpc_server_count_state(c, database, path, "new_job", count_out);
 }
 
 kb_error_t kb_rpc_server_count_processing(kb_conn_t *c, const char *database,
                                           const char *path, int *count_out) {
     return rpc_server_count_state(c, database, path, "processing", count_out);
 }
 
 /* ================================================================
  * Push (SERIALIZABLE + advisory lock)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *server_path;
     const char *request_id;
     const char *rpc_action;
     const char *request_payload;
     const char *transaction_tag;
     int         priority;
     const char *rpc_client_queue;
 } rpc_push_ctx_t;
 
 static kb_error_t rpc_push_fn(kb_conn_t *c, void *ctx) {
     rpc_push_ctx_t *rc = ctx;
 
     /* Hash server_path for advisory lock */
     unsigned long hash = 5381;
     for (const char *p = rc->server_path; *p; p++) hash = hash * 31 + *p;
 
     /*
      * We're inside an auto-BEGIN from kb_common.
      * End it and start a fresh SERIALIZABLE transaction.
      */
     kb_error_t err = kb_exec(c,
         "COMMIT", NULL, 0, NULL);
     if (err != KB_OK) return err;
 
     err = kb_exec(c,
         "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", NULL, 0, NULL);
     if (err != KB_OK) return err;
 
     /* Advisory lock */
     char lock_sql[128];
     snprintf(lock_sql, sizeof(lock_sql),
              "SELECT pg_advisory_xact_lock(%ld)", (long)(hash & 0x7FFFFFFF));
     err = kb_exec(c, lock_sql, NULL, 0, NULL);
     if (err != KB_OK) return err;
 
     /* Find a free slot (state='empty') */
     char *esc_path = kb_escape_literal(c, rc->server_path);
     if (!esc_path) return KB_ERR_PG;
 
     char *find_sql = kb_sprintf(
         "SELECT id FROM %s_rpc_server "
         "WHERE server_path = %s::ltree AND state = 'empty' "
         "ORDER BY id ASC LIMIT 1 "
         "FOR UPDATE SKIP LOCKED",
         rc->database, esc_path);
     PQfreemem(esc_path);
     if (!find_sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     err = kb_query(c, find_sql, NULL, 0, &rs);
     free(find_sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows == 0) {
         kb_resultset_free(rs);
         return KB_ERR_BUSY;
     }
     int slot_id = kb_rs_get_int(rs, 0, "id");
     kb_resultset_free(rs);
 
     /* Fill the slot */
     char *esc_req_id = kb_escape_literal(c, rc->request_id ? rc->request_id : "");
     char *esc_action = kb_escape_literal(c, rc->rpc_action ? rc->rpc_action : "");
     char *esc_payload = kb_escape_literal(c, rc->request_payload ? rc->request_payload : "{}");
     char *esc_tag = kb_escape_literal(c, rc->transaction_tag ? rc->transaction_tag : "");
     char *esc_client = kb_escape_literal(c, rc->rpc_client_queue ? rc->rpc_client_queue : "");
 
     char pri_str[32], id_str[32];
     snprintf(pri_str, sizeof(pri_str), "%d", rc->priority);
     snprintf(id_str, sizeof(id_str), "%d", slot_id);
 
     char *upd_sql = kb_sprintf(
         "UPDATE %s_rpc_server SET "
         "request_id = %s::uuid, rpc_action = %s, "
         "request_payload = %s::jsonb, "
         "request_timestamp = timezone('UTC', now()), "
         "transaction_tag = %s, "
         "state = 'new_job', priority = %s, "
         "processing_timestamp = NULL, completed_timestamp = NULL, "
         "rpc_client_queue = %s::ltree "
         "WHERE id = %s",
         rc->database,
         esc_req_id, esc_action, esc_payload, esc_tag,
         pri_str, esc_client, id_str);
 
     PQfreemem(esc_req_id);
     PQfreemem(esc_action);
     PQfreemem(esc_payload);
     PQfreemem(esc_tag);
     PQfreemem(esc_client);
     if (!upd_sql) return KB_ERR_NOMEM;
 
     err = kb_exec(c, upd_sql, NULL, 0, NULL);
     free(upd_sql);
     if (err != KB_OK) return err;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_rpc_server_push(kb_conn_t *c, const char *database,
                               const char *server_path,
                               const char *request_id,
                               const char *rpc_action,
                               const char *request_payload,
                               const char *transaction_tag,
                               int priority,
                               const char *rpc_client_queue,
                               int max_retries, int base_delay_ms) {
     rpc_push_ctx_t ctx = {
         database, server_path, request_id, rpc_action,
         request_payload, transaction_tag, priority, rpc_client_queue
     };
     return kb_retry(c, rpc_push_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Peek (claim oldest new_job → processing)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     kb_rpc_server_job_t *job;
 } rpc_peek_ctx_t;
 
 static kb_error_t rpc_peek_fn(kb_conn_t *c, void *ctx) {
     rpc_peek_ctx_t *rc = ctx;
     rc->job->found = false;
 
     char *esc_path = kb_escape_literal(c, rc->path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_server SET state = 'processing', "
         "processing_timestamp = timezone('UTC', now()) "
         "WHERE id = ("
         "  SELECT id FROM %s_rpc_server "
         "  WHERE server_path = %s::ltree AND state = 'new_job' "
         "  ORDER BY priority ASC, request_timestamp ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ") RETURNING id, server_path::text, request_id, rpc_action, "
         "request_payload, request_timestamp, transaction_tag, "
         "state, priority, rpc_client_queue::text",
         rc->database, rc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0) {
         rc->job->found = true;
         rc->job->id = kb_rs_get_int(rs, 0, "id");
         rc->job->priority = kb_rs_get_int(rs, 0, "priority");
 
         const char *v;
         v = kb_rs_get(rs, 0, "server_path");
         rc->job->server_path = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "request_id");
         rc->job->request_id = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "rpc_action");
         rc->job->rpc_action = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "request_payload");
         rc->job->request_payload = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "transaction_tag");
         rc->job->transaction_tag = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "state");
         rc->job->state = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "rpc_client_queue");
         rc->job->rpc_client_queue = v ? kb_strdup(v) : NULL;
 
         kb_resultset_free(rs);
         return kb_commit(c);
     }
 
     kb_resultset_free(rs);
     return KB_ERR_NOT_FOUND;
 }
 
 kb_error_t kb_rpc_server_peek(kb_conn_t *c, const char *database,
                               const char *path,
                               kb_rpc_server_job_t *job_out,
                               int max_retries, int base_delay_ms) {
     if (!job_out) return KB_ERR_NULL_ARG;
     memset(job_out, 0, sizeof(*job_out));
     rpc_peek_ctx_t ctx = { database, path, job_out };
     return kb_retry(c, rpc_peek_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Complete (reset slot back to 'empty')
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     int job_id;
 } rpc_complete_ctx_t;
 
 static kb_error_t rpc_complete_fn(kb_conn_t *c, void *ctx) {
     rpc_complete_ctx_t *rc = ctx;
 
     char id_str[32];
     snprintf(id_str, sizeof(id_str), "%d", rc->job_id);
 
     char *esc_path = kb_escape_literal(c, rc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock row before resetting */
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_server SET "
         "state = 'empty', "
         "completed_timestamp = timezone('UTC', now()), "
         "rpc_action = 'none', "
         "request_payload = '{}'::jsonb, "
         "transaction_tag = '', "
         "priority = 0, "
         "processing_timestamp = NULL, "
         "rpc_client_queue = NULL "
         "WHERE id = ("
         "  SELECT id FROM %s_rpc_server "
         "  WHERE id = %s AND server_path = %s::ltree "
         "  FOR UPDATE"
         ")",
         rc->database, rc->database, id_str, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_rpc_server_complete(kb_conn_t *c, const char *database,
                                   const char *path, int job_id,
                                   int max_retries, int base_delay_ms) {
     rpc_complete_ctx_t ctx = { database, path, job_id };
     return kb_retry(c, rpc_complete_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Clear all jobs for a server path (reset to 'empty')
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
 } rpc_clear_ctx_t;
 
 static kb_error_t rpc_clear_fn(kb_conn_t *c, void *ctx) {
     rpc_clear_ctx_t *rc = ctx;
 
     char *esc_path = kb_escape_literal(c, rc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock all rows for this path before batch reset */
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_server SET "
         "state = 'empty', "
         "rpc_action = 'none', "
         "request_payload = '{}'::jsonb, "
         "transaction_tag = '', "
         "priority = 0, "
         "processing_timestamp = NULL, "
         "completed_timestamp = timezone('UTC', now()), "
         "rpc_client_queue = NULL "
         "WHERE id IN ("
         "  SELECT id FROM %s_rpc_server "
         "  WHERE server_path = %s::ltree "
         "  FOR UPDATE"
         ")",
         rc->database, rc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_rpc_server_clear(kb_conn_t *c, const char *database,
                                const char *path,
                                int max_retries, int base_delay_ms) {
     rpc_clear_ctx_t ctx = { database, path };
     return kb_retry(c, rpc_clear_fn, &ctx, max_retries, base_delay_ms);
 }