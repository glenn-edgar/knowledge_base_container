/*
 * kb_rpc_client.c
 * Knowledge Base C Library (PostgreSQL) — RPC client reply queue
 *
 * Table: {database}_rpc_client
 * Columns: id, request_id (uuid NOT NULL), client_path (ltree NOT NULL),
 *          server_path (ltree NOT NULL), transaction_tag (text NOT NULL default 'none'),
 *          rpc_action (text NOT NULL default 'none'),
 *          response_payload (jsonb NOT NULL), response_timestamp (timestamptz NOT NULL),
 *          is_new_result (bool NOT NULL default false)
 *
 * State machine:
 *   Free slot:   is_new_result = FALSE  (available or consumed)
 *   Has result:  is_new_result = TRUE   (pending result waiting to be consumed)
 *
 * Concurrent write safety:
 *   - push_reply: FOR UPDATE SKIP LOCKED on free slot + retry
 *   - peek_reply: FOR UPDATE SKIP LOCKED (non-blocking consume) + retry
 *   - clear: FOR UPDATE on all path rows + retry
 */

 #include "kb_rpc_client.h"
 #include <stdlib.h>
 #include <string.h>
 
 void kb_rpc_client_reply_free(kb_rpc_client_reply_t *reply) {
     if (!reply) return;
     free(reply->request_id);
     free(reply->client_path);
     free(reply->server_path);
     free(reply->rpc_action);
     free(reply->response_payload);
     memset(reply, 0, sizeof(*reply));
 }
 
 /* ================================================================
  * Slot counting
  * ================================================================ */
 
 static kb_error_t client_count_where(kb_conn_t *c, const char *database,
                                      const char *path, const char *extra,
                                      int *count_out) {
     if (!c || !database || !path || !count_out) return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT COUNT(*) as cnt FROM %s_rpc_client "
         "WHERE client_path = %s::ltree AND %s",
         database, esc_path, extra);
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
 
 /* Free: is_new_result = FALSE */
 kb_error_t kb_rpc_client_free_slots(kb_conn_t *c, const char *database,
                                     const char *path, int *count_out) {
     return client_count_where(c, database, path,
                               "is_new_result = FALSE", count_out);
 }
 
 /* Queued: is_new_result = TRUE */
 kb_error_t kb_rpc_client_queued_slots(kb_conn_t *c, const char *database,
                                       const char *path, int *count_out) {
     return client_count_where(c, database, path,
                               "is_new_result = TRUE", count_out);
 }
 
 /* ================================================================
  * Push reply (claim a free slot and fill it)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *client_path;
     const char *request_id;
     const char *server_path;
     const char *rpc_action;
     const char *transaction_tag;
     const char *response_payload;
 } client_push_ctx_t;
 
 static kb_error_t client_push_fn(kb_conn_t *c, void *ctx) {
     client_push_ctx_t *cc = ctx;
 
     char *esc_cpath = kb_escape_literal(c, cc->client_path);
     char *esc_req = kb_escape_literal(c, cc->request_id ? cc->request_id : "");
     char *esc_spath = kb_escape_literal(c, cc->server_path ? cc->server_path : "");
     char *esc_action = kb_escape_literal(c, cc->rpc_action ? cc->rpc_action : "");
     char *esc_tag = kb_escape_literal(c, cc->transaction_tag ? cc->transaction_tag : "");
     char *esc_payload = kb_escape_literal(c, cc->response_payload ? cc->response_payload : "{}");
 
     if (!esc_cpath) {
         if (esc_req) PQfreemem(esc_req);
         if (esc_spath) PQfreemem(esc_spath);
         if (esc_action) PQfreemem(esc_action);
         if (esc_tag) PQfreemem(esc_tag);
         if (esc_payload) PQfreemem(esc_payload);
         return KB_ERR_PG;
     }
 
     /* Find a free slot (is_new_result=FALSE) and fill it */
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_client SET "
         "request_id = %s::uuid, server_path = %s::ltree, "
         "rpc_action = %s, transaction_tag = %s, "
         "response_payload = %s::jsonb, "
         "response_timestamp = timezone('UTC', now()), "
         "is_new_result = TRUE "
         "WHERE id = ("
         "  SELECT id FROM %s_rpc_client "
         "  WHERE client_path = %s::ltree AND is_new_result = FALSE "
         "  ORDER BY id ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ")",
         cc->database,
         esc_req, esc_spath, esc_action, esc_tag, esc_payload,
         cc->database, esc_cpath);
 
     PQfreemem(esc_cpath);
     PQfreemem(esc_req);
     PQfreemem(esc_spath);
     PQfreemem(esc_action);
     PQfreemem(esc_tag);
     PQfreemem(esc_payload);
 
     if (!sql) return KB_ERR_NOMEM;
 
     int affected = 0;
     kb_error_t err = kb_exec(c, sql, NULL, 0, &affected);
     free(sql);
     if (err != KB_OK) return err;
     if (affected == 0) return KB_ERR_BUSY;
 
     return kb_commit(c);
 }
 
 kb_error_t kb_rpc_client_push_reply(kb_conn_t *c, const char *database,
                                     const char *client_path,
                                     const char *request_id,
                                     const char *server_path,
                                     const char *rpc_action,
                                     const char *transaction_tag,
                                     const char *response_payload,
                                     int max_retries, int base_delay_ms) {
     client_push_ctx_t ctx = {
         database, client_path, request_id, server_path,
         rpc_action, transaction_tag, response_payload
     };
     return kb_retry(c, client_push_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Peek and claim reply (consume oldest pending result)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     kb_rpc_client_reply_t *reply;
 } client_peek_ctx_t;
 
 static kb_error_t client_peek_fn(kb_conn_t *c, void *ctx) {
     client_peek_ctx_t *cc = ctx;
     cc->reply->found = false;
 
     char *esc_path = kb_escape_literal(c, cc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Find oldest pending result (is_new_result=TRUE), consume it */
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_client SET "
         "is_new_result = FALSE, "
         "response_timestamp = timezone('UTC', now()) "
         "WHERE id = ("
         "  SELECT id FROM %s_rpc_client "
         "  WHERE client_path = %s::ltree AND is_new_result = TRUE "
         "  ORDER BY id ASC LIMIT 1 "
         "  FOR UPDATE SKIP LOCKED"
         ") RETURNING id, request_id, client_path::text, server_path::text, "
         "rpc_action, response_payload",
         cc->database, cc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0) {
         cc->reply->found = true;
         cc->reply->id = kb_rs_get_int(rs, 0, "id");
         const char *v;
         v = kb_rs_get(rs, 0, "request_id");
         cc->reply->request_id = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "client_path");
         cc->reply->client_path = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "server_path");
         cc->reply->server_path = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "rpc_action");
         cc->reply->rpc_action = v ? kb_strdup(v) : NULL;
         v = kb_rs_get(rs, 0, "response_payload");
         cc->reply->response_payload = v ? kb_strdup(v) : NULL;
         kb_resultset_free(rs);
         return kb_commit(c);
     }
 
     kb_resultset_free(rs);
     return KB_ERR_NOT_FOUND;
 }
 
 kb_error_t kb_rpc_client_peek_reply(kb_conn_t *c, const char *database,
                                     const char *path,
                                     kb_rpc_client_reply_t *reply_out,
                                     int max_retries, int base_delay_ms) {
     if (!reply_out) return KB_ERR_NULL_ARG;
     memset(reply_out, 0, sizeof(*reply_out));
     client_peek_ctx_t ctx = { database, path, reply_out };
     return kb_retry(c, client_peek_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Clear (reset all slots to free)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
 } client_clear_ctx_t;
 
 static kb_error_t client_clear_fn(kb_conn_t *c, void *ctx) {
     client_clear_ctx_t *cc = ctx;
 
     char *esc_path = kb_escape_literal(c, cc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock all rows for this path before batch reset */
     char *sql = kb_sprintf(
         "UPDATE %s_rpc_client SET "
         "response_payload = '{}'::jsonb, "
         "rpc_action = 'none', "
         "transaction_tag = 'none', "
         "response_timestamp = timezone('UTC', now()), "
         "is_new_result = FALSE "
         "WHERE id IN ("
         "  SELECT id FROM %s_rpc_client "
         "  WHERE client_path = %s::ltree "
         "  FOR UPDATE"
         ")",
         cc->database, cc->database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_rpc_client_clear(kb_conn_t *c, const char *database,
                                const char *path,
                                int max_retries, int base_delay_ms) {
     client_clear_ctx_t ctx = { database, path };
     return kb_retry(c, client_clear_fn, &ctx, max_retries, base_delay_ms);
 }