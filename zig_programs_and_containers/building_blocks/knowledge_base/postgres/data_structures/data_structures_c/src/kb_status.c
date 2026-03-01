/*
 * kb_status.c
 * Knowledge Base C Library (PostgreSQL) — Status key-value store
 *
 * Table: {database}_status
 * Columns: id (serial), data (json), path (ltree UNIQUE)
 *
 * Concurrent write safety:
 *   - get: uses FOR SHARE to prevent reading mid-update
 *   - set: UPSERT (INSERT ... ON CONFLICT DO UPDATE) is inherently
 *     atomic in PostgreSQL; wrapped in retry for serialization failures
 *   - set_multiple: batch UPSERT in a single transaction
 */

 #include "kb_status.h"
 #include <stdlib.h>
 #include <string.h>
 
 /* ================================================================
  * Get status data (with FOR SHARE for read consistency)
  * ================================================================ */
 
 kb_error_t kb_status_get(kb_conn_t *c, const char *database,
                          const char *path, char **data_out) {
     if (!c || !database || !path || !data_out) return KB_ERR_NULL_ARG;
     *data_out = NULL;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT data FROM %s_status "
         "WHERE path = %s::ltree FOR SHARE",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows == 0) {
         kb_resultset_free(rs);
         return KB_ERR_NOT_FOUND;
     }
 
     const char *v = kb_rs_get(rs, 0, "data");
     if (v) *data_out = kb_strdup(v);
     kb_resultset_free(rs);
     return *data_out ? KB_OK : KB_ERR_NOT_FOUND;
 }
 
 /* ================================================================
  * Set status data (atomic UPSERT)
  * ================================================================ */
 
 typedef struct {
     const char *database;
     const char *path;
     const char *data;
 } status_set_ctx_t;
 
 static kb_error_t status_set_fn(kb_conn_t *c, void *ctx) {
     status_set_ctx_t *sc = ctx;
 
     char *esc_path = kb_escape_literal(c, sc->path);
     char *esc_data = kb_escape_literal(c, sc->data);
     if (!esc_path || !esc_data) {
         if (esc_path) PQfreemem(esc_path);
         if (esc_data) PQfreemem(esc_data);
         return KB_ERR_PG;
     }
 
     char *sql = kb_sprintf(
         "INSERT INTO %s_status (path, data) "
         "VALUES (%s::ltree, %s::jsonb) "
         "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
         sc->database, esc_path, esc_data);
     PQfreemem(esc_path);
     PQfreemem(esc_data);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_status_set(kb_conn_t *c, const char *database,
                          const char *path, const char *data,
                          int max_retries, int base_delay_ms) {
     if (!c || !database || !path || !data) return KB_ERR_NULL_ARG;
     status_set_ctx_t ctx = { database, path, data };
     return kb_retry(c, status_set_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* ================================================================
  * Set multiple status values in a single transaction
  * ================================================================ */
 
 typedef struct {
     const char  *database;
     const char **paths;
     const char **data_values;
     int          count;
 } status_multi_ctx_t;
 
 static kb_error_t status_multi_fn(kb_conn_t *c, void *ctx) {
     status_multi_ctx_t *mc = ctx;
 
     for (int i = 0; i < mc->count; i++) {
         char *esc_path = kb_escape_literal(c, mc->paths[i]);
         char *esc_data = kb_escape_literal(c, mc->data_values[i]);
         if (!esc_path || !esc_data) {
             if (esc_path) PQfreemem(esc_path);
             if (esc_data) PQfreemem(esc_data);
             return KB_ERR_PG;
         }
 
         char *sql = kb_sprintf(
             "INSERT INTO %s_status (path, data) "
             "VALUES (%s::ltree, %s::jsonb) "
             "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
             mc->database, esc_path, esc_data);
         PQfreemem(esc_path);
         PQfreemem(esc_data);
         if (!sql) return KB_ERR_NOMEM;
 
         kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
         free(sql);
         if (err != KB_OK) return err;
     }
 
     return kb_commit(c);
 }
 
 kb_error_t kb_status_set_multiple(kb_conn_t *c, const char *database,
                                   const char **paths,
                                   const char **data_values, int count,
                                   int max_retries, int base_delay_ms) {
     if (!c || !database || !paths || !data_values || count <= 0)
         return KB_ERR_NULL_ARG;
     status_multi_ctx_t ctx = { database, paths, data_values, count };
     return kb_retry(c, status_multi_fn, &ctx, max_retries, base_delay_ms);
 }