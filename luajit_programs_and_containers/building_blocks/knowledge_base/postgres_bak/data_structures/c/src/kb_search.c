/*
 * kb_search.c
 * Knowledge Base C Library (PostgreSQL) — CTE-based progressive query builder
 *
 * Mirrors LuaJIT kb_search.lua. SQL is built via string interpolation
 * with PQescapeLiteral because ltree operators (<@, ~, etc.) and JSONB
 * operators are incompatible with PQexecParams placeholders.
 *
 * The ? operator conflict (DBI issue #2 in the LuaJIT port) doesn't
 * apply here since we don't use prepared statements, but we still use
 * jsonb_exists() for consistency.
 */

 #include "kb_search.h"
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 /* ================================================================
  * Create / Destroy
  * ================================================================ */
 
 kb_error_t kb_search_create(kb_conn_t *conn, const char *database,
                             kb_search_t **out) {
     if (!conn || !database || !out) return KB_ERR_NULL_ARG;
 
     kb_search_t *ks = calloc(1, sizeof(kb_search_t));
     if (!ks) return KB_ERR_NOMEM;
 
     ks->conn = conn;
     ks->database = kb_strdup(database);
     ks->filter_count = 0;
     ks->last_result = NULL;
 
     *out = ks;
     return KB_OK;
 }
 
 void kb_search_destroy(kb_search_t *ks) {
     if (!ks) return;
     kb_search_clear(ks);
     if (ks->last_result) kb_resultset_free(ks->last_result);
     free(ks->database);
     free(ks);
 }
 
 void kb_search_clear(kb_search_t *ks) {
     if (!ks) return;
     for (int i = 0; i < ks->filter_count; i++) {
         free(ks->cte_parts[i]);
         ks->cte_parts[i] = NULL;
     }
     ks->filter_count = 0;
     if (ks->last_result) {
         kb_resultset_free(ks->last_result);
         ks->last_result = NULL;
     }
 }
 
 /* ================================================================
  * Filter chain
  * ================================================================
  *
  * Each filter appends a CTE that narrows from the previous CTE.
  * The naming convention is: step0, step1, step2, ...
  *
  * step0 = base table.
  * step1 = first filter applied to step0.
  * etc.
  */
 
 static const char *prev_step(kb_search_t *ks) {
     if (ks->filter_count == 0) return ks->database;
     static char buf[32];
     snprintf(buf, sizeof(buf), "step%d", ks->filter_count - 1);
     return buf;
 }
 
 static kb_error_t add_filter(kb_search_t *ks, char *sql) {
     if (ks->filter_count >= KB_MAX_FILTERS) {
         free(sql);
         return KB_ERR_OVERFLOW;
     }
     ks->cte_parts[ks->filter_count++] = sql;
     return KB_OK;
 }
 
 kb_error_t kb_search_label(kb_search_t *ks, const char *label) {
     if (!ks || !label) return KB_ERR_NULL_ARG;
     char *esc = kb_escape_literal(ks->conn, label);
     if (!esc) return KB_ERR_PG;
     char *sql = kb_sprintf("step%d AS (SELECT * FROM %s WHERE label = %s)",
                            ks->filter_count, prev_step(ks), esc);
     PQfreemem(esc);
     return add_filter(ks, sql);
 }
 
 kb_error_t kb_search_name(kb_search_t *ks, const char *name) {
     if (!ks || !name) return KB_ERR_NULL_ARG;
     char *esc = kb_escape_literal(ks->conn, name);
     if (!esc) return KB_ERR_PG;
     char *sql = kb_sprintf("step%d AS (SELECT * FROM %s WHERE name = %s)",
                            ks->filter_count, prev_step(ks), esc);
     PQfreemem(esc);
     return add_filter(ks, sql);
 }
 
 kb_error_t kb_search_property_key(kb_search_t *ks, const char *key) {
     if (!ks || !key) return KB_ERR_NULL_ARG;
     char *esc = kb_escape_literal(ks->conn, key);
     if (!esc) return KB_ERR_PG;
     /* Use jsonb_exists() instead of ? operator (DBI compatibility) */
     char *sql = kb_sprintf(
         "step%d AS (SELECT * FROM %s WHERE jsonb_exists(properties::jsonb, %s))",
         ks->filter_count, prev_step(ks), esc);
     PQfreemem(esc);
     return add_filter(ks, sql);
 }
 
 kb_error_t kb_search_property_value(kb_search_t *ks, const char *key,
                                     const char *value) {
     if (!ks || !key || !value) return KB_ERR_NULL_ARG;
     char *esc_key = kb_escape_literal(ks->conn, key);
     char *esc_val = kb_escape_literal(ks->conn, value);
     if (!esc_key || !esc_val) {
         if (esc_key) PQfreemem(esc_key);
         if (esc_val) PQfreemem(esc_val);
         return KB_ERR_PG;
     }
     char *sql = kb_sprintf(
         "step%d AS (SELECT * FROM %s WHERE properties::jsonb->>%s = %s)",
         ks->filter_count, prev_step(ks), esc_key, esc_val);
     PQfreemem(esc_key);
     PQfreemem(esc_val);
     return add_filter(ks, sql);
 }
 
 kb_error_t kb_search_path(kb_search_t *ks, const char *path_pattern) {
     if (!ks || !path_pattern) return KB_ERR_NULL_ARG;
     char *esc = kb_escape_literal(ks->conn, path_pattern);
     if (!esc) return KB_ERR_PG;
     /* ltree ~ lquery pattern match */
     char *sql = kb_sprintf(
         "step%d AS (SELECT * FROM %s WHERE path ~ %s::lquery)",
         ks->filter_count, prev_step(ks), esc);
     PQfreemem(esc);
     return add_filter(ks, sql);
 }
 
 /* ================================================================
  * Execute assembled CTE query
  * ================================================================ */
 
 kb_error_t kb_search_execute(kb_search_t *ks) {
     if (!ks) return KB_ERR_NULL_ARG;
     if (ks->last_result) {
         kb_resultset_free(ks->last_result);
         ks->last_result = NULL;
     }
 
     if (ks->filter_count == 0) {
         /* No filters — select all from base table */
         char *sql = kb_sprintf("SELECT * FROM %s", ks->database);
         kb_error_t err = kb_query(ks->conn, sql, NULL, 0, &ks->last_result);
         free(sql);
         return err;
     }
 
     /* Build WITH ... SELECT */
     /* Calculate total length */
     size_t total = 64; /* overhead */
     for (int i = 0; i < ks->filter_count; i++)
         total += strlen(ks->cte_parts[i]) + 4;
     total += 64; /* final SELECT */
 
     char *sql = malloc(total);
     if (!sql) return KB_ERR_NOMEM;
 
     strcpy(sql, "WITH ");
     for (int i = 0; i < ks->filter_count; i++) {
         if (i > 0) strcat(sql, ", ");
         strcat(sql, ks->cte_parts[i]);
     }
 
     char final_step[32];
     snprintf(final_step, sizeof(final_step), "step%d", ks->filter_count - 1);
     strcat(sql, " SELECT * FROM ");
     strcat(sql, final_step);
 
     kb_error_t err = kb_query(ks->conn, sql, NULL, 0, &ks->last_result);
     free(sql);
     return err;
 }
 
 const kb_resultset_t *kb_search_results(const kb_search_t *ks) {
     return ks ? ks->last_result : NULL;
 }
 
 /* ================================================================
  * Convenience: find paths
  * ================================================================ */
 
 kb_error_t kb_search_find_paths(kb_search_t *ks, const char *label,
                                 const char *name,
                                 char ***paths_out, int *count_out) {
     return kb_search_find_nodes(ks, label, name, NULL, 0, NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_search_find_description(kb_search_t *ks, const char *path,
                                       char **json_out) {
     if (!ks || !path || !json_out) return KB_ERR_NULL_ARG;
     *json_out = NULL;
 
     char *esc = kb_escape_literal(ks->conn, path);
     if (!esc) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT properties FROM %s WHERE path = %s::ltree",
         ks->database, esc);
     PQfreemem(esc);
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(ks->conn, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows > 0) {
         const char *v = kb_rs_get(rs, 0, "properties");
         if (v) *json_out = kb_strdup(v);
     }
     kb_resultset_free(rs);
     return *json_out ? KB_OK : KB_ERR_NOT_FOUND;
 }
 
 /* ================================================================
  * Generic node finder with multiple filters
  * ================================================================ */
 
 kb_error_t kb_search_find_nodes(kb_search_t *ks,
                                 const char *label,
                                 const char *name,
                                 const kb_prop_filter_t *props, int nprops,
                                 const char *path_pattern,
                                 char ***paths_out, int *count_out) {
     if (!ks || !paths_out || !count_out) return KB_ERR_NULL_ARG;
     *paths_out = NULL;
     *count_out = 0;
 
     kb_search_clear(ks);
 
     if (label) {
         kb_error_t e = kb_search_label(ks, label);
         if (e != KB_OK) return e;
     }
     if (name) {
         kb_error_t e = kb_search_name(ks, name);
         if (e != KB_OK) return e;
     }
     if (props) {
         for (int i = 0; i < nprops; i++) {
             kb_error_t e;
             if (props[i].value)
                 e = kb_search_property_value(ks, props[i].key, props[i].value);
             else
                 e = kb_search_property_key(ks, props[i].key);
             if (e != KB_OK) return e;
         }
     }
     if (path_pattern) {
         kb_error_t e = kb_search_path(ks, path_pattern);
         if (e != KB_OK) return e;
     }
 
     kb_error_t err = kb_search_execute(ks);
     if (err != KB_OK) return err;
 
     const kb_resultset_t *rs = kb_search_results(ks);
     if (!rs || rs->nrows == 0) return KB_OK;
 
     char **paths = calloc(rs->nrows, sizeof(char *));
     if (!paths) return KB_ERR_NOMEM;
 
     int count = 0;
     for (int i = 0; i < rs->nrows; i++) {
         const char *p = kb_rs_get(rs, i, "path");
         if (p) paths[count++] = kb_strdup(p);
     }
 
     *paths_out = paths;
     *count_out = count;
     return KB_OK;
 }
 
 /* ================================================================
  * Specialized finders by label
  * ================================================================ */
 
 kb_error_t kb_find_status_paths(kb_search_t *ks,
                                 char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_STATUS_FIELD", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_job_paths(kb_search_t *ks,
                              char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_JOB_QUEUE", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_stream_paths(kb_search_t *ks,
                                 char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_STREAM_FIELD", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_bit_structure_paths(kb_search_t *ks,
                                        char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_BIT_MASK", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_rpc_server_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_RPC_SERVER_FIELD", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_rpc_client_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_RPC_CLIENT_FIELD", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_document_paths(kb_search_t *ks,
                                   char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, "KB_JSONB_FIELD", NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_node_paths(kb_search_t *ks,
                               const char *label,
                               char ***paths_out, int *count_out) {
     return kb_search_find_paths(ks, label, NULL,
                                 paths_out, count_out);
 }
 
 kb_error_t kb_find_link_paths(kb_search_t *ks,
                               char ***paths_out, int *count_out) {
     kb_search_clear(ks);
     kb_search_label(ks, "KB_LINK");
     /* Actually, links are found via has_link = true */
     kb_search_clear(ks);
     /* Use direct SQL for has_link */
     char *sql = kb_sprintf(
         "SELECT DISTINCT path FROM %s WHERE has_link = true",
         ks->database);
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(ks->conn, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (!rs || rs->nrows == 0) {
         if (rs) kb_resultset_free(rs);
         *paths_out = NULL;
         *count_out = 0;
         return KB_OK;
     }
 
     char **paths = calloc(rs->nrows, sizeof(char *));
     int count = 0;
     for (int i = 0; i < rs->nrows; i++) {
         const char *p = kb_rs_get(rs, i, "path");
         if (p) paths[count++] = kb_strdup(p);
     }
     kb_resultset_free(rs);
 
     *paths_out = paths;
     *count_out = count;
     return KB_OK;
 }
 
 kb_error_t kb_find_link_mount_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out) {
     char *sql = kb_sprintf(
         "SELECT DISTINCT path FROM %s WHERE has_link_mount = true",
         ks->database);
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(ks->conn, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (!rs || rs->nrows == 0) {
         if (rs) kb_resultset_free(rs);
         *paths_out = NULL;
         *count_out = 0;
         return KB_OK;
     }
 
     char **paths = calloc(rs->nrows, sizeof(char *));
     int count = 0;
     for (int i = 0; i < rs->nrows; i++) {
         const char *p = kb_rs_get(rs, i, "path");
         if (p) paths[count++] = kb_strdup(p);
     }
     kb_resultset_free(rs);
 
     *paths_out = paths;
     *count_out = count;
     return KB_OK;
 }
 
 void kb_free_paths(char **paths, int count) {
     if (!paths) return;
     for (int i = 0; i < count; i++) free(paths[i]);
     free(paths);
 }