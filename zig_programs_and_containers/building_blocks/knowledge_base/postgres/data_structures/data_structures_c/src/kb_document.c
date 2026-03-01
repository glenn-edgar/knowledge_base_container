/*
 * kb_document.c
 * Knowledge Base C Library (PostgreSQL) — JSONB document operations
 *
 * Mirrors LuaJIT kb_document_table.lua.
 * Table: {database}_document
 *
 * Concurrent write safety:
 *   All write operations lock the target row with FOR UPDATE before
 *   modifying, preventing lost updates from concurrent writers.
 *   Read operations are lock-free (tolerate slightly stale reads).
 *
 * Key design decisions matching the LuaJIT version:
 * - Uses jsonb_exists/jsonb_exists_any/jsonb_exists_all instead of ?/?|/?&
 * - SQL via string interpolation + PQescapeLiteral (no $1 params)
 * - Auto-rollback on error, commit on success
 * - dot-separated json_path → ARRAY['a','b'] for PostgreSQL path operators
 */

 #include "kb_document.h"
 #include <cjson/cJSON.h>
 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ================================================================
  * Internal helpers
  * ================================================================ */
 
 /* Build optional type filter clause: " AND type = 'foo'" or "" */
 static char *build_type_filter(kb_conn_t *c, const char *doc_type) {
     if (!doc_type || doc_type[0] == '\0') return kb_strdup("");
     char *esc = kb_escape_literal(c, doc_type);
     if (!esc) return kb_strdup("");
     char *tf = kb_sprintf(" AND type = %s", esc);
     PQfreemem(esc);
     return tf;
 }
 
 /*
  * Convert dot-separated path to PostgreSQL text array literal.
  * "address.city" → "ARRAY['address','city']"
  * Returns heap-allocated string. Caller must free.
  */
 static char *path_to_pg_array(kb_conn_t *c, const char *json_path) {
     if (!json_path || json_path[0] == '\0') return kb_strdup("'{}'");
 
     /* Count segments */
     int nseg = 1;
     for (const char *p = json_path; *p; p++)
         if (*p == '.') nseg++;
 
     /* Build ARRAY['a','b'] */
     size_t buflen = strlen(json_path) * 4 + 64;
     char *buf = malloc(buflen);
     if (!buf) return NULL;
     strcpy(buf, "ARRAY[");
 
     char *copy = kb_strdup(json_path);
     char *saveptr;
     char *tok = strtok_r(copy, ".", &saveptr);
     int first = 1;
     while (tok) {
         if (!first) strcat(buf, ",");
         first = 0;
         char *esc = kb_escape_literal(c, tok);
         if (esc) {
             strcat(buf, esc);
             PQfreemem(esc);
         }
         tok = strtok_r(NULL, ".", &saveptr);
     }
     free(copy);
     strcat(buf, "]::text[]");
     return buf;
 }
 
 /* Check if path is empty/root */
 static bool is_root_path(const char *json_path) {
     return (!json_path || json_path[0] == '\0' ||
             (json_path[0] == '{' && json_path[1] == '}'));
 }
 
 /* Helper to get a boolean result from a single-column query */
 static kb_error_t query_bool(kb_conn_t *c, const char *sql,
                              const char *col, bool *out) {
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     if (err != KB_OK) return err;
     if (rs->nrows == 0) {
         kb_resultset_free(rs);
         *out = false;
         return KB_OK;
     }
     *out = kb_rs_get_bool(rs, 0, col);
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 /*
  * Lock a document row FOR UPDATE before writing.
  * Returns the row ID if found, or -1 if not found.
  * The lock is held until COMMIT or ROLLBACK.
  */
 static int doc_lock_row(kb_conn_t *c, const char *database,
                         const char *esc_ltree, const char *tf) {
     char *sql = kb_sprintf(
         "SELECT id FROM %s_document "
         "WHERE ltree = %s::ltree%s FOR UPDATE",
         database, esc_ltree, tf);
     if (!sql) return -1;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK || !rs || rs->nrows == 0) {
         if (rs) kb_resultset_free(rs);
         return -1;
     }
 
     int id = kb_rs_get_int(rs, 0, "id");
     kb_resultset_free(rs);
     return id;
 }
 
 /* Build ARRAY['a','b'] from C string array */
 static char *keys_to_pg_array(kb_conn_t *c, const char **keys, int nkeys) {
     size_t buflen = 64;
     for (int i = 0; i < nkeys; i++) buflen += strlen(keys[i]) * 3 + 8;
     char *buf = malloc(buflen);
     if (!buf) return NULL;
     strcpy(buf, "ARRAY[");
     for (int i = 0; i < nkeys; i++) {
         if (i > 0) strcat(buf, ",");
         char *esc = kb_escape_literal(c, keys[i]);
         if (esc) { strcat(buf, esc); PQfreemem(esc); }
     }
     strcat(buf, "]::text[]");
     return buf;
 }
 
 /* ================================================================
  * Core JSONB Read (no lock, tolerates stale reads)
  * ================================================================ */
 
 kb_error_t kb_doc_get(kb_conn_t *c, const char *database,
                       const char *ltree_path, const char *json_path,
                       bool as_text, const char *doc_type,
                       char **value_out) {
     if (!c || !database || !ltree_path || !value_out)
         return KB_ERR_NULL_ARG;
     *value_out = NULL;
 
     char *esc_path = kb_escape_literal(c, ltree_path);
     char *tf = build_type_filter(c, doc_type);
     if (!esc_path || !tf) {
         if (esc_path) PQfreemem(esc_path);
         free(tf);
         return KB_ERR_PG;
     }
 
     char *sql;
     if (is_root_path(json_path)) {
         sql = kb_sprintf("SELECT data FROM %s_document WHERE ltree = %s::ltree%s",
                          database, esc_path, tf);
     } else {
         char *arr = path_to_pg_array(c, json_path);
         if (!arr) { PQfreemem(esc_path); free(tf); return KB_ERR_NOMEM; }
 
         int nseg = 1;
         for (const char *p = json_path; *p; p++)
             if (*p == '.') nseg++;
 
         if (nseg == 1) {
             char *esc_key = kb_escape_literal(c, json_path);
             const char *op = as_text ? "->>" : "->";
             sql = kb_sprintf("SELECT data %s %s as val FROM %s_document "
                              "WHERE ltree = %s::ltree%s",
                              op, esc_key, database, esc_path, tf);
             PQfreemem(esc_key);
         } else {
             const char *op = as_text ? "#>>" : "#>";
             sql = kb_sprintf("SELECT data %s %s as val FROM %s_document "
                              "WHERE ltree = %s::ltree%s",
                              op, arr, database, esc_path, tf);
         }
         free(arr);
     }
     PQfreemem(esc_path);
     free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs->nrows == 0) {
         kb_resultset_free(rs);
         return KB_ERR_NOT_FOUND;
     }
 
     const char *col = is_root_path(json_path) ? "data" : "val";
     const char *v = kb_rs_get(rs, 0, col);
     if (v) *value_out = kb_strdup(v);
     kb_resultset_free(rs);
     return *value_out ? KB_OK : KB_ERR_NOT_FOUND;
 }
 
 /* ================================================================
  * Core JSONB Write (all writes lock the row first)
  * ================================================================ */
 
 kb_error_t kb_doc_set(kb_conn_t *c, const char *database,
                       const char *ltree_path, const char *json_path,
                       const char *value_json, bool create_missing,
                       const char *doc_type) {
     if (!c || !database || !ltree_path || !value_json)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_val = kb_escape_literal(c, value_json);
     char *tf = build_type_filter(c, doc_type);
     if (!esc_ltree || !esc_val || !tf) {
         if (esc_ltree) PQfreemem(esc_ltree);
         if (esc_val) PQfreemem(esc_val);
         free(tf);
         return KB_ERR_PG;
     }
 
     /* Lock the row before writing */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); PQfreemem(esc_val); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     char *sql;
     if (is_root_path(json_path)) {
         sql = kb_sprintf(
             "UPDATE %s_document SET data = %s::jsonb, "
             "updated_at = CURRENT_TIMESTAMP "
             "WHERE ltree = %s::ltree%s",
             database, esc_val, esc_ltree, tf);
     } else {
         char *arr = path_to_pg_array(c, json_path);
         if (!arr) { PQfreemem(esc_ltree); PQfreemem(esc_val); free(tf); return KB_ERR_NOMEM; }
         sql = kb_sprintf(
             "UPDATE %s_document SET data = jsonb_set("
             "data, %s, %s::jsonb, %s), "
             "updated_at = CURRENT_TIMESTAMP "
             "WHERE ltree = %s::ltree%s",
             database, arr, esc_val,
             create_missing ? "true" : "false",
             esc_ltree, tf);
         free(arr);
     }
     PQfreemem(esc_ltree);
     PQfreemem(esc_val);
     free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_doc_delete_key(kb_conn_t *c, const char *database,
                              const char *ltree_path, const char *key,
                              const char *doc_type) {
     if (!c || !database || !ltree_path || !key) return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_key = kb_escape_literal(c, key);
     char *tf = build_type_filter(c, doc_type);
 
     /* Lock before write */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); PQfreemem(esc_key); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_document SET data = data - %s, "
         "updated_at = CURRENT_TIMESTAMP "
         "WHERE ltree = %s::ltree%s",
         database, esc_key, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_key); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_doc_delete_path(kb_conn_t *c, const char *database,
                               const char *ltree_path, const char *json_path,
                               const char *doc_type) {
     if (!c || !database || !ltree_path || !json_path) return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     /* Lock before write */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); free(arr); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_document SET data = data #- %s, "
         "updated_at = CURRENT_TIMESTAMP "
         "WHERE ltree = %s::ltree%s",
         database, arr, esc_ltree, tf);
     PQfreemem(esc_ltree); free(arr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 /* ================================================================
  * Key Existence (read-only, no lock)
  * ================================================================ */
 
 kb_error_t kb_doc_has_key(kb_conn_t *c, const char *database,
                           const char *ltree_path, const char *key,
                           const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !key || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_key = kb_escape_literal(c, key);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_exists(data, %s) as has_key "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         esc_key, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_key); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "has_key", result_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_doc_has_any_keys(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char **keys, int nkeys,
                                const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !keys || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *karr = keys_to_pg_array(c, keys, nkeys);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_exists_any(data, %s) as has_any "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         karr, database, esc_ltree, tf);
     PQfreemem(esc_ltree); free(karr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "has_any", result_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_doc_has_all_keys(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char **keys, int nkeys,
                                const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !keys || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *karr = keys_to_pg_array(c, keys, nkeys);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_exists_all(data, %s) as has_all "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         karr, database, esc_ltree, tf);
     PQfreemem(esc_ltree); free(karr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "has_all", result_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Containment (read-only, no lock)
  * ================================================================ */
 
 kb_error_t kb_doc_contains(kb_conn_t *c, const char *database,
                            const char *ltree_path,
                            const char *contained_json,
                            const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !contained_json || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_json = kb_escape_literal(c, contained_json);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT data @> %s::jsonb as contains "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         esc_json, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_json); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "contains", result_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_doc_contained_by(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *container_json,
                                const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !container_json || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_json = kb_escape_literal(c, container_json);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT data <@ %s::jsonb as contained "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         esc_json, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_json); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "contained", result_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * JSONPath Operations (read-only, no lock)
  * ================================================================ */
 
 kb_error_t kb_doc_path_exists(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *jsonpath_query,
                               const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !jsonpath_query || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_jp = kb_escape_literal(c, jsonpath_query);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_path_exists(data, %s::jsonpath) as path_exists "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         esc_jp, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_jp); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "path_exists", result_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_doc_path_query(kb_conn_t *c, const char *database,
                              const char *ltree_path,
                              const char *jsonpath_query,
                              const char *doc_type, char **json_out) {
     if (!c || !database || !ltree_path || !jsonpath_query || !json_out)
         return KB_ERR_NULL_ARG;
     *json_out = NULL;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_jp = kb_escape_literal(c, jsonpath_query);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_path_query_array(data, %s::jsonpath) as results "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         esc_jp, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_jp); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) return err;
 
     if (rs && rs->nrows > 0) {
         const char *v = kb_rs_get(rs, 0, "results");
         if (v) *json_out = kb_strdup(v);
     }
     kb_resultset_free(rs);
     return *json_out ? KB_OK : KB_ERR_NOT_FOUND;
 }
 
 kb_error_t kb_doc_query(kb_conn_t *c, const char *database,
                         const char *ltree_path,
                         const char *jsonb_filter,
                         const char *doc_type,
                         kb_resultset_t **rs_out) {
     if (!c || !database || !ltree_path || !jsonb_filter || !rs_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_filter = kb_escape_literal(c, jsonb_filter);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT id, ltree::text as ltree, type, data "
         "FROM %s_document "
         "WHERE ltree = %s::ltree AND data @> %s::jsonb%s",
         database, esc_ltree, esc_filter, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_filter); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Array Operations (writes lock the row first)
  * ================================================================ */
 
 kb_error_t kb_doc_array_append(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *json_path,
                                const char *item_json,
                                const char *doc_type) {
     if (!c || !database || !ltree_path || !json_path || !item_json)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_item = kb_escape_literal(c, item_json);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     /* Lock before write */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); PQfreemem(esc_item); free(arr); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_document SET data = jsonb_set("
         "data, %s, COALESCE(data #> %s, '[]'::jsonb) || %s::jsonb, true"
         "), updated_at = CURRENT_TIMESTAMP "
         "WHERE ltree = %s::ltree%s",
         database, arr, arr, esc_item, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_item); free(arr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_doc_array_prepend(kb_conn_t *c, const char *database,
                                 const char *ltree_path,
                                 const char *json_path,
                                 const char *item_json,
                                 const char *doc_type) {
     if (!c || !database || !ltree_path || !json_path || !item_json)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_item = kb_escape_literal(c, item_json);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     /* Lock before write */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); PQfreemem(esc_item); free(arr); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     char *sql = kb_sprintf(
         "UPDATE %s_document SET data = jsonb_set("
         "data, %s, %s::jsonb || COALESCE(data #> %s, '[]'::jsonb), true"
         "), updated_at = CURRENT_TIMESTAMP "
         "WHERE ltree = %s::ltree%s",
         database, arr, esc_item, arr, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_item); free(arr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_doc_array_remove_index(kb_conn_t *c, const char *database,
                                      const char *ltree_path,
                                      const char *json_path, int index,
                                      const char *doc_type,
                                      char **removed_out) {
     if (!c || !database || !ltree_path || !json_path)
         return KB_ERR_NULL_ARG;
     if (removed_out) *removed_out = NULL;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     /* Lock before read+write (atomic remove) */
     if (doc_lock_row(c, database, esc_ltree, tf) < 0) {
         PQfreemem(esc_ltree); free(arr); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     /* Step 1: Get the item being removed (row is locked, safe to read) */
     char *sel_sql = kb_sprintf(
         "SELECT (data #> %s) -> %d as item "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         arr, index, database, esc_ltree, tf);
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sel_sql, NULL, 0, &rs);
     free(sel_sql);
     if (err != KB_OK) { PQfreemem(esc_ltree); free(arr); free(tf); return err; }
 
     char *removed = NULL;
     if (rs && rs->nrows > 0) {
         const char *v = kb_rs_get(rs, 0, "item");
         if (v) removed = kb_strdup(v);
     }
     kb_resultset_free(rs);
 
     if (!removed) {
         PQfreemem(esc_ltree); free(arr); free(tf);
         return KB_ERR_NOT_FOUND;
     }
 
     /* Step 2: Remove the element (still holding FOR UPDATE lock) */
     char *upd_sql = kb_sprintf(
         "UPDATE %s_document SET data = jsonb_set("
         "data, %s, (data #> %s) - %d, true"
         "), updated_at = CURRENT_TIMESTAMP "
         "WHERE ltree = %s::ltree%s",
         database, arr, arr, index, esc_ltree, tf);
     PQfreemem(esc_ltree); free(arr); free(tf);
     if (!upd_sql) { free(removed); return KB_ERR_NOMEM; }
 
     err = kb_exec(c, upd_sql, NULL, 0, NULL);
     free(upd_sql);
     if (err == KB_OK) err = kb_commit(c);
 
     if (err == KB_OK && removed_out)
         *removed_out = removed;
     else
         free(removed);
     return err;
 }
 
 kb_error_t kb_doc_array_contains(kb_conn_t *c, const char *database,
                                  const char *ltree_path,
                                  const char *json_path,
                                  const char *item_json,
                                  const char *doc_type, bool *result_out) {
     if (!c || !database || !ltree_path || !json_path || !item_json || !result_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *esc_item = kb_escape_literal(c, item_json);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT (data #> %s) @> ('[' || %s || ']')::jsonb as contains "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         arr, esc_item, database, esc_ltree, tf);
     PQfreemem(esc_ltree); PQfreemem(esc_item); free(arr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = query_bool(c, sql, "contains", result_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_doc_array_elements(kb_conn_t *c, const char *database,
                                  const char *ltree_path,
                                  const char *json_path,
                                  const char *doc_type,
                                  kb_resultset_t **rs_out) {
     if (!c || !database || !ltree_path || !json_path || !rs_out)
         return KB_ERR_NULL_ARG;
 
     char *esc_ltree = kb_escape_literal(c, ltree_path);
     char *arr = path_to_pg_array(c, json_path);
     char *tf = build_type_filter(c, doc_type);
 
     char *sql = kb_sprintf(
         "SELECT jsonb_array_elements(data #> %s) as element "
         "FROM %s_document WHERE ltree = %s::ltree%s",
         arr, database, esc_ltree, tf);
     PQfreemem(esc_ltree); free(arr); free(tf);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 /* ================================================================
  * Queue (FIFO) / Stack (LIFO) — built on array operations
  * (Write ops inherit locking from the array functions above)
  * ================================================================ */
 
 static const char *default_queue_path(const char *qp) {
     return (qp && qp[0]) ? qp : "items";
 }
 
 kb_error_t kb_doc_enqueue(kb_conn_t *c, const char *database,
                           const char *ltree_path, const char *item_json,
                           const char *queue_path, const char *doc_type) {
     return kb_doc_array_append(c, database, ltree_path,
                                default_queue_path(queue_path),
                                item_json, doc_type);
 }
 
 kb_error_t kb_doc_dequeue(kb_conn_t *c, const char *database,
                           const char *ltree_path,
                           const char *queue_path, const char *doc_type,
                           char **item_out) {
     return kb_doc_array_remove_index(c, database, ltree_path,
                                      default_queue_path(queue_path),
                                      0, doc_type, item_out);
 }
 
 kb_error_t kb_doc_peek(kb_conn_t *c, const char *database,
                        const char *ltree_path,
                        const char *queue_path, int index,
                        const char *doc_type, char **item_out) {
     if (!item_out) return KB_ERR_NULL_ARG;
     *item_out = NULL;
 
     char *queue_json = NULL;
     kb_error_t err = kb_doc_get(c, database, ltree_path,
                                 default_queue_path(queue_path),
                                 false, doc_type, &queue_json);
     if (err != KB_OK) return err;
 
     cJSON *arr = cJSON_Parse(queue_json);
     free(queue_json);
     if (!arr || !cJSON_IsArray(arr)) {
         if (arr) cJSON_Delete(arr);
         return KB_ERR_NOT_FOUND;
     }
 
     int sz = cJSON_GetArraySize(arr);
     if (index < 0 || index >= sz) {
         cJSON_Delete(arr);
         return KB_ERR_NOT_FOUND;
     }
 
     cJSON *item = cJSON_GetArrayItem(arr, index);
     if (item) {
         char *s = cJSON_PrintUnformatted(item);
         *item_out = s;
     }
     cJSON_Delete(arr);
     return *item_out ? KB_OK : KB_ERR_NOT_FOUND;
 }
 
 kb_error_t kb_doc_queue_size(kb_conn_t *c, const char *database,
                              const char *ltree_path,
                              const char *queue_path, const char *doc_type,
                              int *size_out) {
     if (!size_out) return KB_ERR_NULL_ARG;
     *size_out = 0;
 
     char *queue_json = NULL;
     kb_error_t err = kb_doc_get(c, database, ltree_path,
                                 default_queue_path(queue_path),
                                 false, doc_type, &queue_json);
     if (err != KB_OK) return (err == KB_ERR_NOT_FOUND) ? KB_OK : err;
 
     cJSON *arr = cJSON_Parse(queue_json);
     free(queue_json);
     if (arr && cJSON_IsArray(arr))
         *size_out = cJSON_GetArraySize(arr);
     if (arr) cJSON_Delete(arr);
     return KB_OK;
 }
 
 kb_error_t kb_doc_queue_is_empty(kb_conn_t *c, const char *database,
                                  const char *ltree_path,
                                  const char *queue_path, const char *doc_type,
                                  bool *empty_out) {
     if (!empty_out) return KB_ERR_NULL_ARG;
     int sz = 0;
     kb_error_t err = kb_doc_queue_size(c, database, ltree_path,
                                        queue_path, doc_type, &sz);
     *empty_out = (sz == 0);
     return err;
 }
 
 kb_error_t kb_doc_queue_clear(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *queue_path, const char *doc_type) {
     /* Uses kb_doc_set which locks the row */
     return kb_doc_set(c, database, ltree_path,
                       default_queue_path(queue_path),
                       "[]", true, doc_type);
 }
 
 kb_error_t kb_doc_queue_get_all(kb_conn_t *c, const char *database,
                                 const char *ltree_path,
                                 const char *queue_path, const char *doc_type,
                                 char **json_out) {
     return kb_doc_get(c, database, ltree_path,
                       default_queue_path(queue_path),
                       false, doc_type, json_out);
 }
 
 /* Stack: push = prepend (to index 0), pop = remove from index 0 */
 kb_error_t kb_doc_push(kb_conn_t *c, const char *database,
                        const char *ltree_path, const char *item_json,
                        const char *queue_path, const char *doc_type) {
     return kb_doc_array_prepend(c, database, ltree_path,
                                 default_queue_path(queue_path),
                                 item_json, doc_type);
 }
 
 kb_error_t kb_doc_pop(kb_conn_t *c, const char *database,
                       const char *ltree_path,
                       const char *queue_path, const char *doc_type,
                       char **item_out) {
     /* LIFO: push prepends to index 0, pop removes from index 0 */
     return kb_doc_array_remove_index(c, database, ltree_path,
                                      default_queue_path(queue_path),
                                      0, doc_type, item_out);
 }
 
 /* Metadata */
 kb_error_t kb_doc_get_metadata(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *metadata_path,
                                const char *doc_type, char **json_out) {
     const char *mp = (metadata_path && metadata_path[0]) ? metadata_path : "metadata";
     return kb_doc_get(c, database, ltree_path, mp, false, doc_type, json_out);
 }
 
 kb_error_t kb_doc_set_metadata(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *metadata_path,
                                const char *metadata_json,
                                const char *doc_type) {
     /* Uses kb_doc_set which locks the row */
     const char *mp = (metadata_path && metadata_path[0]) ? metadata_path : "metadata";
     return kb_doc_set(c, database, ltree_path, mp, metadata_json, true, doc_type);
 }