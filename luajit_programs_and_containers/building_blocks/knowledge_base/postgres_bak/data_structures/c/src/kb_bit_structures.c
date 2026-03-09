/*
 * kb_bit_structures.c
 * Knowledge Base C Library (PostgreSQL) — Bit mask ops + S-expression evaluator
 *
 * Table: bit_mask_table (global)
 * Columns: node_id (varchar PK), bit_mask (bigint)
 *
 * Concurrent write safety:
 *   - set_mask: locks row with FOR UPDATE, then updates, with retry
 *   - set (single bit): atomic read-modify-write under FOR UPDATE + retry
 *   - reads: no lock (tolerate stale)
 */

 #include "kb_bit_structures.h"
 #include "kb_search.h"
 #include <cjson/cJSON.h>
 #include <stdlib.h>
 #include <string.h>
 #include <math.h>
 #include <stdio.h>
 
 /* 64-bit bit helper */
 static int64_t bit_mask_for(int pos) {
     if (pos < 0 || pos > 63) return 0;
     return (int64_t)1 << pos;
 }
 
 /* ================================================================
  * Low-level mask operations
  * ================================================================ */
 
 kb_error_t kb_bit_get_mask(kb_conn_t *c, const char *database,
                            const char *path, int64_t *mask_out) {
     (void)database;
     if (!c || !path || !mask_out) return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT bit_mask as mask FROM bit_mask_table WHERE node_id = %s",
         esc_path);
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
 
     *mask_out = kb_rs_get_int64(rs, 0, "mask");
     kb_resultset_free(rs);
     return KB_OK;
 }
 
 /* --- set_mask with FOR UPDATE + retry --- */
 
 typedef struct {
     const char *path;
     int64_t     mask;
 } bit_set_mask_ctx_t;
 
 static kb_error_t bit_set_mask_fn(kb_conn_t *c, void *ctx) {
     bit_set_mask_ctx_t *bc = ctx;
 
     char *esc_path = kb_escape_literal(c, bc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock row first */
     char *lock_sql = kb_sprintf(
         "SELECT node_id FROM bit_mask_table WHERE node_id = %s FOR UPDATE",
         esc_path);
     if (!lock_sql) { PQfreemem(esc_path); return KB_ERR_NOMEM; }
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, lock_sql, NULL, 0, &rs);
     free(lock_sql);
     if (err != KB_OK) { PQfreemem(esc_path); return err; }
     if (!rs || rs->nrows == 0) {
         if (rs) kb_resultset_free(rs);
         PQfreemem(esc_path);
         return KB_ERR_NOT_FOUND;
     }
     kb_resultset_free(rs);
 
     /* Update with lock held */
     char mask_str[32];
     snprintf(mask_str, sizeof(mask_str), "%ld", (long)bc->mask);
 
     char *sql = kb_sprintf(
         "UPDATE bit_mask_table SET bit_mask = %s WHERE node_id = %s",
         mask_str, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     err = kb_exec(c, sql, NULL, 0, NULL);
     free(sql);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_bit_set_mask(kb_conn_t *c, const char *database,
                            const char *path, int64_t mask,
                            int max_retries, int base_delay_ms) {
     (void)database;
     if (!c || !path) return KB_ERR_NULL_ARG;
     bit_set_mask_ctx_t ctx = { path, mask };
     return kb_retry(c, bit_set_mask_fn, &ctx, max_retries, base_delay_ms);
 }
 
 /* --- set single bit: atomic read-modify-write under FOR UPDATE + retry --- */
 
 typedef struct {
     const char *path;
     int         bit_pos;
     bool        value;
 } bit_set_ctx_t;
 
 static kb_error_t bit_set_fn(kb_conn_t *c, void *ctx) {
     bit_set_ctx_t *bc = ctx;
 
     char *esc_path = kb_escape_literal(c, bc->path);
     if (!esc_path) return KB_ERR_PG;
 
     /* Lock and read current mask in one query */
     char *sql = kb_sprintf(
         "SELECT bit_mask as mask FROM bit_mask_table "
         "WHERE node_id = %s FOR UPDATE",
         esc_path);
     if (!sql) { PQfreemem(esc_path); return KB_ERR_NOMEM; }
 
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_query(c, sql, NULL, 0, &rs);
     free(sql);
     if (err != KB_OK) { PQfreemem(esc_path); return err; }
     if (!rs || rs->nrows == 0) {
         if (rs) kb_resultset_free(rs);
         PQfreemem(esc_path);
         return KB_ERR_NOT_FOUND;
     }
 
     int64_t mask = kb_rs_get_int64(rs, 0, "mask");
     kb_resultset_free(rs);
 
     /* Modify bit */
     int64_t bm = bit_mask_for(bc->bit_pos);
     if (bc->value)
         mask |= bm;
     else
         mask &= ~bm;
 
     /* Write back (still holding FOR UPDATE lock) */
     char mask_str[32];
     snprintf(mask_str, sizeof(mask_str), "%ld", (long)mask);
 
     char *upd = kb_sprintf(
         "UPDATE bit_mask_table SET bit_mask = %s WHERE node_id = %s",
         mask_str, esc_path);
     PQfreemem(esc_path);
     if (!upd) return KB_ERR_NOMEM;
 
     err = kb_exec(c, upd, NULL, 0, NULL);
     free(upd);
     if (err == KB_OK) err = kb_commit(c);
     return err;
 }
 
 kb_error_t kb_bit_set(kb_conn_t *c, const char *database,
                       const char *path, int bit_pos, bool value,
                       int max_retries, int base_delay_ms) {
     (void)database;
     if (bit_pos < 0 || bit_pos > 63) return KB_ERR_INVALID;
     if (!c || !path) return KB_ERR_NULL_ARG;
     bit_set_ctx_t ctx = { path, bit_pos, value };
     return kb_retry(c, bit_set_fn, &ctx, max_retries, base_delay_ms);
 }
 
 kb_error_t kb_bit_get(kb_conn_t *c, const char *database,
                       const char *path, int bit_pos, bool *value_out) {
     if (!value_out || bit_pos < 0 || bit_pos > 63) return KB_ERR_INVALID;
 
     int64_t mask = 0;
     kb_error_t err = kb_bit_get_mask(c, database, path, &mask);
     if (err != KB_OK) return err;
 
     *value_out = (mask & bit_mask_for(bit_pos)) != 0;
     return KB_OK;
 }
 
 /* ================================================================
  * Assemble bit definitions from KB properties
  * ================================================================ */
 
 kb_error_t kb_bit_assemble_data(kb_search_t *ks, const char *path,
                                 kb_bit_defs_t **defs_out) {
     if (!ks || !path || !defs_out) return KB_ERR_NULL_ARG;
     *defs_out = NULL;
 
     char *json = NULL;
     kb_error_t err = kb_search_find_description(ks, path, &json);
     if (err != KB_OK) return err;
 
     cJSON *root = cJSON_Parse(json);
     free(json);
     if (!root) return KB_ERR_JSON;
 
     cJSON *bits = cJSON_GetObjectItem(root, "bits");
     if (!bits || !cJSON_IsArray(bits)) {
         cJSON_Delete(root);
         return KB_ERR_JSON;
     }
 
     int count = cJSON_GetArraySize(bits);
     kb_bit_defs_t *defs = calloc(1, sizeof(kb_bit_defs_t));
     if (!defs) { cJSON_Delete(root); return KB_ERR_NOMEM; }
 
     defs->defs = calloc(count, sizeof(kb_bit_def_t));
     defs->count = count;
 
     for (int i = 0; i < count; i++) {
         cJSON *item = cJSON_GetArrayItem(bits, i);
         cJSON *name_j = cJSON_GetObjectItem(item, "name");
         cJSON *bit_j = cJSON_GetObjectItem(item, "bit");
         if (name_j && cJSON_IsString(name_j))
             defs->defs[i].name = kb_strdup(name_j->valuestring);
         if (bit_j && cJSON_IsNumber(bit_j))
             defs->defs[i].bit_position = bit_j->valueint;
     }
 
     cJSON_Delete(root);
     *defs_out = defs;
     return KB_OK;
 }
 
 void kb_bit_defs_free(kb_bit_defs_t *defs) {
     if (!defs) return;
     for (int i = 0; i < defs->count; i++)
         free(defs->defs[i].name);
     free(defs->defs);
     free(defs);
 }
 
 /* ================================================================
  * S-expression evaluator
  * ================================================================ */
 
 static int find_bit_by_name(const kb_bit_defs_t *defs, const char *name) {
     if (!defs || !name) return -1;
     for (int i = 0; i < defs->count; i++) {
         if (defs->defs[i].name && strcmp(defs->defs[i].name, name) == 0)
             return defs->defs[i].bit_position;
     }
     char *end;
     long val = strtol(name, &end, 10);
     if (*end == '\0' && val >= 0 && val <= 63) return (int)val;
     return -1;
 }
 
 static bool eval_node(cJSON *node, int64_t mask, int64_t prev_mask,
                       const kb_bit_defs_t *defs) {
     if (!node || !cJSON_IsArray(node)) return false;
 
     int size = cJSON_GetArraySize(node);
     if (size < 1) return false;
 
     cJSON *op = cJSON_GetArrayItem(node, 0);
     if (!op || !cJSON_IsString(op)) return false;
     const char *op_str = op->valuestring;
 
     if (strcmp(op_str, "bit") == 0) {
         if (size < 2) return false;
         cJSON *arg = cJSON_GetArrayItem(node, 1);
         int bit_pos = -1;
         if (cJSON_IsNumber(arg))
             bit_pos = arg->valueint;
         else if (cJSON_IsString(arg))
             bit_pos = find_bit_by_name(defs, arg->valuestring);
         if (bit_pos < 0 || bit_pos > 63) return false;
         return (mask & bit_mask_for(bit_pos)) != 0;
     }
 
     if (strcmp(op_str, "bit_changed") == 0) {
         if (size < 2) return false;
         cJSON *arg = cJSON_GetArrayItem(node, 1);
         int bit_pos = -1;
         if (cJSON_IsNumber(arg))
             bit_pos = arg->valueint;
         else if (cJSON_IsString(arg))
             bit_pos = find_bit_by_name(defs, arg->valuestring);
         if (bit_pos < 0 || bit_pos > 63) return false;
         int64_t bm = bit_mask_for(bit_pos);
         return (mask & bm) != (prev_mask & bm);
     }
 
     if (strcmp(op_str, "and") == 0) {
         for (int i = 1; i < size; i++) {
             if (!eval_node(cJSON_GetArrayItem(node, i), mask, prev_mask, defs))
                 return false;
         }
         return true;
     }
 
     if (strcmp(op_str, "or") == 0) {
         for (int i = 1; i < size; i++) {
             if (eval_node(cJSON_GetArrayItem(node, i), mask, prev_mask, defs))
                 return true;
         }
         return false;
     }
 
     if (strcmp(op_str, "not") == 0) {
         if (size < 2) return false;
         return !eval_node(cJSON_GetArrayItem(node, 1), mask, prev_mask, defs);
     }
 
     return false;
 }
 
 kb_error_t kb_bit_eval_sexpr(kb_conn_t *c, const char *database,
                              const char *path,
                              const char *sexpr_json,
                              const kb_bit_defs_t *defs,
                              int64_t prev_mask,
                              bool *result_out) {
     if (!c || !database || !path || !sexpr_json || !result_out)
         return KB_ERR_NULL_ARG;
 
     int64_t mask = 0;
     kb_error_t err = kb_bit_get_mask(c, database, path, &mask);
     if (err != KB_OK) return err;
 
     cJSON *root = cJSON_Parse(sexpr_json);
     if (!root) return KB_ERR_JSON;
 
     *result_out = eval_node(root, mask, prev_mask, defs);
     cJSON_Delete(root);
     return KB_OK;
 }