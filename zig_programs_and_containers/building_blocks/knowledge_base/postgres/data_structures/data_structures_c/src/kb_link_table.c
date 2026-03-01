/*
 * kb_link_table.c
 * Knowledge Base C Library (PostgreSQL) — Link table queries
 *
 * Table: {database}_link
 * Columns: id, link_name (varchar), parent_node_kb (varchar),
 *          parent_path (ltree), created_at
 *
 * Table: {database}_link_mount
 * Columns: id, link_name (varchar), knowledge_base (varchar),
 *          mount_path (ltree), description (varchar), created_at
 *
 * Concurrent safety: All operations are read-only queries.
 * Link tables are populated at construction time and not modified at runtime.
 */

 #include "kb_link_table.h"
 #include <stdlib.h>
 #include <string.h>
 
 kb_error_t kb_link_query(kb_conn_t *c, const char *database,
                          const char *path, kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT * FROM %s_link WHERE parent_path = %s::ltree",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_link_mount_query(kb_conn_t *c, const char *database,
                                const char *path, kb_resultset_t **rs_out) {
     if (!c || !database || !path || !rs_out) return KB_ERR_NULL_ARG;
 
     char *esc_path = kb_escape_literal(c, path);
     if (!esc_path) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT * FROM %s_link_mount WHERE mount_path = %s::ltree",
         database, esc_path);
     PQfreemem(esc_path);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_link_query_by_name(kb_conn_t *c, const char *database,
                                  const char *link_name,
                                  kb_resultset_t **rs_out) {
     if (!c || !database || !link_name || !rs_out) return KB_ERR_NULL_ARG;
 
     char *esc_name = kb_escape_literal(c, link_name);
     if (!esc_name) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT * FROM %s_link WHERE link_name = %s",
         database, esc_name);
     PQfreemem(esc_name);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_link_mount_query_by_name(kb_conn_t *c, const char *database,
                                        const char *link_name,
                                        kb_resultset_t **rs_out) {
     if (!c || !database || !link_name || !rs_out) return KB_ERR_NULL_ARG;
 
     char *esc_name = kb_escape_literal(c, link_name);
     if (!esc_name) return KB_ERR_PG;
 
     char *sql = kb_sprintf(
         "SELECT * FROM %s_link_mount WHERE link_name = %s",
         database, esc_name);
     PQfreemem(esc_name);
     if (!sql) return KB_ERR_NOMEM;
 
     kb_error_t err = kb_query(c, sql, NULL, 0, rs_out);
     free(sql);
     return err;
 }
 
 kb_error_t kb_link_decode_nodes(kb_conn_t *c, const char *database,
                                 const char *path,
                                 char ***paths_out, int *count_out) {
     if (!c || !database || !path || !paths_out || !count_out)
         return KB_ERR_NULL_ARG;
     *paths_out = NULL;
     *count_out = 0;
 
     /* Get link entries for this parent_path */
     kb_resultset_t *rs = NULL;
     kb_error_t err = kb_link_query(c, database, path, &rs);
     if (err != KB_OK) return err;
 
     if (rs->nrows == 0) {
         kb_resultset_free(rs);
         return KB_OK;
     }
 
     /*
      * Each link row has a link_name.
      * Look up the link_mount table to find mount_path for each link_name.
      */
     char **paths = calloc(rs->nrows, sizeof(char *));
     int count = 0;
     for (int i = 0; i < rs->nrows; i++) {
         const char *lname = kb_rs_get(rs, i, "link_name");
         if (!lname) continue;
 
         kb_resultset_t *mount_rs = NULL;
         err = kb_link_mount_query_by_name(c, database, lname, &mount_rs);
         if (err == KB_OK && mount_rs && mount_rs->nrows > 0) {
             const char *mp = kb_rs_get(mount_rs, 0, "mount_path");
             if (mp) paths[count++] = kb_strdup(mp);
         }
         if (mount_rs) kb_resultset_free(mount_rs);
     }
     kb_resultset_free(rs);
 
     *paths_out = paths;
     *count_out = count;
     return KB_OK;
 }