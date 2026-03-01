/*
 * kb_link_table.h
 * Knowledge Base C Library (PostgreSQL) — Link table + link mount queries
 *
 * Table: {database}_link
 * Columns: id, link_name, parent_node_kb, parent_path (ltree), created_at
 *
 * Table: {database}_link_mount
 * Columns: id, link_name, knowledge_base, mount_path (ltree), description, created_at
 *
 * Concurrent safety: All operations are read-only queries.
 * Link tables are populated at construction time and not modified at runtime.
 */

 #ifndef KB_LINK_TABLE_H
 #define KB_LINK_TABLE_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* Query link table by parent_path */
 kb_error_t kb_link_query(kb_conn_t *c, const char *database,
                          const char *path, kb_resultset_t **rs_out);
 
 /* Query link_mount table by mount_path */
 kb_error_t kb_link_mount_query(kb_conn_t *c, const char *database,
                                const char *path, kb_resultset_t **rs_out);
 
 /* Query link table by link_name */
 kb_error_t kb_link_query_by_name(kb_conn_t *c, const char *database,
                                  const char *link_name,
                                  kb_resultset_t **rs_out);
 
 /* Query link_mount table by link_name */
 kb_error_t kb_link_mount_query_by_name(kb_conn_t *c, const char *database,
                                        const char *link_name,
                                        kb_resultset_t **rs_out);
 
 /* Resolve link_name → mount_path for all links from a parent_path */
 kb_error_t kb_link_decode_nodes(kb_conn_t *c, const char *database,
                                 const char *path,
                                 char ***paths_out, int *count_out);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_LINK_TABLE_H */