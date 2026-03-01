/*
 * kb_search.h
 * Knowledge Base C Library (PostgreSQL) — CTE-based progressive query builder
 *
 * Mirrors LuaJIT kb_search.lua. Provides:
 * - Progressive CTE filter chain (label, name, property key/value, path)
 * - Node discovery: find paths, descriptions, property values
 * - Specialized finders: status nodes, job nodes, stream nodes,
 *   bit structure nodes, RPC server/client nodes, link/mount nodes
 *
 * All SQL uses string interpolation with PQescapeLiteral (not $1 params)
 * because ltree operators are incompatible with prepared statements.
 */

 #ifndef KB_SEARCH_H
 #define KB_SEARCH_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* Maximum CTE filter stages */
 #define KB_MAX_FILTERS 16
 
 typedef struct {
     kb_conn_t      *conn;
     char           *database;      /* knowledge base name (table prefix) */
     int             filter_count;
     char           *cte_parts[KB_MAX_FILTERS]; /* SQL CTE fragments */
     kb_resultset_t *last_result;               /* owned, freed on next query */
 } kb_search_t;
 
 /*
  * Create a search context.
  * database: knowledge base name (e.g. "knowledge_base")
  */
 kb_error_t kb_search_create(kb_conn_t *conn, const char *database,
                             kb_search_t **out);
 void       kb_search_destroy(kb_search_t *ks);
 
 /* Clear all filters for a new query */
 void       kb_search_clear(kb_search_t *ks);
 
 /* ----------------------------------------------------------------
  * Filter chain (additive, each narrows previous results)
  * ---------------------------------------------------------------- */
 
 kb_error_t kb_search_label(kb_search_t *ks, const char *label);
 kb_error_t kb_search_name(kb_search_t *ks, const char *name);
 kb_error_t kb_search_property_key(kb_search_t *ks, const char *key);
 kb_error_t kb_search_property_value(kb_search_t *ks, const char *key,
                                     const char *value);
 kb_error_t kb_search_path(kb_search_t *ks, const char *path_pattern);
 
 /* Execute the assembled CTE query */
 kb_error_t kb_search_execute(kb_search_t *ks);
 
 /* Access results from last execute */
 const kb_resultset_t *kb_search_results(const kb_search_t *ks);
 
 /* ----------------------------------------------------------------
  * Convenience discovery functions
  * (clear filters, build query, execute, return specific data)
  * ---------------------------------------------------------------- */
 
 /*
  * Find paths matching filters. Returns array of path strings.
  * Caller must free each string and the array.
  */
 kb_error_t kb_search_find_paths(kb_search_t *ks, const char *label,
                                 const char *name,
                                 char ***paths_out, int *count_out);
 
 /*
  * Find description (properties JSON) for a path.
  * Returns heap-allocated JSON string. Caller must free.
  */
 kb_error_t kb_search_find_description(kb_search_t *ks, const char *path,
                                       char **json_out);
 
 /*
  * Find node IDs matching label + optional name/properties/path.
  * Used internally by specialized finders.
  *
  * props: array of {key, value} pairs, NULL-terminated.
  * Returns paths and count.
  */
 typedef struct {
     const char *key;
     const char *value;   /* NULL means key-only filter */
 } kb_prop_filter_t;
 
 kb_error_t kb_search_find_nodes(kb_search_t *ks,
                                 const char *label,
                                 const char *name,
                                 const kb_prop_filter_t *props, int nprops,
                                 const char *path_pattern,
                                 char ***paths_out, int *count_out);
 
 /* ----------------------------------------------------------------
  * Specialized label-based finders
  * Each returns discovered paths for that subsystem.
  * ---------------------------------------------------------------- */
 
 kb_error_t kb_find_status_paths(kb_search_t *ks,
                                 char ***paths_out, int *count_out);
 
 kb_error_t kb_find_job_paths(kb_search_t *ks,
                              char ***paths_out, int *count_out);
 
 kb_error_t kb_find_stream_paths(kb_search_t *ks,
                                 char ***paths_out, int *count_out);
 
 kb_error_t kb_find_bit_structure_paths(kb_search_t *ks,
                                        char ***paths_out, int *count_out);
 
 kb_error_t kb_find_rpc_server_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out);
 
 kb_error_t kb_find_rpc_client_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out);
 
 kb_error_t kb_find_document_paths(kb_search_t *ks,
                                   char ***paths_out, int *count_out);
 
 /* Generic: find nodes by any label */
 kb_error_t kb_find_node_paths(kb_search_t *ks,
                               const char *label,
                               char ***paths_out, int *count_out);
 
 kb_error_t kb_find_link_paths(kb_search_t *ks,
                               char ***paths_out, int *count_out);
 
 kb_error_t kb_find_link_mount_paths(kb_search_t *ks,
                                     char ***paths_out, int *count_out);
 
 /* Free a path array returned by the find functions */
 void kb_free_paths(char **paths, int count);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_SEARCH_H */