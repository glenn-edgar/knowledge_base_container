/*
 * kb_status_table.h
 * Knowledge Base C Port — Status data CRUD with UPSERT
 *
 * Mirrors LuaJIT kb_status_table.lua / Python kb_status.py.
 * Provides get/set status data by path, with node ID lookup
 * via KB_Search using label "KB_STATUS_FIELD".
 */

#ifndef KB_STATUS_TABLE_H
#define KB_STATUS_TABLE_H

#include "kb_common.h"
#include "kb_query_support.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_status_table kb_status_table_t;

kb_status_table_t *kb_status_table_create(kb_search_t *ks,
                                           const char *database);
void kb_status_table_destroy(kb_status_table_t *st);

/* Find node IDs for status fields matching filters */
kb_error_t kb_status_find_node_id(kb_status_table_t *st,
                                   const char *node_name,
                                   const char *node_path,
                                   int *node_id_out);

/* Get status data (JSON string) for a path.
 * Caller must free(*data_out). */
kb_error_t kb_status_get_data(kb_status_table_t *st, const char *path,
                               char **data_out);

/* Set status data (JSON string) for a path. Uses UPSERT. */
kb_error_t kb_status_set_data(kb_status_table_t *st, const char *path,
                               const char *data_json);

#ifdef __cplusplus
}
#endif

#endif /* KB_STATUS_TABLE_H */
