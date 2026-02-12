/*
 * kb_link_table.h
 * Knowledge Base C Port — Link table queries
 *
 * Mirrors LuaJIT kb_link_table.lua.
 * Queries link relationships by link_name or node_path.
 */

#ifndef KB_LINK_TABLE_H
#define KB_LINK_TABLE_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_link_table kb_link_table_t;

kb_link_table_t *kb_link_table_create(sqlite3 *db, const char *database);
void             kb_link_table_destroy(kb_link_table_t *lt);

/* Get links by link_name. Result returned to caller (must kb_result_free). */
kb_error_t kb_link_get_by_link_name(kb_link_table_t *lt,
                                     const char *link_name,
                                     kb_result_t *result);

/* Get links by node_path. */
kb_error_t kb_link_get_by_node_path(kb_link_table_t *lt,
                                     const char *node_path,
                                     kb_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* KB_LINK_TABLE_H */
