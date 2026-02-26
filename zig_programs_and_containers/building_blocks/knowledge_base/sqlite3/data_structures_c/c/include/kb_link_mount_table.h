/*
 * kb_link_mount_table.h
 * Knowledge Base C Port — Link mount table queries
 *
 * Mirrors LuaJIT kb_link_mount_table.lua.
 */

#ifndef KB_LINK_MOUNT_TABLE_H
#define KB_LINK_MOUNT_TABLE_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_link_mount_table kb_link_mount_table_t;

kb_link_mount_table_t *kb_link_mount_table_create(sqlite3 *db,
                                                   const char *database);
void kb_link_mount_table_destroy(kb_link_mount_table_t *lmt);

kb_error_t kb_link_mount_get_by_link_name(kb_link_mount_table_t *lmt,
                                           const char *link_name,
                                           kb_result_t *result);

kb_error_t kb_link_mount_get_by_mount_path(kb_link_mount_table_t *lmt,
                                            const char *mount_path,
                                            kb_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* KB_LINK_MOUNT_TABLE_H */
