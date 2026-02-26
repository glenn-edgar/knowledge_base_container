/*
 * kb_link_mount_table.c
 * Knowledge Base C Port — Link mount table queries
 */

#include "kb_link_mount_table.h"

#include <stdlib.h>

struct kb_link_mount_table {
    sqlite3 *db;
    char    *table_name;
};

kb_link_mount_table_t *kb_link_mount_table_create(sqlite3 *db,
                                                   const char *database)
{
    if (!db || !database) return NULL;

    kb_link_mount_table_t *lmt = (kb_link_mount_table_t *)calloc(1, sizeof(*lmt));
    if (!lmt) return NULL;

    lmt->db = db;
    lmt->table_name = kb_sprintf("%s_link_mount_table", database);
    if (!lmt->table_name) {
        free(lmt);
        return NULL;
    }
    return lmt;
}

void kb_link_mount_table_destroy(kb_link_mount_table_t *lmt)
{
    if (!lmt) return;
    free(lmt->table_name);
    free(lmt);
}

kb_error_t kb_link_mount_get_by_link_name(kb_link_mount_table_t *lmt,
                                           const char *link_name,
                                           kb_result_t *result)
{
    if (!lmt || !link_name || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    char *sql = kb_sprintf("SELECT * FROM %s WHERE link_name = ?",
                           lmt->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(link_name) };
    kb_error_t err = kb_query_exec(lmt->db, sql, params, 1, result);
    free(sql);
    return err;
}

kb_error_t kb_link_mount_get_by_mount_path(kb_link_mount_table_t *lmt,
                                            const char *mount_path,
                                            kb_result_t *result)
{
    if (!lmt || !mount_path || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    char *sql = kb_sprintf("SELECT * FROM %s WHERE mount_path = ?",
                           lmt->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(mount_path) };
    kb_error_t err = kb_query_exec(lmt->db, sql, params, 1, result);
    free(sql);
    return err;
}
