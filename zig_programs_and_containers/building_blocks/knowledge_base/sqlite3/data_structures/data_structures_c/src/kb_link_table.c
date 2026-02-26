/*
 * kb_link_table.c
 * Knowledge Base C Port — Link table queries
 */

#include "kb_link_table.h"

#include <stdlib.h>

struct kb_link_table {
    sqlite3 *db;
    char    *table_name;
};

kb_link_table_t *kb_link_table_create(sqlite3 *db, const char *database)
{
    if (!db || !database) return NULL;

    kb_link_table_t *lt = (kb_link_table_t *)calloc(1, sizeof(*lt));
    if (!lt) return NULL;

    lt->db = db;
    lt->table_name = kb_sprintf("%s_link_table", database);
    if (!lt->table_name) {
        free(lt);
        return NULL;
    }
    return lt;
}

void kb_link_table_destroy(kb_link_table_t *lt)
{
    if (!lt) return;
    free(lt->table_name);
    free(lt);
}

kb_error_t kb_link_get_by_link_name(kb_link_table_t *lt,
                                     const char *link_name,
                                     kb_result_t *result)
{
    if (!lt || !link_name || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    char *sql = kb_sprintf("SELECT * FROM %s WHERE link_name = ?",
                           lt->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(link_name) };
    kb_error_t err = kb_query_exec(lt->db, sql, params, 1, result);
    free(sql);
    return err;
}

kb_error_t kb_link_get_by_node_path(kb_link_table_t *lt,
                                     const char *node_path,
                                     kb_result_t *result)
{
    if (!lt || !node_path || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    char *sql = kb_sprintf("SELECT * FROM %s WHERE node_path = ?",
                           lt->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(node_path) };
    kb_error_t err = kb_query_exec(lt->db, sql, params, 1, result);
    free(sql);
    return err;
}
