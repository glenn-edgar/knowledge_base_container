/*
 * kb_status_table.c
 * Knowledge Base C Port — Status data CRUD
 *
 * Mirrors LuaJIT kb_status_table.lua.
 */

#include "kb_status_table.h"

#include <stdlib.h>
#include <string.h>

struct kb_status_table {
    kb_search_t *ks;          /* borrowed */
    sqlite3     *db;
    char        *table_name;  /* "<database>_status_table" */
};

kb_status_table_t *kb_status_table_create(kb_search_t *ks,
                                           const char *database)
{
    if (!ks || !database) return NULL;

    kb_status_table_t *st = (kb_status_table_t *)calloc(1, sizeof(*st));
    if (!st) return NULL;

    st->ks = ks;
    st->db = kb_search_get_db(ks);
    st->table_name = kb_sprintf("%s_status_table", database);

    if (!st->table_name) {
        free(st);
        return NULL;
    }

    return st;
}

void kb_status_table_destroy(kb_status_table_t *st)
{
    if (!st) return;
    free(st->table_name);
    free(st);
}

kb_error_t kb_status_find_node_id(kb_status_table_t *st,
                                   const char *node_name,
                                   const char *node_path,
                                   int *node_id_out)
{
    if (!st || !node_id_out) return KB_ERR_NULL_ARG;

    kb_search_clear_filters(st->ks);
    kb_search_label(st->ks, "KB_STATUS_FIELD");

    if (node_name) kb_search_name(st->ks, node_name);
    if (node_path) kb_search_path(st->ks, node_path);

    kb_error_t err = kb_search_execute(st->ks);
    if (err != KB_OK) return err;

    const kb_result_t *results = kb_search_results(st->ks);
    if (results->count == 0) return KB_ERR_NOT_FOUND;

    *node_id_out = kb_row_get_int(results, 0, "id", 0);
    return KB_OK;
}

kb_error_t kb_status_get_data(kb_status_table_t *st, const char *path,
                               char **data_out)
{
    if (!st || !path || !data_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf("SELECT data FROM %s WHERE path = ?",
                           st->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(st->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    const char *data = kb_row_get(&result, 0, "data");
    *data_out = kb_strdup(data ? data : "{}");

    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_status_set_data(kb_status_table_t *st, const char *path,
                               const char *data_json)
{
    if (!st || !path || !data_json) return KB_ERR_NULL_ARG;

    /* UPSERT: INSERT OR REPLACE */
    char *sql = kb_sprintf(
        "INSERT INTO %s (path, data) VALUES (?, ?) "
        "ON CONFLICT(path) DO UPDATE SET data = excluded.data",
        st->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_error_t err = kb_begin_immediate(st->db, 3, 100);
    if (err != KB_OK) {
        free(sql);
        return err;
    }

    kb_bind_param_t params[] = {
        KB_PARAM_TEXT(path),
        KB_PARAM_TEXT(data_json),
    };

    kb_result_t result;
    kb_result_init(&result);
    err = kb_query_exec(st->db, sql, params, 2, &result);
    free(sql);
    kb_result_free(&result);

    if (err != KB_OK) {
        kb_rollback(st->db);
        return err;
    }

    return kb_commit(st->db);
}
