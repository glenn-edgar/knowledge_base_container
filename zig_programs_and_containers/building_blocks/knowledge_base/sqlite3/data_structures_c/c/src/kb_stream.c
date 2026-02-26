/*
 * kb_stream.c
 * Knowledge Base C Port — Circular buffer stream data
 *
 * Mirrors LuaJIT kb_stream.lua.
 */

#include "kb_stream.h"

#include <stdlib.h>
#include <string.h>

struct kb_stream {
    kb_search_t *ks;          /* borrowed */
    sqlite3     *db;
    char        *table_name;  /* "<database>_stream_table" */
};

kb_stream_t *kb_stream_create(kb_search_t *ks, const char *database)
{
    if (!ks || !database) return NULL;

    kb_stream_t *st = (kb_stream_t *)calloc(1, sizeof(*st));
    if (!st) return NULL;

    st->ks = ks;
    st->db = kb_search_get_db(ks);
    st->table_name = kb_sprintf("%s_stream_table", database);

    if (!st->table_name) {
        free(st);
        return NULL;
    }
    return st;
}

void kb_stream_destroy(kb_stream_t *st)
{
    if (!st) return;
    free(st->table_name);
    free(st);
}

kb_error_t kb_stream_push_data(kb_stream_t *st, const char *path,
                                const char *data_json)
{
    if (!st || !path || !data_json) return KB_ERR_NULL_ARG;

    char ts[32];
    kb_timestamp_now(ts, sizeof(ts));

    /* Get current write_index and max_entries for this path */
    char *sql_meta = kb_sprintf(
        "SELECT write_index, max_entries FROM %s WHERE path = ? LIMIT 1",
        st->table_name);
    if (!sql_meta) return KB_ERR_NOMEM;

    kb_bind_param_t p_meta[] = { KB_PARAM_TEXT(path) };
    kb_result_t meta;
    kb_result_init(&meta);

    kb_error_t err = kb_query_exec(st->db, sql_meta, p_meta, 1, &meta);
    free(sql_meta);

    if (err != KB_OK || meta.count == 0) {
        kb_result_free(&meta);
        return (err != KB_OK) ? err : KB_ERR_NOT_FOUND;
    }

    int write_index = kb_row_get_int(&meta, 0, "write_index", 0);
    int max_entries = kb_row_get_int(&meta, 0, "max_entries", 100);
    kb_result_free(&meta);

    int next_index = (write_index + 1) % max_entries;

    /* Update the entry at write_index */
    err = kb_begin_immediate(st->db, 3, 100);
    if (err != KB_OK) return err;

    char *sql_update = kb_sprintf(
        "UPDATE %s SET data = ?, recorded_at = ? "
        "WHERE path = ? AND entry_index = ?",
        st->table_name);
    if (!sql_update) {
        kb_rollback(st->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_upd[] = {
        KB_PARAM_TEXT(data_json),
        KB_PARAM_TEXT(ts),
        KB_PARAM_TEXT(path),
        KB_PARAM_INT(write_index),
    };

    kb_result_t wr;
    kb_result_init(&wr);
    err = kb_query_exec(st->db, sql_update, p_upd, 4, &wr);
    free(sql_update);
    kb_result_free(&wr);

    if (err != KB_OK) {
        kb_rollback(st->db);
        return err;
    }

    /* Update write_index */
    char *sql_idx = kb_sprintf(
        "UPDATE %s SET write_index = ? WHERE path = ? AND entry_index = 0",
        st->table_name);
    if (!sql_idx) {
        kb_rollback(st->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_idx[] = {
        KB_PARAM_INT(next_index),
        KB_PARAM_TEXT(path),
    };

    kb_result_init(&wr);
    err = kb_query_exec(st->db, sql_idx, p_idx, 2, &wr);
    free(sql_idx);
    kb_result_free(&wr);

    if (err != KB_OK) {
        kb_rollback(st->db);
        return err;
    }

    return kb_commit(st->db);
}

kb_error_t kb_stream_list_data(kb_stream_t *st, const char *path,
                                const char *recorded_after,
                                const char *recorded_before,
                                kb_result_t *result)
{
    if (!st || !path || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    /* Build query with optional time filters */
    char *sql = NULL;
    kb_bind_param_t params[3];
    int n_params = 0;

    if (recorded_after && recorded_before) {
        sql = kb_sprintf(
            "SELECT * FROM %s WHERE path = ? "
            "AND recorded_at >= ? AND recorded_at <= ? "
            "AND recorded_at IS NOT NULL "
            "ORDER BY recorded_at ASC",
            st->table_name);
        params[0] = KB_PARAM_TEXT(path);
        params[1] = KB_PARAM_TEXT(recorded_after);
        params[2] = KB_PARAM_TEXT(recorded_before);
        n_params = 3;
    } else if (recorded_after) {
        sql = kb_sprintf(
            "SELECT * FROM %s WHERE path = ? "
            "AND recorded_at >= ? "
            "AND recorded_at IS NOT NULL "
            "ORDER BY recorded_at ASC",
            st->table_name);
        params[0] = KB_PARAM_TEXT(path);
        params[1] = KB_PARAM_TEXT(recorded_after);
        n_params = 2;
    } else if (recorded_before) {
        sql = kb_sprintf(
            "SELECT * FROM %s WHERE path = ? "
            "AND recorded_at <= ? "
            "AND recorded_at IS NOT NULL "
            "ORDER BY recorded_at ASC",
            st->table_name);
        params[0] = KB_PARAM_TEXT(path);
        params[1] = KB_PARAM_TEXT(recorded_before);
        n_params = 2;
    } else {
        sql = kb_sprintf(
            "SELECT * FROM %s WHERE path = ? "
            "AND recorded_at IS NOT NULL "
            "ORDER BY recorded_at ASC",
            st->table_name);
        params[0] = KB_PARAM_TEXT(path);
        n_params = 1;
    }

    if (!sql) return KB_ERR_NOMEM;

    kb_error_t err = kb_query_exec(st->db, sql, params, n_params, result);
    free(sql);
    return err;
}

kb_error_t kb_stream_clear_data(kb_stream_t *st, const char *path)
{
    if (!st || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "UPDATE %s SET data = NULL, recorded_at = NULL WHERE path = ?",
        st->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(st->db, sql, params, 1, &result);
    free(sql);
    kb_result_free(&result);
    return err;
}

kb_error_t kb_stream_get_write_index(kb_stream_t *st, const char *path,
                                      int *index_out)
{
    if (!st || !path || !index_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT write_index FROM %s WHERE path = ? LIMIT 1",
        st->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(st->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK || result.count == 0) {
        kb_result_free(&result);
        return (err != KB_OK) ? err : KB_ERR_NOT_FOUND;
    }

    *index_out = kb_row_get_int(&result, 0, "write_index", 0);
    kb_result_free(&result);
    return KB_OK;
}
