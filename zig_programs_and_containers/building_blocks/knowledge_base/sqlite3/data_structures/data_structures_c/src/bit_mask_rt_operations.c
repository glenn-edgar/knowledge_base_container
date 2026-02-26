/*
 * bit_mask_rt_operations.c
 * Knowledge Base C Port — Bit mask runtime operations
 *
 * Mirrors LuaJIT bit_mask_rt_operations.lua.
 */

#include "bit_mask_rt_operations.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct kb_bit_mask_ops {
    sqlite3 *db;
    char    *table_name;  /* e.g., "<database>_bit_mask_store" */
};

kb_bit_mask_ops_t *kb_bit_mask_ops_create(sqlite3 *db, const char *database)
{
    if (!db || !database) return NULL;

    kb_bit_mask_ops_t *ops = (kb_bit_mask_ops_t *)calloc(1, sizeof(*ops));
    if (!ops) return NULL;

    ops->db = db;
    ops->table_name = kb_sprintf("%s_bit_mask_store", database);
    if (!ops->table_name) {
        free(ops);
        return NULL;
    }
    return ops;
}

void kb_bit_mask_ops_destroy(kb_bit_mask_ops_t *ops)
{
    if (!ops) return;
    free(ops->table_name);
    free(ops);
}

/* ================================================================
 * Get/set single bits
 * ================================================================ */

kb_error_t kb_bit_get(kb_bit_mask_ops_t *ops, const char *path,
                      int bit_position, int *value_out)
{
    if (!ops || !path || !value_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf("SELECT bit_mask FROM %s WHERE path = ?",
                           ops->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(ops->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    int64_t mask = kb_row_get_int64(&result, 0, "bit_mask", 0);
    *value_out = (int)((mask >> bit_position) & 1);

    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_bit_set(kb_bit_mask_ops_t *ops, const char *path,
                      int bit_position, int value)
{
    if (!ops || !path) return KB_ERR_NULL_ARG;

    /* Read current mask */
    char *sql_read = kb_sprintf("SELECT bit_mask, change_mask FROM %s WHERE path = ?",
                                ops->table_name);
    if (!sql_read) return KB_ERR_NOMEM;

    kb_bind_param_t params_read[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(ops->db, sql_read, params_read, 1, &result);
    free(sql_read);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    int64_t old_mask = kb_row_get_int64(&result, 0, "bit_mask", 0);
    int64_t change_mask = kb_row_get_int64(&result, 0, "change_mask", 0);
    kb_result_free(&result);

    /* Compute new mask */
    int64_t new_mask;
    if (value) {
        new_mask = old_mask | ((int64_t)1 << bit_position);
    } else {
        new_mask = old_mask & ~((int64_t)1 << bit_position);
    }

    /* Update change_mask: mark changed bit */
    if (new_mask != old_mask) {
        change_mask |= ((int64_t)1 << bit_position);
    }

    /* Write back */
    char *sql_write = kb_sprintf(
        "UPDATE %s SET bit_mask = ?, change_mask = ? WHERE path = ?",
        ops->table_name);
    if (!sql_write) return KB_ERR_NOMEM;

    kb_bind_param_t params_write[] = {
        KB_PARAM_INT64(new_mask),
        KB_PARAM_INT64(change_mask),
        KB_PARAM_TEXT(path),
    };

    kb_result_t wr;
    kb_result_init(&wr);
    err = kb_query_exec(ops->db, sql_write, params_write, 3, &wr);
    free(sql_write);
    kb_result_free(&wr);

    return err;
}

/* ================================================================
 * Full mask operations
 * ================================================================ */

kb_error_t kb_bit_get_mask(kb_bit_mask_ops_t *ops, const char *path,
                           int64_t *mask_out)
{
    if (!ops || !path || !mask_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf("SELECT bit_mask FROM %s WHERE path = ?",
                           ops->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(ops->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }
    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    *mask_out = kb_row_get_int64(&result, 0, "bit_mask", 0);
    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_bit_set_mask(kb_bit_mask_ops_t *ops, const char *path,
                           int64_t mask)
{
    if (!ops || !path) return KB_ERR_NULL_ARG;

    /* Read current for change detection */
    int64_t old_mask = 0;
    kb_error_t err = kb_bit_get_mask(ops, path, &old_mask);
    if (err != KB_OK) return err;

    int64_t changed_bits = old_mask ^ mask;

    char *sql = kb_sprintf(
        "UPDATE %s SET bit_mask = ?, change_mask = change_mask | ? WHERE path = ?",
        ops->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = {
        KB_PARAM_INT64(mask),
        KB_PARAM_INT64(changed_bits),
        KB_PARAM_TEXT(path),
    };

    kb_result_t result;
    kb_result_init(&result);
    err = kb_query_exec(ops->db, sql, params, 3, &result);
    free(sql);
    kb_result_free(&result);
    return err;
}

kb_error_t kb_bit_get_change_mask(kb_bit_mask_ops_t *ops, const char *path,
                                   int64_t *mask_out)
{
    if (!ops || !path || !mask_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf("SELECT change_mask FROM %s WHERE path = ?",
                           ops->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(ops->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK || result.count == 0) {
        kb_result_free(&result);
        return (err != KB_OK) ? err : KB_ERR_NOT_FOUND;
    }

    *mask_out = kb_row_get_int64(&result, 0, "change_mask", 0);
    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_bit_clear_change_mask(kb_bit_mask_ops_t *ops, const char *path)
{
    if (!ops || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf("UPDATE %s SET change_mask = 0 WHERE path = ?",
                           ops->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(ops->db, sql, params, 1, &result);
    free(sql);
    kb_result_free(&result);
    return err;
}
