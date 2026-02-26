/*
 * kb_common.c
 * Knowledge Base C Port — Shared utilities implementation
 */

#include "kb_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

const char *kb_error_str(kb_error_t err)
{
    switch (err) {
    case KB_OK:            return "OK";
    case KB_ERR_NULL_ARG:  return "NULL argument";
    case KB_ERR_SQLITE:    return "SQLite error";
    case KB_ERR_NOT_FOUND: return "Not found";
    case KB_ERR_JSON:      return "JSON error";
    case KB_ERR_NOMEM:     return "Out of memory";
    case KB_ERR_BUSY:      return "Database busy";
    case KB_ERR_INVALID:   return "Invalid argument";
    case KB_ERR_OVERFLOW:  return "Overflow";
    case KB_ERR_STATE:     return "Invalid state";
    case KB_ERR_EXTENSION: return "Extension load failed";
    default:               return "Unknown error";
    }
}

char *kb_strdup(const char *s)
{
    if (!s) return NULL;
    size_t len = strlen(s);
    char *dup = (char *)malloc(len + 1);
    if (dup) memcpy(dup, s, len + 1);
    return dup;
}

char *kb_sprintf(const char *fmt, ...)
{
    va_list args, args2;
    va_start(args, fmt);
    va_copy(args2, args);
    int len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);
    if (len < 0) { va_end(args2); return NULL; }
    char *buf = (char *)malloc((size_t)len + 1);
    if (buf) vsnprintf(buf, (size_t)len + 1, fmt, args2);
    va_end(args2);
    return buf;
}

void kb_result_init(kb_result_t *result)
{
    if (result) memset(result, 0, sizeof(*result));
}

static void kb_row_free(kb_row_t *row)
{
    if (!row) return;
    for (int i = 0; i < row->col_count; i++) {
        free(row->col_names[i]);
        free(row->col_values[i]);
    }
}

void kb_result_free(kb_result_t *result)
{
    if (!result) return;
    for (int i = 0; i < result->count; i++) kb_row_free(&result->rows[i]);
    free(result->rows);
    memset(result, 0, sizeof(*result));
}

static kb_error_t kb_result_append(kb_result_t *result, const kb_row_t *row)
{
    if (result->count >= result->capacity) {
        int new_cap = result->capacity == 0 ? 16 : result->capacity * 2;
        kb_row_t *nr = (kb_row_t *)realloc(result->rows, (size_t)new_cap * sizeof(kb_row_t));
        if (!nr) return KB_ERR_NOMEM;
        result->rows = nr;
        result->capacity = new_cap;
    }
    result->rows[result->count++] = *row;
    return KB_OK;
}

const char *kb_row_get(const kb_result_t *result, int row_idx, const char *col_name)
{
    if (!result || row_idx < 0 || row_idx >= result->count || !col_name) return NULL;
    const kb_row_t *row = &result->rows[row_idx];
    for (int i = 0; i < row->col_count; i++)
        if (row->col_names[i] && strcmp(row->col_names[i], col_name) == 0)
            return row->col_values[i];
    return NULL;
}

int kb_row_get_int(const kb_result_t *result, int row_idx, const char *col_name, int dflt)
{
    const char *v = kb_row_get(result, row_idx, col_name);
    return v ? atoi(v) : dflt;
}

int64_t kb_row_get_int64(const kb_result_t *result, int row_idx, const char *col_name, int64_t dflt)
{
    const char *v = kb_row_get(result, row_idx, col_name);
    return v ? strtoll(v, NULL, 10) : dflt;
}

double kb_row_get_double(const kb_result_t *result, int row_idx, const char *col_name, double dflt)
{
    const char *v = kb_row_get(result, row_idx, col_name);
    return v ? strtod(v, NULL) : dflt;
}

kb_error_t kb_sql_exec(sqlite3 *db, const char *sql, char **err_msg)
{
    if (!db || !sql) return KB_ERR_NULL_ARG;
    char *errmsg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        if (err_msg && errmsg) *err_msg = kb_strdup(errmsg);
        if (errmsg) sqlite3_free(errmsg);
        return KB_ERR_SQLITE;
    }
    return KB_OK;
}

kb_error_t kb_query_exec(sqlite3 *db, const char *sql,
                         const kb_bind_param_t *params, int n_params,
                         kb_result_t *result)
{
    if (!db || !sql || !result) return KB_ERR_NULL_ARG;
    kb_result_init(result);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "  [SQL ERROR] prepare: %s\n    SQL: %.120s\n",
                sqlite3_errmsg(db), sql);
        return KB_ERR_SQLITE;
    }

    for (int i = 0; i < n_params; i++) {
        int idx = i + 1;
        switch (params[i].type) {
        case KB_BIND_NULL:   rc = sqlite3_bind_null(stmt, idx); break;
        case KB_BIND_TEXT:   rc = sqlite3_bind_text(stmt, idx, params[i].val.text, -1, SQLITE_TRANSIENT); break;
        case KB_BIND_INT:    rc = sqlite3_bind_int(stmt, idx, params[i].val.i); break;
        case KB_BIND_INT64:  rc = sqlite3_bind_int64(stmt, idx, params[i].val.i64); break;
        case KB_BIND_DOUBLE: rc = sqlite3_bind_double(stmt, idx, params[i].val.d); break;
        }
        if (rc != SQLITE_OK) { sqlite3_finalize(stmt); return KB_ERR_SQLITE; }
    }

    int col_count = sqlite3_column_count(stmt);
    if (col_count > KB_MAX_COLUMNS) col_count = KB_MAX_COLUMNS;

    char *col_name_cache[KB_MAX_COLUMNS];
    for (int c = 0; c < col_count; c++) {
        col_name_cache[c] = kb_strdup(sqlite3_column_name(stmt, c));
        if (!col_name_cache[c]) {
            for (int j = 0; j < c; j++) free(col_name_cache[j]);
            sqlite3_finalize(stmt);
            return KB_ERR_NOMEM;
        }
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        kb_row_t row;
        memset(&row, 0, sizeof(row));
        row.col_count = col_count;
        for (int c = 0; c < col_count; c++) {
            row.col_names[c] = kb_strdup(col_name_cache[c]);
            if (sqlite3_column_type(stmt, c) == SQLITE_NULL)
                row.col_values[c] = NULL;
            else {
                const char *text = (const char *)sqlite3_column_text(stmt, c);
                row.col_values[c] = text ? kb_strdup(text) : NULL;
            }
        }
        kb_error_t err = kb_result_append(result, &row);
        if (err != KB_OK) {
            kb_row_free(&row);
            for (int c = 0; c < col_count; c++) free(col_name_cache[c]);
            sqlite3_finalize(stmt);
            return err;
        }
    }

    for (int c = 0; c < col_count; c++) free(col_name_cache[c]);
    result->changes = sqlite3_changes(db);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? KB_OK : KB_ERR_SQLITE;
}

static void sleep_ms(int ms)
{
#ifdef _WIN32
    Sleep(ms);
#else
    usleep(ms * 1000);
#endif
}

kb_error_t kb_begin_immediate(sqlite3 *db, int max_retries, int retry_delay_ms)
{
    if (!db) return KB_ERR_NULL_ARG;
    for (int attempt = 0; attempt <= max_retries; attempt++) {
        int rc = sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
        if (rc == SQLITE_OK) return KB_OK;
        if (rc != SQLITE_BUSY) return KB_ERR_SQLITE;
        if (attempt < max_retries && retry_delay_ms > 0) sleep_ms(retry_delay_ms);
    }
    return KB_ERR_BUSY;
}

kb_error_t kb_commit(sqlite3 *db)
{
    if (!db) return KB_ERR_NULL_ARG;
    return (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK) ? KB_OK : KB_ERR_SQLITE;
}

kb_error_t kb_rollback(sqlite3 *db)
{
    if (!db) return KB_ERR_NULL_ARG;
    return (sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL) == SQLITE_OK) ? KB_OK : KB_ERR_SQLITE;
}

void kb_timestamp_now(char *buf, size_t buf_size)
{
    if (!buf || buf_size < 20) return;
    time_t now = time(NULL);
    struct tm *utc = gmtime(&now);
    strftime(buf, buf_size, "%Y-%m-%dT%H:%M:%S", utc);
}

kb_error_t kb_open_database(const char *db_path, const char *ltree_path, sqlite3 **db_out)
{
    if (!db_path || !db_out) return KB_ERR_NULL_ARG;
    int rc = sqlite3_open(db_path, db_out);
    if (rc != SQLITE_OK) {
        if (*db_out) { sqlite3_close(*db_out); *db_out = NULL; }
        return KB_ERR_SQLITE;
    }
    if (ltree_path) {
        sqlite3_db_config(*db_out, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);
        char *errmsg = NULL;
        rc = sqlite3_load_extension(*db_out, ltree_path, NULL, &errmsg);
        if (rc != SQLITE_OK) {
            if (errmsg) sqlite3_free(errmsg);
            sqlite3_close(*db_out);
            *db_out = NULL;
            return KB_ERR_EXTENSION;
        }
    }
    return KB_OK;
}

void kb_close_database(sqlite3 *db) { if (db) sqlite3_close(db); }

