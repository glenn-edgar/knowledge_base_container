/*
 * kb_common.h
 * Knowledge Base C Port — Shared types, error codes, SQL helpers
 */

#ifndef KB_COMMON_H
#define KB_COMMON_H

#include <sqlite3.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    KB_OK              =  0,
    KB_ERR_NULL_ARG    = -1,
    KB_ERR_SQLITE      = -2,
    KB_ERR_NOT_FOUND   = -3,
    KB_ERR_JSON        = -4,
    KB_ERR_NOMEM       = -5,
    KB_ERR_BUSY        = -6,
    KB_ERR_INVALID     = -7,
    KB_ERR_OVERFLOW    = -8,
    KB_ERR_STATE       = -9,
    KB_ERR_EXTENSION   = -10,
} kb_error_t;

const char *kb_error_str(kb_error_t err);

#define KB_MAX_COLUMNS 32

typedef struct {
    int   col_count;
    char *col_names[KB_MAX_COLUMNS];
    char *col_values[KB_MAX_COLUMNS];
} kb_row_t;

typedef struct {
    kb_row_t *rows;
    int       count;
    int       capacity;
    int       changes;
} kb_result_t;

void kb_result_init(kb_result_t *result);
void kb_result_free(kb_result_t *result);

typedef enum {
    KB_BIND_NULL,
    KB_BIND_TEXT,
    KB_BIND_INT,
    KB_BIND_INT64,
    KB_BIND_DOUBLE,
} kb_bind_type_t;

typedef struct {
    kb_bind_type_t type;
    union {
        const char *text;
        int         i;
        int64_t     i64;
        double      d;
    } val;
} kb_bind_param_t;

#define KB_PARAM_NULL       (kb_bind_param_t){ .type = KB_BIND_NULL }
#define KB_PARAM_TEXT(s)    (kb_bind_param_t){ .type = KB_BIND_TEXT,   .val.text = (s) }
#define KB_PARAM_INT(v)     (kb_bind_param_t){ .type = KB_BIND_INT,    .val.i = (v) }
#define KB_PARAM_INT64(v)   (kb_bind_param_t){ .type = KB_BIND_INT64,  .val.i64 = (v) }
#define KB_PARAM_DOUBLE(v)  (kb_bind_param_t){ .type = KB_BIND_DOUBLE, .val.d = (v) }

kb_error_t kb_sql_exec(sqlite3 *db, const char *sql, char **err_msg);
kb_error_t kb_query_exec(sqlite3 *db, const char *sql,
                         const kb_bind_param_t *params, int n_params,
                         kb_result_t *result);

const char *kb_row_get(const kb_result_t *result, int row_idx, const char *col_name);
int kb_row_get_int(const kb_result_t *result, int row_idx, const char *col_name, int dflt);
int64_t kb_row_get_int64(const kb_result_t *result, int row_idx, const char *col_name, int64_t dflt);
double kb_row_get_double(const kb_result_t *result, int row_idx, const char *col_name, double dflt);

kb_error_t kb_begin_immediate(sqlite3 *db, int max_retries, int retry_delay_ms);
kb_error_t kb_commit(sqlite3 *db);
kb_error_t kb_rollback(sqlite3 *db);

void kb_timestamp_now(char *buf, size_t buf_size);

kb_error_t kb_open_database(const char *db_path, const char *ltree_path, sqlite3 **db_out);
void kb_close_database(sqlite3 *db);

char *kb_strdup(const char *s);
char *kb_sprintf(const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif /* KB_COMMON_H */
