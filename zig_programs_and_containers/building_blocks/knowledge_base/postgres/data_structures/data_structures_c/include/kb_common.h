/*
 * kb_common.h
 * Knowledge Base C Library (PostgreSQL) — Shared types, error codes, utilities
 *
 * Foundation header included by every module. Provides:
 * - Error codes
 * - Result row abstraction (kb_row_t / kb_resultset_t)
 * - PGconn wrapper with autocommit=false semantics
 * - SQL execution helpers with auto-rollback on error
 * - Retry with exponential backoff
 * - String/JSON utilities
 *
 * Mirrors LuaJIT kb_search.lua core connection handling
 */

#ifndef KB_COMMON_H
#define KB_COMMON_H

#include <libpq-fe.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Error codes
 * ================================================================ */

typedef enum {
    KB_OK              =  0,
    KB_ERR_NULL_ARG    = -1,
    KB_ERR_PG          = -2,   /* PostgreSQL returned an error */
    KB_ERR_NOT_FOUND   = -3,
    KB_ERR_JSON        = -4,
    KB_ERR_NOMEM       = -5,
    KB_ERR_BUSY        = -6,   /* Lock not acquired / retry exhausted */
    KB_ERR_INVALID     = -7,
    KB_ERR_OVERFLOW    = -8,
    KB_ERR_STATE       = -9,
} kb_error_t;

const char *kb_error_str(kb_error_t err);

/* ================================================================
 * Database connection
 * ================================================================ */

typedef struct {
    PGconn *conn;
} kb_conn_t;

/*
 * Connect to PostgreSQL.  conninfo is a libpq connection string, e.g.:
 *   "host=localhost port=5432 dbname=knowledge_base user=postgres password=secret"
 *
 * Sets autocommit OFF (BEGIN issued).  All operations run inside a
 * transaction; call kb_commit() / kb_rollback() as needed.
 */
kb_error_t kb_connect(const char *conninfo, kb_conn_t **out);

/*
 * Connect using individual parameters (convenience).
 */
kb_error_t kb_connect_params(const char *host, const char *port,
                             const char *dbname, const char *user,
                             const char *password, kb_conn_t **out);

void       kb_disconnect(kb_conn_t *c);

kb_error_t kb_commit(kb_conn_t *c);
kb_error_t kb_rollback(kb_conn_t *c);
kb_error_t kb_begin(kb_conn_t *c);

/* ================================================================
 * Result set abstraction
 * ================================================================
 *
 * Wraps PGresult into a row-oriented structure with named column
 * access (like LuaJIT's fetch(true) → {col=val} tables).
 */

typedef struct {
    int         ncols;
    int         nrows;
    char      **colnames;   /* [ncols] */
    char     ***values;     /* [nrows][ncols], NULL for SQL NULL */
    PGresult   *pg_result;  /* owned; freed on kb_resultset_free */
} kb_resultset_t;

/*
 * Execute SQL with optional parameters. Returns result set.
 * On SQL error: issues ROLLBACK + BEGIN automatically, sets *rs = NULL.
 *
 * params/nparams: libpq parameterized query ($1, $2, ...).
 * Pass nparams=0, params=NULL for non-parameterized queries.
 */
kb_error_t kb_query(kb_conn_t *c, const char *sql,
                    const char *const *params, int nparams,
                    kb_resultset_t **rs);

/*
 * Execute SQL that returns no rows (INSERT/UPDATE/DELETE).
 * Returns affected row count in *affected (if non-NULL).
 */
kb_error_t kb_exec(kb_conn_t *c, const char *sql,
                   const char *const *params, int nparams,
                   int *affected);

/*
 * Execute raw SQL (no params, no result). For BEGIN/COMMIT/ROLLBACK etc.
 */
kb_error_t kb_exec_simple(kb_conn_t *c, const char *sql);

void       kb_resultset_free(kb_resultset_t *rs);

/* Row accessor helpers */
const char *kb_rs_get(const kb_resultset_t *rs, int row, const char *col);
int         kb_rs_get_int(const kb_resultset_t *rs, int row, const char *col);
int64_t     kb_rs_get_int64(const kb_resultset_t *rs, int row, const char *col);
bool        kb_rs_get_bool(const kb_resultset_t *rs, int row, const char *col);

/* ================================================================
 * String utilities
 * ================================================================ */

char *kb_strdup(const char *s);
char *kb_sprintf(const char *fmt, ...);

/*
 * Escape a string for inclusion in SQL (single-quote wrapping).
 * Uses PQescapeLiteral. Caller must PQfreemem() the result.
 */
char *kb_escape_literal(kb_conn_t *c, const char *str);

/*
 * Escape an identifier (table/column name).
 * Uses PQescapeIdentifier. Caller must PQfreemem() the result.
 */
char *kb_escape_identifier(kb_conn_t *c, const char *str);

/* ================================================================
 * Retry helper
 * ================================================================ */

typedef kb_error_t (*kb_retry_fn)(kb_conn_t *c, void *ctx);

/*
 * Retry a function up to max_retries times with exponential backoff.
 * base_delay_ms: initial delay in milliseconds.
 */
kb_error_t kb_retry(kb_conn_t *c, kb_retry_fn fn, void *ctx,
                    int max_retries, int base_delay_ms);

/* ================================================================
 * Timestamp helper
 * ================================================================ */

/*
 * Get current UTC timestamp as ISO-8601 string.
 * Returns heap-allocated string, caller must free().
 */
char *kb_timestamp_now(void);

#ifdef __cplusplus
}
#endif

#endif /* KB_COMMON_H */
