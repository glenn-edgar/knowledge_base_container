/*
 * kb_common.c
 * Knowledge Base C Library (PostgreSQL) — Core utilities implementation
 */

#include "kb_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

/* ================================================================
 * Error strings
 * ================================================================ */

const char *kb_error_str(kb_error_t err) {
    switch (err) {
        case KB_OK:           return "OK";
        case KB_ERR_NULL_ARG: return "NULL argument";
        case KB_ERR_PG:       return "PostgreSQL error";
        case KB_ERR_NOT_FOUND:return "Not found";
        case KB_ERR_JSON:     return "JSON error";
        case KB_ERR_NOMEM:    return "Out of memory";
        case KB_ERR_BUSY:     return "Busy / retry exhausted";
        case KB_ERR_INVALID:  return "Invalid argument";
        case KB_ERR_OVERFLOW: return "Overflow";
        case KB_ERR_STATE:    return "Invalid state";
        default:              return "Unknown error";
    }
}

/* ================================================================
 * Connection management
 * ================================================================ */

kb_error_t kb_connect(const char *conninfo, kb_conn_t **out) {
    if (!conninfo || !out) return KB_ERR_NULL_ARG;

    kb_conn_t *c = calloc(1, sizeof(kb_conn_t));
    if (!c) return KB_ERR_NOMEM;

    c->conn = PQconnectdb(conninfo);
    if (PQstatus(c->conn) != CONNECTION_OK) {
        fprintf(stderr, "[KB] Connection failed: %s\n", PQerrorMessage(c->conn));
        PQfinish(c->conn);
        free(c);
        return KB_ERR_PG;
    }

    /* autocommit OFF — start transaction */
    PGresult *r = PQexec(c->conn, "BEGIN");
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        fprintf(stderr, "[KB] BEGIN failed: %s\n", PQerrorMessage(c->conn));
        PQclear(r);
        PQfinish(c->conn);
        free(c);
        return KB_ERR_PG;
    }
    PQclear(r);

    *out = c;
    return KB_OK;
}

kb_error_t kb_connect_params(const char *host, const char *port,
                             const char *dbname, const char *user,
                             const char *password, kb_conn_t **out) {
    char conninfo[1024];
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%s dbname=%s user=%s password=%s",
             host ? host : "localhost",
             port ? port : "5432",
             dbname ? dbname : "knowledge_base",
             user ? user : "postgres",
             password ? password : "");
    return kb_connect(conninfo, out);
}

void kb_disconnect(kb_conn_t *c) {
    if (!c) return;
    if (c->conn) {
        PGresult *r = PQexec(c->conn, "ROLLBACK");
        if (r) PQclear(r);
        PQfinish(c->conn);
    }
    free(c);
}

kb_error_t kb_commit(kb_conn_t *c) {
    if (!c) return KB_ERR_NULL_ARG;
    PGresult *r = PQexec(c->conn, "COMMIT");
    ExecStatusType s = PQresultStatus(r);
    PQclear(r);
    if (s != PGRES_COMMAND_OK) return KB_ERR_PG;
    /* Start new transaction */
    r = PQexec(c->conn, "BEGIN");
    PQclear(r);
    return KB_OK;
}

kb_error_t kb_rollback(kb_conn_t *c) {
    if (!c) return KB_ERR_NULL_ARG;
    PGresult *r = PQexec(c->conn, "ROLLBACK");
    ExecStatusType s = PQresultStatus(r);
    PQclear(r);
    if (s != PGRES_COMMAND_OK) return KB_ERR_PG;
    /* Start new transaction */
    r = PQexec(c->conn, "BEGIN");
    PQclear(r);
    return KB_OK;
}

kb_error_t kb_begin(kb_conn_t *c) {
    if (!c) return KB_ERR_NULL_ARG;
    PGresult *r = PQexec(c->conn, "BEGIN");
    ExecStatusType s = PQresultStatus(r);
    PQclear(r);
    return (s == PGRES_COMMAND_OK) ? KB_OK : KB_ERR_PG;
}

/* ================================================================
 * SQL execution with auto-rollback on error
 * ================================================================
 * Mirrors LuaJIT _raw_query / _raw_query_one pattern:
 *   On error → ROLLBACK + BEGIN to clear aborted state
 */

static void auto_rollback_begin(kb_conn_t *c) {
    PGresult *r = PQexec(c->conn, "ROLLBACK");
    if (r) PQclear(r);
    r = PQexec(c->conn, "BEGIN");
    if (r) PQclear(r);
}

kb_error_t kb_query(kb_conn_t *c, const char *sql,
                    const char *const *params, int nparams,
                    kb_resultset_t **rs) {
    if (!c || !sql || !rs) return KB_ERR_NULL_ARG;
    *rs = NULL;

    PGresult *pgr;
    if (nparams > 0 && params) {
        pgr = PQexecParams(c->conn, sql, nparams, NULL, params, NULL, NULL, 0);
    } else {
        pgr = PQexec(c->conn, sql);
    }

    ExecStatusType status = PQresultStatus(pgr);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        fprintf(stderr, "[KB SQL ERROR] %s\n  SQL: %.200s\n",
                PQresultErrorMessage(pgr), sql);
        PQclear(pgr);
        auto_rollback_begin(c);
        return KB_ERR_PG;
    }

    int nrows = PQntuples(pgr);
    int ncols = PQnfields(pgr);

    kb_resultset_t *result = calloc(1, sizeof(kb_resultset_t));
    if (!result) { PQclear(pgr); return KB_ERR_NOMEM; }

    result->pg_result = pgr;
    result->nrows = nrows;
    result->ncols = ncols;

    /* Column names */
    result->colnames = calloc(ncols, sizeof(char *));
    if (!result->colnames) { kb_resultset_free(result); return KB_ERR_NOMEM; }
    for (int i = 0; i < ncols; i++) {
        result->colnames[i] = kb_strdup(PQfname(pgr, i));
    }

    /* Values */
    result->values = calloc(nrows, sizeof(char **));
    if (nrows > 0 && !result->values) { kb_resultset_free(result); return KB_ERR_NOMEM; }
    for (int r = 0; r < nrows; r++) {
        result->values[r] = calloc(ncols, sizeof(char *));
        if (!result->values[r]) { kb_resultset_free(result); return KB_ERR_NOMEM; }
        for (int col = 0; col < ncols; col++) {
            if (!PQgetisnull(pgr, r, col)) {
                result->values[r][col] = kb_strdup(PQgetvalue(pgr, r, col));
            }
        }
    }

    *rs = result;
    return KB_OK;
}

kb_error_t kb_exec(kb_conn_t *c, const char *sql,
                   const char *const *params, int nparams,
                   int *affected) {
    if (!c || !sql) return KB_ERR_NULL_ARG;

    PGresult *pgr;
    if (nparams > 0 && params) {
        pgr = PQexecParams(c->conn, sql, nparams, NULL, params, NULL, NULL, 0);
    } else {
        pgr = PQexec(c->conn, sql);
    }

    ExecStatusType status = PQresultStatus(pgr);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        fprintf(stderr, "[KB SQL ERROR] %s\n  SQL: %.200s\n",
                PQresultErrorMessage(pgr), sql);
        PQclear(pgr);
        auto_rollback_begin(c);
        return KB_ERR_PG;
    }

    if (affected) {
        const char *ct = PQcmdTuples(pgr);
        *affected = (ct && ct[0]) ? atoi(ct) : 0;
    }
    PQclear(pgr);
    return KB_OK;
}

kb_error_t kb_exec_simple(kb_conn_t *c, const char *sql) {
    return kb_exec(c, sql, NULL, 0, NULL);
}

void kb_resultset_free(kb_resultset_t *rs) {
    if (!rs) return;
    if (rs->colnames) {
        for (int i = 0; i < rs->ncols; i++) free(rs->colnames[i]);
        free(rs->colnames);
    }
    if (rs->values) {
        for (int r = 0; r < rs->nrows; r++) {
            if (rs->values[r]) {
                for (int c = 0; c < rs->ncols; c++) free(rs->values[r][c]);
                free(rs->values[r]);
            }
        }
        free(rs->values);
    }
    if (rs->pg_result) PQclear(rs->pg_result);
    free(rs);
}

/* ================================================================
 * Result set accessors
 * ================================================================ */

static int find_col(const kb_resultset_t *rs, const char *col) {
    if (!rs || !col) return -1;
    for (int i = 0; i < rs->ncols; i++) {
        if (rs->colnames[i] && strcmp(rs->colnames[i], col) == 0)
            return i;
    }
    return -1;
}

const char *kb_rs_get(const kb_resultset_t *rs, int row, const char *col) {
    int ci = find_col(rs, col);
    if (ci < 0 || row < 0 || row >= rs->nrows) return NULL;
    return rs->values[row][ci];
}

int kb_rs_get_int(const kb_resultset_t *rs, int row, const char *col) {
    const char *v = kb_rs_get(rs, row, col);
    return v ? atoi(v) : 0;
}

int64_t kb_rs_get_int64(const kb_resultset_t *rs, int row, const char *col) {
    const char *v = kb_rs_get(rs, row, col);
    return v ? (int64_t)atoll(v) : 0;
}

bool kb_rs_get_bool(const kb_resultset_t *rs, int row, const char *col) {
    const char *v = kb_rs_get(rs, row, col);
    if (!v) return false;
    return (v[0] == 't' || v[0] == 'T' || v[0] == '1');
}

/* ================================================================
 * String utilities
 * ================================================================ */

char *kb_strdup(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char *d = malloc(len);
    if (d) memcpy(d, s, len);
    return d;
}

char *kb_sprintf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (len < 0) return NULL;

    char *buf = malloc(len + 1);
    if (!buf) return NULL;

    va_start(ap, fmt);
    vsnprintf(buf, len + 1, fmt, ap);
    va_end(ap);
    return buf;
}

char *kb_escape_literal(kb_conn_t *c, const char *str) {
    if (!c || !str) return NULL;
    return PQescapeLiteral(c->conn, str, strlen(str));
}

char *kb_escape_identifier(kb_conn_t *c, const char *str) {
    if (!c || !str) return NULL;
    return PQescapeIdentifier(c->conn, str, strlen(str));
}

/* ================================================================
 * Retry with exponential backoff
 * ================================================================ */

kb_error_t kb_retry(kb_conn_t *c, kb_retry_fn fn, void *ctx,
                    int max_retries, int base_delay_ms) {
    kb_error_t err = KB_OK;
    int delay = base_delay_ms;

    for (int attempt = 0; attempt <= max_retries; attempt++) {
        err = fn(c, ctx);
        if (err == KB_OK) return KB_OK;
        if (attempt < max_retries) {
            usleep(delay * 1000);
            delay *= 2;
        }
    }
    return err;
}

/* ================================================================
 * Timestamp
 * ================================================================ */

char *kb_timestamp_now(void) {
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    char *buf = malloc(32);
    if (!buf) return NULL;
    strftime(buf, 32, "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}
