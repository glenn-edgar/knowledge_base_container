/*
 * kb_job_queue.c
 * Knowledge Base C Port — Job queue implementation
 *
 * Mirrors LuaJIT kb_job_queue.lua.
 * Jobs cycle through states: free → queued → free
 */

#include "kb_job_queue.h"

#include <stdlib.h>
#include <string.h>

struct kb_job_queue {
    kb_search_t *ks;
    sqlite3     *db;
    char        *table_name;
};

kb_job_queue_t *kb_job_queue_create(kb_search_t *ks, const char *database)
{
    if (!ks || !database) return NULL;

    kb_job_queue_t *jq = (kb_job_queue_t *)calloc(1, sizeof(*jq));
    if (!jq) return NULL;

    jq->ks = ks;
    jq->db = kb_search_get_db(ks);
    jq->table_name = kb_sprintf("%s_job_queue", database);

    if (!jq->table_name) {
        free(jq);
        return NULL;
    }
    return jq;
}

void kb_job_queue_destroy(kb_job_queue_t *jq)
{
    if (!jq) return;
    free(jq->table_name);
    free(jq);
}

kb_error_t kb_job_find_node_id(kb_job_queue_t *jq, const char *node_name,
                                const char *node_path, int *node_id_out)
{
    if (!jq || !node_id_out) return KB_ERR_NULL_ARG;

    kb_search_clear_filters(jq->ks);
    kb_search_label(jq->ks, "KB_JOB_FIELD");
    if (node_name) kb_search_name(jq->ks, node_name);
    if (node_path) kb_search_path(jq->ks, node_path);

    kb_error_t err = kb_search_execute(jq->ks);
    if (err != KB_OK) return err;

    const kb_result_t *results = kb_search_results(jq->ks);
    if (results->count == 0) return KB_ERR_NOT_FOUND;

    *node_id_out = kb_row_get_int(results, 0, "id", 0);
    return KB_OK;
}

kb_error_t kb_job_get_queued_number(kb_job_queue_t *jq, const char *path,
                                     int *count_out)
{
    if (!jq || !path || !count_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT COUNT(*) as cnt FROM %s WHERE path = ? AND state = 'queued'",
        jq->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(jq->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    *count_out = (result.count > 0) ? kb_row_get_int(&result, 0, "cnt", 0) : 0;
    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_job_get_free_number(kb_job_queue_t *jq, const char *path,
                                   int *count_out)
{
    if (!jq || !path || !count_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT COUNT(*) as cnt FROM %s WHERE path = ? AND state = 'free'",
        jq->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(jq->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    *count_out = (result.count > 0) ? kb_row_get_int(&result, 0, "cnt", 0) : 0;
    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_job_push(kb_job_queue_t *jq, const char *path,
                        const char *data_json, int priority)
{
    if (!jq || !path || !data_json) return KB_ERR_NULL_ARG;

    char ts[32];
    kb_timestamp_now(ts, sizeof(ts));

    kb_error_t err = kb_begin_immediate(jq->db, 3, 100);
    if (err != KB_OK) return err;

    /* Find a free slot */
    char *sql_find = kb_sprintf(
        "SELECT id FROM %s WHERE path = ? AND state = 'free' "
        "ORDER BY id ASC LIMIT 1",
        jq->table_name);
    if (!sql_find) {
        kb_rollback(jq->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_find[] = { KB_PARAM_TEXT(path) };
    kb_result_t res_find;
    kb_result_init(&res_find);

    err = kb_query_exec(jq->db, sql_find, p_find, 1, &res_find);
    free(sql_find);

    if (err != KB_OK || res_find.count == 0) {
        kb_result_free(&res_find);
        kb_rollback(jq->db);
        return (err != KB_OK) ? err : KB_ERR_OVERFLOW;
    }

    int record_id = kb_row_get_int(&res_find, 0, "id", 0);
    kb_result_free(&res_find);

    /* Claim the slot */
    char *sql_claim = kb_sprintf(
        "UPDATE %s SET state = 'queued', data = ?, priority = ?, "
        "queued_at = ? WHERE id = ?",
        jq->table_name);
    if (!sql_claim) {
        kb_rollback(jq->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_claim[] = {
        KB_PARAM_TEXT(data_json),
        KB_PARAM_INT(priority),
        KB_PARAM_TEXT(ts),
        KB_PARAM_INT(record_id),
    };

    kb_result_t wr;
    kb_result_init(&wr);
    err = kb_query_exec(jq->db, sql_claim, p_claim, 4, &wr);
    free(sql_claim);
    kb_result_free(&wr);

    if (err != KB_OK) {
        kb_rollback(jq->db);
        return err;
    }

    return kb_commit(jq->db);
}

kb_error_t kb_job_peek(kb_job_queue_t *jq, const char *path,
                        char **data_out, int *record_id_out)
{
    if (!jq || !path || !data_out || !record_id_out) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT id, data FROM %s WHERE path = ? AND state = 'queued' "
        "ORDER BY priority DESC, queued_at ASC LIMIT 1",
        jq->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(jq->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    *record_id_out = kb_row_get_int(&result, 0, "id", 0);
    const char *data = kb_row_get(&result, 0, "data");
    *data_out = kb_strdup(data ? data : "{}");

    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_job_complete(kb_job_queue_t *jq, const char *path,
                            int record_id)
{
    if (!jq || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "UPDATE %s SET state = 'free', data = NULL, priority = 0, "
        "queued_at = NULL WHERE id = ? AND path = ?",
        jq->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = {
        KB_PARAM_INT(record_id),
        KB_PARAM_TEXT(path),
    };

    kb_result_t result;
    kb_result_init(&result);
    kb_error_t err = kb_query_exec(jq->db, sql, params, 2, &result);
    free(sql);
    kb_result_free(&result);
    return err;
}

kb_error_t kb_job_clear(kb_job_queue_t *jq, const char *path)
{
    if (!jq || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "UPDATE %s SET state = 'free', data = NULL, priority = 0, "
        "queued_at = NULL WHERE path = ?",
        jq->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(jq->db, sql, params, 1, &result);
    free(sql);
    kb_result_free(&result);
    return err;
}
