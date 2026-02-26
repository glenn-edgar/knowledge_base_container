/*
 * kb_rpc_server.c
 * Knowledge Base C Port — RPC server queue
 *
 * Mirrors LuaJIT kb_rpc_server.lua.
 * 4-state machine: empty → new_job → processing → empty
 */

#include "kb_rpc_server.h"
#include "kb_uuid.h"

#include <stdlib.h>
#include <string.h>

struct kb_rpc_server {
    kb_search_t *ks;
    sqlite3     *db;
    char        *table_name;
};

kb_rpc_server_t *kb_rpc_server_create(kb_search_t *ks, const char *database)
{
    if (!ks || !database) return NULL;

    kb_rpc_server_t *rs = (kb_rpc_server_t *)calloc(1, sizeof(*rs));
    if (!rs) return NULL;

    rs->ks = ks;
    rs->db = kb_search_get_db(ks);
    rs->table_name = kb_sprintf("%s_rpc_server_queue", database);

    if (!rs->table_name) {
        free(rs);
        return NULL;
    }
    return rs;
}

void kb_rpc_server_destroy(kb_rpc_server_t *rs)
{
    if (!rs) return;
    free(rs->table_name);
    free(rs);
}

kb_error_t kb_rpc_server_push(kb_rpc_server_t *rs, const char *path,
                               const char *rpc_action,
                               const char *data_json,
                               int priority,
                               const char *rpc_client_queue,
                               char *uuid_out, size_t uuid_size)
{
    if (!rs || !path || !rpc_action || !data_json)
        return KB_ERR_NULL_ARG;

    char ts[32];
    kb_timestamp_now(ts, sizeof(ts));

    char uuid[KB_UUID_LEN];
    kb_uuid4(uuid, sizeof(uuid));

    if (uuid_out && uuid_size >= KB_UUID_LEN) {
        memcpy(uuid_out, uuid, KB_UUID_LEN);
    }

    kb_error_t err = kb_begin_immediate(rs->db, 3, 100);
    if (err != KB_OK) return err;

    /* Find an empty slot */
    char *sql_find = kb_sprintf(
        "SELECT id FROM %s WHERE path = ? AND state = 'empty' "
        "ORDER BY id ASC LIMIT 1",
        rs->table_name);
    if (!sql_find) {
        kb_rollback(rs->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_find[] = { KB_PARAM_TEXT(path) };
    kb_result_t res_find;
    kb_result_init(&res_find);

    err = kb_query_exec(rs->db, sql_find, p_find, 1, &res_find);
    free(sql_find);

    if (err != KB_OK || res_find.count == 0) {
        kb_result_free(&res_find);
        kb_rollback(rs->db);
        return (err != KB_OK) ? err : KB_ERR_OVERFLOW;
    }

    int record_id = kb_row_get_int(&res_find, 0, "id", 0);
    kb_result_free(&res_find);

    /* Claim the slot */
    char *sql_claim = kb_sprintf(
        "UPDATE %s SET state = 'new_job', request_uuid = ?, "
        "rpc_action = ?, data = ?, priority = ?, "
        "rpc_client_queue = ?, queued_at = ? WHERE id = ?",
        rs->table_name);
    if (!sql_claim) {
        kb_rollback(rs->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_claim[] = {
        KB_PARAM_TEXT(uuid),
        KB_PARAM_TEXT(rpc_action),
        KB_PARAM_TEXT(data_json),
        KB_PARAM_INT(priority),
        KB_PARAM_TEXT(rpc_client_queue ? rpc_client_queue : ""),
        KB_PARAM_TEXT(ts),
        KB_PARAM_INT(record_id),
    };

    kb_result_t wr;
    kb_result_init(&wr);
    err = kb_query_exec(rs->db, sql_claim, p_claim, 7, &wr);
    free(sql_claim);
    kb_result_free(&wr);

    if (err != KB_OK) {
        kb_rollback(rs->db);
        return err;
    }

    return kb_commit(rs->db);
}

kb_error_t kb_rpc_server_peek(kb_rpc_server_t *rs, const char *path,
                               char **data_out, char **uuid_out,
                               char **action_out, int *record_id_out)
{
    if (!rs || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT id, request_uuid, rpc_action, data FROM %s "
        "WHERE path = ? AND state = 'new_job' "
        "ORDER BY priority DESC, queued_at ASC LIMIT 1",
        rs->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(rs->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }
    if (result.count == 0) {
        kb_result_free(&result);
        return KB_ERR_NOT_FOUND;
    }

    if (record_id_out)
        *record_id_out = kb_row_get_int(&result, 0, "id", 0);
    if (data_out) {
        const char *d = kb_row_get(&result, 0, "data");
        *data_out = kb_strdup(d ? d : "{}");
    }
    if (uuid_out) {
        const char *u = kb_row_get(&result, 0, "request_uuid");
        *uuid_out = kb_strdup(u ? u : "");
    }
    if (action_out) {
        const char *a = kb_row_get(&result, 0, "rpc_action");
        *action_out = kb_strdup(a ? a : "");
    }

    kb_result_free(&result);
    return KB_OK;
}

kb_error_t kb_rpc_server_claim(kb_rpc_server_t *rs, const char *path,
                                int record_id)
{
    if (!rs || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "UPDATE %s SET state = 'processing' "
        "WHERE id = ? AND path = ? AND state = 'new_job'",
        rs->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = {
        KB_PARAM_INT(record_id),
        KB_PARAM_TEXT(path),
    };

    kb_result_t result;
    kb_result_init(&result);
    kb_error_t err = kb_query_exec(rs->db, sql, params, 2, &result);
    free(sql);

    if (err == KB_OK && result.changes == 0) {
        err = KB_ERR_STATE;
    }

    kb_result_free(&result);
    return err;
}

kb_error_t kb_rpc_server_complete(kb_rpc_server_t *rs, const char *path,
                                   int record_id)
{
    if (!rs || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "UPDATE %s SET state = 'empty', request_uuid = NULL, "
        "rpc_action = NULL, data = NULL, priority = 0, "
        "rpc_client_queue = NULL, queued_at = NULL "
        "WHERE id = ? AND path = ? AND state = 'processing'",
        rs->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = {
        KB_PARAM_INT(record_id),
        KB_PARAM_TEXT(path),
    };

    kb_result_t result;
    kb_result_init(&result);
    kb_error_t err = kb_query_exec(rs->db, sql, params, 2, &result);
    free(sql);

    if (err == KB_OK && result.changes == 0) {
        err = KB_ERR_STATE;
    }

    kb_result_free(&result);
    return err;
}

kb_error_t kb_rpc_server_get_state_counts(kb_rpc_server_t *rs,
                                           const char *path,
                                           int *empty_out,
                                           int *new_job_out,
                                           int *processing_out)
{
    if (!rs || !path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT "
        "COUNT(*) FILTER (WHERE state = 'empty') as empty_count, "
        "COUNT(*) FILTER (WHERE state = 'new_job') as new_job_count, "
        "COUNT(*) FILTER (WHERE state = 'processing') as processing_count "
        "FROM %s WHERE path = ?",
        rs->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(rs->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (empty_out)
        *empty_out = kb_row_get_int(&result, 0, "empty_count", 0);
    if (new_job_out)
        *new_job_out = kb_row_get_int(&result, 0, "new_job_count", 0);
    if (processing_out)
        *processing_out = kb_row_get_int(&result, 0, "processing_count", 0);

    kb_result_free(&result);
    return KB_OK;
}
