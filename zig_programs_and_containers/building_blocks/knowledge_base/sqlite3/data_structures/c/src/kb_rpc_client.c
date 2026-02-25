/*
 * kb_rpc_client.c
 * Knowledge Base C Port — RPC client reply queue
 *
 * Mirrors LuaJIT kb_rpc_client.lua.
 * 2-state toggle: free ↔ queued
 */

#include "kb_rpc_client.h"
#include "kb_uuid.h"

#include <stdlib.h>
#include <string.h>

struct kb_rpc_client {
    kb_search_t *ks;
    sqlite3     *db;
    char        *table_name;
};

kb_rpc_client_t *kb_rpc_client_create(kb_search_t *ks, const char *database)
{
    if (!ks || !database) return NULL;

    kb_rpc_client_t *rc = (kb_rpc_client_t *)calloc(1, sizeof(*rc));
    if (!rc) return NULL;

    rc->ks = ks;
    rc->db = kb_search_get_db(ks);
    rc->table_name = kb_sprintf("%s_rpc_client_queue", database);

    if (!rc->table_name) {
        free(rc);
        return NULL;
    }
    return rc;
}

void kb_rpc_client_destroy(kb_rpc_client_t *rc)
{
    if (!rc) return;
    free(rc->table_name);
    free(rc);
}

kb_error_t kb_rpc_client_push_and_claim(kb_rpc_client_t *rc,
                                         const char *client_path,
                                         const char *request_uuid,
                                         const char *server_path,
                                         const char *rpc_action,
                                         const char *transaction_tag,
                                         const char *reply_data_json)
{
    if (!rc || !client_path || !request_uuid || !reply_data_json)
        return KB_ERR_NULL_ARG;

    char ts[32];
    kb_timestamp_now(ts, sizeof(ts));

    kb_error_t err = kb_begin_immediate(rc->db, 3, 100);
    if (err != KB_OK) return err;

    /* Find a free slot */
    char *sql_find = kb_sprintf(
        "SELECT id FROM %s WHERE path = ? AND state = 'free' "
        "ORDER BY id ASC LIMIT 1",
        rc->table_name);
    if (!sql_find) {
        kb_rollback(rc->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_find[] = { KB_PARAM_TEXT(client_path) };
    kb_result_t res_find;
    kb_result_init(&res_find);

    err = kb_query_exec(rc->db, sql_find, p_find, 1, &res_find);
    free(sql_find);

    if (err != KB_OK || res_find.count == 0) {
        kb_result_free(&res_find);
        kb_rollback(rc->db);
        return (err != KB_OK) ? err : KB_ERR_OVERFLOW;
    }

    int record_id = kb_row_get_int(&res_find, 0, "id", 0);
    kb_result_free(&res_find);

    /* Claim and fill */
    char *sql_claim = kb_sprintf(
        "UPDATE %s SET state = 'queued', request_uuid = ?, "
        "server_path = ?, rpc_action = ?, transaction_tag = ?, "
        "reply_data = ?, replied_at = ? WHERE id = ?",
        rc->table_name);
    if (!sql_claim) {
        kb_rollback(rc->db);
        return KB_ERR_NOMEM;
    }

    kb_bind_param_t p_claim[] = {
        KB_PARAM_TEXT(request_uuid),
        KB_PARAM_TEXT(server_path ? server_path : ""),
        KB_PARAM_TEXT(rpc_action ? rpc_action : ""),
        KB_PARAM_TEXT(transaction_tag ? transaction_tag : ""),
        KB_PARAM_TEXT(reply_data_json),
        KB_PARAM_TEXT(ts),
        KB_PARAM_INT(record_id),
    };

    kb_result_t wr;
    kb_result_init(&wr);
    err = kb_query_exec(rc->db, sql_claim, p_claim, 7, &wr);
    free(sql_claim);
    kb_result_free(&wr);

    if (err != KB_OK) {
        kb_rollback(rc->db);
        return err;
    }

    return kb_commit(rc->db);
}

kb_error_t kb_rpc_client_peek_reply(kb_rpc_client_t *rc,
                                     const char *client_path,
                                     char **reply_data_out,
                                     char **uuid_out,
                                     char **action_out,
                                     int *record_id_out)
{
    if (!rc || !client_path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT id, request_uuid, rpc_action, reply_data FROM %s "
        "WHERE path = ? AND state = 'queued' "
        "ORDER BY replied_at ASC LIMIT 1",
        rc->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(client_path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(rc->db, sql, params, 1, &result);
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
    if (reply_data_out) {
        const char *d = kb_row_get(&result, 0, "reply_data");
        *reply_data_out = kb_strdup(d ? d : "{}");
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

kb_error_t kb_rpc_client_clear_reply(kb_rpc_client_t *rc,
                                      const char *client_path,
                                      int record_id)
{
    if (!rc || !client_path) return KB_ERR_NULL_ARG;

    char new_uuid[KB_UUID_LEN];
    kb_uuid4(new_uuid, sizeof(new_uuid));

    char *sql = kb_sprintf(
        "UPDATE %s SET state = 'free', request_uuid = ?, "
        "server_path = NULL, rpc_action = NULL, transaction_tag = NULL, "
        "reply_data = NULL, replied_at = NULL "
        "WHERE id = ? AND path = ? AND state = 'queued'",
        rc->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = {
        KB_PARAM_TEXT(new_uuid),
        KB_PARAM_INT(record_id),
        KB_PARAM_TEXT(client_path),
    };

    kb_result_t result;
    kb_result_init(&result);
    kb_error_t err = kb_query_exec(rc->db, sql, params, 3, &result);
    free(sql);

    if (err == KB_OK && result.changes == 0) {
        err = KB_ERR_STATE;
    }

    kb_result_free(&result);
    return err;
}

kb_error_t kb_rpc_client_get_state_counts(kb_rpc_client_t *rc,
                                           const char *client_path,
                                           int *free_out,
                                           int *queued_out)
{
    if (!rc || !client_path) return KB_ERR_NULL_ARG;

    char *sql = kb_sprintf(
        "SELECT "
        "COUNT(*) FILTER (WHERE state = 'free') as free_count, "
        "COUNT(*) FILTER (WHERE state = 'queued') as queued_count "
        "FROM %s WHERE path = ?",
        rc->table_name);
    if (!sql) return KB_ERR_NOMEM;

    kb_bind_param_t params[] = { KB_PARAM_TEXT(client_path) };
    kb_result_t result;
    kb_result_init(&result);

    kb_error_t err = kb_query_exec(rc->db, sql, params, 1, &result);
    free(sql);

    if (err != KB_OK) {
        kb_result_free(&result);
        return err;
    }

    if (free_out)
        *free_out = kb_row_get_int(&result, 0, "free_count", 0);
    if (queued_out)
        *queued_out = kb_row_get_int(&result, 0, "queued_count", 0);

    kb_result_free(&result);
    return KB_OK;
}
