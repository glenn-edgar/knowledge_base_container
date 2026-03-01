/*
 * kb_rpc_client.h
 * Knowledge Base C Library (PostgreSQL) — RPC client reply queue
 *
 * Mirrors LuaJIT kb_rpc_client.lua
 * Table: {database}_rpc_client_table
 * 2-state toggle: is_new_result = true/false
 */

#ifndef KB_RPC_CLIENT_H
#define KB_RPC_CLIENT_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    bool  found;
    int   id;
    char *request_id;
    char *client_path;
    char *server_path;
    char *rpc_action;
    char *response_payload;
} kb_rpc_client_reply_t;

void kb_rpc_client_reply_free(kb_rpc_client_reply_t *reply);

/* Count free slots (is_new_result=true, response_payload IS NULL) */
kb_error_t kb_rpc_client_free_slots(kb_conn_t *c, const char *database,
                                    const char *path, int *count_out);

/* Count queued slots (is_new_result=true, response_payload IS NOT NULL) */
kb_error_t kb_rpc_client_queued_slots(kb_conn_t *c, const char *database,
                                      const char *path, int *count_out);

/* Push and claim reply data into a free slot */
kb_error_t kb_rpc_client_push_reply(kb_conn_t *c, const char *database,
                                    const char *client_path,
                                    const char *request_id,
                                    const char *server_path,
                                    const char *rpc_action,
                                    const char *transaction_tag,
                                    const char *response_payload,
                                    int max_retries, int base_delay_ms);

/* Peek and claim a reply */
kb_error_t kb_rpc_client_peek_reply(kb_conn_t *c, const char *database,
                                    const char *path,
                                    kb_rpc_client_reply_t *reply_out,
                                    int max_retries, int base_delay_ms);

/* Clear all replies for a path */
kb_error_t kb_rpc_client_clear(kb_conn_t *c, const char *database,
                               const char *path,
                               int max_retries, int base_delay_ms);

#ifdef __cplusplus
}
#endif

#endif /* KB_RPC_CLIENT_H */
