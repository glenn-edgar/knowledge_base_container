/*
 * kb_rpc_client.h
 * Knowledge Base C Port — RPC client reply queue: 2-state toggle
 *
 * Mirrors LuaJIT kb_rpc_client.lua.
 * States: free ↔ queued
 */

#ifndef KB_RPC_CLIENT_H
#define KB_RPC_CLIENT_H

#include "kb_common.h"
#include "kb_query_support.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_rpc_client kb_rpc_client_t;

kb_rpc_client_t *kb_rpc_client_create(kb_search_t *ks, const char *database);
void             kb_rpc_client_destroy(kb_rpc_client_t *rc);

/* Push reply data and claim the slot atomically.
 * Sets state to 'queued' with the reply data. */
kb_error_t kb_rpc_client_push_and_claim(kb_rpc_client_t *rc,
                                         const char *client_path,
                                         const char *request_uuid,
                                         const char *server_path,
                                         const char *rpc_action,
                                         const char *transaction_tag,
                                         const char *reply_data_json);

/* Peek at queued reply. Caller must free outputs. */
kb_error_t kb_rpc_client_peek_reply(kb_rpc_client_t *rc,
                                     const char *client_path,
                                     char **reply_data_out,
                                     char **uuid_out,
                                     char **action_out,
                                     int *record_id_out);

/* Clear reply (queued → free) */
kb_error_t kb_rpc_client_clear_reply(kb_rpc_client_t *rc,
                                      const char *client_path,
                                      int record_id);

/* Get counts of free vs queued slots */
kb_error_t kb_rpc_client_get_state_counts(kb_rpc_client_t *rc,
                                           const char *client_path,
                                           int *free_out,
                                           int *queued_out);

#ifdef __cplusplus
}
#endif

#endif /* KB_RPC_CLIENT_H */
