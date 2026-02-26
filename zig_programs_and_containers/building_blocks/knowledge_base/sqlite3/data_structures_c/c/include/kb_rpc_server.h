/*
 * kb_rpc_server.h
 * Knowledge Base C Port — RPC server queue: 4-state machine
 *
 * Mirrors LuaJIT kb_rpc_server.lua.
 * States: empty → new_job → processing → empty
 */

#ifndef KB_RPC_SERVER_H
#define KB_RPC_SERVER_H

#include "kb_common.h"
#include "kb_query_support.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_rpc_server kb_rpc_server_t;

kb_rpc_server_t *kb_rpc_server_create(kb_search_t *ks, const char *database);
void             kb_rpc_server_destroy(kb_rpc_server_t *rs);

/* Push a new RPC job. Returns the request_uuid assigned. uuid_out must be >= 37 bytes. */
kb_error_t kb_rpc_server_push(kb_rpc_server_t *rs, const char *path,
                               const char *rpc_action,
                               const char *data_json,
                               int priority,
                               const char *rpc_client_queue,
                               char *uuid_out, size_t uuid_size);

/* Peek at the next new_job. Caller must free data_out. */
kb_error_t kb_rpc_server_peek(kb_rpc_server_t *rs, const char *path,
                               char **data_out, char **uuid_out,
                               char **action_out, int *record_id_out);

/* Claim a job for processing (new_job → processing) */
kb_error_t kb_rpc_server_claim(kb_rpc_server_t *rs, const char *path,
                                int record_id);

/* Complete a job (processing → empty) */
kb_error_t kb_rpc_server_complete(kb_rpc_server_t *rs, const char *path,
                                   int record_id);

/* Get count of jobs in each state */
kb_error_t kb_rpc_server_get_state_counts(kb_rpc_server_t *rs,
                                           const char *path,
                                           int *empty_out,
                                           int *new_job_out,
                                           int *processing_out);

#ifdef __cplusplus
}
#endif

#endif /* KB_RPC_SERVER_H */
