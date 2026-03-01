/*
 * kb_rpc_server.h
 * Knowledge Base C Library (PostgreSQL) — RPC server queue
 *
 * Table: {database}_rpc_server
 * 3-state machine: empty → new_job → processing → (reset to empty)
 * Uses SERIALIZABLE isolation + pg_advisory_xact_lock for push.
 * Uses FOR UPDATE SKIP LOCKED for peek.
 */

 #ifndef KB_RPC_SERVER_H
 #define KB_RPC_SERVER_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 typedef struct {
     bool  found;
     int   id;
     char *server_path;
     char *request_id;
     char *rpc_action;
     char *request_payload;
     char *transaction_tag;
     char *state;
     int   priority;
     char *rpc_client_queue;
 } kb_rpc_server_job_t;
 
 void kb_rpc_server_job_free(kb_rpc_server_job_t *job);
 
 /* Count jobs by state */
 kb_error_t kb_rpc_server_count_new(kb_conn_t *c, const char *database,
                                    const char *path, int *count_out);
 kb_error_t kb_rpc_server_count_processing(kb_conn_t *c, const char *database,
                                           const char *path, int *count_out);
 
 /*
  * Push a new RPC request into the server queue.
  * Uses SERIALIZABLE + advisory lock for atomicity.
  */
 kb_error_t kb_rpc_server_push(kb_conn_t *c, const char *database,
                               const char *server_path,
                               const char *request_id,
                               const char *rpc_action,
                               const char *request_payload,
                               const char *transaction_tag,
                               int priority,
                               const char *client_path,
                               int max_retries, int base_delay_ms);
 
 /*
  * Peek at the highest-priority new job.
  * Transitions state from 'new' to 'processing'.
  */
 kb_error_t kb_rpc_server_peek(kb_conn_t *c, const char *database,
                               const char *path,
                               kb_rpc_server_job_t *job_out,
                               int max_retries, int base_delay_ms);
 
 /* Mark job completed (resets slot) */
 kb_error_t kb_rpc_server_complete(kb_conn_t *c, const char *database,
                                   const char *path, int job_id,
                                   int max_retries, int base_delay_ms);
 
 /* Clear all jobs for a path */
 kb_error_t kb_rpc_server_clear(kb_conn_t *c, const char *database,
                                const char *path,
                                int max_retries, int base_delay_ms);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_RPC_SERVER_H */