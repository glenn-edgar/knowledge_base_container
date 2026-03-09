/*
 * kb_job_queue.h
 * Knowledge Base C Library (PostgreSQL) — Job queue operations
 *
 * Table: {database}_job
 * Columns: id, path (ltree), schedule_at, started_at, completed_at,
 *          is_active (bool), valid (bool), data (jsonb)
 *
 * State machine:
 *   Free slot:    valid=FALSE, is_active=FALSE, data=NULL
 *   Queued:       valid=TRUE,  is_active=FALSE  (pushed, waiting)
 *   Active:       valid=TRUE,  is_active=TRUE   (being processed)
 *   Completed:    valid=FALSE, is_active=FALSE   (recycled to free)
 *
 * Concurrent write safety:
 *   - push: FOR UPDATE SKIP LOCKED on free slot, with retry
 *   - peek: FOR UPDATE SKIP LOCKED on pending job, with retry
 *   - complete: FOR UPDATE on target row by ID, with retry
 *   - clear: FOR UPDATE on all path rows, with retry
 */

 #ifndef KB_JOB_QUEUE_H
 #define KB_JOB_QUEUE_H
 
 #include "kb_common.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 typedef struct {
     bool found;
     int  id;
     char *data;   /* heap-allocated JSON, caller must free */
 } kb_job_info_t;
 
 /* Count free (valid=FALSE) slots */
 kb_error_t kb_job_free_count(kb_conn_t *c, const char *database,
                              const char *path, int *count_out);
 
 /* Count queued (valid=TRUE, is_active=FALSE) slots */
 kb_error_t kb_job_queued_count(kb_conn_t *c, const char *database,
                                const char *path, int *count_out);
 
 /* Count active (valid=TRUE, is_active=TRUE) slots */
 kb_error_t kb_job_active_count(kb_conn_t *c, const char *database,
                                const char *path, int *count_out);
 
 /* Push job data into the next free slot.
  * Uses FOR UPDATE SKIP LOCKED + retry. */
 kb_error_t kb_job_push(kb_conn_t *c, const char *database,
                        const char *path, const char *data,
                        int max_retries, int base_delay_ms);
 
 /* Peek at the oldest queued job (atomically claim: is_active=TRUE).
  * Uses FOR UPDATE SKIP LOCKED + retry. */
 kb_error_t kb_job_peek(kb_conn_t *c, const char *database,
                        const char *path, kb_job_info_t *info_out,
                        int max_retries, int base_delay_ms);
 
 /* Mark a job as completed (recycle slot).
  * Uses FOR UPDATE + retry. */
 kb_error_t kb_job_complete(kb_conn_t *c, const char *database,
                            int job_id,
                            int max_retries, int base_delay_ms);
 
 /* Clear all jobs for a path (reset all slots to free).
  * Uses FOR UPDATE + retry. */
 kb_error_t kb_job_clear(kb_conn_t *c, const char *database,
                         const char *path,
                         int max_retries, int base_delay_ms);
 
 /* List all pending (valid=TRUE, is_active=FALSE) jobs.
  * Returns result set with columns: id, data, schedule_at. */
 kb_error_t kb_job_list_pending(kb_conn_t *c, const char *database,
                                const char *path,
                                kb_resultset_t **rs_out);
 
 /* List all active (valid=TRUE, is_active=TRUE) jobs.
  * Returns result set with columns: id, data, started_at. */
 kb_error_t kb_job_list_active(kb_conn_t *c, const char *database,
                               const char *path,
                               kb_resultset_t **rs_out);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_JOB_QUEUE_H */