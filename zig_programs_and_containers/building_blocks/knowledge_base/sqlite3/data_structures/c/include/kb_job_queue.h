/*
 * kb_job_queue.h
 * Knowledge Base C Port — Job queue: push/peek/complete/clear
 *
 * Mirrors LuaJIT kb_job_queue.lua / Python kb_job_queue.py.
 */

#ifndef KB_JOB_QUEUE_H
#define KB_JOB_QUEUE_H

#include "kb_common.h"
#include "kb_query_support.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kb_job_queue kb_job_queue_t;

kb_job_queue_t *kb_job_queue_create(kb_search_t *ks, const char *database);
void            kb_job_queue_destroy(kb_job_queue_t *jq);

/* Find node ID for a job queue field */
kb_error_t kb_job_find_node_id(kb_job_queue_t *jq, const char *node_name,
                                const char *node_path, int *node_id_out);

/* Get count of queued/free entries */
kb_error_t kb_job_get_queued_number(kb_job_queue_t *jq, const char *path,
                                     int *count_out);
kb_error_t kb_job_get_free_number(kb_job_queue_t *jq, const char *path,
                                   int *count_out);

/* Push a job. data_json is the job payload. priority is optional (default 1). */
kb_error_t kb_job_push(kb_job_queue_t *jq, const char *path,
                        const char *data_json, int priority);

/* Peek at the highest-priority queued job. Caller must free(*data_out). */
kb_error_t kb_job_peek(kb_job_queue_t *jq, const char *path,
                        char **data_out, int *record_id_out);

/* Complete (remove) a job by record_id */
kb_error_t kb_job_complete(kb_job_queue_t *jq, const char *path,
                            int record_id);

/* Clear all jobs for path */
kb_error_t kb_job_clear(kb_job_queue_t *jq, const char *path);

#ifdef __cplusplus
}
#endif

#endif /* KB_JOB_QUEUE_H */
