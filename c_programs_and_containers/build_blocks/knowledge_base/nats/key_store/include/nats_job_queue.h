/**
 * @file nats_job_queue.h
 * @brief Distributed Job Queue built on top of KeyStore
 *
 * Features:
 *   - Priority-based job processing (higher priority = processed first)
 *   - Automatic retries with configurable max_retries
 *   - Job timeout support
 *   - Multiple named queues
 *   - Worker registration and heartbeats
 *   - Job result storage
 *   - Queue statistics tracking
 *   - Stale job cleanup
 *
 * Keys layout in the KV store:
 *   job.<job_id>                          – job data (JSON)
 *   queue.<queue>.<priority_key>.<job_id> – queue entry (value = job_id)
 *   stats.queue.<queue>.<status>          – counter per status
 *   worker.<worker_id>.current_job        – current job id
 *   worker.<worker_id>.last_seen          – ISO timestamp
 */

 #ifndef NATS_JOB_QUEUE_H
 #define NATS_JOB_QUEUE_H
 
 #include "nats_key_store.h"
 #include <cjson/cJSON.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ------------------------------------------------------------------ */
 /*  Job status                                                         */
 /* ------------------------------------------------------------------ */
 
 typedef enum {
     JOB_PENDING   = 0,
     JOB_RUNNING   = 1,
     JOB_COMPLETED = 2,
     JOB_FAILED    = 3,
     JOB_CANCELLED = 4,
     JOB_RETRYING  = 5,
 } JobStatus;
 
 const char *job_status_str(JobStatus st);
 JobStatus   job_status_from_str(const char *s);
 
 /* ------------------------------------------------------------------ */
 /*  Job structure                                                      */
 /* ------------------------------------------------------------------ */
 
 typedef struct {
     char        id[64];             /* UUID-like job identifier        */
     char        queue[64];          /* queue name                      */
     char       *payload_json;       /* JSON string – caller owns       */
     JobStatus   status;
     int         priority;           /* higher = processed first        */
     int         max_retries;
     int         retry_count;
     char        created_at[32];     /* ISO 8601 timestamp              */
     char        started_at[32];     /* "" if not started               */
     char        completed_at[32];   /* "" if not completed             */
     char       *error;              /* error message – caller owns     */
     char       *result_json;        /* result JSON – caller owns       */
     char        worker_id[64];      /* "" if not claimed               */
     int         timeout_seconds;    /* 0 = no timeout                  */
 } Job;
 
 /**
  * Initialize a Job with defaults.
  * Generates a unique id and sets created_at to now.
  */
 void job_init(Job *job);
 
 /**
  * Free heap-allocated fields inside a Job (payload, error, result).
  * Does NOT free the Job struct itself.
  */
 void job_free(Job *job);
 
 /**
  * Serialize a Job to a JSON string.  Caller must free().
  */
 char *job_to_json(const Job *job);
 
 /**
  * Deserialize a Job from a JSON string.
  * Returns KS_OK on success.  On success the caller must eventually
  * call job_free() on the result.
  */
 ks_status_t job_from_json(const char *json, Job *out);
 
 /* ------------------------------------------------------------------ */
 /*  JobQueue handle                                                    */
 /* ------------------------------------------------------------------ */
 
 typedef struct JobQueue JobQueue;
 
 /**
  * Create a JobQueue that uses the given KeyStore for storage.
  * The KeyStore must already exist (created via ks_create).
  * The JobQueue does NOT own the KeyStore – caller manages its lifetime.
  *
  * @param worker_id  Unique name for this worker.  If NULL, one is generated.
  */
 ks_status_t jq_create(JobQueue **out, KeyStore *ks, const char *worker_id);
 
 /**
  * Destroy the JobQueue handle.  Does NOT destroy the KeyStore.
  */
 void jq_destroy(JobQueue *jq);
 
 /**
  * Get the worker_id assigned to this queue.
  */
 const char *jq_worker_id(const JobQueue *jq);
 
 /* ------------------------------------------------------------------ */
 /*  Job submission                                                     */
 /* ------------------------------------------------------------------ */
 
 /**
  * Submit a job to the queue.
  *
  * @param payload_json  JSON payload for the job.
  * @param queue         Queue name (e.g. "default").
  * @param priority      Higher = processed first.
  * @param max_retries   Max retry attempts on failure.
  * @param timeout_sec   Timeout in seconds (0 = no timeout).
  * @param[out] job_id   Receives the generated job id.  Caller must free().
  */
 ks_status_t jq_submit(JobQueue *jq,
                       const char *payload_json,
                       const char *queue,
                       int priority,
                       int max_retries,
                       int timeout_sec,
                       char **job_id);
 
 /* ------------------------------------------------------------------ */
 /*  Job retrieval                                                      */
 /* ------------------------------------------------------------------ */
 
 /**
  * Get a job by id.
  * Caller must call job_free(out) when done.
  */
 ks_status_t jq_get_job(JobQueue *jq, const char *job_id, Job *out);
 
 /**
  * Cancel a pending job.
  * @param[out] cancelled  True if the job was actually cancelled.
  */
 ks_status_t jq_cancel_job(JobQueue *jq, const char *job_id, bool *cancelled);
 
 /* ------------------------------------------------------------------ */
 /*  Worker operations                                                  */
 /* ------------------------------------------------------------------ */
 
 /**
  * Claim the next available job from the listed queues (priority order).
  *
  * @param queues      Array of queue names to check.
  * @param num_queues  Length of queues array.
  * @param[out] out    Filled with claimed job data.  Caller must job_free().
  *
  * @return KS_OK if a job was claimed, KS_ERR_NOT_FOUND if none available.
  */
 ks_status_t jq_claim_job(JobQueue *jq,
                          const char **queues, size_t num_queues,
                          Job *out);
 
 /**
  * Mark a job as completed with an optional result.
  *
  * @param result_json  JSON result string, or NULL.
  * @param[out] ok      True if the job was successfully completed.
  */
 ks_status_t jq_complete_job(JobQueue *jq, const char *job_id,
                             const char *result_json, bool *ok);
 
 /**
  * Mark a job as failed with an error message.
  * If retry_count < max_retries the job is re-queued as PENDING.
  * Otherwise it is marked FAILED permanently.
  *
  * @param[out] ok  True if the failure was recorded.
  */
 ks_status_t jq_fail_job(JobQueue *jq, const char *job_id,
                         const char *error, bool *ok);
 
 /* ------------------------------------------------------------------ */
 /*  Statistics                                                         */
 /* ------------------------------------------------------------------ */
 
 typedef struct {
     int64_t pending;
     int64_t running;
     int64_t completed;
     int64_t failed;
     int64_t cancelled;
 } JqStats;
 
 /**
  * Get statistics for a named queue.
  */
 ks_status_t jq_get_stats(JobQueue *jq, const char *queue, JqStats *stats);
 
 /* ------------------------------------------------------------------ */
 /*  Monitoring                                                         */
 /* ------------------------------------------------------------------ */
 
 typedef struct {
     char  worker_id[64];
     char  last_seen[32];
     char  current_job[64];   /* "" if idle */
 } JqWorkerInfo;
 
 /**
  * Get a list of recently-active workers.
  *
  * @param staleness_sec  Workers not seen within this many seconds are excluded.
  * @param[out] workers   Caller must free() the array (not individual elements).
  * @param[out] count     Number of workers returned.
  */
 ks_status_t jq_get_active_workers(JobQueue *jq, int staleness_sec,
                                   JqWorkerInfo **workers, size_t *count);
 
 /**
  * Clean up jobs stuck in RUNNING state for longer than timeout_sec.
  * Resets them to PENDING so another worker can pick them up.
  *
  * @param[out] cleaned  Number of jobs reset.
  */
 ks_status_t jq_cleanup_stale_jobs(JobQueue *jq, int timeout_sec,
                                   int *cleaned);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* NATS_JOB_QUEUE_H */