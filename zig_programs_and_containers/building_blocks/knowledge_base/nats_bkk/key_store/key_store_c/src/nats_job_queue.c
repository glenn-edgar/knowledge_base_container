/**
 * @file nats_job_queue.c
 * @brief Distributed Job Queue built on KeyStore – implementation
 */

 #define _GNU_SOURCE   /* strdup, timegm */

 #include "nats_job_queue.h"
 
 #include <ctype.h>
 #include <inttypes.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 
 /* ------------------------------------------------------------------ */
 /*  Internal struct                                                    */
 /* ------------------------------------------------------------------ */
 
 struct JobQueue {
     KeyStore *ks;           /* borrowed – caller manages lifetime */
     char      worker_id[64];
 };
 
 /* ------------------------------------------------------------------ */
 /*  Utility: ISO 8601 timestamp (UTC)                                  */
 /* ------------------------------------------------------------------ */
 
 static void iso_now(char *buf, size_t len)
 {
     time_t t = time(NULL);
     struct tm tm;
     gmtime_r(&t, &tm);
     strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &tm);
 }
 
 /* Parse an ISO timestamp to time_t (UTC). Returns (time_t)-1 on error. */
 static time_t iso_to_time(const char *s)
 {
     if (!s || !*s) return (time_t)-1;
     struct tm tm;
     memset(&tm, 0, sizeof(tm));
     if (sscanf(s, "%d-%d-%dT%d:%d:%d",
                &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                &tm.tm_hour, &tm.tm_min, &tm.tm_sec) != 6)
         return (time_t)-1;
     tm.tm_year -= 1900;
     tm.tm_mon  -= 1;
     return timegm(&tm);
 }
 
 /* ------------------------------------------------------------------ */
 /*  Utility: simple pseudo-UUID (hex)                                  */
 /* ------------------------------------------------------------------ */
 
 static void gen_uuid(char *buf, size_t len)
 {
     /* 32 hex chars from random bytes – not cryptographic, good enough */
     static int seeded = 0;
     if (!seeded) { srand((unsigned)time(NULL) ^ (unsigned)clock()); seeded = 1; }
 
     const char hex[] = "0123456789abcdef";
     size_t n = (len - 1 < 32) ? len - 1 : 32;
     for (size_t i = 0; i < n; i++)
         buf[i] = hex[rand() % 16];
     buf[n] = '\0';
 }
 
 /* ------------------------------------------------------------------ */
 /*  JobStatus string conversion                                        */
 /* ------------------------------------------------------------------ */
 
 static const char *status_strings[] = {
     "pending", "running", "completed", "failed", "cancelled", "retrying"
 };
 
 const char *job_status_str(JobStatus st)
 {
     if (st >= 0 && st <= JOB_RETRYING)
         return status_strings[st];
     return "unknown";
 }
 
 JobStatus job_status_from_str(const char *s)
 {
     if (!s) return JOB_PENDING;
     for (int i = 0; i <= JOB_RETRYING; i++) {
         if (strcmp(s, status_strings[i]) == 0)
             return (JobStatus)i;
     }
     return JOB_PENDING;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Job helpers                                                        */
 /* ------------------------------------------------------------------ */
 
 void job_init(Job *job)
 {
     if (!job) return;
     memset(job, 0, sizeof(*job));
     gen_uuid(job->id, sizeof(job->id));
     snprintf(job->queue, sizeof(job->queue), "default");
     job->status         = JOB_PENDING;
     job->max_retries    = 3;
     job->timeout_seconds = 300;
     iso_now(job->created_at, sizeof(job->created_at));
 }
 
 void job_free(Job *job)
 {
     if (!job) return;
     free(job->payload_json);  job->payload_json = NULL;
     free(job->error);         job->error        = NULL;
     free(job->result_json);   job->result_json  = NULL;
 }
 
 char *job_to_json(const Job *job)
 {
     if (!job) return NULL;
 
     cJSON *obj = cJSON_CreateObject();
     if (!obj) return NULL;
 
     cJSON_AddStringToObject(obj, "id",             job->id);
     cJSON_AddStringToObject(obj, "queue",          job->queue);
     cJSON_AddStringToObject(obj, "status",         job_status_str(job->status));
     cJSON_AddNumberToObject(obj, "priority",       job->priority);
     cJSON_AddNumberToObject(obj, "max_retries",    job->max_retries);
     cJSON_AddNumberToObject(obj, "retry_count",    job->retry_count);
     cJSON_AddStringToObject(obj, "created_at",     job->created_at);
     cJSON_AddStringToObject(obj, "started_at",     job->started_at);
     cJSON_AddStringToObject(obj, "completed_at",   job->completed_at);
     cJSON_AddStringToObject(obj, "worker_id",      job->worker_id);
     cJSON_AddNumberToObject(obj, "timeout_seconds", job->timeout_seconds);
 
     /* payload – embed as object if valid JSON, otherwise as string */
     if (job->payload_json && job->payload_json[0]) {
         cJSON *p = cJSON_Parse(job->payload_json);
         if (p)
             cJSON_AddItemToObject(obj, "payload", p);
         else
             cJSON_AddStringToObject(obj, "payload", job->payload_json);
     } else {
         cJSON_AddObjectToObject(obj, "payload");
     }
 
     /* error */
     if (job->error && job->error[0])
         cJSON_AddStringToObject(obj, "error", job->error);
     else
         cJSON_AddNullToObject(obj, "error");
 
     /* result */
     if (job->result_json && job->result_json[0]) {
         cJSON *r = cJSON_Parse(job->result_json);
         if (r)
             cJSON_AddItemToObject(obj, "result", r);
         else
             cJSON_AddStringToObject(obj, "result", job->result_json);
     } else {
         cJSON_AddNullToObject(obj, "result");
     }
 
     char *json = cJSON_PrintUnformatted(obj);
     cJSON_Delete(obj);
     return json;
 }
 
 static char *cjson_get_string(cJSON *obj, const char *key)
 {
     cJSON *item = cJSON_GetObjectItem(obj, key);
     if (item && cJSON_IsString(item) && item->valuestring)
         return item->valuestring;
     return NULL;
 }
 
 static int cjson_get_int(cJSON *obj, const char *key, int def)
 {
     cJSON *item = cJSON_GetObjectItem(obj, key);
     if (item && cJSON_IsNumber(item))
         return item->valueint;
     return def;
 }
 
 ks_status_t job_from_json(const char *json, Job *out)
 {
     if (!json || !out)
         return KS_ERR_INVALID_ARG;
 
     memset(out, 0, sizeof(*out));
 
     cJSON *obj = cJSON_Parse(json);
     if (!obj)
         return KS_ERR_DECODE;
 
     /* Required string fields */
     const char *s;
 
     s = cjson_get_string(obj, "id");
     if (s) snprintf(out->id, sizeof(out->id), "%s", s);
 
     s = cjson_get_string(obj, "queue");
     if (s) snprintf(out->queue, sizeof(out->queue), "%s", s);
     else   snprintf(out->queue, sizeof(out->queue), "default");
 
     s = cjson_get_string(obj, "status");
     out->status = s ? job_status_from_str(s) : JOB_PENDING;
 
     out->priority        = cjson_get_int(obj, "priority", 0);
     out->max_retries     = cjson_get_int(obj, "max_retries", 3);
     out->retry_count     = cjson_get_int(obj, "retry_count", 0);
     out->timeout_seconds = cjson_get_int(obj, "timeout_seconds", 300);
 
     s = cjson_get_string(obj, "created_at");
     if (s) snprintf(out->created_at, sizeof(out->created_at), "%s", s);
 
     s = cjson_get_string(obj, "started_at");
     if (s) snprintf(out->started_at, sizeof(out->started_at), "%s", s);
 
     s = cjson_get_string(obj, "completed_at");
     if (s) snprintf(out->completed_at, sizeof(out->completed_at), "%s", s);
 
     s = cjson_get_string(obj, "worker_id");
     if (s) snprintf(out->worker_id, sizeof(out->worker_id), "%s", s);
 
     /* payload – store as JSON string regardless */
     cJSON *payload = cJSON_GetObjectItem(obj, "payload");
     if (payload) {
         char *ps = cJSON_PrintUnformatted(payload);
         out->payload_json = ps;  /* may be NULL on OOM */
     }
 
     /* error */
     s = cjson_get_string(obj, "error");
     if (s) out->error = strdup(s);
 
     /* result */
     cJSON *result = cJSON_GetObjectItem(obj, "result");
     if (result && !cJSON_IsNull(result)) {
         char *rs = cJSON_PrintUnformatted(result);
         out->result_json = rs;
     }
 
     cJSON_Delete(obj);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  JobQueue create / destroy                                          */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_create(JobQueue **out, KeyStore *ks, const char *worker_id)
 {
     if (!out || !ks)
         return KS_ERR_INVALID_ARG;
 
     JobQueue *jq = calloc(1, sizeof(*jq));
     if (!jq)
         return KS_ERR_MEMORY;
 
     jq->ks = ks;
 
     if (worker_id && *worker_id) {
         snprintf(jq->worker_id, sizeof(jq->worker_id), "%s", worker_id);
     } else {
         char hex[16];
         gen_uuid(hex, sizeof(hex));
         hex[8] = '\0';
         snprintf(jq->worker_id, sizeof(jq->worker_id), "worker-%s", hex);
     }
 
     *out = jq;
     return KS_OK;
 }
 
 void jq_destroy(JobQueue *jq)
 {
     free(jq);
 }
 
 const char *jq_worker_id(const JobQueue *jq)
 {
     return jq ? jq->worker_id : NULL;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Submit                                                             */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_submit(JobQueue *jq,
                       const char *payload_json,
                       const char *queue,
                       int priority,
                       int max_retries,
                       int timeout_sec,
                       char **job_id_out)
 {
     if (!jq || !payload_json)
         return KS_ERR_INVALID_ARG;
 
     if (!queue || !*queue) queue = "default";
 
     Job job;
     job_init(&job);
     snprintf(job.queue, sizeof(job.queue), "%s", queue);
     job.priority         = priority;
     job.max_retries      = max_retries;
     job.timeout_seconds  = timeout_sec > 0 ? timeout_sec : 300;
     job.payload_json     = strdup(payload_json);
     if (!job.payload_json) return KS_ERR_MEMORY;
 
     /* Serialize and store job data */
     char *json = job_to_json(&job);
     if (!json) { job_free(&job); return KS_ERR_ENCODE; }
 
     char job_key[128];
     snprintf(job_key, sizeof(job_key), "job.%s", job.id);
 
     ks_status_t st = ks_put(jq->ks, job_key, json, NULL);
     free(json);
     if (st != KS_OK) { job_free(&job); return st; }
 
     /* Add to queue with priority encoding: 999999 - priority so that
        lexicographic sort gives highest priority first */
     char queue_key[256];
     snprintf(queue_key, sizeof(queue_key), "queue.%s.%06d.%s",
              queue, 1000000 - priority, job.id);
     st = ks_put(jq->ks, queue_key, job.id, NULL);
     if (st != KS_OK) { job_free(&job); return st; }
 
     /* Update stats counter */
     char stat_key[128];
     snprintf(stat_key, sizeof(stat_key), "stats.queue.%s.pending", queue);
     ks_increment(jq->ks, stat_key, 1, NULL);
 
     /* Return the job id */
     if (job_id_out) {
         *job_id_out = strdup(job.id);
         if (!*job_id_out) { job_free(&job); return KS_ERR_MEMORY; }
     }
 
     job_free(&job);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Get job                                                            */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_get_job(JobQueue *jq, const char *job_id, Job *out)
 {
     if (!jq || !job_id || !out)
         return KS_ERR_INVALID_ARG;
 
     char key[128];
     snprintf(key, sizeof(key), "job.%s", job_id);
 
     char *json = NULL;
     ks_status_t st = ks_get(jq->ks, key, &json);
     if (st != KS_OK)
         return st;
 
     st = job_from_json(json, out);
     free(json);
     return st;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Cancel job                                                         */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_cancel_job(JobQueue *jq, const char *job_id, bool *cancelled)
 {
     if (!jq || !job_id || !cancelled)
         return KS_ERR_INVALID_ARG;
 
     *cancelled = false;
 
     Job job;
     ks_status_t st = jq_get_job(jq, job_id, &job);
     if (st != KS_OK) return st;
 
     if (job.status != JOB_PENDING) {
         job_free(&job);
         return KS_OK;  /* not cancellable, but not an error */
     }
 
     /* Update status */
     job.status = JOB_CANCELLED;
     char *json = job_to_json(&job);
     if (!json) { job_free(&job); return KS_ERR_ENCODE; }
 
     char job_key[128];
     snprintf(job_key, sizeof(job_key), "job.%s", job_id);
     st = ks_put(jq->ks, job_key, json, NULL);
     free(json);
     if (st != KS_OK) { job_free(&job); return st; }
 
     /* Remove from queue – find matching queue entry */
     char pattern[256];
     snprintf(pattern, sizeof(pattern), "queue.%s.*.%s", job.queue, job_id);
 
     char **keys = NULL;
     size_t count = 0;
     ks_keys(jq->ks, pattern, &keys, &count);
     for (size_t i = 0; i < count; i++)
         ks_delete(jq->ks, keys[i]);
     ks_free_keys(keys, count);
 
     /* Update stats */
     char stat_key[128];
     snprintf(stat_key, sizeof(stat_key), "stats.queue.%s.pending", job.queue);
     ks_decrement(jq->ks, stat_key, 1, NULL);
     snprintf(stat_key, sizeof(stat_key), "stats.queue.%s.cancelled", job.queue);
     ks_increment(jq->ks, stat_key, 1, NULL);
 
     *cancelled = true;
     job_free(&job);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Claim job                                                          */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_claim_job(JobQueue *jq,
                          const char **queues, size_t num_queues,
                          Job *out)
 {
     if (!jq || !out)
         return KS_ERR_INVALID_ARG;
 
     /* Default queue */
     const char *default_q[] = {"default"};
     if (!queues || num_queues == 0) {
         queues     = default_q;
         num_queues = 1;
     }
 
     memset(out, 0, sizeof(*out));
 
     for (size_t q = 0; q < num_queues; q++) {
         char pattern[128];
         snprintf(pattern, sizeof(pattern), "queue.%s.*", queues[q]);
 
         char **queue_keys = NULL;
         size_t qk_count = 0;
         ks_status_t st = ks_keys(jq->ks, pattern, &queue_keys, &qk_count);
         if (st != KS_OK || qk_count == 0) {
             ks_free_keys(queue_keys, qk_count);
             continue;
         }
 
         /* Keys are sorted lexicographically – lowest number = highest priority */
         for (size_t i = 0; i < qk_count; i++) {
             char *val = NULL;
             st = ks_get(jq->ks, queue_keys[i], &val);
             if (st != KS_OK || !val)
                 continue;
 
             /* val is the job_id */
             Job job;
             st = jq_get_job(jq, val, &job);
             if (st != KS_OK || job.status != JOB_PENDING) {
                 free(val);
                 job_free(&job);
                 continue;
             }
 
             /* Claim it */
             job.status = JOB_RUNNING;
             snprintf(job.worker_id, sizeof(job.worker_id), "%s", jq->worker_id);
             iso_now(job.started_at, sizeof(job.started_at));
 
             char *json = job_to_json(&job);
             if (!json) { free(val); job_free(&job); continue; }
 
             char job_key[128];
             snprintf(job_key, sizeof(job_key), "job.%s", val);
             st = ks_put(jq->ks, job_key, json, NULL);
             free(json);
 
             if (st != KS_OK) { free(val); job_free(&job); continue; }
 
             /* Remove from queue */
             ks_delete(jq->ks, queue_keys[i]);
 
             /* Update stats */
             char stat_key[128];
             snprintf(stat_key, sizeof(stat_key),
                      "stats.queue.%s.pending", queues[q]);
             ks_decrement(jq->ks, stat_key, 1, NULL);
             snprintf(stat_key, sizeof(stat_key),
                      "stats.queue.%s.running", queues[q]);
             ks_increment(jq->ks, stat_key, 1, NULL);
 
             /* Worker tracking */
             char wk[128];
             snprintf(wk, sizeof(wk), "worker.%s.current_job", jq->worker_id);
             ks_put(jq->ks, wk, val, NULL);
 
             char ts[32];
             iso_now(ts, sizeof(ts));
             snprintf(wk, sizeof(wk), "worker.%s.last_seen", jq->worker_id);
             ks_put(jq->ks, wk, ts, NULL);
 
             /* Copy to output */
             *out = job;
             /* job's heap pointers are now owned by *out */
 
             free(val);
             ks_free_keys(queue_keys, qk_count);
             return KS_OK;
         }
 
         ks_free_keys(queue_keys, qk_count);
     }
 
     return KS_ERR_NOT_FOUND;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Complete job                                                       */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_complete_job(JobQueue *jq, const char *job_id,
                             const char *result_json, bool *ok)
 {
     if (!jq || !job_id || !ok)
         return KS_ERR_INVALID_ARG;
     *ok = false;
 
     Job job;
     ks_status_t st = jq_get_job(jq, job_id, &job);
     if (st != KS_OK) return st;
 
     if (job.status != JOB_RUNNING ||
         strcmp(job.worker_id, jq->worker_id) != 0) {
         job_free(&job);
         return KS_OK;
     }
 
     job.status = JOB_COMPLETED;
     iso_now(job.completed_at, sizeof(job.completed_at));
 
     free(job.result_json);
     job.result_json = result_json ? strdup(result_json) : NULL;
 
     char *json = job_to_json(&job);
     if (!json) { job_free(&job); return KS_ERR_ENCODE; }
 
     char job_key[128];
     snprintf(job_key, sizeof(job_key), "job.%s", job_id);
     st = ks_put(jq->ks, job_key, json, NULL);
     free(json);
     if (st != KS_OK) { job_free(&job); return st; }
 
     /* Clear worker current_job */
     char wk[128];
     snprintf(wk, sizeof(wk), "worker.%s.current_job", jq->worker_id);
     ks_delete(jq->ks, wk);
 
     /* Update stats */
     char stat_key[128];
     snprintf(stat_key, sizeof(stat_key), "stats.queue.%s.running", job.queue);
     ks_decrement(jq->ks, stat_key, 1, NULL);
     snprintf(stat_key, sizeof(stat_key), "stats.queue.%s.completed", job.queue);
     ks_increment(jq->ks, stat_key, 1, NULL);
 
     *ok = true;
     job_free(&job);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Fail job                                                           */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_fail_job(JobQueue *jq, const char *job_id,
                         const char *error, bool *ok)
 {
     if (!jq || !job_id || !ok)
         return KS_ERR_INVALID_ARG;
     *ok = false;
 
     Job job;
     ks_status_t st = jq_get_job(jq, job_id, &job);
     if (st != KS_OK) return st;
 
     if (job.status != JOB_RUNNING ||
         strcmp(job.worker_id, jq->worker_id) != 0) {
         job_free(&job);
         return KS_OK;
     }
 
     job.retry_count++;
 
     if (job.retry_count < job.max_retries) {
         /* Re-queue for retry */
         free(job.error);
         char errbuf[512];
         snprintf(errbuf, sizeof(errbuf), "Retry %d/%d: %s",
                  job.retry_count, job.max_retries, error ? error : "unknown");
         job.error = strdup(errbuf);
 
         job.status = JOB_PENDING;
         job.worker_id[0]  = '\0';
         job.started_at[0] = '\0';
 
         /* Re-add to queue */
         char queue_key[256];
         snprintf(queue_key, sizeof(queue_key), "queue.%s.%06d.%s",
                  job.queue, 1000000 - job.priority, job.id);
         ks_put(jq->ks, queue_key, job.id, NULL);
 
         /* Stats: running-- pending++ */
         char sk[128];
         snprintf(sk, sizeof(sk), "stats.queue.%s.running", job.queue);
         ks_decrement(jq->ks, sk, 1, NULL);
         snprintf(sk, sizeof(sk), "stats.queue.%s.pending", job.queue);
         ks_increment(jq->ks, sk, 1, NULL);
     } else {
         /* Permanently failed */
         job.status = JOB_FAILED;
         free(job.error);
         job.error = error ? strdup(error) : strdup("max retries exceeded");
         iso_now(job.completed_at, sizeof(job.completed_at));
 
         /* Stats: running-- failed++ */
         char sk[128];
         snprintf(sk, sizeof(sk), "stats.queue.%s.running", job.queue);
         ks_decrement(jq->ks, sk, 1, NULL);
         snprintf(sk, sizeof(sk), "stats.queue.%s.failed", job.queue);
         ks_increment(jq->ks, sk, 1, NULL);
     }
 
     /* Save updated job */
     char *json = job_to_json(&job);
     if (!json) { job_free(&job); return KS_ERR_ENCODE; }
 
     char job_key[128];
     snprintf(job_key, sizeof(job_key), "job.%s", job.id);
     st = ks_put(jq->ks, job_key, json, NULL);
     free(json);
 
     /* Clear worker current_job */
     char wk[128];
     snprintf(wk, sizeof(wk), "worker.%s.current_job", jq->worker_id);
     ks_delete(jq->ks, wk);
 
     *ok = true;
     job_free(&job);
     return st;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Statistics                                                         */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_get_stats(JobQueue *jq, const char *queue, JqStats *stats)
 {
     if (!jq || !queue || !stats)
         return KS_ERR_INVALID_ARG;
 
     memset(stats, 0, sizeof(*stats));
 
     const char *names[] = {"pending", "running", "completed", "failed", "cancelled"};
     int64_t *ptrs[]     = {&stats->pending, &stats->running, &stats->completed,
                            &stats->failed, &stats->cancelled};
 
     for (int i = 0; i < 5; i++) {
         char key[128];
         snprintf(key, sizeof(key), "stats.queue.%s.%s", queue, names[i]);
         char *val = NULL;
         if (ks_get(jq->ks, key, &val) == KS_OK && val) {
             *ptrs[i] = strtoll(val, NULL, 10);
             free(val);
         }
     }
 
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Active workers                                                     */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_get_active_workers(JobQueue *jq, int staleness_sec,
                                   JqWorkerInfo **workers, size_t *count)
 {
     if (!jq || !workers || !count)
         return KS_ERR_INVALID_ARG;
 
     *workers = NULL;
     *count = 0;
 
     char **keys = NULL;
     size_t nkeys = 0;
     ks_status_t st = ks_keys(jq->ks, "worker.*.last_seen", &keys, &nkeys);
     if (st != KS_OK || nkeys == 0) {
         ks_free_keys(keys, nkeys);
         return KS_OK;
     }
 
     time_t now = time(NULL);
     JqWorkerInfo *arr = calloc(nkeys, sizeof(*arr));
     if (!arr) { ks_free_keys(keys, nkeys); return KS_ERR_MEMORY; }
 
     size_t n = 0;
     for (size_t i = 0; i < nkeys; i++) {
         /* Extract worker_id from "worker.<id>.last_seen" */
         const char *p = keys[i] + 7; /* skip "worker." */
         const char *dot = strrchr(p, '.');
         if (!dot) continue;
         size_t wid_len = (size_t)(dot - p);
         if (wid_len >= sizeof(arr[0].worker_id)) continue;
 
         char wid[64];
         memcpy(wid, p, wid_len);
         wid[wid_len] = '\0';
 
         char *last_seen_val = NULL;
         ks_get(jq->ks, keys[i], &last_seen_val);
         if (!last_seen_val) continue;
 
         /* Check staleness */
         time_t ts = iso_to_time(last_seen_val);
         if (ts == (time_t)-1 || difftime(now, ts) > staleness_sec) {
             free(last_seen_val);
             continue;
         }
 
         snprintf(arr[n].worker_id, sizeof(arr[n].worker_id), "%s", wid);
         snprintf(arr[n].last_seen, sizeof(arr[n].last_seen), "%s", last_seen_val);
         free(last_seen_val);
 
         /* Get current job */
         char cj_key[128];
         snprintf(cj_key, sizeof(cj_key), "worker.%s.current_job", wid);
         char *cj_val = NULL;
         if (ks_get(jq->ks, cj_key, &cj_val) == KS_OK && cj_val) {
             snprintf(arr[n].current_job, sizeof(arr[n].current_job), "%s", cj_val);
             free(cj_val);
         }
 
         n++;
     }
 
     ks_free_keys(keys, nkeys);
 
     *workers = arr;
     *count = n;
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Cleanup stale jobs                                                 */
 /* ------------------------------------------------------------------ */
 
 ks_status_t jq_cleanup_stale_jobs(JobQueue *jq, int timeout_sec,
                                   int *cleaned)
 {
     if (!jq || !cleaned)
         return KS_ERR_INVALID_ARG;
     *cleaned = 0;
 
     time_t now = time(NULL);
 
     char **keys = NULL;
     size_t nkeys = 0;
     ks_status_t st = ks_keys(jq->ks, "job.*", &keys, &nkeys);
     if (st != KS_OK || nkeys == 0) {
         ks_free_keys(keys, nkeys);
         return KS_OK;
     }
 
     for (size_t i = 0; i < nkeys; i++) {
         char *json = NULL;
         if (ks_get(jq->ks, keys[i], &json) != KS_OK || !json)
             continue;
 
         Job job;
         if (job_from_json(json, &job) != KS_OK) { free(json); continue; }
         free(json);
 
         if (job.status != JOB_RUNNING || !job.started_at[0]) {
             job_free(&job);
             continue;
         }
 
         time_t started = iso_to_time(job.started_at);
         if (started == (time_t)-1 || difftime(now, started) <= timeout_sec) {
             job_free(&job);
             continue;
         }
 
         /* Stale – reset to pending */
         job.status = JOB_PENDING;
         job.worker_id[0]  = '\0';
         job.started_at[0] = '\0';
         free(job.error);
         job.error = strdup("Reset due to stale worker");
 
         char *new_json = job_to_json(&job);
         if (new_json) {
             char job_key[128];
             snprintf(job_key, sizeof(job_key), "job.%s", job.id);
             ks_put(jq->ks, job_key, new_json, NULL);
             free(new_json);
 
             /* Re-add to queue */
             char queue_key[256];
             snprintf(queue_key, sizeof(queue_key), "queue.%s.%06d.%s",
                      job.queue, 1000000 - job.priority, job.id);
             ks_put(jq->ks, queue_key, job.id, NULL);
 
             (*cleaned)++;
         }
 
         job_free(&job);
     }
 
     ks_free_keys(keys, nkeys);
     return KS_OK;
 }