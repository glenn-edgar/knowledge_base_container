/**
 * @file test_job_queue.c
 * @brief Test driver for nats_job_queue module.
 *
 * Requires a running NATS server at 127.0.0.1:4222 with JetStream:
 *   docker run -p 4222:4222 nats:latest -js
 *
 * Usage:
 *   ./test_job_queue              # run all tests
 *   ./test_job_queue tests        # tests only
 *   ./test_job_queue demo         # interactive demo
 */

 #define _POSIX_C_SOURCE 200809L

 #include <inttypes.h>
 #include <stdbool.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 #include <unistd.h>
 
 #include "nats_key_store.h"
 #include "nats_job_queue.h"
 #include <cjson/cJSON.h>
 
 /* ------------------------------------------------------------------ */
 /*  Minimal test framework                                             */
 /* ------------------------------------------------------------------ */
 
 static int tests_run    = 0;
 static int tests_passed = 0;
 static int tests_failed = 0;
 
 #define TEST_SERVER "nats://127.0.0.1:4222"
 
 #define RUN_TEST(fn)                                               \
     do {                                                           \
         tests_run++;                                               \
         printf("  %-50s ", #fn);                                   \
         fflush(stdout);                                            \
         if (fn()) {                                                \
             tests_passed++;                                        \
             printf("[PASS]\n");                                    \
         } else {                                                   \
             tests_failed++;                                        \
             printf("[FAIL]\n");                                    \
         }                                                          \
     } while (0)
 
 #define EXPECT(cond)                                               \
     do {                                                           \
         if (!(cond)) {                                             \
             fprintf(stderr, "    FAIL at %s:%d: %s\n",             \
                     __FILE__, __LINE__, #cond);                    \
             return false;                                          \
         }                                                          \
     } while (0)
 
 #define EXPECT_OK(st) EXPECT((st) == KS_OK)
 
 /* ------------------------------------------------------------------ */
 /*  Helpers                                                            */
 /* ------------------------------------------------------------------ */
 
 static void cleanup_bucket(KeyStore *ks)
 {
     char **keys = NULL;
     size_t count = 0;
     if (ks_keys(ks, NULL, &keys, &count) == KS_OK) {
         for (size_t i = 0; i < count; i++)
             ks_delete(ks, keys[i]);
         ks_free_keys(keys, count);
     }
 }
 
 static KeyStore *make_ks(const char *bucket)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = bucket;
     KeyStore *ks = NULL;
     ks_create(&ks, &cfg);
     return ks;
 }
 
 /* ================================================================== */
 /*  Job serialization tests                                            */
 /* ================================================================== */
 
 static bool test_job_init_and_serialize(void)
 {
     Job job;
     job_init(&job);
 
     EXPECT(strlen(job.id) > 0);
     EXPECT(strcmp(job.queue, "default") == 0);
     EXPECT(job.status == JOB_PENDING);
     EXPECT(job.max_retries == 3);
     EXPECT(strlen(job.created_at) > 0);
 
     job.payload_json = strdup("{\"task\":\"test\",\"value\":42}");
     job.priority = 5;
 
     char *json = job_to_json(&job);
     EXPECT(json != NULL);
     EXPECT(strstr(json, "pending") != NULL);
     EXPECT(strstr(json, "test") != NULL);
 
     /* Round-trip */
     Job job2;
     EXPECT_OK(job_from_json(json, &job2));
     EXPECT(strcmp(job2.id, job.id) == 0);
     EXPECT(strcmp(job2.queue, "default") == 0);
     EXPECT(job2.status == JOB_PENDING);
     EXPECT(job2.priority == 5);
     EXPECT(job2.payload_json != NULL);
     EXPECT(strstr(job2.payload_json, "test") != NULL);
 
     free(json);
     job_free(&job);
     job_free(&job2);
     return true;
 }
 
 static bool test_job_status_strings(void)
 {
     EXPECT(strcmp(job_status_str(JOB_PENDING), "pending") == 0);
     EXPECT(strcmp(job_status_str(JOB_RUNNING), "running") == 0);
     EXPECT(strcmp(job_status_str(JOB_COMPLETED), "completed") == 0);
     EXPECT(strcmp(job_status_str(JOB_FAILED), "failed") == 0);
     EXPECT(strcmp(job_status_str(JOB_CANCELLED), "cancelled") == 0);
     EXPECT(strcmp(job_status_str(JOB_RETRYING), "retrying") == 0);
 
     EXPECT(job_status_from_str("pending") == JOB_PENDING);
     EXPECT(job_status_from_str("running") == JOB_RUNNING);
     EXPECT(job_status_from_str("completed") == JOB_COMPLETED);
     EXPECT(job_status_from_str("failed") == JOB_FAILED);
 
     return true;
 }
 
 /* ================================================================== */
 /*  Submit and get tests                                               */
 /* ================================================================== */
 
 static bool test_submit_and_get(void)
 {
     KeyStore *ks = make_ks("test_jq_submit");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     char *job_id = NULL;
     EXPECT_OK(jq_submit(jq, "{\"task\":\"test\",\"data\":123}",
                         "test", 5, 3, 300, &job_id));
     EXPECT(job_id != NULL);
     EXPECT(strlen(job_id) > 0);
 
     Job job;
     EXPECT_OK(jq_get_job(jq, job_id, &job));
     EXPECT(job.status == JOB_PENDING);
     EXPECT(job.priority == 5);
     EXPECT(job.payload_json != NULL);
     EXPECT(strstr(job.payload_json, "test") != NULL);
 
     job_free(&job);
     free(job_id);
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Claim and complete tests                                           */
 /* ================================================================== */
 
 static bool test_claim_and_complete(void)
 {
     KeyStore *ks = make_ks("test_jq_claim");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     char *job_id = NULL;
     EXPECT_OK(jq_submit(jq, "{\"task\":\"process\"}",
                         "test", 0, 3, 300, &job_id));
 
     /* Claim */
     Job claimed;
     const char *queues[] = {"test"};
     EXPECT_OK(jq_claim_job(jq, queues, 1, &claimed));
     EXPECT(strcmp(claimed.id, job_id) == 0);
     EXPECT(claimed.status == JOB_RUNNING);
     EXPECT(strcmp(claimed.worker_id, "test-worker") == 0);
     EXPECT(strlen(claimed.started_at) > 0);
 
     /* Complete */
     bool ok = false;
     EXPECT_OK(jq_complete_job(jq, job_id, "{\"output\":42}", &ok));
     EXPECT(ok == true);
 
     /* Verify completed */
     Job done;
     EXPECT_OK(jq_get_job(jq, job_id, &done));
     EXPECT(done.status == JOB_COMPLETED);
     EXPECT(done.result_json != NULL);
     EXPECT(strstr(done.result_json, "42") != NULL);
     EXPECT(strlen(done.completed_at) > 0);
 
     job_free(&claimed);
     job_free(&done);
     free(job_id);
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Priority ordering test                                             */
 /* ================================================================== */
 
 static bool test_priority_ordering(void)
 {
     KeyStore *ks = make_ks("test_jq_prio");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     /* Submit with priorities: 1, 10, 5, 8 */
     int prios[] = {1, 10, 5, 8};
     for (int i = 0; i < 4; i++) {
         char payload[64];
         snprintf(payload, sizeof(payload), "{\"priority\":%d}", prios[i]);
         EXPECT_OK(jq_submit(jq, payload, "prio_test", prios[i], 3, 300, NULL));
     }
 
     /* Claim all 4 and verify descending priority order */
     int claimed_prios[4] = {0};
     const char *queues[] = {"prio_test"};
     for (int i = 0; i < 4; i++) {
         Job job;
         ks_status_t st = jq_claim_job(jq, queues, 1, &job);
         EXPECT(st == KS_OK);
         claimed_prios[i] = job.priority;
         /* Complete the job so worker can claim next */
         bool ok;
         jq_complete_job(jq, job.id, NULL, &ok);
         job_free(&job);
     }
 
     EXPECT(claimed_prios[0] == 10);
     EXPECT(claimed_prios[1] == 8);
     EXPECT(claimed_prios[2] == 5);
     EXPECT(claimed_prios[3] == 1);
 
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Cancel test                                                        */
 /* ================================================================== */
 
 static bool test_cancel_job(void)
 {
     KeyStore *ks = make_ks("test_jq_cancel");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     char *job_id = NULL;
     EXPECT_OK(jq_submit(jq, "{\"task\":\"cancel_me\"}",
                         "test", 0, 3, 300, &job_id));
 
     bool cancelled = false;
     EXPECT_OK(jq_cancel_job(jq, job_id, &cancelled));
     EXPECT(cancelled == true);
 
     Job job;
     EXPECT_OK(jq_get_job(jq, job_id, &job));
     EXPECT(job.status == JOB_CANCELLED);
 
     /* Cannot cancel again */
     cancelled = false;
     EXPECT_OK(jq_cancel_job(jq, job_id, &cancelled));
     EXPECT(cancelled == false);
 
     job_free(&job);
     free(job_id);
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Retry on failure test                                              */
 /* ================================================================== */
 
 static bool test_retry_on_failure(void)
 {
     KeyStore *ks = make_ks("test_jq_retry");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     char *job_id = NULL;
     EXPECT_OK(jq_submit(jq, "{\"task\":\"retry_test\"}",
                         "test", 0, 2, 300, &job_id));
 
     const char *queues[] = {"test"};
 
     /* First attempt – fail */
     Job job;
     EXPECT_OK(jq_claim_job(jq, queues, 1, &job));
     bool ok;
     EXPECT_OK(jq_fail_job(jq, job_id, "First failure", &ok));
     EXPECT(ok == true);
     job_free(&job);
 
     /* Job should be back to pending with retry_count=1 */
     EXPECT_OK(jq_get_job(jq, job_id, &job));
     EXPECT(job.status == JOB_PENDING);
     EXPECT(job.retry_count == 1);
     job_free(&job);
 
     /* Second attempt – fail again */
     EXPECT_OK(jq_claim_job(jq, queues, 1, &job));
     EXPECT_OK(jq_fail_job(jq, job_id, "Second failure", &ok));
     EXPECT(ok == true);
     job_free(&job);
 
     /* Should now be permanently failed (max_retries=2, retry_count=2) */
     EXPECT_OK(jq_get_job(jq, job_id, &job));
     EXPECT(job.status == JOB_FAILED);
     EXPECT(job.retry_count == 2);
     EXPECT(job.error != NULL);
     job_free(&job);
 
     free(job_id);
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Queue statistics test                                              */
 /* ================================================================== */
 
 static bool test_queue_stats(void)
 {
     KeyStore *ks = make_ks("test_jq_stats");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     /* Submit 2 jobs */
     char *id1 = NULL, *id2 = NULL;
     EXPECT_OK(jq_submit(jq, "{\"task\":1}", "stats_test", 0, 3, 300, &id1));
     EXPECT_OK(jq_submit(jq, "{\"task\":2}", "stats_test", 0, 3, 300, &id2));
 
     JqStats stats;
     EXPECT_OK(jq_get_stats(jq, "stats_test", &stats));
     EXPECT(stats.pending == 2);
 
     /* Claim and complete one */
     const char *queues[] = {"stats_test"};
     Job job;
     EXPECT_OK(jq_claim_job(jq, queues, 1, &job));
     bool ok;
     EXPECT_OK(jq_complete_job(jq, job.id, NULL, &ok));
 
     /* Cancel the other */
     const char *other_id = (strcmp(job.id, id1) == 0) ? id2 : id1;
     bool cancelled;
     EXPECT_OK(jq_cancel_job(jq, other_id, &cancelled));
     EXPECT(cancelled == true);
 
     /* Verify final stats */
     EXPECT_OK(jq_get_stats(jq, "stats_test", &stats));
     EXPECT(stats.completed == 1);
     EXPECT(stats.cancelled == 1);
     EXPECT(stats.pending == 0);
 
     job_free(&job);
     free(id1);
     free(id2);
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Worker monitoring test                                             */
 /* ================================================================== */
 
 static bool test_active_workers(void)
 {
     KeyStore *ks = make_ks("test_jq_workers");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     /* Create 3 workers */
     JobQueue *w1 = NULL, *w2 = NULL, *w3 = NULL;
     EXPECT_OK(jq_create(&w1, ks, "worker-1"));
     EXPECT_OK(jq_create(&w2, ks, "worker-2"));
     EXPECT_OK(jq_create(&w3, ks, "worker-3"));
 
     /* Submit 3 jobs */
     for (int i = 0; i < 3; i++) {
         char p[32];
         snprintf(p, sizeof(p), "{\"task\":%d}", i);
         EXPECT_OK(jq_submit(w1, p, "monitor", 0, 3, 300, NULL));
     }
 
     /* Each worker claims one */
     const char *queues[] = {"monitor"};
     Job j1, j2, j3;
     EXPECT_OK(jq_claim_job(w1, queues, 1, &j1));
     EXPECT_OK(jq_claim_job(w2, queues, 1, &j2));
     EXPECT_OK(jq_claim_job(w3, queues, 1, &j3));
 
     /* Check active workers */
     JqWorkerInfo *workers = NULL;
     size_t wcount = 0;
     EXPECT_OK(jq_get_active_workers(w1, 30, &workers, &wcount));
     EXPECT(wcount == 3);
 
     /* Verify all workers are listed */
     bool found1 = false, found2 = false, found3 = false;
     for (size_t i = 0; i < wcount; i++) {
         if (strcmp(workers[i].worker_id, "worker-1") == 0) found1 = true;
         if (strcmp(workers[i].worker_id, "worker-2") == 0) found2 = true;
         if (strcmp(workers[i].worker_id, "worker-3") == 0) found3 = true;
     }
     EXPECT(found1 && found2 && found3);
 
     free(workers);
     job_free(&j1);
     job_free(&j2);
     job_free(&j3);
     cleanup_bucket(ks);
     jq_destroy(w1);
     jq_destroy(w2);
     jq_destroy(w3);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  No jobs available test                                             */
 /* ================================================================== */
 
 static bool test_claim_empty_queue(void)
 {
     KeyStore *ks = make_ks("test_jq_empty");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     Job job;
     const char *queues[] = {"empty"};
     ks_status_t st = jq_claim_job(jq, queues, 1, &job);
     EXPECT(st == KS_ERR_NOT_FOUND);
 
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Complete by wrong worker test                                      */
 /* ================================================================== */
 
 static bool test_complete_wrong_worker(void)
 {
     KeyStore *ks = make_ks("test_jq_wrong");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *w1 = NULL, *w2 = NULL;
     EXPECT_OK(jq_create(&w1, ks, "worker-1"));
     EXPECT_OK(jq_create(&w2, ks, "worker-2"));
 
     char *job_id = NULL;
     EXPECT_OK(jq_submit(w1, "{\"task\":\"test\"}", "test", 0, 3, 300, &job_id));
 
     /* Worker-1 claims */
     const char *queues[] = {"test"};
     Job job;
     EXPECT_OK(jq_claim_job(w1, queues, 1, &job));
 
     /* Worker-2 tries to complete — should fail (ok=false) */
     bool ok = true;
     EXPECT_OK(jq_complete_job(w2, job_id, NULL, &ok));
     EXPECT(ok == false);
 
     /* Job should still be running */
     Job check;
     EXPECT_OK(jq_get_job(w1, job_id, &check));
     EXPECT(check.status == JOB_RUNNING);
 
     job_free(&job);
     job_free(&check);
     free(job_id);
     cleanup_bucket(ks);
     jq_destroy(w1);
     jq_destroy(w2);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Multiple queues test                                               */
 /* ================================================================== */
 
 static bool test_multiple_queues(void)
 {
     KeyStore *ks = make_ks("test_jq_multi");
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     EXPECT_OK(jq_create(&jq, ks, "test-worker"));
 
     EXPECT_OK(jq_submit(jq, "{\"q\":\"alpha\"}", "alpha", 1, 3, 300, NULL));
     EXPECT_OK(jq_submit(jq, "{\"q\":\"beta\"}",  "beta",  5, 3, 300, NULL));
 
     /* Claim from both queues — alpha checked first, but only has prio 1 */
     const char *queues[] = {"alpha", "beta"};
     Job job;
     EXPECT_OK(jq_claim_job(jq, queues, 2, &job));
     /* Should get alpha first since it's checked first */
     EXPECT(strcmp(job.queue, "alpha") == 0);
     bool ok;
     jq_complete_job(jq, job.id, NULL, &ok);
     job_free(&job);
 
     EXPECT_OK(jq_claim_job(jq, queues, 2, &job));
     EXPECT(strcmp(job.queue, "beta") == 0);
     jq_complete_job(jq, job.id, NULL, &ok);
     job_free(&job);
 
     cleanup_bucket(ks);
     jq_destroy(jq);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  Demo                                                               */
 /* ================================================================== */
 
 static void run_demo(void)
 {
     printf("\n");
     printf("====================================================\n");
     printf("  NATS JobQueue Demo (C)\n");
     printf("====================================================\n");
 
     KeyStore *ks = make_ks("demo_jq");
     if (ks_connect(ks) != KS_OK) {
         fprintf(stderr, "Failed to connect to NATS\n");
         ks_destroy(ks);
         return;
     }
     cleanup_bucket(ks);
 
     JobQueue *jq = NULL;
     jq_create(&jq, ks, "demo-worker");
 
     /* 1. Submit jobs with different priorities */
     printf("\n1. Submit jobs with different priorities:\n");
     const struct { int prio; const char *task; } submissions[] = {
         {10, "urgent-task"},
         { 5, "normal-task"},
         { 1, "low-priority-task"},
         { 8, "high-priority-task"},
     };
     char *submitted_ids[4] = {NULL};
 
     for (int i = 0; i < 4; i++) {
         char payload[64];
         snprintf(payload, sizeof(payload), "{\"task\":\"%s\",\"index\":%d}",
                  submissions[i].task, i);
         jq_submit(jq, payload, "demo", submissions[i].prio, 3, 300,
                   &submitted_ids[i]);
         printf("   Submitted: %s (priority=%d, id=%.8s...)\n",
                submissions[i].task, submissions[i].prio, submitted_ids[i]);
     }
 
     /* 2. Process in priority order */
     printf("\n2. Process jobs in priority order:\n");
     const char *queues[] = {"demo"};
     for (int i = 0; i < 4; i++) {
         Job job;
         if (jq_claim_job(jq, queues, 1, &job) == KS_OK) {
             printf("   Processing: %s (priority=%d)\n",
                    job.payload_json, job.priority);
             bool ok;
             jq_complete_job(jq, job.id, "{\"done\":true}", &ok);
             printf("   ✓ Completed (priority=%d)\n", job.priority);
             job_free(&job);
         }
     }
 
     /* 3. Stats */
     printf("\n3. Queue statistics:\n");
     JqStats stats;
     jq_get_stats(jq, "demo", &stats);
     printf("   pending:   %" PRId64 "\n", stats.pending);
     printf("   running:   %" PRId64 "\n", stats.running);
     printf("   completed: %" PRId64 "\n", stats.completed);
     printf("   failed:    %" PRId64 "\n", stats.failed);
     printf("   cancelled: %" PRId64 "\n", stats.cancelled);
 
     /* 4. Retry demo */
     printf("\n4. Retry on failure:\n");
     char *retry_id = NULL;
     jq_submit(jq, "{\"task\":\"retry-me\"}", "retry_demo", 0, 3, 300, &retry_id);
     printf("   Submitted retry job: %.8s...\n", retry_id);
 
     for (int attempt = 1; attempt <= 2; attempt++) {
         Job job;
         const char *rq[] = {"retry_demo"};
         if (jq_claim_job(jq, rq, 1, &job) == KS_OK) {
             printf("   Attempt %d: Simulating failure...\n", attempt);
             bool ok;
             char err[64];
             snprintf(err, sizeof(err), "Simulated failure %d", attempt);
             jq_fail_job(jq, retry_id, err, &ok);
             job_free(&job);
         }
     }
 
     /* Third attempt succeeds */
     {
         Job job;
         const char *rq[] = {"retry_demo"};
         if (jq_claim_job(jq, rq, 1, &job) == KS_OK) {
             printf("   Attempt 3: Processing successfully...\n");
             bool ok;
             jq_complete_job(jq, job.id, "{\"success\":true}", &ok);
 
             Job final;
             jq_get_job(jq, job.id, &final);
             printf("   ✓ Completed after %d retries\n", final.retry_count);
             job_free(&final);
             job_free(&job);
         }
     }
     free(retry_id);
 
     /* 5. Multi-worker demo */
     printf("\n5. Worker monitoring:\n");
     JobQueue *w2 = NULL, *w3 = NULL;
     jq_create(&w2, ks, "worker-2");
     jq_create(&w3, ks, "worker-3");
 
     for (int i = 0; i < 3; i++) {
         char p[32];
         snprintf(p, sizeof(p), "{\"task\":%d}", i);
         jq_submit(jq, p, "monitor", 0, 3, 300, NULL);
     }
 
     const char *mq[] = {"monitor"};
     Job mj1, mj2, mj3;
     jq_claim_job(jq, mq, 1, &mj1);
     jq_claim_job(w2, mq, 1, &mj2);
     jq_claim_job(w3, mq, 1, &mj3);
 
     JqWorkerInfo *workers = NULL;
     size_t wcount = 0;
     jq_get_active_workers(jq, 30, &workers, &wcount);
     printf("   Active workers: %zu\n", wcount);
     for (size_t i = 0; i < wcount; i++) {
         const char *jinfo = workers[i].current_job[0]
                             ? workers[i].current_job : "idle";
         printf("   - %s: job %.8s...\n", workers[i].worker_id, jinfo);
     }
     free(workers);
 
     job_free(&mj1);
     job_free(&mj2);
     job_free(&mj3);
     jq_destroy(w2);
     jq_destroy(w3);
 
     /* 6. Cleanup */
     printf("\n6. Cleanup:\n");
     cleanup_bucket(ks);
     printf("   Done.\n");
 
     for (int i = 0; i < 4; i++)
         free(submitted_ids[i]);
 
     jq_destroy(jq);
     ks_destroy(ks);
 
     printf("\n====================================================\n");
 }
 
 /* ================================================================== */
 /*  Main                                                               */
 /* ================================================================== */
 
 int main(int argc, char **argv)
 {
     const char *mode = (argc > 1) ? argv[1] : "all";
 
     printf("\n======================================================================\n");
     printf("  NATS JobQueue Test Suite (C)\n");
     printf("  Server: %s\n", TEST_SERVER);
     printf("======================================================================\n");
 
     if (strcmp(mode, "demo") == 0) {
         run_demo();
         return 0;
     }
 
     if (strcmp(mode, "all") == 0 || strcmp(mode, "tests") == 0) {
         printf("\n--- Job Serialization ---\n");
         RUN_TEST(test_job_init_and_serialize);
         RUN_TEST(test_job_status_strings);
 
         printf("\n--- Job Queue Operations ---\n");
         RUN_TEST(test_submit_and_get);
         RUN_TEST(test_claim_and_complete);
         RUN_TEST(test_priority_ordering);
         RUN_TEST(test_cancel_job);
         RUN_TEST(test_retry_on_failure);
         RUN_TEST(test_queue_stats);
         RUN_TEST(test_claim_empty_queue);
         RUN_TEST(test_complete_wrong_worker);
         RUN_TEST(test_multiple_queues);
 
         printf("\n--- Worker Monitoring ---\n");
         RUN_TEST(test_active_workers);
     }
 
     printf("\n======================================================================\n");
     printf("  Results: %d run, %d passed, %d failed (%.1f%%)\n",
            tests_run, tests_passed, tests_failed,
            tests_run > 0 ? (100.0 * tests_passed / tests_run) : 0.0);
     printf("======================================================================\n\n");
 
     return tests_failed > 0 ? 1 : 0;
 }