/*
 * test_driver.c
 * Knowledge Base C Library (PostgreSQL) — Comprehensive integration test
 *
 * Mirrors LuaJIT test_driver.lua (minus document table tests).
 *
 * Usage:
 *   POSTGRES_PASSWORD=yourpassword ./test_driver
 *
 * Expects the knowledge base to be already constructed by:
 *   POSTGRES_PASSWORD=yourpassword luajit test_construct_data_tables.lua False True
 *
 * Environment variables:
 *   POSTGRES_PASSWORD — required
 *   POSTGRES_HOST     — default: localhost
 *   POSTGRES_PORT     — default: 5432
 *   POSTGRES_DB       — default: knowledge_base
 *   POSTGRES_USER     — default: postgres
 *   KB_DATABASE       — default: knowledge_base (table prefix)
 */

 #include "kb_all.h"
 #include <cjson/cJSON.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <assert.h>
 #include <time.h>
 
 /* ================================================================
  * UUID v4 generator (good enough for testing)
  * ================================================================ */
 
 static void generate_uuid(char *buf, size_t buflen) {
     /* Format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx */
     static int seeded = 0;
     if (!seeded) { srand((unsigned)(time(NULL) ^ (long)buf)); seeded = 1; }
     unsigned char bytes[16];
     for (int i = 0; i < 16; i++) bytes[i] = rand() & 0xFF;
     bytes[6] = (bytes[6] & 0x0F) | 0x40;  /* version 4 */
     bytes[8] = (bytes[8] & 0x3F) | 0x80;  /* variant 1 */
     snprintf(buf, buflen,
         "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
         bytes[0], bytes[1], bytes[2], bytes[3],
         bytes[4], bytes[5], bytes[6], bytes[7],
         bytes[8], bytes[9], bytes[10], bytes[11],
         bytes[12], bytes[13], bytes[14], bytes[15]);
 }
 
 /* ================================================================
  * Test macros
  * ================================================================ */
 
 static int test_pass = 0;
 static int test_fail = 0;
 static int test_total = 0;
 
 #define SECTION(name) \
     printf("\n===== %s =====\n", name)
 
 #define ASSERT_OK(expr, msg) do { \
     test_total++; \
     kb_error_t _err = (expr); \
     if (_err == KB_OK) { \
         test_pass++; \
         printf("  [PASS] %s\n", msg); \
     } else { \
         test_fail++; \
         printf("  [FAIL] %s — error: %s (%d)\n", msg, kb_error_str(_err), _err); \
     } \
 } while(0)
 
 #define ASSERT_ERR(expr, expected, msg) do { \
     test_total++; \
     kb_error_t _err = (expr); \
     if (_err == (expected)) { \
         test_pass++; \
         printf("  [PASS] %s (expected %s)\n", msg, kb_error_str(expected)); \
     } else { \
         test_fail++; \
         printf("  [FAIL] %s — got %s, expected %s\n", msg, \
                kb_error_str(_err), kb_error_str(expected)); \
     } \
 } while(0)
 
 #define ASSERT_TRUE(cond, msg) do { \
     test_total++; \
     if (cond) { \
         test_pass++; \
         printf("  [PASS] %s\n", msg); \
     } else { \
         test_fail++; \
         printf("  [FAIL] %s\n", msg); \
     } \
 } while(0)
 
 #define ASSERT_INT_EQ(a, b, msg) do { \
     test_total++; \
     if ((a) == (b)) { \
         test_pass++; \
         printf("  [PASS] %s (%d == %d)\n", msg, (int)(a), (int)(b)); \
     } else { \
         test_fail++; \
         printf("  [FAIL] %s (%d != %d)\n", msg, (int)(a), (int)(b)); \
     } \
 } while(0)
 
 #define ASSERT_STR_CONTAINS(haystack, needle, msg) do { \
     test_total++; \
     if ((haystack) && strstr((haystack), (needle))) { \
         test_pass++; \
         printf("  [PASS] %s\n", msg); \
     } else { \
         test_fail++; \
         printf("  [FAIL] %s — '%s' not in '%s'\n", msg, \
                (needle), (haystack) ? (haystack) : "(null)"); \
     } \
 } while(0)
 
 /* ================================================================
  * Test functions
  * ================================================================ */
 
 static void test_search(kb_search_t *ks) {
     SECTION("KB Search / Discovery");
 
     char **paths = NULL;
     int count = 0;
 
     /* Find status paths */
     ASSERT_OK(kb_find_status_paths(ks, &paths, &count),
               "find_status_paths");
     printf("    Found %d status path(s)\n", count);
     ASSERT_TRUE(count > 0, "at least one status path found");
     for (int i = 0; i < count; i++)
         printf("      [%d] %s\n", i, paths[i]);
     kb_free_paths(paths, count);
 
     /* Find job paths */
     ASSERT_OK(kb_find_job_paths(ks, &paths, &count),
               "find_job_paths");
     printf("    Found %d job path(s)\n", count);
     kb_free_paths(paths, count);
 
     /* Find stream paths */
     ASSERT_OK(kb_find_stream_paths(ks, &paths, &count),
               "find_stream_paths");
     printf("    Found %d stream path(s)\n", count);
     kb_free_paths(paths, count);
 
     /* Find bit structure paths */
     ASSERT_OK(kb_find_bit_structure_paths(ks, &paths, &count),
               "find_bit_structure_paths");
     printf("    Found %d bit structure path(s)\n", count);
     kb_free_paths(paths, count);
 
     /* Find RPC server paths */
     ASSERT_OK(kb_find_rpc_server_paths(ks, &paths, &count),
               "find_rpc_server_paths");
     printf("    Found %d RPC server path(s)\n", count);
     kb_free_paths(paths, count);
 
     /* Find RPC client paths */
     ASSERT_OK(kb_find_rpc_client_paths(ks, &paths, &count),
               "find_rpc_client_paths");
     printf("    Found %d RPC client path(s)\n", count);
     kb_free_paths(paths, count);
 
     /* Test CTE filter chain */
     kb_search_clear(ks);
     kb_search_label(ks, "KB_STATUS_FIELD");
     ASSERT_OK(kb_search_execute(ks), "CTE filter: label only");
     const kb_resultset_t *rs = kb_search_results(ks);
     ASSERT_TRUE(rs && rs->nrows > 0, "CTE filter returned rows");
     printf("    CTE label filter: %d rows\n", rs ? rs->nrows : 0);
 }
 
 static void test_status(kb_conn_t *c, kb_search_t *ks, const char *database) {
     SECTION("Status Data");
 
     /* Discover a status path */
     char **paths = NULL;
     int count = 0;
     kb_error_t err = kb_find_status_paths(ks, &paths, &count);
     if (err != KB_OK || count == 0) {
         printf("  [SKIP] No status paths found\n");
         return;
     }
     const char *path = paths[0];
     printf("    Using status path: %s\n", path);
 
     /* Set */
     ASSERT_OK(kb_status_set(c, database, path, "{\"value\":42,\"name\":\"test\"}", 3, 100),
               "status_set");
 
     /* Get */
     char *data = NULL;
     ASSERT_OK(kb_status_get(c, database, path, &data),
               "status_get");
     if (data) {
         ASSERT_STR_CONTAINS(data, "42", "status data contains 42");
         printf("    Got: %s\n", data);
         free(data);
     }
 
     /* Overwrite */
     ASSERT_OK(kb_status_set(c, database, path, "{\"value\":99}", 3, 100),
               "status_set overwrite");
 
     ASSERT_OK(kb_status_get(c, database, path, &data),
               "status_get after overwrite");
     if (data) {
         ASSERT_STR_CONTAINS(data, "99", "status data updated to 99");
         free(data);
     }
 
     kb_free_paths(paths, count);
 }
 
 static void test_job_queue(kb_conn_t *c, kb_search_t *ks, const char *database) {
     SECTION("Job Queue");
 
     char **paths = NULL;
     int count = 0;
     kb_error_t err = kb_find_job_paths(ks, &paths, &count);
     if (err != KB_OK || count == 0) {
         printf("  [SKIP] No job paths found\n");
         return;
     }
     const char *path = paths[0];
     printf("    Using job path: %s\n", path);
 
     /* Clear */
     ASSERT_OK(kb_job_clear(c, database, path, 3, 100), "job_clear");
 
     /* Check free count */
     int free_count = 0;
     ASSERT_OK(kb_job_free_count(c, database, path, &free_count),
               "job_free_count");
     printf("    Free slots: %d\n", free_count);
     ASSERT_TRUE(free_count > 0, "have free slots after clear");
 
     /* Push */
     ASSERT_OK(kb_job_push(c, database, path,
                           "{\"task\":\"backup\",\"priority\":1}", 3, 100),
               "job_push");
 
     /* Check queued count */
     int queued = 0;
     ASSERT_OK(kb_job_queued_count(c, database, path, &queued),
               "job_queued_count");
     ASSERT_INT_EQ(queued, 1, "one job queued");
 
     /* Peek */
     kb_job_info_t info = {0};
     ASSERT_OK(kb_job_peek(c, database, path, &info, 3, 100),
               "job_peek");
     ASSERT_TRUE(info.found, "peek found a job");
     if (info.data) {
         ASSERT_STR_CONTAINS(info.data, "backup", "job data contains backup");
         printf("    Peeked job id=%d data=%s\n", info.id, info.data);
     }
 
     /* Complete */
     if (info.found) {
         ASSERT_OK(kb_job_complete(c, database, info.id, 3, 100),
                   "job_complete");
     }
     free(info.data);
 
     /* Verify free count restored */
     ASSERT_OK(kb_job_free_count(c, database, path, &free_count),
               "job_free_count after complete");
     printf("    Free slots after complete: %d\n", free_count);
 
     kb_free_paths(paths, count);
 }
 
 static void test_stream(kb_conn_t *c, kb_search_t *ks, const char *database) {
     SECTION("Stream Data");
 
     char **paths = NULL;
     int count = 0;
     kb_error_t err = kb_find_stream_paths(ks, &paths, &count);
     if (err != KB_OK || count == 0) {
         printf("  [SKIP] No stream paths found\n");
         return;
     }
     const char *path = paths[0];
     printf("    Using stream path: %s\n", path);
 
     /* Clear first */
     ASSERT_OK(kb_stream_clear(c, database, path, 3, 100),
               "stream_clear");
 
     /* Count after clear (should be 0 valid) */
     int valid_count = 0;
     ASSERT_OK(kb_stream_count(c, database, path, &valid_count),
               "stream_count after clear");
     ASSERT_INT_EQ(valid_count, 0, "no valid entries after clear");
 
     /* Total count (pre-allocated slots) */
     int total_count = 0;
     ASSERT_OK(kb_stream_count_total(c, database, path, &total_count),
               "stream_count_total");
     printf("    Total slots: %d\n", total_count);
     ASSERT_TRUE(total_count > 0, "have pre-allocated slots");
 
     /* Push some data */
     ASSERT_OK(kb_stream_push(c, database, path,
                              "{\"temp\":72.5,\"unit\":\"F\"}", 3, 100),
               "stream_push 1");
     ASSERT_OK(kb_stream_push(c, database, path,
                              "{\"temp\":73.1,\"unit\":\"F\"}", 3, 100),
               "stream_push 2");
     ASSERT_OK(kb_stream_push(c, database, path,
                              "{\"temp\":74.0,\"unit\":\"F\"}", 3, 100),
               "stream_push 3");
 
     /* Count valid */
     ASSERT_OK(kb_stream_count(c, database, path, &valid_count),
               "stream_count after push");
     ASSERT_INT_EQ(valid_count, 3, "3 valid entries after push");
 
     /* List all valid */
     kb_resultset_t *rs = NULL;
     ASSERT_OK(kb_stream_list(c, database, path, NULL, NULL, &rs),
               "stream_list all");
     if (rs) {
         printf("    Stream entries: %d\n", rs->nrows);
         ASSERT_TRUE(rs->nrows == 3, "list returns 3 entries");
         for (int i = 0; i < rs->nrows && i < 3; i++) {
             const char *d = kb_rs_get(rs, i, "data");
             const char *ts = kb_rs_get(rs, i, "recorded_at");
             printf("      [%d] %s @ %s\n", i, d ? d : "(null)", ts ? ts : "?");
         }
         kb_resultset_free(rs);
     }
 
     /* Latest */
     rs = NULL;
     ASSERT_OK(kb_stream_latest(c, database, path, &rs),
               "stream_latest");
     if (rs) {
         ASSERT_TRUE(rs->nrows == 1, "latest returns 1 row");
         const char *d = kb_rs_get(rs, 0, "data");
         if (d) {
             printf("    Latest: %s\n", d);
             ASSERT_STR_CONTAINS(d, "74.0", "latest is 74.0");
         }
         kb_resultset_free(rs);
     }
 
     /* Statistics */
     rs = NULL;
     ASSERT_OK(kb_stream_statistics(c, database, path, &rs),
               "stream_statistics");
     if (rs && rs->nrows > 0) {
         int tc = (int)kb_rs_get_int64(rs, 0, "total_count");
         int vc = (int)kb_rs_get_int64(rs, 0, "valid_count");
         int ic = (int)kb_rs_get_int64(rs, 0, "invalid_count");
         printf("    Stats: total=%d valid=%d invalid=%d\n", tc, vc, ic);
         ASSERT_INT_EQ(vc, 3, "stats valid_count = 3");
         ASSERT_INT_EQ(tc, total_count, "stats total matches count_total");
         kb_resultset_free(rs);
     }
 
     /* Clear and verify */
     ASSERT_OK(kb_stream_clear(c, database, path, 3, 100),
               "stream_clear final");
     ASSERT_OK(kb_stream_count(c, database, path, &valid_count),
               "stream_count after final clear");
     ASSERT_INT_EQ(valid_count, 0, "no valid after final clear");
 
     kb_free_paths(paths, count);
 }
 
 static void test_rpc_server(kb_conn_t *c, kb_search_t *ks, const char *database) {
     SECTION("RPC Server");
 
     char **paths = NULL;
     int count = 0;
     kb_error_t err = kb_find_rpc_server_paths(ks, &paths, &count);
     if (err != KB_OK || count == 0) {
         printf("  [SKIP] No RPC server paths found\n");
         return;
     }
     const char *path = paths[0];
     printf("    Using RPC server path: %s\n", path);
 
     /* Clear */
     ASSERT_OK(kb_rpc_server_clear(c, database, path, 3, 100),
               "rpc_server_clear");
 
     /* Count new (should be 0 after clear) */
     int new_count = 0;
     ASSERT_OK(kb_rpc_server_count_new(c, database, path, &new_count),
               "rpc_server_count_new after clear");
     ASSERT_INT_EQ(new_count, 0, "no new jobs after clear");
 
     /* Find a client path for the push */
     char **client_paths = NULL;
     int client_count = 0;
     kb_find_rpc_client_paths(ks, &client_paths, &client_count);
     const char *client_path = (client_count > 0) ? client_paths[0] : path;
 
     /* Push with priority */
     char uuid1[40], uuid2[40];
     generate_uuid(uuid1, sizeof(uuid1));
     generate_uuid(uuid2, sizeof(uuid2));
 
     ASSERT_OK(kb_rpc_server_push(c, database, path,
                                  uuid1, "process_data",
                                  "{\"input\":\"test\"}", "tx_001",
                                  2, client_path, 3, 100),
               "rpc_server_push priority=2");
 
     ASSERT_OK(kb_rpc_server_push(c, database, path,
                                  uuid2, "urgent_task",
                                  "{\"input\":\"urgent\"}", "tx_002",
                                  1, client_path, 3, 100),
               "rpc_server_push priority=1");
 
     /* Count */
     ASSERT_OK(kb_rpc_server_count_new(c, database, path, &new_count),
               "rpc_server_count_new after push");
     ASSERT_INT_EQ(new_count, 2, "two new jobs");
 
     /* Peek (should get priority=1 first) */
     kb_rpc_server_job_t job = {0};
     ASSERT_OK(kb_rpc_server_peek(c, database, path, &job, 3, 100),
               "rpc_server_peek");
     ASSERT_TRUE(job.found, "peek found a job");
     if (job.found) {
         printf("    Peeked: id=%d action=%s priority=%d\n",
                job.id, job.rpc_action ? job.rpc_action : "?", job.priority);
         ASSERT_INT_EQ(job.priority, 1, "highest priority first");
         ASSERT_TRUE(job.rpc_action && strcmp(job.rpc_action, "urgent_task") == 0,
                     "urgent_task peeked first");
 
         /* Complete it */
         ASSERT_OK(kb_rpc_server_complete(c, database, path, job.id, 3, 100),
                   "rpc_server_complete");
     }
     kb_rpc_server_job_free(&job);
 
     /* Peek again (should get priority=2) */
     memset(&job, 0, sizeof(job));
     ASSERT_OK(kb_rpc_server_peek(c, database, path, &job, 3, 100),
               "rpc_server_peek second");
     if (job.found) {
         ASSERT_INT_EQ(job.priority, 2, "second priority next");
         ASSERT_OK(kb_rpc_server_complete(c, database, path, job.id, 3, 100),
                   "rpc_server_complete second");
     }
     kb_rpc_server_job_free(&job);
 
     if (client_paths) kb_free_paths(client_paths, client_count);
     kb_free_paths(paths, count);
 }
 
 static void test_rpc_client(kb_conn_t *c, kb_search_t *ks, const char *database) {
     SECTION("RPC Client");
 
     char **paths = NULL;
     int count = 0;
     kb_error_t err = kb_find_rpc_client_paths(ks, &paths, &count);
     if (err != KB_OK || count == 0) {
         printf("  [SKIP] No RPC client paths found\n");
         return;
     }
     const char *path = paths[0];
     printf("    Using RPC client path: %s\n", path);
 
     /* Clear */
     ASSERT_OK(kb_rpc_client_clear(c, database, path, 3, 100),
               "rpc_client_clear");
 
     /* Free slots */
     int free_slots = 0;
     ASSERT_OK(kb_rpc_client_free_slots(c, database, path, &free_slots),
               "rpc_client_free_slots");
     printf("    Free slots: %d\n", free_slots);
     ASSERT_TRUE(free_slots > 0, "have free slots");
 
     /* Push reply */
     char client_uuid[40];
     generate_uuid(client_uuid, sizeof(client_uuid));
     ASSERT_OK(kb_rpc_client_push_reply(c, database, path,
                                        client_uuid, "server.path",
                                        "process_data", "tx_001",
                                        "{\"result\":\"success\"}", 3, 100),
               "rpc_client_push_reply");
 
     /* Queued count */
     int queued = 0;
     ASSERT_OK(kb_rpc_client_queued_slots(c, database, path, &queued),
               "rpc_client_queued_slots");
     ASSERT_INT_EQ(queued, 1, "one reply queued");
 
     /* Peek */
     kb_rpc_client_reply_t reply = {0};
     ASSERT_OK(kb_rpc_client_peek_reply(c, database, path, &reply, 3, 100),
               "rpc_client_peek_reply");
     ASSERT_TRUE(reply.found, "peek found reply");
     if (reply.found) {
         printf("    Reply: id=%d payload=%s\n",
                reply.id, reply.response_payload ? reply.response_payload : "?");
         ASSERT_STR_CONTAINS(reply.response_payload, "success",
                             "reply contains success");
     }
     kb_rpc_client_reply_free(&reply);
 
     /* Clear */
     ASSERT_OK(kb_rpc_client_clear(c, database, path, 3, 100),
               "rpc_client_clear final");
 
     kb_free_paths(paths, count);
 }
 
 static void test_bit_structures(kb_conn_t *c, kb_search_t *ks,
                                 const char *database) {
     SECTION("Bit Structures");
 
     /*
      * The bit_mask_table uses node_id (from properties.record_id)
      * not the ltree path. We need to do a full KB search and extract
      * record_id from the properties JSON.
      */
     kb_search_clear(ks);
     kb_search_label(ks, "KB_BIT_MASK");
     kb_error_t err = kb_search_execute(ks);
     if (err != KB_OK) {
         printf("  [SKIP] KB search for bit structures failed\n");
         return;
     }
 
     kb_resultset_t *rs = ks->last_result;
     if (!rs || rs->nrows == 0) {
         printf("  [SKIP] No bit structure nodes found\n");
         return;
     }
     printf("    Found %d bit structure node(s)\n", rs->nrows);
 
     /* Extract record_id from properties JSON of first row */
     const char *props_json = kb_rs_get(rs, 0, "properties");
     if (!props_json) {
         printf("  [SKIP] No properties column in bit structure row\n");
         return;
     }
 
     cJSON *props = cJSON_Parse(props_json);
     if (!props) {
         printf("  [SKIP] Could not parse properties JSON\n");
         return;
     }
 
     cJSON *record_id_item = cJSON_GetObjectItem(props, "record_id");
     if (!record_id_item || !cJSON_IsString(record_id_item)) {
         printf("  [SKIP] No record_id in properties\n");
         cJSON_Delete(props);
         return;
     }
 
     char *node_id = kb_strdup(record_id_item->valuestring);
     cJSON_Delete(props);
 
     printf("    Using node_id: %s\n", node_id);
 
     /* Verify we can read the mask */
     int64_t mask = 0;
     ASSERT_OK(kb_bit_get_mask(c, database, node_id, &mask),
               "bit_get_mask initial read");
     printf("    Initial mask: %ld\n", (long)mask);
 
     /* Reset mask to 0 */
     ASSERT_OK(kb_bit_set_mask(c, database, node_id, 0, 3, 100),
               "bit_set_mask to 0");
 
     /* Set bit 0 */
     ASSERT_OK(kb_bit_set(c, database, node_id, 0, true, 3, 100),
               "bit_set bit 0 = true");
 
     /* Verify mask = 1 */
     mask = 0;
     ASSERT_OK(kb_bit_get_mask(c, database, node_id, &mask),
               "bit_get_mask after set bit 0");
     ASSERT_INT_EQ((int)mask, 1, "mask = 1 after bit 0 set");
 
     /* Set bit 4 */
     ASSERT_OK(kb_bit_set(c, database, node_id, 4, true, 3, 100),
               "bit_set bit 4 = true");
 
     ASSERT_OK(kb_bit_get_mask(c, database, node_id, &mask),
               "bit_get_mask after set bit 4");
     ASSERT_INT_EQ((int)mask, 17, "mask = 17 (bit0 + bit4)");
 
     /* Get individual bits */
     bool val = false;
     ASSERT_OK(kb_bit_get(c, database, node_id, 0, &val),
               "bit_get bit 0");
     ASSERT_TRUE(val, "bit 0 is set");
 
     ASSERT_OK(kb_bit_get(c, database, node_id, 1, &val),
               "bit_get bit 1");
     ASSERT_TRUE(!val, "bit 1 is not set");
 
     ASSERT_OK(kb_bit_get(c, database, node_id, 4, &val),
               "bit_get bit 4");
     ASSERT_TRUE(val, "bit 4 is set");
 
     /* S-expression evaluation */
     bool result = false;
 
     /* ["and", ["bit", 0], ["bit", 4]] — both set → true */
     ASSERT_OK(kb_bit_eval_sexpr(c, database, node_id,
                                 "[\"and\", [\"bit\", 0], [\"bit\", 4]]",
                                 NULL, 0, &result),
               "sexpr: and(bit0, bit4)");
     ASSERT_TRUE(result, "and(bit0, bit4) = true");
 
     /* ["or", ["bit", 1], ["bit", 2]] — neither set → false */
     ASSERT_OK(kb_bit_eval_sexpr(c, database, node_id,
                                 "[\"or\", [\"bit\", 1], [\"bit\", 2]]",
                                 NULL, 0, &result),
               "sexpr: or(bit1, bit2)");
     ASSERT_TRUE(!result, "or(bit1, bit2) = false");
 
     /* ["not", ["bit", 1]] → true */
     ASSERT_OK(kb_bit_eval_sexpr(c, database, node_id,
                                 "[\"not\", [\"bit\", 1]]",
                                 NULL, 0, &result),
               "sexpr: not(bit1)");
     ASSERT_TRUE(result, "not(bit1) = true");
 
     /* ["bit_changed", 0] with prev_mask=0 → bit0 changed */
     ASSERT_OK(kb_bit_eval_sexpr(c, database, node_id,
                                 "[\"bit_changed\", 0]",
                                 NULL, 0, &result),
               "sexpr: bit_changed(0) prev=0");
     ASSERT_TRUE(result, "bit_changed(0) with prev=0 = true");
 
     /* ["bit_changed", 0] with prev_mask=17 → bit0 unchanged */
     ASSERT_OK(kb_bit_eval_sexpr(c, database, node_id,
                                 "[\"bit_changed\", 0]",
                                 NULL, 17, &result),
               "sexpr: bit_changed(0) prev=17");
     ASSERT_TRUE(!result, "bit_changed(0) with prev=17 = false");
 
     /* Reset */
     ASSERT_OK(kb_bit_set_mask(c, database, node_id, 0, 3, 100),
               "bit_set_mask reset to 0");
 
     free(node_id);
 }
 
 /* ================================================================
  * Document Table Tests
  * ================================================================
  * Mirrors LuaJIT test_document_table: JSONB get/set, key existence,
  * containment, array ops, queue (FIFO), stack (LIFO), edge cases.
  */
 
 static void test_document_table(kb_conn_t *c, kb_search_t *ks,
                                 const char *database, const char *label_field) {
     char section_name[128];
     snprintf(section_name, sizeof(section_name), "Document Table (%s)", label_field);
     SECTION(section_name);
 
     /* Discover document path via KB search */
     kb_search_clear(ks);
     kb_search_label(ks, "KB_JSONB_FIELD");
     kb_search_name(ks, label_field);
     kb_error_t err = kb_search_execute(ks);
     if (err != KB_OK || !ks->last_result || ks->last_result->nrows == 0) {
         printf("  [SKIP] No document node found for '%s'\n", label_field);
         return;
     }
 
     const char *doc_path = kb_rs_get(ks->last_result, 0, "path");
     if (!doc_path) {
         printf("  [SKIP] No path column in document result\n");
         return;
     }
     char *path = kb_strdup(doc_path);
     printf("    document path: %s\n", path);
 
     char *val = NULL;
     bool bval = false;
     int ival = 0;
 
     /* --- Set entire document --- */
     ASSERT_OK(kb_doc_set(c, database, path, "",
         "{\"name\":\"Test\",\"role\":\"admin\",\"tags\":[\"python\",\"postgres\"],"
         "\"address\":{\"city\":\"LA\",\"zip\":\"90001\"}}", true, NULL),
         "jsonb_set entire document");
 
     /* --- Get entire document --- */
     ASSERT_OK(kb_doc_get(c, database, path, "", false, NULL, &val),
         "jsonb_get entire document");
     if (val) {
         ASSERT_STR_CONTAINS(val, "Test", "doc contains Test");
         printf("    doc: %s\n", val);
         free(val); val = NULL;
     }
 
     /* --- Get name as JSON --- */
     ASSERT_OK(kb_doc_get(c, database, path, "name", false, NULL, &val),
         "jsonb_get name (JSON)");
     if (val) { printf("    name (JSON): %s\n", val); free(val); val = NULL; }
 
     /* --- Get name as text --- */
     ASSERT_OK(kb_doc_get(c, database, path, "name", true, NULL, &val),
         "jsonb_get name (text)");
     if (val) {
         ASSERT_STR_CONTAINS(val, "Test", "name text = Test");
         printf("    name (text): %s\n", val);
         free(val); val = NULL;
     }
 
     /* --- Get nested path --- */
     ASSERT_OK(kb_doc_get(c, database, path, "address.city", true, NULL, &val),
         "jsonb_get address.city");
     if (val) {
         ASSERT_STR_CONTAINS(val, "LA", "city = LA");
         printf("    address.city: %s\n", val);
         free(val); val = NULL;
     }
 
     /* --- Key existence --- */
     ASSERT_OK(kb_doc_has_key(c, database, path, "role", NULL, &bval),
         "has_key role");
     ASSERT_TRUE(bval, "has role key");
 
     const char *any_keys[] = {"role", "nonexistent"};
     ASSERT_OK(kb_doc_has_any_keys(c, database, path, any_keys, 2, NULL, &bval),
         "has_any_keys [role, nonexistent]");
     ASSERT_TRUE(bval, "has any of role/nonexistent");
 
     const char *all_keys[] = {"name", "role"};
     ASSERT_OK(kb_doc_has_all_keys(c, database, path, all_keys, 2, NULL, &bval),
         "has_all_keys [name, role]");
     ASSERT_TRUE(bval, "has all name+role");
 
     const char *all_keys2[] = {"name", "nonexistent"};
     ASSERT_OK(kb_doc_has_all_keys(c, database, path, all_keys2, 2, NULL, &bval),
         "has_all_keys [name, nonexistent]");
     ASSERT_TRUE(!bval, "does not have all name+nonexistent");
 
     /* --- Containment --- */
     ASSERT_OK(kb_doc_contains(c, database, path,
         "{\"role\":\"admin\"}", NULL, &bval),
         "contains {role:admin}");
     ASSERT_TRUE(bval, "contains role=admin");
 
     ASSERT_OK(kb_doc_contains(c, database, path,
         "{\"role\":\"user\"}", NULL, &bval),
         "contains {role:user}");
     ASSERT_TRUE(!bval, "does not contain role=user");
 
     /* --- Array contains --- */
     ASSERT_OK(kb_doc_array_contains(c, database, path, "tags",
         "\"python\"", NULL, &bval),
         "array tags contains python");
     ASSERT_TRUE(bval, "tags has python");
 
     ASSERT_OK(kb_doc_array_contains(c, database, path, "tags",
         "\"ruby\"", NULL, &bval),
         "array tags contains ruby");
     ASSERT_TRUE(!bval, "tags does not have ruby");
 
     /* --- JSONPath --- */
     ASSERT_OK(kb_doc_path_exists(c, database, path,
         "$.role ? (@ == \"admin\")", NULL, &bval),
         "path_exists role==admin");
     ASSERT_TRUE(bval, "jsonpath role==admin exists");
 
     ASSERT_OK(kb_doc_path_query(c, database, path,
         "$.tags[*]", NULL, &val),
         "path_query $.tags[*]");
     if (val) {
         printf("    path_query tags: %s\n", val);
         ASSERT_STR_CONTAINS(val, "python", "path_query has python");
         free(val); val = NULL;
     }
 
     /* --- Set and delete --- */
     ASSERT_OK(kb_doc_set(c, database, path, "status",
         "\"active\"", true, NULL),
         "jsonb_set status=active");
 
     ASSERT_OK(kb_doc_get(c, database, path, "status", true, NULL, &val),
         "jsonb_get status");
     if (val) {
         ASSERT_STR_CONTAINS(val, "active", "status = active");
         free(val); val = NULL;
     }
 
     ASSERT_OK(kb_doc_delete_key(c, database, path, "status", NULL),
         "jsonb_delete_key status");
 
     err = kb_doc_get(c, database, path, "status", true, NULL, &val);
     ASSERT_TRUE(val == NULL || err != KB_OK, "status deleted (null)");
     if (val) { free(val); val = NULL; }
 
     ASSERT_OK(kb_doc_delete_path(c, database, path, "address.zip", NULL),
         "jsonb_delete_path address.zip");
 
     /* --- Array elements --- */
     kb_resultset_t *elem_rs = NULL;
     ASSERT_OK(kb_doc_array_elements(c, database, path, "tags", NULL, &elem_rs),
         "array_elements tags");
     if (elem_rs) {
         printf("    tag elements: %d rows\n", elem_rs->nrows);
         ASSERT_TRUE(elem_rs->nrows >= 2, "at least 2 tag elements");
         kb_resultset_free(elem_rs);
     }
 
     /* --- Queue (FIFO) --- */
     printf("\n  --- Queue (FIFO) ---\n");
     ASSERT_OK(kb_doc_queue_clear(c, database, path, NULL, NULL),
         "queue_clear");
 
     ASSERT_OK(kb_doc_enqueue(c, database, path,
         "{\"task\":\"Task 1\",\"priority\":1}", NULL, NULL),
         "enqueue Task 1");
     ASSERT_OK(kb_doc_enqueue(c, database, path,
         "{\"task\":\"Task 2\",\"priority\":2}", NULL, NULL),
         "enqueue Task 2");
     ASSERT_OK(kb_doc_enqueue(c, database, path,
         "{\"task\":\"Task 3\",\"priority\":3}", NULL, NULL),
         "enqueue Task 3");
 
     ASSERT_OK(kb_doc_queue_size(c, database, path, NULL, NULL, &ival),
         "queue_size after 3 enqueues");
     ASSERT_INT_EQ(ival, 3, "queue size = 3");
 
     ASSERT_OK(kb_doc_dequeue(c, database, path, NULL, NULL, &val),
         "dequeue (FIFO)");
     if (val) {
         printf("    dequeued: %s\n", val);
         ASSERT_STR_CONTAINS(val, "Task 1", "dequeued Task 1 (FIFO)");
         free(val); val = NULL;
     }
 
     ASSERT_OK(kb_doc_peek(c, database, path, NULL, 0, NULL, &val),
         "peek index 0");
     if (val) {
         ASSERT_STR_CONTAINS(val, "Task 2", "peek shows Task 2");
         free(val); val = NULL;
     }
 
     ASSERT_OK(kb_doc_queue_size(c, database, path, NULL, NULL, &ival),
         "queue_size after dequeue");
     ASSERT_INT_EQ(ival, 2, "queue size = 2 after dequeue");
 
     /* --- Stack (LIFO) --- */
     printf("\n  --- Stack (LIFO) ---\n");
     ASSERT_OK(kb_doc_queue_clear(c, database, path, NULL, NULL),
         "clear for stack test");
 
     ASSERT_OK(kb_doc_push(c, database, path,
         "{\"message\":\"First\"}", NULL, NULL),
         "push First");
     ASSERT_OK(kb_doc_push(c, database, path,
         "{\"message\":\"Second\"}", NULL, NULL),
         "push Second");
     ASSERT_OK(kb_doc_push(c, database, path,
         "{\"message\":\"Third\"}", NULL, NULL),
         "push Third");
 
     ASSERT_OK(kb_doc_pop(c, database, path, NULL, NULL, &val),
         "pop (LIFO)");
     if (val) {
         printf("    popped: %s\n", val);
         ASSERT_STR_CONTAINS(val, "Third", "popped Third (LIFO)");
         free(val); val = NULL;
     }
 
     ASSERT_OK(kb_doc_pop(c, database, path, NULL, NULL, &val),
         "pop second (LIFO)");
     if (val) {
         ASSERT_STR_CONTAINS(val, "Second", "popped Second (LIFO)");
         free(val); val = NULL;
     }
 
     ASSERT_OK(kb_doc_queue_size(c, database, path, NULL, NULL, &ival),
         "stack size after 2 pops");
     ASSERT_INT_EQ(ival, 1, "stack size = 1");
 
     /* --- Edge cases --- */
     printf("\n  --- Edge Cases ---\n");
     ASSERT_OK(kb_doc_queue_clear(c, database, path, NULL, NULL),
         "clear for edge cases");
 
     err = kb_doc_dequeue(c, database, path, NULL, NULL, &val);
     printf("    dequeue from empty: %s (err=%d)\n",
            val ? val : "NULL", err);
     if (val) { free(val); val = NULL; }
 
     err = kb_doc_pop(c, database, path, NULL, NULL, &val);
     printf("    pop from empty: %s (err=%d)\n",
            val ? val : "NULL", err);
     if (val) { free(val); val = NULL; }
 
     bool empty = false;
     ASSERT_OK(kb_doc_queue_is_empty(c, database, path, NULL, NULL, &empty),
         "queue_is_empty on empty");
     ASSERT_TRUE(empty, "empty queue is empty");
 
     /* Queue get_all */
     ASSERT_OK(kb_doc_enqueue(c, database, path,
         "{\"data\":\"test\"}", NULL, NULL),
         "enqueue for get_all");
     ASSERT_OK(kb_doc_queue_get_all(c, database, path, NULL, NULL, &val),
         "queue_get_all");
     if (val) {
         printf("    get_all: %s\n", val);
         ASSERT_STR_CONTAINS(val, "test", "get_all contains test");
         free(val); val = NULL;
     }
 
     /* Final cleanup */
     ASSERT_OK(kb_doc_queue_clear(c, database, path, NULL, NULL),
         "final queue_clear");
 
     free(path);
 }
 
 static void test_link_tables(kb_conn_t *c, kb_search_t *ks,
                              const char *database) {
     SECTION("Link Tables");
 
     char **paths = NULL;
     int count = 0;
 
     /* Find nodes with links */
     kb_error_t err = kb_find_link_paths(ks, &paths, &count);
     printf("    Link paths found: %d\n", count);
     if (count > 0) {
         for (int i = 0; i < count && i < 5; i++)
             printf("      [%d] %s\n", i, paths[i]);
 
         /* Try to query link table for first path */
         kb_resultset_t *rs = NULL;
         err = kb_link_query(c, database, paths[0], &rs);
         if (err == KB_OK && rs) {
             printf("    Link entries for %s: %d rows\n", paths[0], rs->nrows);
             kb_resultset_free(rs);
         }
 
         /* Decode link nodes */
         char **linked = NULL;
         int lcount = 0;
         err = kb_link_decode_nodes(c, database, paths[0], &linked, &lcount);
         if (err == KB_OK) {
             printf("    Decoded link nodes: %d\n", lcount);
             for (int i = 0; i < lcount && i < 5; i++)
                 printf("      → %s\n", linked[i]);
             kb_free_paths(linked, lcount);
         }
 
         kb_free_paths(paths, count);
     }
 
     /* Find mount points */
     err = kb_find_link_mount_paths(ks, &paths, &count);
     printf("    Link mount paths found: %d\n", count);
     if (count > 0) {
         for (int i = 0; i < count && i < 5; i++)
             printf("      [%d] %s\n", i, paths[i]);
         kb_free_paths(paths, count);
     }
 
     ASSERT_TRUE(true, "link tables queried without crash");
 }
 
 /* ================================================================
  * Main
  * ================================================================ */
 
 int main(int argc, char *argv[]) {
     (void)argc; (void)argv;
     printf("Knowledge Base C Library (PostgreSQL) — Integration Test\n");
     printf("========================================================\n");
 
     /* Get connection parameters from environment */
     const char *password = getenv("POSTGRES_PASSWORD");
     const char *host     = getenv("POSTGRES_HOST");
     const char *port     = getenv("POSTGRES_PORT");
     const char *dbname   = getenv("POSTGRES_DB");
     const char *user     = getenv("POSTGRES_USER");
     const char *database = getenv("KB_DATABASE");
 
     if (!password) {
         fprintf(stderr, "Error: POSTGRES_PASSWORD environment variable required\n");
         return 1;
     }
     if (!host) host = "localhost";
     if (!port) port = "5432";
     if (!dbname) dbname = "knowledge_base";
     if (!user) user = "postgres";
     if (!database) database = "knowledge_base";
 
     printf("Connecting: host=%s port=%s db=%s user=%s kb=%s\n",
            host, port, dbname, user, database);
 
     /* Connect */
     kb_conn_t *conn = NULL;
     kb_error_t err = kb_connect_params(host, port, dbname, user, password, &conn);
     if (err != KB_OK) {
         fprintf(stderr, "Failed to connect: %s\n", kb_error_str(err));
         return 1;
     }
     printf("Connected successfully.\n");
 
     /* Create search context */
     kb_search_t *ks = NULL;
     err = kb_search_create(conn, database, &ks);
     if (err != KB_OK) {
         fprintf(stderr, "Failed to create search: %s\n", kb_error_str(err));
         kb_disconnect(conn);
         return 1;
     }
 
     /* Run all tests */
     test_search(ks);
     test_status(conn, ks, database);
     test_job_queue(conn, ks, database);
     test_stream(conn, ks, database);
     test_rpc_server(conn, ks, database);
     test_rpc_client(conn, ks, database);
     test_bit_structures(conn, ks, database);
     test_document_table(conn, ks, database, "info1_jsonb");
     test_document_table(conn, ks, database, "info2_jsonb");
     test_document_table(conn, ks, database, "info3_jsonb");
     test_link_tables(conn, ks, database);
 
     /* Summary */
     printf("\n========================================================\n");
     printf("Results: %d/%d passed", test_pass, test_total);
     if (test_fail > 0)
         printf(", %d FAILED", test_fail);
     printf("\n========================================================\n");
 
     /* Cleanup */
     kb_search_destroy(ks);
     kb_disconnect(conn);
 
     return test_fail > 0 ? 1 : 0;
 }