/**
 * @file test_nats_kv.c
 * @brief Test driver for nats_key_store and nats_kb_store libraries.
 *
 * Requires a running NATS server at 127.0.0.1:4222 with JetStream:
 *   docker run -p 4222:4222 nats:latest -js
 *
 * Build with the provided Makefile.
 */

 #define _POSIX_C_SOURCE 200809L   /* strdup */

 #include <assert.h>
 #include <inttypes.h>
 #include <stdbool.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 
 #include "nats_key_store.h"
 #include "nats_kb_store.h"
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
 /*  Helper                                                             */
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
 
 /* ================================================================== */
 /*  KeyStore tests                                                     */
 /* ================================================================== */
 
 static bool test_ks_put_get_string(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
 
     uint64_t rev = 0;
     EXPECT_OK(ks_put(ks, "test.string", "\"Hello, World!\"", &rev));
     EXPECT(rev > 0);
 
     char *val = NULL;
     EXPECT_OK(ks_get(ks, "test.string", &val));
     EXPECT(val != NULL);
     EXPECT(strstr(val, "Hello, World!") != NULL);
     free(val);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_put_get_json(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
 
     const char *json = "{\"name\":\"John\",\"age\":30,\"active\":true}";
     EXPECT_OK(ks_put(ks, "test.json", json, NULL));
 
     char *val = NULL;
     EXPECT_OK(ks_get(ks, "test.json", &val));
     EXPECT(val != NULL);
 
     cJSON *obj = cJSON_Parse(val);
     EXPECT(obj != NULL);
     EXPECT(strcmp(cJSON_GetObjectItem(obj, "name")->valuestring, "John") == 0);
     EXPECT(cJSON_GetObjectItem(obj, "age")->valueint == 30);
     cJSON_Delete(obj);
     free(val);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_delete(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
 
     EXPECT_OK(ks_put(ks, "test.del", "\"temp\"", NULL));
     bool ex = false;
     EXPECT_OK(ks_exists(ks, "test.del", &ex));
     EXPECT(ex == true);
 
     EXPECT_OK(ks_delete(ks, "test.del"));
     EXPECT_OK(ks_exists(ks, "test.del", &ex));
     EXPECT(ex == false);
 
     char *val = NULL;
     EXPECT(ks_get(ks, "test.del", &val) == KS_ERR_NOT_FOUND);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_exists(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
 
     bool ex = false;
     EXPECT_OK(ks_exists(ks, "test.no_such_key_xyz", &ex));
     EXPECT(ex == false);
 
     EXPECT_OK(ks_put(ks, "test.exists", "\"yes\"", NULL));
     EXPECT_OK(ks_exists(ks, "test.exists", &ex));
     EXPECT(ex == true);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_keys_pattern(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks_keys";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     EXPECT_OK(ks_put(ks, "test.user.1", "\"Alice\"", NULL));
     EXPECT_OK(ks_put(ks, "test.user.2", "\"Bob\"", NULL));
     EXPECT_OK(ks_put(ks, "test.admin.1", "\"Charlie\"", NULL));
     EXPECT_OK(ks_put(ks, "test.config", "\"settings\"", NULL));
 
     char **keys = NULL;
     size_t count = 0;
 
     EXPECT_OK(ks_keys(ks, "test.user.*", &keys, &count));
     EXPECT(count == 2);
     ks_free_keys(keys, count);
 
     EXPECT_OK(ks_keys(ks, "test.*", &keys, &count));
     EXPECT(count >= 4);
     ks_free_keys(keys, count);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_increment(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks_inc";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     int64_t val = 0;
     EXPECT_OK(ks_increment(ks, "test.ctr", 1, &val));
     EXPECT(val == 1);
     EXPECT_OK(ks_increment(ks, "test.ctr", 1, &val));
     EXPECT(val == 2);
     EXPECT_OK(ks_increment(ks, "test.ctr", 5, &val));
     EXPECT(val == 7);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_decrement(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks_dec";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     EXPECT_OK(ks_put(ks, "test.cd", "10", NULL));
     int64_t val = 0;
     EXPECT_OK(ks_decrement(ks, "test.cd", 1, &val));
     EXPECT(val == 9);
     EXPECT_OK(ks_decrement(ks, "test.cd", 3, &val));
     EXPECT(val == 6);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_increment_non_numeric(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks_nan";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     EXPECT_OK(ks_put(ks, "test.txt", "\"not a number\"", NULL));
     int64_t val = 0;
     EXPECT(ks_increment(ks, "test.txt", 1, &val) == KS_ERR_NOT_NUMERIC);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_missing_key(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_ks_miss";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
 
     char *val = NULL;
     EXPECT(ks_get(ks, "totally.missing", &val) == KS_ERR_NOT_FOUND);
     EXPECT(val == NULL);
 
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_sync_wrappers(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_sync";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
 
     EXPECT_OK(ks_put_sync(ks, "sync.key", "\"sync_value\"", NULL));
 
     char *val = NULL;
     EXPECT_OK(ks_get_sync(ks, "sync.key", &val));
     EXPECT(val != NULL);
     EXPECT(strstr(val, "sync_value") != NULL);
     free(val);
 
     bool ex = false;
     EXPECT_OK(ks_exists_sync(ks, "sync.key", &ex));
     EXPECT(ex == true);
 
     EXPECT_OK(ks_delete_sync(ks, "sync.key"));
     EXPECT_OK(ks_exists_sync(ks, "sync.key", &ex));
     EXPECT(ex == false);
 
     ks_destroy(ks);
     return true;
 }
 
 static bool test_ks_performance(void)
 {
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "test_c_perf";
 
     KeyStore *ks = NULL;
     EXPECT_OK(ks_create(&ks, &cfg));
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     clock_t start = clock();
 
     for (int i = 0; i < 100; i++) {
         char key[64], val[64];
         snprintf(key, sizeof(key), "test.perf.%d", i);
         snprintf(val, sizeof(val), "\"value_%d\"", i);
         EXPECT_OK(ks_put(ks, key, val, NULL));
     }
     for (int i = 0; i < 100; i++) {
         char key[64];
         snprintf(key, sizeof(key), "test.perf.%d", i);
         char *v = NULL;
         EXPECT_OK(ks_get(ks, key, &v));
         EXPECT(v != NULL);
         free(v);
     }
 
     char **keys = NULL;
     size_t count = 0;
     EXPECT_OK(ks_keys(ks, "test.perf.*", &keys, &count));
     EXPECT(count == 100);
     ks_free_keys(keys, count);
 
     double elapsed = (double)(clock() - start) / CLOCKS_PER_SEC;
     printf("(%.3fs) ", elapsed);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
     return true;
 }
 
 /* ================================================================== */
 /*  KbStore tests                                                      */
 /* ================================================================== */
 
 static bool test_kb_validate_topic(void)
 {
     EXPECT(kb_validate_topic("valid.topic") == KS_OK);
     EXPECT(kb_validate_topic("a.b.c") == KS_OK);
     EXPECT(kb_validate_topic("simple") == KS_OK);
     EXPECT(kb_validate_topic("dashes-and_under") == KS_OK);
 
     EXPECT(kb_validate_topic(NULL) != KS_OK);
     EXPECT(kb_validate_topic("") != KS_OK);
     EXPECT(kb_validate_topic(".leading") != KS_OK);
     EXPECT(kb_validate_topic("trailing.") != KS_OK);
     EXPECT(kb_validate_topic("double..dot") != KS_OK);
     EXPECT(kb_validate_topic("bad space") != KS_OK);
 
     return true;
 }
 
 static bool test_kb_validate_names(void)
 {
     EXPECT(kb_validate_label_name("person") == KS_OK);
     EXPECT(kb_validate_label_name("my-label") == KS_OK);
     EXPECT(kb_validate_label_name("label_1") == KS_OK);
     EXPECT(kb_validate_label_name(NULL) != KS_OK);
     EXPECT(kb_validate_label_name("") != KS_OK);
     EXPECT(kb_validate_label_name("has.dot") != KS_OK);
 
     EXPECT(kb_validate_node_name("node.1") == KS_OK);
     EXPECT(kb_validate_node_name("simple") == KS_OK);
     EXPECT(kb_validate_node_name("has space") != KS_OK);
 
     return true;
 }
 
 static bool test_kb_pop_key(void)
 {
     char *out = NULL;
 
     EXPECT_OK(kb_pop_key("company.employees.person.alice", &out));
     EXPECT(strcmp(out, "company.employees") == 0);
     free(out);
 
     EXPECT_OK(kb_pop_key("a.b.c", &out));
     EXPECT(strcmp(out, "a") == 0);
     free(out);
 
     EXPECT_OK(kb_pop_key("deep.topic.path.label.node", &out));
     EXPECT(strcmp(out, "deep.topic.path") == 0);
     free(out);
 
     EXPECT(kb_pop_key("a.b", &out) != KS_OK);
     EXPECT(kb_pop_key("single", &out) != KS_OK);
 
     return true;
 }
 
 static bool test_kb_validate_key_format(void)
 {
     EXPECT(kb_validate_key_format("valid.topic.label.node") == true);
     EXPECT(kb_validate_key_format("a.b.c") == true);
     EXPECT(kb_validate_key_format("deep.multi.seg.label.node") == true);
 
     EXPECT(kb_validate_key_format("invalid") == false);
     EXPECT(kb_validate_key_format("also.invalid") == false);
     EXPECT(kb_validate_key_format("") == false);
     EXPECT(kb_validate_key_format(NULL) == false);
 
     return true;
 }
 
 static bool test_kb_store_and_get(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb", "Test KB"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     const char *lj = "{\"type\":\"entity\",\"description\":\"A person\"}";
     const char *nj = "{\"id\":\"p001\",\"data\":{\"name\":\"Alice\",\"age\":30}}";
 
     char *out_key = NULL;
     EXPECT_OK(kb_store(kb, "company.employees", "person", "alice",
                        lj, nj, true, &out_key));
     EXPECT(out_key != NULL);
     EXPECT(strcmp(out_key, "company.employees.person.alice") == 0);
 
     KbEntry entry;
     EXPECT_OK(kb_get(kb, out_key, &entry));
     EXPECT(entry.label_json != NULL);
     EXPECT(entry.node_json != NULL);
 
     cJSON *label = cJSON_Parse(entry.label_json);
     EXPECT(label != NULL);
     EXPECT(strcmp(cJSON_GetObjectItem(label, "type")->valuestring, "entity") == 0);
     cJSON_Delete(label);
 
     cJSON *node = cJSON_Parse(entry.node_json);
     EXPECT(node != NULL);
     cJSON *data = cJSON_GetObjectItem(node, "data");
     EXPECT(strcmp(cJSON_GetObjectItem(data, "name")->valuestring, "Alice") == 0);
     cJSON_Delete(node);
 
     kb_entry_free(&entry);
     free(out_key);
     cleanup_bucket(ks);
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_store_non_composite(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_nc", "Test"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     const char *lj = "{\"type\":\"team\",\"description\":\"Dev team\"}";
     const char *nj = "{\"id\":\"t001\",\"data\":{\"members\":[\"a\",\"b\"]}}";
 
     char *out_key = NULL;
     EXPECT_OK(kb_store(kb, "company.dept", "team", "backend",
                        lj, nj, false, &out_key));
     EXPECT(out_key != NULL);
     EXPECT(strcmp(out_key, "company.dept") == 0);
     free(out_key);
 
     cleanup_bucket(ks);
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_delete(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_del", "Test"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     const char *lj = "{\"type\":\"x\",\"description\":\"y\"}";
     const char *nj = "{\"id\":\"1\",\"data\":{}}";
     char *key = NULL;
     EXPECT_OK(kb_store(kb, "test.topic", "lbl", "nd", lj, nj, true, &key));
 
     EXPECT_OK(kb_delete(kb, key));
 
     KbEntry entry;
     EXPECT(kb_get(kb, key, &entry) == KS_ERR_NOT_FOUND);
 
     free(key);
     cleanup_bucket(ks);
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_list_keys(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_list", "Test"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     const char *lj = "{\"type\":\"x\",\"description\":\"y\"}";
     const char *nj = "{\"id\":\"1\",\"data\":{}}";
 
     EXPECT_OK(kb_store(kb, "alpha", "l1", "n1", lj, nj, true, NULL));
     EXPECT_OK(kb_store(kb, "alpha", "l2", "n2", lj, nj, true, NULL));
     EXPECT_OK(kb_store(kb, "beta", "l1", "n1", lj, nj, true, NULL));
 
     char **keys = NULL;
     size_t count = 0;
     EXPECT_OK(kb_list_keys(kb, NULL, &keys, &count));
     EXPECT(count == 3);
     ks_free_keys(keys, count);
 
     EXPECT_OK(kb_list_keys(kb, "alpha", &keys, &count));
     EXPECT(count == 2);
     ks_free_keys(keys, count);
 
     cleanup_bucket(ks);
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_stats(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_stats", "Test"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
     cleanup_bucket(ks);
 
     const char *lj = "{\"type\":\"x\",\"description\":\"y\"}";
     const char *nj = "{\"id\":\"1\",\"data\":{}}";
 
     EXPECT_OK(kb_store(kb, "topic1", "l1", "n1", lj, nj, true, NULL));
     EXPECT_OK(kb_store(kb, "topic1", "l2", "n2", lj, nj, true, NULL));
     EXPECT_OK(kb_store(kb, "topic2.sub", "l1", "n1", lj, nj, true, NULL));
 
     KbStats stats;
     EXPECT_OK(kb_get_stats(kb, &stats));
     EXPECT(stats.total_kb_keys == 3);
     EXPECT(stats.total_topics == 2);
 
     kb_stats_free(&stats);
     cleanup_bucket(ks);
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_sync_wrappers(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_sync", "Test"));
 
     const char *lj = "{\"type\":\"x\",\"description\":\"y\"}";
     const char *nj = "{\"id\":\"1\",\"data\":{\"val\":42}}";
 
     char *key = NULL;
     EXPECT_OK(kb_store_sync(kb, "sync.topic", "label", "node",
                             lj, nj, true, &key));
     EXPECT(key != NULL);
 
     KbEntry entry;
     EXPECT_OK(kb_get_sync(kb, key, &entry));
     EXPECT(entry.label_json != NULL);
     kb_entry_free(&entry);
 
     EXPECT_OK(kb_delete_sync(kb, key));
     free(key);
 
     kb_destroy(kb);
     return true;
 }
 
 static bool test_kb_validation_errors(void)
 {
     KbStore *kb = NULL;
     EXPECT_OK(kb_create(&kb, TEST_SERVER, "test_c_kb_val", "Test"));
 
     KeyStore *ks = kb_get_keystore(kb);
     EXPECT_OK(ks_connect(ks));
 
     /* Invalid topic */
     ks_status_t st = kb_store(kb, ".bad", "l", "n",
                               "{\"type\":\"a\",\"description\":\"b\"}",
                               "{\"id\":\"1\",\"data\":{}}",
                               true, NULL);
     EXPECT(st == KS_ERR_INVALID_ARG);
 
     /* Invalid label (dot not allowed) */
     st = kb_store(kb, "good", "bad.label", "n",
                   "{\"type\":\"a\",\"description\":\"b\"}",
                   "{\"id\":\"1\",\"data\":{}}",
                   true, NULL);
     EXPECT(st == KS_ERR_INVALID_ARG);
 
     /* Invalid JSON */
     st = kb_store(kb, "good", "label", "node",
                   "not json", "{\"id\":\"1\",\"data\":{}}",
                   true, NULL);
     EXPECT(st == KS_ERR_INVALID_ARG);
 
     ks_disconnect(ks);
     kb_destroy(kb);
     return true;
 }
 
 /* ================================================================== */
 /*  Demo                                                               */
 /* ================================================================== */
 
 static void run_demo(void)
 {
     printf("\n");
     printf("====================================================\n");
     printf("  NATS KeyStore + KbStore Demo (C)\n");
     printf("====================================================\n");
 
     /* --- KeyStore demo --- */
     printf("\n--- KeyStore Demo ---\n");
 
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server = TEST_SERVER;
     cfg.bucket = "demo_c";
 
     KeyStore *ks = NULL;
     if (ks_create(&ks, &cfg) != KS_OK || ks_connect(ks) != KS_OK) {
         fprintf(stderr, "Failed to create/connect KeyStore\n");
         ks_destroy(ks);
         return;
     }
     cleanup_bucket(ks);
 
     printf("\n1. Basic put/get:\n");
     ks_put(ks, "demo.name", "\"Alice\"", NULL);
     char *val = NULL;
     ks_get(ks, "demo.name", &val);
     printf("   demo.name = %s\n", val);
     free(val);
 
     ks_put(ks, "demo.user", "{\"id\":1,\"name\":\"Bob\",\"age\":30}", NULL);
     ks_get(ks, "demo.user", &val);
     printf("   demo.user = %s\n", val);
     free(val);
 
     printf("\n2. Counters:\n");
     int64_t cnt = 0;
     ks_increment(ks, "demo.visits", 1, &cnt);
     printf("   visits = %" PRId64 "\n", cnt);
     ks_increment(ks, "demo.visits", 5, &cnt);
     printf("   visits = %" PRId64 "\n", cnt);
     ks_decrement(ks, "demo.visits", 2, &cnt);
     printf("   visits = %" PRId64 "\n", cnt);
 
     printf("\n3. Key listing:\n");
     char **keys = NULL;
     size_t kcount = 0;
     ks_keys(ks, "demo.*", &keys, &kcount);
     printf("   Found %zu keys:\n", kcount);
     for (size_t i = 0; i < kcount; i++)
         printf("     - %s\n", keys[i]);
     ks_free_keys(keys, kcount);
 
     cleanup_bucket(ks);
     ks_destroy(ks);
 
     /* --- KbStore demo --- */
     printf("\n--- KbStore Demo ---\n");
 
     KbStore *kb = NULL;
     if (kb_create(&kb, TEST_SERVER, "demo_c_kb", "Demo KB") != KS_OK) {
         fprintf(stderr, "Failed to create KbStore\n");
         return;
     }
 
     KeyStore *kbs = kb_get_keystore(kb);
     if (ks_connect(kbs) != KS_OK) {
         fprintf(stderr, "Failed to connect KbStore\n");
         kb_destroy(kb);
         return;
     }
     cleanup_bucket(kbs);
 
     printf("\n4. Store KB entries:\n");
     const char *l1 = "{\"type\":\"entity\",\"description\":\"Person\",\"category\":\"human\"}";
     const char *n1 = "{\"id\":\"p001\",\"data\":{\"name\":\"Alice Johnson\",\"age\":30,"
                      "\"skills\":[\"Python\",\"ML\"]}}";
     char *kb_key = NULL;
     kb_store(kb, "company.employees", "person", "alice_johnson",
              l1, n1, true, &kb_key);
     printf("   Stored: %s\n", kb_key);
 
     const char *l2 = "{\"type\":\"org_unit\",\"description\":\"Backend team\"}";
     const char *n2 = "{\"id\":\"t001\",\"data\":{\"members\":[\"alice\",\"bob\"]}}";
     char *key2 = NULL;
     kb_store(kb, "company.dept.eng", "team", "backend", l2, n2, false, &key2);
     printf("   Stored (base): %s\n", key2);
     free(key2);
 
     printf("\n5. Retrieve:\n");
     KbEntry entry;
     if (kb_get(kb, kb_key, &entry) == KS_OK) {
         printf("   Label: %s\n", entry.label_json);
         printf("   Node:  %s\n", entry.node_json);
         kb_entry_free(&entry);
     }
 
     printf("\n6. Pop key:\n");
     char *popped = NULL;
     kb_pop_key(kb_key, &popped);
     printf("   %s -> %s\n", kb_key, popped);
     free(popped);
     free(kb_key);
 
     printf("\n7. Stats:\n");
     KbStats stats;
     if (kb_get_stats(kb, &stats) == KS_OK) {
         printf("   KB keys: %zu, Topics: %zu\n", stats.total_kb_keys, stats.total_topics);
         for (size_t i = 0; i < stats.topic_array_len; i++)
             printf("     %s: %zu\n", stats.topic_names[i], stats.topic_counts[i]);
         kb_stats_free(&stats);
     }
 
     printf("\n8. Validation:\n");
     const char *tv[] = {"valid.topic.label.node", "invalid", "a.b", "", NULL};
     for (int i = 0; tv[i]; i++)
         printf("   '%s' -> %s\n", tv[i],
                kb_validate_key_format(tv[i]) ? "valid" : "invalid");
 
     cleanup_bucket(kbs);
     ks_disconnect(kbs);
     kb_destroy(kb);
 
     printf("\n====================================================\n");
 }
 
 /* ================================================================== */
 /*  Main                                                               */
 /* ================================================================== */
 
 int main(int argc, char **argv)
 {
     const char *mode = (argc > 1) ? argv[1] : "all";
 
     printf("\n======================================================================\n");
     printf("  NATS KeyStore + KbStore Test Suite (C)\n");
     printf("  Server: %s\n", TEST_SERVER);
     printf("======================================================================\n");
 
     if (strcmp(mode, "demo") == 0) {
         run_demo();
         return 0;
     }
 
     if (strcmp(mode, "all") == 0 || strcmp(mode, "keystore") == 0) {
         printf("\n--- KeyStore Tests ---\n");
         RUN_TEST(test_ks_put_get_string);
         RUN_TEST(test_ks_put_get_json);
         RUN_TEST(test_ks_delete);
         RUN_TEST(test_ks_exists);
         RUN_TEST(test_ks_keys_pattern);
         RUN_TEST(test_ks_increment);
         RUN_TEST(test_ks_decrement);
         RUN_TEST(test_ks_increment_non_numeric);
         RUN_TEST(test_ks_missing_key);
         RUN_TEST(test_ks_sync_wrappers);
         RUN_TEST(test_ks_performance);
     }
 
     if (strcmp(mode, "all") == 0 || strcmp(mode, "kbstore") == 0) {
         printf("\n--- KbStore Tests ---\n");
         RUN_TEST(test_kb_validate_topic);
         RUN_TEST(test_kb_validate_names);
         RUN_TEST(test_kb_pop_key);
         RUN_TEST(test_kb_validate_key_format);
         RUN_TEST(test_kb_store_and_get);
         RUN_TEST(test_kb_store_non_composite);
         RUN_TEST(test_kb_delete);
         RUN_TEST(test_kb_list_keys);
         RUN_TEST(test_kb_stats);
         RUN_TEST(test_kb_sync_wrappers);
         RUN_TEST(test_kb_validation_errors);
     }
 
     printf("\n======================================================================\n");
     printf("  Results: %d run, %d passed, %d failed (%.1f%%)\n",
            tests_run, tests_passed, tests_failed,
            tests_run > 0 ? (100.0 * tests_passed / tests_run) : 0.0);
     printf("======================================================================\n\n");
 
     return tests_failed > 0 ? 1 : 0;
 }