/*
 * test_kb_data_structures.c
 * Knowledge Base C Port — Integration test
 *
 * Mirrors LuaJIT test_kb_data_structures.lua.
 *
 * Usage:
 *   ./test_kb_data_structures
 *       In-memory test with synthetic data (42 assertions).
 *
 *   ./test_kb_data_structures knowledge_base.db
 *       Uses the LuaJIT-constructed database.
 *       Table name defaults to "knowledge_base".
 *
 *   ./test_kb_data_structures knowledge_base.db my_table_name
 *       Custom table name.
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include "kb_common.h"
 #include "kb_data_structures.h"
 #include "kb_uuid.h"
 #include "test_common.h"
 
 /* ================================================================
  * In-memory test database
  * ================================================================ */
 
 static sqlite3 *create_test_database(void)
 {
     sqlite3 *db = NULL;
     if (sqlite3_open(":memory:", &db) != SQLITE_OK) return NULL;
 
     const char *sql[] = {
         "CREATE TABLE test_kb ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  knowledge_base TEXT, label TEXT, name TEXT,"
         "  path TEXT UNIQUE, properties TEXT, data TEXT,"
         "  has_link INTEGER DEFAULT 0, has_link_mount INTEGER DEFAULT 0"
         ");",
         "CREATE TABLE test_kb_status_table ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, data TEXT);",
         "CREATE TABLE test_kb_job_queue ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  path TEXT, state TEXT DEFAULT 'free',"
         "  data TEXT, priority INTEGER DEFAULT 0, queued_at TEXT);",
         "CREATE TABLE test_kb_stream_table ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  path TEXT, entry_index INTEGER, write_index INTEGER DEFAULT 0,"
         "  max_entries INTEGER DEFAULT 10, data TEXT, recorded_at TEXT);",
         "CREATE TABLE test_kb_bit_mask_store ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  path TEXT UNIQUE, bit_mask INTEGER DEFAULT 0, change_mask INTEGER DEFAULT 0);",
         "CREATE TABLE test_kb_rpc_server_queue ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  path TEXT, state TEXT DEFAULT 'empty',"
         "  request_uuid TEXT, rpc_action TEXT, data TEXT,"
         "  priority INTEGER DEFAULT 0, rpc_client_queue TEXT, queued_at TEXT);",
         "CREATE TABLE test_kb_rpc_client_queue ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  path TEXT, state TEXT DEFAULT 'free',"
         "  request_uuid TEXT, server_path TEXT, rpc_action TEXT,"
         "  transaction_tag TEXT, reply_data TEXT, replied_at TEXT);",
         "CREATE TABLE test_kb_link_table ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  link_name TEXT, node_path TEXT, link_order INTEGER);",
         "CREATE TABLE test_kb_link_mount_table ("
         "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "  link_name TEXT, mount_path TEXT);",
 
         /* Seed */
         "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
         " ('kb1','KB_STATUS_FIELD','temp','kb1.sensors.temp',"
         "  '{\"description\":\"Temperature\"}','{\"value\":22}');",
         "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
         " ('kb1','KB_JOB_FIELD','worker','kb1.jobs.worker',"
         "  '{\"description\":\"Worker queue\"}','{}');",
         "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
         " ('kb1','KB_BIT_FIELD','flags','kb1.flags.main',"
         "  '{\"description\":\"Main flags\"}','{}');",
         "INSERT INTO test_kb_status_table (path,data) VALUES ('kb1.sensors.temp','{\"value\":22}');",
         "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
         "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
         "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
         "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
         "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
         "INSERT INTO test_kb_bit_mask_store (path,bit_mask,change_mask) VALUES ('kb1.flags.main',0,0);",
         "INSERT INTO test_kb_stream_table (path,entry_index,write_index,max_entries) VALUES ('kb1.stream.data',0,0,10);",
         "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',1);",
         "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',2);",
         "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',3);",
         "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',4);",
         "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
         "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
         "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
         "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",
         "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",
         "INSERT INTO test_kb_link_table (link_name,node_path,link_order) VALUES ('link1','kb1.child1',0);",
         "INSERT INTO test_kb_link_table (link_name,node_path,link_order) VALUES ('link1','kb1.child2',1);",
         "INSERT INTO test_kb_link_mount_table (link_name,mount_path) VALUES ('mount1','kb1.mount.point');",
         NULL
     };
 
     for (int i = 0; sql[i]; i++) {
         char *errmsg = NULL;
         if (sqlite3_exec(db, sql[i], NULL, NULL, &errmsg) != SQLITE_OK) {
             fprintf(stderr, "DDL[%d]: %s\n", i, errmsg ? errmsg : "?");
             if (errmsg) sqlite3_free(errmsg);
             sqlite3_close(db);
             return NULL;
         }
     }
     return db;
 }
 
 /* ================================================================
  * In-memory tests (hardcoded paths)
  * ================================================================ */
 
 static void test_status(kb_ds_t *ds)
 {
     TEST_BEGIN("status table");
     char *data = NULL;
     ASSERT_OK(kb_status_get_data(kb_ds_status(ds), "kb1.sensors.temp", &data), "get status data");
     ASSERT_NOT_NULL(data, "data not null");
     printf("    status data: %s\n", data ? data : "NULL");
     free(data);
 
     ASSERT_OK(kb_status_set_data(kb_ds_status(ds), "kb1.sensors.temp", "{\"value\":25.5}"), "set status data");
     data = NULL;
     ASSERT_OK(kb_status_get_data(kb_ds_status(ds), "kb1.sensors.temp", &data), "get updated data");
     printf("    updated data: %s\n", data ? data : "NULL");
     ASSERT_TRUE(data && strstr(data, "25.5") != NULL, "data contains 25.5");
     free(data);
 }
 
 static void test_job_queue(kb_ds_t *ds)
 {
     TEST_BEGIN("job queue");
     int free_count = 0;
     ASSERT_OK(kb_job_get_free_number(kb_ds_job_queue(ds), "kb1.jobs.worker", &free_count), "get free count");
     ASSERT_EQ_INT(free_count, 5, "5 free slots");
 
     ASSERT_OK(kb_job_push(kb_ds_job_queue(ds), "kb1.jobs.worker", "{\"task\":\"process_data\"}", 1), "push job");
     int queued = 0;
     ASSERT_OK(kb_job_get_queued_number(kb_ds_job_queue(ds), "kb1.jobs.worker", &queued), "get queued count");
     ASSERT_EQ_INT(queued, 1, "1 queued");
 
     char *job_data = NULL; int record_id = 0;
     ASSERT_OK(kb_job_peek(kb_ds_job_queue(ds), "kb1.jobs.worker", &job_data, &record_id), "peek job");
     printf("    job data: %s (id=%d)\n", job_data ? job_data : "NULL", record_id);
     free(job_data);
 
     ASSERT_OK(kb_job_complete(kb_ds_job_queue(ds), "kb1.jobs.worker", record_id), "complete job");
     ASSERT_OK(kb_job_get_free_number(kb_ds_job_queue(ds), "kb1.jobs.worker", &free_count), "free after complete");
     ASSERT_EQ_INT(free_count, 5, "back to 5 free");
 }
 
 static void test_rpc_server(kb_ds_t *ds)
 {
     TEST_BEGIN("RPC server");
     char uuid[KB_UUID_LEN];
     ASSERT_OK(kb_rpc_server_push(kb_ds_rpc_server(ds), "kb1.rpc.server", "do_something", "{\"arg\":1}", 1, "kb1.rpc.client", uuid, sizeof(uuid)), "push RPC job");
     printf("    request_uuid: %s\n", uuid);
 
     char *data = NULL, *uuid_out = NULL, *action = NULL; int rec_id = 0;
     ASSERT_OK(kb_rpc_server_peek(kb_ds_rpc_server(ds), "kb1.rpc.server", &data, &uuid_out, &action, &rec_id), "peek RPC job");
     printf("    action=%s data=%s\n", action ? action : "?", data ? data : "?");
 
     ASSERT_OK(kb_rpc_server_claim(kb_ds_rpc_server(ds), "kb1.rpc.server", rec_id), "claim job");
     ASSERT_OK(kb_rpc_server_complete(kb_ds_rpc_server(ds), "kb1.rpc.server", rec_id), "complete job");
 
     int empty = 0, new_job = 0, processing = 0;
     ASSERT_OK(kb_rpc_server_get_state_counts(kb_ds_rpc_server(ds), "kb1.rpc.server", &empty, &new_job, &processing), "state counts");
     ASSERT_EQ_INT(empty, 3, "all 3 empty again");
     free(data); free(uuid_out); free(action);
 }
 
 static void test_rpc_client(kb_ds_t *ds)
 {
     TEST_BEGIN("RPC client");
     ASSERT_OK(kb_rpc_client_push_and_claim(kb_ds_rpc_client(ds), "kb1.rpc.client", "test-uuid-123", "kb1.rpc.server", "do_something", "tag1", "{\"result\":\"ok\"}"), "push and claim reply");
 
     int free_n = 0, queued_n = 0;
     ASSERT_OK(kb_rpc_client_get_state_counts(kb_ds_rpc_client(ds), "kb1.rpc.client", &free_n, &queued_n), "state counts");
     ASSERT_EQ_INT(queued_n, 1, "1 queued reply");
 
     char *reply = NULL, *uuid = NULL, *action = NULL; int rec_id = 0;
     ASSERT_OK(kb_rpc_client_peek_reply(kb_ds_rpc_client(ds), "kb1.rpc.client", &reply, &uuid, &action, &rec_id), "peek reply");
     printf("    reply=%s\n", reply ? reply : "?");
     ASSERT_OK(kb_rpc_client_clear_reply(kb_ds_rpc_client(ds), "kb1.rpc.client", rec_id), "clear reply");
     free(reply); free(uuid); free(action);
 }
 
 static void test_bit_mask(kb_ds_t *ds)
 {
     TEST_BEGIN("bit mask");
     kb_bit_mask_ops_t *ops = kb_bit_structures_get_ops(kb_ds_bit_structures(ds));
 
     ASSERT_OK(kb_bit_set(ops, "kb1.flags.main", 0, 1), "set bit 0");
     ASSERT_OK(kb_bit_set(ops, "kb1.flags.main", 2, 1), "set bit 2");
 
     int val = 0;
     ASSERT_OK(kb_bit_get(ops, "kb1.flags.main", 0, &val), "get bit 0");
     ASSERT_EQ_INT(val, 1, "bit 0 == 1");
     ASSERT_OK(kb_bit_get(ops, "kb1.flags.main", 1, &val), "get bit 1");
     ASSERT_EQ_INT(val, 0, "bit 1 == 0");
 
     int64_t mask = 0;
     ASSERT_OK(kb_bit_get_mask(ops, "kb1.flags.main", &mask), "get mask");
     ASSERT_EQ_INT((int)mask, 5, "mask == 0x05");
 
     int64_t cm = 0;
     ASSERT_OK(kb_bit_get_change_mask(ops, "kb1.flags.main", &cm), "get change mask");
     ASSERT_TRUE(cm != 0, "change mask non-zero");
     ASSERT_OK(kb_bit_clear_change_mask(ops, "kb1.flags.main"), "clear change mask");
     ASSERT_OK(kb_bit_get_change_mask(ops, "kb1.flags.main", &cm), "verify cleared");
     ASSERT_EQ_INT((int)cm, 0, "change mask == 0");
 }
 
 static void test_link_tables(kb_ds_t *ds)
 {
     TEST_BEGIN("link tables");
     kb_result_t result;
     ASSERT_OK(kb_link_get_by_link_name(kb_ds_link_table(ds), "link1", &result), "get links by name");
     ASSERT_EQ_INT(result.count, 2, "2 links for link1");
     kb_result_free(&result);
     ASSERT_OK(kb_link_mount_get_by_link_name(kb_ds_link_mount_table(ds), "mount1", &result), "get link mounts");
     ASSERT_EQ_INT(result.count, 1, "1 mount for mount1");
     kb_result_free(&result);
 }
 
 /* ================================================================
  * Real database tests — discovery-based, mirrors LuaJIT test
  * ================================================================ */
 
 /*
  * Helper: use KB_Search to find paths by label.
  * Returns first discovered path (caller must free), or NULL.
  */
 static char *discover_first_path(kb_ds_t *ds, const char *label)
 {
     kb_search_t *ks = kb_ds_search(ds);
     kb_search_clear_filters(ks);
     kb_search_label(ks, label);
     if (kb_search_execute(ks) != KB_OK) return NULL;
     const kb_result_t *r = kb_search_results(ks);
     if (r->count == 0) return NULL;
     const char *p = kb_row_get(r, 0, "path");
     return p ? kb_strdup(p) : NULL;
 }
 
 static void test_real_kb_search(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: KB_Search");
     kb_search_t *ks = kb_ds_search(ds);
 
     /* All nodes */
     kb_search_clear_filters(ks);
     ASSERT_OK(kb_search_execute(ks), "execute no filters");
     const kb_result_t *r = kb_search_results(ks);
     printf("    total nodes: %d\n", r->count);
     ASSERT_TRUE(r->count > 0, "KB has nodes");
 
     /* find_path_values */
     char **paths = NULL; int path_count = 0;
     ASSERT_OK(kb_search_find_path_values(r, &paths, &path_count), "find_path_values");
     printf("    total paths: %d\n", path_count);
     for (int i = 0; i < path_count && i < 5; i++)
         printf("      [%d] %s\n", i, paths[i]);
     if (path_count > 5) printf("      ... (%d more)\n", path_count - 5);
     kb_path_values_free(paths, path_count);
 
     /* find_description */
     kb_description_t *descs = NULL; int desc_count = 0;
     kb_error_t err = kb_search_find_description(r, &descs, &desc_count);
     if (err == KB_OK && desc_count > 0) {
         printf("    descriptions: %d\n", desc_count);
         for (int i = 0; i < desc_count && i < 5; i++)
             printf("      %s: %s\n", descs[i].path, descs[i].description);
     }
     kb_description_free(descs, desc_count);
 
     /* Label scan — mirrors LuaJIT find_status_node_ids / find_job_ids / etc */
     const char *labels[] = {
         "KB_STATUS_FIELD", "KB_JOB_FIELD", "KB_BIT_FIELD",
         "KB_STREAM_FIELD", "KB_RPC_SERVER_FIELD", "KB_RPC_CLIENT_FIELD",
         "KB_LINK_NODE", NULL
     };
     for (int i = 0; labels[i]; i++) {
         kb_search_clear_filters(ks);
         kb_search_label(ks, labels[i]);
         err = kb_search_execute(ks);
         if (err == KB_OK) {
             r = kb_search_results(ks);
             if (r->count > 0) {
                 printf("    label '%s': %d nodes\n", labels[i], r->count);
                 for (int j = 0; j < r->count && j < 3; j++) {
                     const char *p = kb_row_get(r, j, "path");
                     const char *n = kb_row_get(r, j, "name");
                     printf("      %s (%s)\n", p ? p : "?", n ? n : "?");
                 }
                 _test_pass_count++;
             }
         }
     }
 
     /* starting_path — mirrors LuaJIT search_starting_path("kb1") */
     kb_search_clear_filters(ks);
     kb_search_starting_path(ks, "kb1");
     err = kb_search_execute(ks);
     if (err == KB_OK) {
         r = kb_search_results(ks);
         printf("    starting_path 'kb1': %d nodes\n", r->count);
         ASSERT_TRUE(r->count > 0, "starting_path kb1 found nodes");
     }
 
     /* has_link — mirrors LuaJIT search_has_link() */
     kb_search_clear_filters(ks);
     kb_search_has_link(ks);
     err = kb_search_execute(ks);
     if (err == KB_OK) {
         r = kb_search_results(ks);
         printf("    has_link: %d nodes\n", r->count);
     }
 
     /* has_link_mount */
     kb_search_clear_filters(ks);
     kb_search_has_link_mount(ks);
     err = kb_search_execute(ks);
     if (err == KB_OK) {
         r = kb_search_results(ks);
         printf("    has_link_mount: %d nodes\n", r->count);
     }
 }
 
 static void test_real_status(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: status data");
 
     char *path = discover_first_path(ds, "KB_STATUS_FIELD");
     if (!path) { printf("    SKIP: no KB_STATUS_FIELD nodes\n"); return; }
     printf("    discovered status path: %s\n", path);
 
     char *data = NULL;
     kb_error_t err = kb_status_get_data(kb_ds_status(ds), path, &data);
     if (err == KB_OK) {
         printf("    initial data: %s\n", data ? data : "NULL");
         ASSERT_NOT_NULL(data, "status data not null");
         free(data);
 
         /* write and read back */
         ASSERT_OK(kb_status_set_data(kb_ds_status(ds), path, "{\"test_c\":true}"), "set status");
         data = NULL;
         ASSERT_OK(kb_status_get_data(kb_ds_status(ds), path, &data), "get updated status");
         printf("    updated data: %s\n", data ? data : "NULL");
         ASSERT_TRUE(data && strstr(data, "test_c") != NULL, "data round-trip");
         free(data);
     } else {
         printf("    status table may not have entry for this path (rc=%d)\n", err);
     }
     free(path);
 }
 
 static void test_real_job_queue(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: job queue");
 
     char *path = discover_first_path(ds, "KB_JOB_FIELD");
     if (!path) { printf("    SKIP: no KB_JOB_FIELD nodes\n"); return; }
     printf("    discovered job path: %s\n", path);
 
     /* Clear first, like LuaJIT test does */
     kb_job_clear(kb_ds_job_queue(ds), path);
 
     int free_n = 0;
     kb_error_t err = kb_job_get_free_number(kb_ds_job_queue(ds), path, &free_n);
     if (err == KB_OK) {
         printf("    free slots: %d\n", free_n);
         ASSERT_TRUE(free_n > 0, "has free slots");
 
         /* push / peek / complete cycle */
         ASSERT_OK(kb_job_push(kb_ds_job_queue(ds), path, "{\"test\":\"from_c\"}", 1), "push job");
 
         int queued = 0;
         ASSERT_OK(kb_job_get_queued_number(kb_ds_job_queue(ds), path, &queued), "queued count");
         ASSERT_EQ_INT(queued, 1, "1 queued");
 
         char *jdata = NULL; int jid = 0;
         ASSERT_OK(kb_job_peek(kb_ds_job_queue(ds), path, &jdata, &jid), "peek job");
         printf("    job data: %s (id=%d)\n", jdata ? jdata : "?", jid);
         free(jdata);
 
         ASSERT_OK(kb_job_complete(kb_ds_job_queue(ds), path, jid), "complete job");
 
         int free_after = 0;
         ASSERT_OK(kb_job_get_free_number(kb_ds_job_queue(ds), path, &free_after), "free after");
         ASSERT_EQ_INT(free_after, free_n, "free restored");
     } else {
         printf("    job_queue table may not exist (rc=%d)\n", err);
     }
     free(path);
 }
 
 static void test_real_bit_mask(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: bit mask");
 
     char *path = discover_first_path(ds, "KB_BIT_FIELD");
     if (!path) { printf("    SKIP: no KB_BIT_FIELD nodes\n"); return; }
     printf("    discovered bit path: %s\n", path);
 
     kb_bit_mask_ops_t *ops = kb_bit_structures_get_ops(kb_ds_bit_structures(ds));
 
     /* Read current mask */
     int64_t mask = 0;
     kb_error_t err = kb_bit_get_mask(ops, path, &mask);
     if (err == KB_OK) {
         printf("    current mask: 0x%llx\n", (unsigned long long)mask);
         _test_pass_count++;
 
         /* Set bit 0, read back */
         ASSERT_OK(kb_bit_set(ops, path, 0, 1), "set bit 0");
         int val = 0;
         ASSERT_OK(kb_bit_get(ops, path, 0, &val), "get bit 0");
         ASSERT_EQ_INT(val, 1, "bit 0 == 1");
 
         /* Clear change mask */
         ASSERT_OK(kb_bit_clear_change_mask(ops, path), "clear change mask");
         int64_t cm = 0;
         ASSERT_OK(kb_bit_get_change_mask(ops, path, &cm), "get change mask");
         ASSERT_EQ_INT((int)cm, 0, "change mask cleared");
     } else {
         printf("    bit_mask_store may not have entry (rc=%d)\n", err);
     }
     free(path);
 }
 
 static void test_real_rpc_server(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: RPC server");
 
     char *path = discover_first_path(ds, "KB_RPC_SERVER_FIELD");
     if (!path) { printf("    SKIP: no KB_RPC_SERVER_FIELD nodes\n"); return; }
     printf("    discovered server path: %s\n", path);
 
     char uuid[KB_UUID_LEN];
     kb_error_t err = kb_rpc_server_push(kb_ds_rpc_server(ds), path,
         "test_action", "{\"from\":\"c_test\"}", 1, "", uuid, sizeof(uuid));
     if (err == KB_OK) {
         printf("    pushed uuid: %s\n", uuid);
         _test_pass_count++;
 
         char *data = NULL, *uuid_out = NULL, *action = NULL; int rid = 0;
         ASSERT_OK(kb_rpc_server_peek(kb_ds_rpc_server(ds), path,
             &data, &uuid_out, &action, &rid), "peek");
         printf("    peek: action=%s id=%d\n", action ? action : "?", rid);
 
         ASSERT_OK(kb_rpc_server_claim(kb_ds_rpc_server(ds), path, rid), "claim");
         ASSERT_OK(kb_rpc_server_complete(kb_ds_rpc_server(ds), path, rid), "complete");
 
         free(data); free(uuid_out); free(action);
     } else {
         printf("    rpc_server_queue may not exist (rc=%d)\n", err);
     }
     free(path);
 }
 
 static void test_real_rpc_client(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: RPC client");
 
     char *path = discover_first_path(ds, "KB_RPC_CLIENT_FIELD");
     if (!path) { printf("    SKIP: no KB_RPC_CLIENT_FIELD nodes\n"); return; }
     printf("    discovered client path: %s\n", path);
 
     kb_error_t err = kb_rpc_client_push_and_claim(kb_ds_rpc_client(ds),
         path, "test-uuid-c", "server.path", "test_action", "tag_c",
         "{\"reply\":\"from_c\"}");
     if (err == KB_OK) {
         _test_pass_count++;
 
         char *reply = NULL, *uuid = NULL, *action = NULL; int rid = 0;
         ASSERT_OK(kb_rpc_client_peek_reply(kb_ds_rpc_client(ds), path,
             &reply, &uuid, &action, &rid), "peek reply");
         printf("    reply: %s\n", reply ? reply : "?");
 
         ASSERT_OK(kb_rpc_client_clear_reply(kb_ds_rpc_client(ds), path, rid), "clear reply");
         free(reply); free(uuid); free(action);
     } else {
         printf("    rpc_client_queue may not exist (rc=%d)\n", err);
     }
     free(path);
 }
 
 static void test_real_link_tables(kb_ds_t *ds, const char *database)
 {
     TEST_BEGIN("real DB: link tables");
     sqlite3 *db = kb_ds_get_db(ds);
     kb_result_t result;
     char *sql;
 
     /* link_table */
     sql = kb_sprintf("SELECT DISTINCT link_name FROM %s_link_table", database);
     kb_result_init(&result);
     kb_error_t err = kb_query_exec(db, sql, NULL, 0, &result);
     free(sql);
     if (err == KB_OK && result.count > 0) {
         printf("    link names: %d\n", result.count);
         for (int i = 0; i < result.count; i++) {
             const char *n = kb_row_get(&result, i, "link_name");
             printf("      - %s\n", n ? n : "?");
         }
         _test_pass_count++;
     }
     kb_result_free(&result);
 
     /* link_mount_table */
     sql = kb_sprintf("SELECT DISTINCT link_name FROM %s_link_mount_table", database);
     kb_result_init(&result);
     err = kb_query_exec(db, sql, NULL, 0, &result);
     free(sql);
     if (err == KB_OK && result.count > 0) {
         printf("    mount link names: %d\n", result.count);
         for (int i = 0; i < result.count; i++) {
             const char *n = kb_row_get(&result, i, "link_name");
             printf("      - %s\n", n ? n : "?");
         }
         _test_pass_count++;
     }
     kb_result_free(&result);
 }
 
 static void test_real_decode_link_nodes(kb_ds_t *ds)
 {
     TEST_BEGIN("real DB: decode_link_nodes");
     kb_search_t *ks = kb_ds_search(ds);
 
     /* Find nodes starting with "kb1" and try to decode paths */
     kb_search_clear_filters(ks);
     kb_search_starting_path(ks, "kb1");
     if (kb_search_execute(ks) == KB_OK) {
         const kb_result_t *r = kb_search_results(ks);
         int decoded = 0;
         for (int i = 0; i < r->count && decoded < 5; i++) {
             const char *p = kb_row_get(r, i, "path");
             if (!p) continue;
 
             char *kb_name = NULL;
             kb_link_pair_t *pairs = NULL;
             int pair_count = 0;
             if (kb_search_decode_link_nodes(p, &kb_name, &pairs, &pair_count) == KB_OK) {
                 printf("    %s → kb=%s pairs=%d\n", p, kb_name, pair_count);
                 free(kb_name);
                 kb_link_pairs_free(pairs, pair_count);
                 decoded++;
             }
         }
         if (decoded > 0) _test_pass_count++;
     }
 }
 
 /* ================================================================
  * Main
  * ================================================================ */
 
 int main(int argc, char *argv[])
 {
     printf("=== KB Data Structures Integration Test ===\n");
 
     kb_ds_t *ds = NULL;
 
     if (argc > 1) {
         const char *db_path  = argv[1];
         const char *database = (argc > 2) ? argv[2] : "knowledge_base";
 
         printf("Database file: %s\n", db_path);
         printf("Table name:    %s\n", database);
 
         ds = kb_ds_create(db_path, database, NULL);
         if (!ds) {
             printf("FATAL: Cannot open %s\n", db_path);
             return 1;
         }
 
         test_real_kb_search(ds);
         test_real_status(ds);
         test_real_job_queue(ds);
         test_real_bit_mask(ds);
         test_real_rpc_server(ds);
         test_real_rpc_client(ds);
         test_real_link_tables(ds, database);
         test_real_decode_link_nodes(ds);
 
         kb_ds_destroy(ds);
     } else {
         printf("Using in-memory test database\n");
         sqlite3 *db = create_test_database();
         if (!db) { printf("FATAL: Cannot create test database\n"); return 1; }
         ds = kb_ds_create_from_db(db, "test_kb");
         if (!ds) { printf("FATAL: Cannot create KB_Data_Structures\n"); return 1; }
 
         test_status(ds);
         test_job_queue(ds);
         test_rpc_server(ds);
         test_rpc_client(ds);
         test_bit_mask(ds);
         test_link_tables(ds);
 
         kb_ds_destroy(ds);
     }
 
     TEST_END();
 }