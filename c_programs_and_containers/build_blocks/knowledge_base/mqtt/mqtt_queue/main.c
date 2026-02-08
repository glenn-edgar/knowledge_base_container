/**
 * main.c - Test driver for mqtt_queue library
 *
 * Mirrors the Python QueueReader / QueuePublisher demos:
 *   1. Create persistent session & register subscription
 *   2. Publish messages while consumer is offline
 *   3. Reconnect and drain the queued messages
 *
 * Build:
 *   gcc -Wall -Wextra -o mqtt_test main.c mqtt_queue.c \
 *       -lmosquitto -lpthread
 *
 * Requires: Mosquitto broker running on localhost:1883
 */

 #define _DEFAULT_SOURCE   /* usleep */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <unistd.h>  /* usleep */
 
 #include "mqtt_queue.h"
 
 #define TOPIC       "work/items/task"
 #define BROKER      "localhost"
 #define PORT        1883
 #define READER_ID   "worker-1"
 #define PUB_ID      "queue-writer"
 
 /* Simple JSON string builder (no dependency on cJSON for the demo) */
 static const char *test_jobs[] = {
     "{\"job_id\":301,\"op\":\"compress\",\"args\":{\"file\":\"a.bin\"}}",
     "{\"job_id\":302,\"op\":\"resize\",\"args\":{\"image\":\"img.jpg\",\"w\":640,\"h\":480}}",
     "{\"job_id\":303,\"op\":\"checksum\",\"args\":{\"file\":\"a.bin\"}}",
 };
 #define NUM_JOBS  (int)(sizeof(test_jobs) / sizeof(test_jobs[0]))
 
 static const char *batch_jobs[] = {
     "{\"job_id\":101,\"op\":\"backup\",\"args\":{\"path\":\"/data\"}}",
     "{\"job_id\":102,\"op\":\"scan\",\"args\":{\"target\":\"network\"}}",
     "{\"job_id\":103,\"op\":\"report\",\"args\":{\"format\":\"pdf\"}}",
 };
 #define NUM_BATCH  (int)(sizeof(batch_jobs) / sizeof(batch_jobs[0]))
 
 /* ------------------------------------------------------------------ */
 /*  Test 1: Publisher standalone test                                   */
 /* ------------------------------------------------------------------ */
 static int test_publisher(void)
 {
     printf("\n=== Test 1: Publisher ===\n\n");
 
     mqtt_queue_config_t cfg = {
         .host          = BROKER,
         .port          = PORT,
         .client_id     = PUB_ID,
         .keepalive     = 60,
         .username      = NULL,
         .password      = NULL,
         .clean_session = true,
     };
 
     mqtt_publisher_t pub;
     if (mqtt_publisher_init(&pub, &cfg) != 0)
         return -1;
 
     printf("Connecting publisher...\n");
     if (mqtt_publisher_connect(&pub, 5000) != 0) {
         mqtt_publisher_destroy(&pub);
         return -1;
     }
 
     /* Publish individual messages */
     printf("\nPublishing individual messages:\n");
     for (int i = 0; i < NUM_JOBS; i++) {
         if (mqtt_publisher_publish(&pub, TOPIC, test_jobs[i], 1, false) == 0) {
             printf("  OK  Published job %d/%d\n", i + 1, NUM_JOBS);
         } else {
             printf("  ERR Failed job %d/%d\n", i + 1, NUM_JOBS);
         }
         usleep(100 * 1000);  /* 100 ms */
     }
 
     /* Batch publish */
     printf("\nBatch publish:\n");
     int ok = mqtt_publisher_publish_batch(&pub, TOPIC, batch_jobs, NUM_BATCH,
                                           1, false, 50);
     printf("Batch result: %d/%d successful\n", ok, NUM_BATCH);
 
     /* QoS 2 test */
     printf("\nQoS 2 publish:\n");
     const char *critical = "{\"job_id\":999,\"op\":\"critical_update\",\"args\":{\"target\":\"database\"}}";
     if (mqtt_publisher_publish(&pub, "work/items/critical", critical, 2, false) == 0) {
         printf("  OK  Critical job published with QoS 2\n");
     }
 
     /* Allow in-flight messages to complete */
     usleep(500 * 1000);
 
     mqtt_publisher_disconnect(&pub);
     mqtt_publisher_destroy(&pub);
     printf("\nPublisher test complete.\n");
     return 0;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Test 2: Persistent session / queued message demo                   */
 /* ------------------------------------------------------------------ */
 static int test_persistent_queue(void)
 {
     printf("\n=== Test 2: Persistent Queue Demo ===\n\n");
 
     mqtt_queue_config_t reader_cfg = {
         .host          = BROKER,
         .port          = PORT,
         .client_id     = READER_ID,
         .keepalive     = 60,
         .username      = NULL,
         .password      = NULL,
         .clean_session = false,   /* persistent session */
     };
 
     /* --- Step 1: create session and register subscription ---------- */
     printf("1. Creating persistent session and registering subscription...\n");
     mqtt_reader_t rdr;
     if (mqtt_reader_init(&rdr, &reader_cfg) != 0)
         return -1;
 
     if (mqtt_reader_connect(&rdr, 5000) != 0) {
         mqtt_reader_destroy(&rdr);
         return -1;
     }
 
     if (mqtt_reader_subscribe(&rdr, TOPIC, 1, 2000) != 0) {
         fprintf(stderr, "Failed to subscribe. Check broker/ACLs.\n");
         mqtt_reader_disconnect(&rdr);
         mqtt_reader_destroy(&rdr);
         return -1;
     }
     printf("   Subscription registered in persistent session\n");
 
     mqtt_reader_disconnect(&rdr);
     mqtt_reader_destroy(&rdr);
     printf("   Disconnected (session persisted)\n\n");
 
     /* --- Step 2: publish while consumer is offline ----------------- */
     printf("2. Publishing messages while consumer is offline...\n");
 
     mqtt_queue_config_t pub_cfg = {
         .host          = BROKER,
         .port          = PORT,
         .client_id     = "offline-publisher",
         .keepalive     = 60,
         .username      = NULL,
         .password      = NULL,
         .clean_session = true,
     };
 
     mqtt_publisher_t pub;
     if (mqtt_publisher_init(&pub, &pub_cfg) != 0)
         return -1;
 
     if (mqtt_publisher_connect(&pub, 5000) != 0) {
         mqtt_publisher_destroy(&pub);
         return -1;
     }
 
     for (int i = 0; i < NUM_JOBS; i++) {
         if (mqtt_publisher_publish(&pub, TOPIC, test_jobs[i], 1, false) == 0) {
             printf("   Published job %d/%d\n", i + 1, NUM_JOBS);
         }
         usleep(100 * 1000);
     }
 
     /* Allow broker to store messages */
     usleep(500 * 1000);
     mqtt_publisher_disconnect(&pub);
     mqtt_publisher_destroy(&pub);
     printf("   Published %d jobs while consumer offline\n\n", NUM_JOBS);
 
     /* --- Step 3: reconnect same client_id and drain ---------------- */
     printf("3. Reconnecting to retrieve queued messages...\n");
 
     mqtt_reader_t rdr2;
     if (mqtt_reader_init(&rdr2, &reader_cfg) != 0)
         return -1;
 
     if (mqtt_reader_connect(&rdr2, 5000) != 0) {
         mqtt_reader_destroy(&rdr2);
         return -1;
     }
 
     /*
      * Mark session_present = true so read_queue skips re-subscribing.
      * (In the Python version, paho returns this in CONNACK flags.
      *  libmosquitto v1 API doesn't surface it; on the second connect
      *  with clean_session=false and same client_id, the session IS
      *  present by MQTT spec.)
      */
     rdr2.session_present = true;
 
     int count = 0;
     mqtt_msg_t *msgs = mqtt_reader_read_queue(&rdr2, TOPIC, 1, 3000, &count);
 
     printf("\n   Retrieved %d queued message(s):\n", count);
     for (mqtt_msg_t *m = msgs; m; m = m->next) {
         printf("     Topic: %s\n", m->topic);
         printf("     Payload: %s\n\n", m->payload);
     }
     mqtt_msg_list_free(msgs);
 
     mqtt_reader_disconnect(&rdr2);
     mqtt_reader_destroy(&rdr2);
     printf("Persistent queue demo complete.\n");
     return 0;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Main                                                               */
 /* ------------------------------------------------------------------ */
 int main(void)
 {
     printf("============================================\n");
     printf("   MQTT Queue Library - Test Driver\n");
     printf("============================================\n");
     printf("Broker: %s:%d\n\n", BROKER, PORT);
 
     mqtt_queue_lib_init();
 
     int rc = 0;
 
     if (test_publisher() != 0) {
         fprintf(stderr, "\n*** Publisher test failed. Is Mosquitto running? ***\n");
         rc = 1;
         goto cleanup;
     }
 
     if (test_persistent_queue() != 0) {
         fprintf(stderr, "\n*** Persistent queue test failed. ***\n");
         rc = 1;
         goto cleanup;
     }
 
     printf("\n============================================\n");
     printf("   All tests passed!\n");
     printf("============================================\n");
 
 cleanup:
     mqtt_queue_lib_cleanup();
     return rc;
 }