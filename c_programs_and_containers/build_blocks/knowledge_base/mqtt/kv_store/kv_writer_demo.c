/**
 * kv_writer_demo.c - Test driver for kv_store_writer library.
 *
 * Compile:
 *   gcc -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L \
 *       -o kv_writer_demo kv_writer_demo.c kv_store_writer.c \
 *       -lmosquitto -lpthread
 */

 #include <stdio.h>
 #include <string.h>
 
 #include "kv_store_writer.h"
 
 int main(void)
 {
     printf("=== KVStoreWriter Demo ===\n\n");
 
     /* Configuration */
     kvw_config_t cfg;
     kvw_config_init(&cfg);
     strncpy(cfg.client_id, "kv-writer-demo", sizeof(cfg.client_id) - 1);
 
     /* Create writer */
     kvw_store_writer_t writer;
     if (kvw_init(&writer, &cfg) != 0) {
         fprintf(stderr, "Failed to initialise writer\n");
         return 1;
     }
 
     /* Connect */
     printf("Connecting to broker...\n");
     if (!kvw_connect(&writer, 5.0)) {
         fprintf(stderr, "Failed to connect to broker. Is Mosquitto running?\n");
         kvw_destroy(&writer);
         return 1;
     }
 
     /* 1. Write single values */
     printf("\n1. Writing single values:\n");
     kvw_write_single(&writer, "demo/config/host",    "192.168.1.1", 1, true, 2.0);
     kvw_write_single(&writer, "demo/config/port",    "8080",        1, true, 2.0);
     kvw_write_single(&writer, "demo/config/enabled", "True",        1, true, 2.0);
 
     /* 2. Write batch of values */
     printf("\n2. Writing batch of values:\n");
     {
         const char *topics[] = {
             "demo/status/cpu",
             "demo/status/memory",
             "demo/status/disk",
             "demo/status/network",
             "demo/status/services",
         };
         const char *values[] = {
             "45.2",
             "78.5",
             "62.1",
             "up",
             "healthy",
         };
         const char *failed[5] = {NULL};
         int count = (int)(sizeof(topics) / sizeof(topics[0]));
 
         int success = kvw_write_batch(&writer, count, topics, values,
                                       1, true, 10.0, failed);
         int nfailed = 0;
         for (int i = 0; i < count; i++) { if (failed[i]) nfailed++; }
         printf("  Batch write: %d successful, %d failed\n", success, nfailed);
     }
 
     /* 3. Update a value */
     printf("\n3. Updating a value:\n");
     kvw_update_single(&writer, "demo/config/port", "9090", 1, 2.0);
 
     /* 4. Delete single value */
     printf("\n4. Deleting single value:\n");
     kvw_delete_single(&writer, "demo/config/enabled", 2.0);
 
     /* 5. Delete batch */
     printf("\n5. Deleting batch of values:\n");
     {
         const char *to_delete[] = {
             "demo/status/network",
             "demo/status/services",
         };
         const char *failed[2] = {NULL};
         int count = (int)(sizeof(to_delete) / sizeof(to_delete[0]));
 
         int success = kvw_delete_batch(&writer, count, to_delete, 10.0, failed);
         int nfailed = 0;
         for (int i = 0; i < count; i++) { if (failed[i]) nfailed++; }
         printf("  Batch delete: %d successful, %d failed\n", success, nfailed);
     }
 
     /* 6. Clear pattern (requires KVStoreReader — skipped) */
     printf("\n6. Clearing pattern (requires KVStoreReader): skipped\n");
 
     printf("\n* Demo completed successfully!\n");
 
     /* Cleanup */
     printf("\nDisconnecting...\n");
     kvw_disconnect(&writer);
     kvw_destroy(&writer);
 
     return 0;
 }