/**
 * main.c - Test driver for topic_subscriber library.
 *
 * Compile:
 *   gcc -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L \
 *       -o demo main.c topic_subscriber.c -lmosquitto -lpthread
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 #include <mosquitto.h>
 
 #include "topic_subscriber.h"   /* was: "topic_subcriber.h" (typo) */
 
 /* ── Callbacks ──────────────────────────────────────────────────────── */
 
 static void on_config_message(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     printf("  [Config Callback] Received: %s = %s\n", msg->topic, msg->payload);
 }
 
 static void on_status_message(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     struct tm tm;
     localtime_r(&msg->timestamp.tv_sec, &tm);
     printf("  [Status Callback] %s: %s at %02d:%02d:%02d\n",
            msg->topic, msg->payload, tm.tm_hour, tm.tm_min, tm.tm_sec);
 }
 
 static void on_any_message(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     printf("  [Generic Callback] Topic: %s, Retained: %s\n",
            msg->topic, msg->retain ? "true" : "false");
 }
 
 static void on_sensor_data(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     char *endptr;
     double value = strtod(msg->payload, &endptr);
     if (*endptr == '\0' && endptr != msg->payload) {
         const char *name = strrchr(msg->topic, '/');
         name = name ? name + 1 : msg->topic;
         printf("  [Sensor Callback] %s: %.2f\n", name, value);
     } else {
         printf("  [Sensor Callback] Invalid sensor data: %s\n", msg->payload);
     }
 }
 
 static void on_alert_message(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     printf("  [Alert] %s\n", msg->payload);
 }
 
 static void on_log_message(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     char trunc[51];
     strncpy(trunc, msg->payload, 50);
     trunc[50] = '\0';
     printf("  [Log] %s...\n", trunc);
 }
 
 static void extra_config_callback(const ts_message_info_t *msg, void *ud)
 {
     (void)ud;
     printf("  [Extra Config] Processing %s\n", msg->topic);
 }
 
 /* ── Main ───────────────────────────────────────────────────────────── */
 
 int main(void)
 {
     printf("=== TopicSubscriber Demo ===\n\n");
 
     /* Configuration */
     ts_config_t cfg;
     ts_config_init(&cfg);
     strncpy(cfg.client_id, "subscriber-demo", sizeof(cfg.client_id) - 1);
     cfg.auto_reconnect  = true;
     cfg.reconnect_delay = 5.0;
 
     /* Create subscriber */
     topic_subscriber_t subscriber;
     if (ts_init(&subscriber, &cfg) != 0) {
         fprintf(stderr, "Failed to initialise subscriber\n");
         return 1;
     }
 
     /* Connect */
     printf("Connecting to broker...\n");
     if (!ts_connect(&subscriber, 5.0)) {
         fprintf(stderr, "Failed to connect to broker. Is Mosquitto running?\n");
         ts_destroy(&subscriber);
         return 1;
     }
 
     /* 1. Subscribe to specific topics with callbacks */
     printf("\n1. Subscribing to specific topics:\n");
     ts_subscribe(&subscriber, "demo/config/+",              on_config_message, NULL, 1, false);
     ts_subscribe(&subscriber, "demo/status/#",              on_status_message, NULL, 1, false);
     ts_subscribe(&subscriber, "demo/sensors/+/temperature", on_sensor_data,    NULL, 2, false);
 
     /* 2. Subscribe to everything with a generic callback */
     printf("\n2. Adding generic subscription:\n");
     ts_subscribe(&subscriber, "#", on_any_message, NULL, 0, false);
 
     /* 3. Subscribe to multiple topics at once */
     printf("\n3. Subscribing to multiple topics:\n");
     {
         const char *topics[]              = {"demo/alerts/+",  "demo/logs/+"};
         ts_message_callback_t callbacks[] = {on_alert_message, on_log_message};
         int qos_vals[]                    = {2,                0};
         const char *failed[2]             = {NULL, NULL};
         int success = ts_subscribe_many(&subscriber, 2, topics, callbacks,
                                         NULL, qos_vals, false, failed);
         int nfailed = 0;
         for (int i = 0; i < 2; i++) { if (failed[i]) nfailed++; }
         printf("  Subscribed to %d topics, %d failed\n", success, nfailed);
     }
 
     /* 4. Show current subscriptions */
     printf("\n4. Current subscriptions:\n");
     {
         char topics[TS_MAX_SUBSCRIPTIONS][TS_MAX_TOPIC_LEN];
         int  qos[TS_MAX_SUBSCRIPTIONS];
         int n = ts_get_subscriptions(&subscriber, topics, qos, TS_MAX_SUBSCRIPTIONS);
         for (int i = 0; i < n; i++) {
             printf("  %s (QoS %d)\n", topics[i], qos[i]);
         }
     }
 
     /* 5. Publish some test messages (using a separate mosquitto client) */
     printf("\n5. Publishing test messages...\n");
     {
         struct mosquitto *pub = mosquitto_new("test-publisher", true, NULL);
         if (pub) {
             mosquitto_connect(pub, "localhost", 1883, 60);
             mosquitto_loop_start(pub);
 
             struct timespec half_sec = {0, 500000000};
             nanosleep(&half_sec, NULL);
 
             typedef struct { const char *topic; const char *payload; } test_msg_t;
             test_msg_t msgs[] = {
                 {"demo/config/host",               "192.168.1.100"},
                 {"demo/config/port",               "8080"},
                 {"demo/status/cpu",                "45.2"},
                 {"demo/status/memory/used",        "78.5"},
                 {"demo/sensors/room1/temperature", "22.5"},
                 {"demo/sensors/room2/temperature", "20.1"},
                 {"demo/alerts/high",               "CPU usage critical"},
                 {"demo/logs/app",                  "Application started successfully"},
             };
             int nmsg = (int)(sizeof(msgs) / sizeof(msgs[0]));
             for (int i = 0; i < nmsg; i++) {
                 mosquitto_publish(pub, NULL, msgs[i].topic,
                                   (int)strlen(msgs[i].payload),
                                   msgs[i].payload, 1, false);
                 printf("  Published: %s\n", msgs[i].topic);
             }
 
             mosquitto_disconnect(pub);
             mosquitto_loop_stop(pub, false);
             mosquitto_destroy(pub);
         }
     }
 
     /* 6. Wait for messages */
     printf("\n6. Waiting for messages (5 seconds)...\n");
     ts_wait_for_messages(&subscriber, 5.0);
 
     /* 7. Show statistics */
     printf("\n7. Statistics:\n");
     {
         ts_stats_t st;
         ts_get_statistics(&subscriber, &st);
         printf("  messages_received:  %lu\n", (unsigned long)st.messages_received);
         printf("  callbacks_executed: %lu\n", (unsigned long)st.callbacks_executed);
         printf("  errors:             %lu\n", (unsigned long)st.errors);
         if (st.has_last_message) {
             struct tm tm;
             localtime_r(&st.last_message_time.tv_sec, &tm);
             printf("  last_message_time:  %02d:%02d:%02d\n",
                    tm.tm_hour, tm.tm_min, tm.tm_sec);
         }
     }
 
     /* 8. Add another callback to existing subscription */
     printf("\n8. Adding additional callback:\n");
     ts_add_callback(&subscriber, "demo/config/+", extra_config_callback, NULL);
 
     /* 9. Unsubscribe from a topic */
     printf("\n9. Unsubscribing from demo/logs/+:\n");
     ts_unsubscribe(&subscriber, "demo/logs/+");
 
     printf("\n* Demo completed successfully!\n");
 
     /* Cleanup */
     printf("\nCleaning up...\n");
     int count = ts_unsubscribe_all(&subscriber);
     printf("  Unsubscribed from %d topics\n", count);
     ts_disconnect(&subscriber);
     printf("  Disconnected\n");
     ts_destroy(&subscriber);
 
     return 0;
 }