/**
 * kv_store_reader.c - Reads retained key/value messages from an MQTT broker.
 *
 * Translated from Python KVStoreReader class.
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

 #include "kv_store_reader.h"

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <errno.h>
 #include <mosquitto.h>
 
 /* ── Internal helpers ───────────────────────────────────────────────── */
 
 static void kvr_now(struct timespec *ts)
 {
     clock_gettime(CLOCK_REALTIME, ts);
 }
 
 static void kvr_deadline(struct timespec *out, double offset_sec)
 {
     kvr_now(out);
     long whole = (long)offset_sec;
     long frac  = (long)((offset_sec - (double)whole) * 1e9);
     out->tv_nsec += frac;
     out->tv_sec  += whole + out->tv_nsec / 1000000000L;
     out->tv_nsec  = out->tv_nsec % 1000000000L;
 }
 
 /**
  * Check if `topic` is one of the configured sentinels.
  * Caller must hold sentinel_lock.
  */
 static bool kvr_is_sentinel(kvr_store_reader_t *kv, const char *topic)
 {
     for (int i = 0; i < kv->sentinel_count; i++) {
         if (strcmp(kv->sentinels[i], topic) == 0) return true;
     }
     return false;
 }
 
 /**
  * Add a KV entry. Overwrites if topic already exists.
  * Caller must hold entries_lock.
  */
 static void kvr_store_entry(kvr_store_reader_t *kv,
                             const char *topic, const char *value)
 {
     /* Check for existing topic — overwrite */
     for (int i = 0; i < kv->entry_count; i++) {
         if (kv->entries[i].active &&
             strcmp(kv->entries[i].topic, topic) == 0) {
             strncpy(kv->entries[i].value, value, KVR_MAX_VALUE_LEN - 1);
             kv->entries[i].value[KVR_MAX_VALUE_LEN - 1] = '\0';
             return;
         }
     }
     /* Append new entry */
     if (kv->entry_count < KVR_MAX_ENTRIES) {
         kvr_entry_t *e = &kv->entries[kv->entry_count++];
         strncpy(e->topic, topic, KVR_MAX_TOPIC_LEN - 1);
         e->topic[KVR_MAX_TOPIC_LEN - 1] = '\0';
         strncpy(e->value, value, KVR_MAX_VALUE_LEN - 1);
         e->value[KVR_MAX_VALUE_LEN - 1] = '\0';
         e->active = true;
     }
 }
 
 /* ── Mosquitto callbacks ────────────────────────────────────────────── */
 
 static void on_connect_cb(struct mosquitto *mosq, void *obj, int reason_code)
 {
     (void)mosq;
     kvr_store_reader_t *kv = (kvr_store_reader_t *)obj;
 
     pthread_mutex_lock(&kv->state_lock);
     kv->connected    = (reason_code == 0);
     kv->connect_done = true;
     pthread_cond_signal(&kv->connect_cond);
     pthread_mutex_unlock(&kv->state_lock);
 
     if (kv->connected) {
         printf("[Connected] Successfully connected to %s:%d\n",
                kv->config.host, kv->config.port);
     } else {
         printf("[Connection Failed] reason_code=%d\n", reason_code);
     }
 }
 
 static void on_disconnect_cb(struct mosquitto *mosq, void *obj, int reason_code)
 {
     (void)mosq;
     kvr_store_reader_t *kv = (kvr_store_reader_t *)obj;
 
     pthread_mutex_lock(&kv->state_lock);
     kv->connected = false;
     pthread_mutex_unlock(&kv->state_lock);
 
     printf("[Disconnected] reason_code=%d\n", reason_code);
 }
 
 static void on_message_cb(struct mosquitto *mosq, void *obj,
                           const struct mosquitto_message *msg)
 {
     (void)mosq;
     kvr_store_reader_t *kv = (kvr_store_reader_t *)obj;
 
     /* Decode payload to string */
     char payload[KVR_MAX_VALUE_LEN];
     if (msg->payload && msg->payloadlen > 0) {
         int copy_len = msg->payloadlen;
         if (copy_len >= KVR_MAX_VALUE_LEN) copy_len = KVR_MAX_VALUE_LEN - 1;
         memcpy(payload, msg->payload, copy_len);
         payload[copy_len] = '\0';
     } else {
         payload[0] = '\0';
     }
 
     /* Check if this is a sentinel topic */
     pthread_mutex_lock(&kv->sentinel_lock);
     if (kvr_is_sentinel(kv, msg->topic)) {
         kv->sentinel_fired = true;
         pthread_cond_signal(&kv->sentinel_cond);
         pthread_mutex_unlock(&kv->sentinel_lock);
         return;
     }
     pthread_mutex_unlock(&kv->sentinel_lock);
 
     /* Truncated preview for log */
     char preview[54];
     int plen = (int)strlen(payload);
     int clen = plen < 50 ? plen : 50;
     memcpy(preview, payload, clen);
     preview[clen] = '\0';
 
     /* Normal KV handling */
     pthread_mutex_lock(&kv->entries_lock);
     if (msg->retain) {
         kvr_store_entry(kv, msg->topic, payload);
         printf("[Retained] %s => %s%s\n", msg->topic, preview,
                plen > 50 ? "..." : "");
     } else {
         printf("[Non-retained] %s => %s%s\n", msg->topic, preview,
                plen > 50 ? "..." : "");
     }
     pthread_mutex_unlock(&kv->entries_lock);
 }
 
 static void on_subscribe_cb(struct mosquitto *mosq, void *obj,
                             int mid, int qos_count, const int *granted_qos)
 {
     (void)mosq;
     (void)qos_count;
     (void)granted_qos;
     kvr_store_reader_t *kv = (kvr_store_reader_t *)obj;
 
     printf("[Subscribed] mid=%d\n", mid);
 
     pthread_mutex_lock(&kv->sub_lock);
     kv->sub_acked = true;
     pthread_cond_signal(&kv->sub_cond);
     pthread_mutex_unlock(&kv->sub_lock);
 }
 
 /* ── Public API ─────────────────────────────────────────────────────── */
 
 void kvr_config_init(kvr_config_t *cfg)
 {
     memset(cfg, 0, sizeof(*cfg));
     strncpy(cfg->host, "localhost", sizeof(cfg->host) - 1);
     cfg->port            = 1883;
     strncpy(cfg->client_id, "kv-reader", sizeof(cfg->client_id) - 1);
     cfg->keepalive       = 60;
     cfg->use_mqttv5      = false;
     cfg->has_credentials = false;
     cfg->clean_session   = true;
 }
 
 int kvr_init(kvr_store_reader_t *kv, const kvr_config_t *cfg)
 {
     memset(kv, 0, sizeof(*kv));
     kv->config = *cfg;
 
     mosquitto_lib_init();
 
     kv->mosq = mosquitto_new(cfg->client_id, cfg->clean_session, kv);
     if (!kv->mosq) {
         fprintf(stderr, "[Error] mosquitto_new failed: %s\n", strerror(errno));
         return -1;
     }
 
     if (cfg->has_credentials) {
         mosquitto_username_pw_set(kv->mosq, cfg->username, cfg->password);
     }
 
     if (cfg->use_mqttv5) {
         int ver = MQTT_PROTOCOL_V5;
         mosquitto_int_option(kv->mosq, MOSQ_OPT_PROTOCOL_VERSION, ver);
     }
 
     mosquitto_connect_callback_set(kv->mosq, on_connect_cb);
     mosquitto_disconnect_callback_set(kv->mosq, on_disconnect_cb);
     mosquitto_message_callback_set(kv->mosq, on_message_cb);
     mosquitto_subscribe_callback_set(kv->mosq, on_subscribe_cb);
 
     pthread_mutex_init(&kv->entries_lock, NULL);
     pthread_mutex_init(&kv->sentinel_lock, NULL);
     pthread_cond_init(&kv->sentinel_cond, NULL);
     pthread_mutex_init(&kv->sub_lock, NULL);
     pthread_cond_init(&kv->sub_cond, NULL);
     pthread_mutex_init(&kv->state_lock, NULL);
     pthread_cond_init(&kv->connect_cond, NULL);
 
     return 0;
 }
 
 bool kvr_connect(kvr_store_reader_t *kv, double timeout_sec)
 {
     pthread_mutex_lock(&kv->state_lock);
     kv->connect_done = false;
     kv->running      = true;
     pthread_mutex_unlock(&kv->state_lock);
 
     int rc = mosquitto_connect(kv->mosq, kv->config.host,
                                kv->config.port, kv->config.keepalive);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] Connection failed: %s\n", mosquitto_strerror(rc));
         kv->running = false;
         return false;
     }
 
     rc = mosquitto_loop_start(kv->mosq);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] loop_start failed: %s\n", mosquitto_strerror(rc));
         kv->running = false;
         return false;
     }
 
     /* Wait for CONNACK */
     struct timespec deadline;
     kvr_deadline(&deadline, timeout_sec);
 
     pthread_mutex_lock(&kv->state_lock);
     while (!kv->connect_done) {
         int wait_rc = pthread_cond_timedwait(&kv->connect_cond,
                                              &kv->state_lock, &deadline);
         if (wait_rc == ETIMEDOUT) break;
     }
     bool ok = kv->connected;
     pthread_mutex_unlock(&kv->state_lock);
 
     if (!ok) {
         fprintf(stderr, "[Error] Connection timeout or refused (%s:%d)\n",
                 kv->config.host, kv->config.port);
         kv->running = false;
         mosquitto_loop_stop(kv->mosq, true);
         return false;
     }
 
     return true;
 }
 
 void kvr_disconnect(kvr_store_reader_t *kv)
 {
     kv->running = false;
     mosquitto_disconnect(kv->mosq);
     mosquitto_loop_stop(kv->mosq, false);
 
     pthread_mutex_lock(&kv->state_lock);
     kv->connected = false;
     pthread_mutex_unlock(&kv->state_lock);
 }
 
 void kvr_destroy(kvr_store_reader_t *kv)
 {
     if (kv->running || kv->connected) {
         kvr_disconnect(kv);
     }
     if (kv->mosq) {
         mosquitto_destroy(kv->mosq);
         kv->mosq = NULL;
     }
     pthread_mutex_destroy(&kv->entries_lock);
     pthread_mutex_destroy(&kv->sentinel_lock);
     pthread_cond_destroy(&kv->sentinel_cond);
     pthread_mutex_destroy(&kv->sub_lock);
     pthread_cond_destroy(&kv->sub_cond);
     pthread_mutex_destroy(&kv->state_lock);
     pthread_cond_destroy(&kv->connect_cond);
 
     mosquitto_lib_cleanup();
 }
 
 int kvr_read_pattern(kvr_store_reader_t *kv,
                      const char *pattern,
                      int qos,
                      double timeout_sec,
                      const char *sentinel_topics[],
                      bool wait_for_sentinel,
                      kvr_entry_t *out_entries,
                      int max_entries)
 {
     if (!kv->connected) {
         fprintf(stderr, "[Error] Not connected. Call kvr_connect() first.\n");
         return 0;
     }
 
     /* Clear previous entries */
     pthread_mutex_lock(&kv->entries_lock);
     kv->entry_count = 0;
     pthread_mutex_unlock(&kv->entries_lock);
 
     /* Setup sentinels */
     pthread_mutex_lock(&kv->sentinel_lock);
     kv->sentinel_fired = false;
     kv->sentinel_count = 0;
     if (sentinel_topics) {
         for (int i = 0; sentinel_topics[i] != NULL && i < KVR_MAX_SENTINELS; i++) {
             strncpy(kv->sentinels[i], sentinel_topics[i], KVR_MAX_TOPIC_LEN - 1);
             kv->sentinels[i][KVR_MAX_TOPIC_LEN - 1] = '\0';
             kv->sentinel_count++;
         }
     }
     pthread_mutex_unlock(&kv->sentinel_lock);
 
     /* Subscribe to pattern */
     pthread_mutex_lock(&kv->sub_lock);
     kv->sub_acked = false;
     pthread_mutex_unlock(&kv->sub_lock);
 
     int rc = mosquitto_subscribe(kv->mosq, NULL, pattern, qos);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] Subscribe failed for %s: %s\n",
                 pattern, mosquitto_strerror(rc));
         /* Clear sentinels */
         pthread_mutex_lock(&kv->sentinel_lock);
         kv->sentinel_count = 0;
         pthread_mutex_unlock(&kv->sentinel_lock);
         return 0;
     }
 
     /* Wait for SUBACK (best-effort, 2 second cap) */
     {
         struct timespec sub_deadline;
         kvr_deadline(&sub_deadline, 2.0);
         pthread_mutex_lock(&kv->sub_lock);
         while (!kv->sub_acked) {
             int w = pthread_cond_timedwait(&kv->sub_cond, &kv->sub_lock,
                                            &sub_deadline);
             if (w == ETIMEDOUT) break;
         }
         pthread_mutex_unlock(&kv->sub_lock);
     }
 
     /* Wait for retained messages: sentinel or timeout */
     if (wait_for_sentinel && kv->sentinel_count > 0) {
         struct timespec deadline;
         kvr_deadline(&deadline, timeout_sec);
         pthread_mutex_lock(&kv->sentinel_lock);
         while (!kv->sentinel_fired) {
             int w = pthread_cond_timedwait(&kv->sentinel_cond,
                                            &kv->sentinel_lock, &deadline);
             if (w == ETIMEDOUT) break;
         }
         pthread_mutex_unlock(&kv->sentinel_lock);
     } else {
         /* Simple time-box sleep */
         struct timespec req;
         req.tv_sec  = (time_t)timeout_sec;
         req.tv_nsec = (long)((timeout_sec - (double)req.tv_sec) * 1e9);
         nanosleep(&req, NULL);
     }
 
     /* Unsubscribe to stop receiving further messages */
     mosquitto_unsubscribe(kv->mosq, NULL, pattern);
 
     /* Copy results to caller, excluding sentinel topics */
     int out_count = 0;
     pthread_mutex_lock(&kv->entries_lock);
     for (int i = 0; i < kv->entry_count && out_count < max_entries; i++) {
         if (!kv->entries[i].active) continue;
 
         /* Skip sentinel topics */
         pthread_mutex_lock(&kv->sentinel_lock);
         bool is_sent = kvr_is_sentinel(kv, kv->entries[i].topic);
         pthread_mutex_unlock(&kv->sentinel_lock);
         if (is_sent) continue;
 
         out_entries[out_count] = kv->entries[i];
         out_count++;
     }
     pthread_mutex_unlock(&kv->entries_lock);
 
     /* Clear sentinels */
     pthread_mutex_lock(&kv->sentinel_lock);
     kv->sentinel_count = 0;
     pthread_mutex_unlock(&kv->sentinel_lock);
 
     return out_count;
 }
 
 bool kvr_read_single(kvr_store_reader_t *kv,
                      const char *topic,
                      double timeout_sec,
                      char *out_value,
                      int out_value_len)
 {
     kvr_entry_t entries[1];
     int n = kvr_read_pattern(kv, topic, 1, timeout_sec,
                              NULL, false, entries, 1);
     if (n > 0 && strcmp(entries[0].topic, topic) == 0) {
         strncpy(out_value, entries[0].value, out_value_len - 1);
         out_value[out_value_len - 1] = '\0';
         return true;
     }
     return false;
 }
 
 int kvr_read_all(kvr_store_reader_t *kv,
                  const char *base_topic,
                  double timeout_sec,
                  const char *sentinel_topics[],
                  bool wait_for_sentinel,
                  kvr_entry_t *out_entries,
                  int max_entries)
 {
     if (!base_topic) base_topic = "#";
     return kvr_read_pattern(kv, base_topic, 1, timeout_sec,
                             sentinel_topics, wait_for_sentinel,
                             out_entries, max_entries);
 }
 
 bool kvr_is_connected(kvr_store_reader_t *kv)
 {
     pthread_mutex_lock(&kv->state_lock);
     bool c = kv->connected;
     pthread_mutex_unlock(&kv->state_lock);
     return c;
 }