/**
 * topic_subscriber.c - MQTT topic subscriber library.
 *
 * Subscribes to MQTT topics and issues callbacks for received messages.
 * Translated from Python TopicSubscriber class.
 *
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

 #include "topic_subscriber.h"

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <errno.h>
 #include <mosquitto.h>
 
 /* ── Internal helpers ───────────────────────────────────────────────── */
 
 static void ts_now(struct timespec *ts)
 {
     clock_gettime(CLOCK_REALTIME, ts);
 }
 
 static int ts_find_sub(topic_subscriber_t *ts, const char *topic)
 {
     for (int i = 0; i < ts->subscription_count; i++) {
         if (ts->subscriptions[i].active &&
             strcmp(ts->subscriptions[i].topic, topic) == 0) {
             return i;
         }
     }
     return -1;
 }
 
 static int ts_alloc_sub(topic_subscriber_t *ts)
 {
     for (int i = 0; i < ts->subscription_count; i++) {
         if (!ts->subscriptions[i].active) return i;
     }
     if (ts->subscription_count < TS_MAX_SUBSCRIPTIONS) {
         return ts->subscription_count++;
     }
     return -1;
 }
 
 static bool ts_topic_matches(const char *pattern, const char *topic)
 {
     bool result = false;
     if (mosquitto_topic_matches_sub(pattern, topic, &result) == MOSQ_ERR_SUCCESS) {
         return result;
     }
     return false;
 }
 
 /* ── Mosquitto callbacks ────────────────────────────────────────────── */
 
 static void on_connect_cb(struct mosquitto *mosq, void *obj, int reason_code)
 {
     (void)mosq;
     topic_subscriber_t *ts = (topic_subscriber_t *)obj;
 
     pthread_mutex_lock(&ts->state_lock);
     ts->connected = (reason_code == 0);
     ts->connect_done = true;
     pthread_cond_signal(&ts->connect_cond);
     pthread_mutex_unlock(&ts->state_lock);
 
     if (ts->connected) {
         printf("[Connected] Successfully connected to %s:%d\n",
                ts->config.host, ts->config.port);
 
         pthread_mutex_lock(&ts->subscriptions_lock);
         for (int i = 0; i < ts->subscription_count; i++) {
             if (!ts->subscriptions[i].active) continue;
             int rc = mosquitto_subscribe(ts->mosq, NULL,
                                          ts->subscriptions[i].topic,
                                          ts->subscriptions[i].qos);
             if (rc == MOSQ_ERR_SUCCESS) {
                 printf("[Resubscribed] %s (QoS %d)\n",
                        ts->subscriptions[i].topic, ts->subscriptions[i].qos);
             } else {
                 printf("[Resubscribe Failed] %s: rc=%d\n",
                        ts->subscriptions[i].topic, rc);
             }
         }
         pthread_mutex_unlock(&ts->subscriptions_lock);
     } else {
         printf("[Connection Failed] reason_code=%d\n", reason_code);
     }
 }
 
 static void on_disconnect_cb(struct mosquitto *mosq, void *obj, int reason_code)
 {
     (void)mosq;
     topic_subscriber_t *ts = (topic_subscriber_t *)obj;
 
     pthread_mutex_lock(&ts->state_lock);
     ts->connected = false;
     pthread_mutex_unlock(&ts->state_lock);
 
     printf("[Disconnected] reason_code=%d\n", reason_code);
 }
 
 static void on_message_cb(struct mosquitto *mosq, void *obj,
                           const struct mosquitto_message *msg)
 {
     (void)mosq;
     topic_subscriber_t *ts = (topic_subscriber_t *)obj;
 
     /* Update statistics */
     pthread_mutex_lock(&ts->stats_lock);
     ts->stats.messages_received++;
     ts_now(&ts->stats.last_message_time);
     ts->stats.has_last_message = true;
     pthread_mutex_unlock(&ts->stats_lock);
 
     /* Build message info */
     ts_message_info_t info;
     memset(&info, 0, sizeof(info));
 
     strncpy(info.topic, msg->topic, TS_MAX_TOPIC_LEN - 1);
     info.topic[TS_MAX_TOPIC_LEN - 1] = '\0';
 
     if (msg->payload && msg->payloadlen > 0) {
         int copy_len = msg->payloadlen;
         if (copy_len >= TS_MAX_PAYLOAD_LEN) {
             copy_len = TS_MAX_PAYLOAD_LEN - 1;
         }
         memcpy(info.payload, msg->payload, copy_len);
         info.payload[copy_len] = '\0';
     }
 
     info.raw_payload     = (uint8_t *)msg->payload;
     info.raw_payload_len = msg->payloadlen;
     info.qos             = msg->qos;
     info.retain          = msg->retain;
     ts_now(&info.timestamp);
 
     /* Truncated preview for log */
     char preview[104];
     int plen = msg->payloadlen < 100 ? msg->payloadlen : 100;
     if (msg->payload) {
         memcpy(preview, msg->payload, plen);
         preview[plen] = '\0';
     } else {
         preview[0] = '\0';
     }
     printf("[Message] %s => %s%s (QoS %d, Retain: %s)\n",
            msg->topic, preview,
            msg->payloadlen > 100 ? "..." : "",
            msg->qos, msg->retain ? "true" : "false");
 
     /* Collect matching callbacks (copy under lock, execute outside) */
     ts_callback_entry_t to_call[TS_MAX_SUBSCRIPTIONS * TS_MAX_CALLBACKS_PER_TOPIC];
     int to_call_count = 0;
 
     pthread_mutex_lock(&ts->subscriptions_lock);
     for (int i = 0; i < ts->subscription_count; i++) {
         if (!ts->subscriptions[i].active) continue;
         if (!ts_topic_matches(ts->subscriptions[i].topic, msg->topic)) continue;
 
         for (int j = 0; j < ts->subscriptions[i].callback_count; j++) {
             if (to_call_count < (int)(sizeof(to_call) / sizeof(to_call[0]))) {
                 to_call[to_call_count++] = ts->subscriptions[i].callbacks[j];
             }
         }
     }
     pthread_mutex_unlock(&ts->subscriptions_lock);
 
     /* Execute callbacks outside of lock */
     for (int i = 0; i < to_call_count; i++) {
         ts_callback_entry_t *cb = &to_call[i];
         cb->fn(&info, cb->user_data);
 
         pthread_mutex_lock(&ts->stats_lock);
         ts->stats.callbacks_executed++;
         pthread_mutex_unlock(&ts->stats_lock);
     }
 }
 
 static void on_subscribe_cb(struct mosquitto *mosq, void *obj,
                             int mid, int qos_count, const int *granted_qos)
 {
     (void)mosq;
     (void)obj;
 
     printf("[Subscribed] mid=%d", mid);
     if (qos_count > 0 && granted_qos) {
         printf(", granted_qos=[");
         for (int i = 0; i < qos_count; i++) {
             printf("%d%s", granted_qos[i], (i < qos_count - 1) ? "," : "");
         }
         printf("]");
     }
     printf("\n");
 }
 
 /* ── Public API ─────────────────────────────────────────────────────── */
 
 void ts_config_init(ts_config_t *cfg)
 {
     memset(cfg, 0, sizeof(*cfg));
     strncpy(cfg->host, "localhost", sizeof(cfg->host) - 1);
     cfg->port              = 1883;
     strncpy(cfg->client_id, "topic-subscriber", sizeof(cfg->client_id) - 1);
     cfg->keepalive         = 60;
     cfg->use_mqttv5        = false;
     cfg->has_credentials   = false;
     cfg->auto_reconnect    = true;
     cfg->reconnect_delay   = 5.0;
     cfg->clean_session     = true;
 }
 
 int ts_init(topic_subscriber_t *ts, const ts_config_t *cfg)
 {
     memset(ts, 0, sizeof(*ts));
     ts->config = *cfg;
 
     mosquitto_lib_init();
 
     ts->mosq = mosquitto_new(cfg->client_id, cfg->clean_session, ts);
     if (!ts->mosq) {
         fprintf(stderr, "[Error] mosquitto_new failed: %s\n", strerror(errno));
         return -1;
     }
 
     if (cfg->has_credentials) {
         mosquitto_username_pw_set(ts->mosq, cfg->username, cfg->password);
     }
 
     if (cfg->use_mqttv5) {
         int ver = MQTT_PROTOCOL_V5;
         mosquitto_int_option(ts->mosq, MOSQ_OPT_PROTOCOL_VERSION, ver);
     }
 
     if (cfg->auto_reconnect) {
         int delay_sec = (int)cfg->reconnect_delay;
         if (delay_sec < 1) delay_sec = 1;
         mosquitto_reconnect_delay_set(ts->mosq, delay_sec, delay_sec * 4, false);
     }
 
     mosquitto_connect_callback_set(ts->mosq, on_connect_cb);
     mosquitto_disconnect_callback_set(ts->mosq, on_disconnect_cb);
     mosquitto_message_callback_set(ts->mosq, on_message_cb);
     mosquitto_subscribe_callback_set(ts->mosq, on_subscribe_cb);
 
     pthread_mutex_init(&ts->subscriptions_lock, NULL);
     pthread_mutex_init(&ts->state_lock, NULL);
     pthread_mutex_init(&ts->stats_lock, NULL);
     pthread_cond_init(&ts->connect_cond, NULL);
 
     return 0;
 }
 
 bool ts_connect(topic_subscriber_t *ts, double timeout_sec)
 {
     pthread_mutex_lock(&ts->state_lock);
     ts->connect_done = false;
     ts->running = true;
     pthread_mutex_unlock(&ts->state_lock);
 
     int rc = mosquitto_connect(ts->mosq, ts->config.host,
                                ts->config.port, ts->config.keepalive);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] Connection failed: %s\n", mosquitto_strerror(rc));
         ts->running = false;
         return false;
     }
 
     rc = mosquitto_loop_start(ts->mosq);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] loop_start failed: %s\n", mosquitto_strerror(rc));
         ts->running = false;
         return false;
     }
 
     /* Wait for CONNACK */
     pthread_mutex_lock(&ts->state_lock);
     if (!ts->connect_done) {
         struct timespec abs_time;
         ts_now(&abs_time);
         long nsec = abs_time.tv_nsec + (long)((timeout_sec - (long)timeout_sec) * 1e9);
         abs_time.tv_sec += (time_t)timeout_sec + nsec / 1000000000L;
         abs_time.tv_nsec = nsec % 1000000000L;
 
         while (!ts->connect_done) {
             int wait_rc = pthread_cond_timedwait(&ts->connect_cond,
                                                  &ts->state_lock, &abs_time);
             if (wait_rc == ETIMEDOUT) break;
         }
     }
     bool ok = ts->connected;
     pthread_mutex_unlock(&ts->state_lock);
 
     if (!ok) {
         fprintf(stderr, "[Error] Connection timeout or refused (%s:%d)\n",
                 ts->config.host, ts->config.port);
         ts->running = false;
         mosquitto_loop_stop(ts->mosq, true);
         return false;
     }
 
     return true;
 }
 
 void ts_disconnect(topic_subscriber_t *ts)
 {
     ts->running = false;
     mosquitto_disconnect(ts->mosq);
     mosquitto_loop_stop(ts->mosq, false);
 
     pthread_mutex_lock(&ts->state_lock);
     ts->connected = false;
     pthread_mutex_unlock(&ts->state_lock);
 }
 
 void ts_destroy(topic_subscriber_t *ts)
 {
     if (ts->running || ts->connected) {
         ts_disconnect(ts);
     }
     if (ts->mosq) {
         mosquitto_destroy(ts->mosq);
         ts->mosq = NULL;
     }
     pthread_mutex_destroy(&ts->subscriptions_lock);
     pthread_mutex_destroy(&ts->state_lock);
     pthread_mutex_destroy(&ts->stats_lock);
     pthread_cond_destroy(&ts->connect_cond);
 
     mosquitto_lib_cleanup();
 }
 
 bool ts_subscribe(topic_subscriber_t *ts,
                   const char *topic,
                   ts_message_callback_t callback,
                   void *user_data,
                   int qos,
                   bool replace)
 {
     if (!ts->connected) {
         fprintf(stderr, "[Error] Not connected. Call ts_connect() first.\n");
         return false;
     }
 
     pthread_mutex_lock(&ts->subscriptions_lock);
 
     int idx = ts_find_sub(ts, topic);
 
     if (idx >= 0) {
         ts_subscription_t *sub = &ts->subscriptions[idx];
 
         if (replace) {
             sub->callbacks[0].fn        = callback;
             sub->callbacks[0].user_data = user_data;
             sub->callback_count         = 1;
         } else {
             if (sub->callback_count < TS_MAX_CALLBACKS_PER_TOPIC) {
                 sub->callbacks[sub->callback_count].fn        = callback;
                 sub->callbacks[sub->callback_count].user_data = user_data;
                 sub->callback_count++;
             } else {
                 fprintf(stderr, "[Error] Max callbacks reached for %s\n", topic);
                 pthread_mutex_unlock(&ts->subscriptions_lock);
                 return false;
             }
         }
         printf("[Already Subscribed] %s, adding callback\n", topic);
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return true;
     }
 
     idx = ts_alloc_sub(ts);
     if (idx < 0) {
         fprintf(stderr, "[Error] Max subscriptions reached\n");
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     ts_subscription_t *sub = &ts->subscriptions[idx];
     memset(sub, 0, sizeof(*sub));
     strncpy(sub->topic, topic, TS_MAX_TOPIC_LEN - 1);
     sub->topic[TS_MAX_TOPIC_LEN - 1] = '\0';
     sub->qos                         = qos;
     sub->callbacks[0].fn             = callback;
     sub->callbacks[0].user_data      = user_data;
     sub->callback_count              = 1;
     sub->active                      = true;
 
     int rc = mosquitto_subscribe(ts->mosq, NULL, topic, qos);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] Subscribe failed for %s: %s\n",
                 topic, mosquitto_strerror(rc));
         sub->active = false;
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     printf("[Subscribed] %s (QoS %d)\n", topic, qos);
     pthread_mutex_unlock(&ts->subscriptions_lock);
     return true;
 }
 
 int ts_subscribe_many(topic_subscriber_t *ts,
                       int count,
                       const char *topics[],
                       ts_message_callback_t callbacks[],
                       void *user_datas[],
                       const int qos_values[],
                       bool replace,
                       const char *failed_topics[])
 {
     int success = 0;
     int fail_idx = 0;
 
     for (int i = 0; i < count; i++) {
         void *ud = user_datas ? user_datas[i] : NULL;
         if (ts_subscribe(ts, topics[i], callbacks[i], ud, qos_values[i], replace)) {
             success++;
         } else {
             if (failed_topics) {
                 failed_topics[fail_idx++] = topics[i];
             }
         }
     }
     return success;
 }
 
 bool ts_unsubscribe(topic_subscriber_t *ts, const char *topic)
 {
     if (!ts->connected) {
         fprintf(stderr, "[Error] Not connected. Call ts_connect() first.\n");
         return false;
     }
 
     pthread_mutex_lock(&ts->subscriptions_lock);
 
     int idx = ts_find_sub(ts, topic);
     if (idx < 0) {
         printf("[Warning] Not subscribed to %s\n", topic);
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     int rc = mosquitto_unsubscribe(ts->mosq, NULL, topic);
     if (rc != MOSQ_ERR_SUCCESS) {
         fprintf(stderr, "[Error] Unsubscribe failed for %s: %s\n",
                 topic, mosquitto_strerror(rc));
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     ts->subscriptions[idx].active         = false;
     ts->subscriptions[idx].callback_count = 0;
 
     printf("[Unsubscribed] %s\n", topic);
     pthread_mutex_unlock(&ts->subscriptions_lock);
     return true;
 }
 
 int ts_unsubscribe_all(topic_subscriber_t *ts)
 {
     char topics[TS_MAX_SUBSCRIPTIONS][TS_MAX_TOPIC_LEN];
     int n = 0;
 
     pthread_mutex_lock(&ts->subscriptions_lock);
     for (int i = 0; i < ts->subscription_count && n < TS_MAX_SUBSCRIPTIONS; i++) {
         if (ts->subscriptions[i].active) {
             strncpy(topics[n], ts->subscriptions[i].topic, TS_MAX_TOPIC_LEN);
             n++;
         }
     }
     pthread_mutex_unlock(&ts->subscriptions_lock);
 
     int count = 0;
     for (int i = 0; i < n; i++) {
         if (ts_unsubscribe(ts, topics[i])) {
             count++;
         }
     }
     return count;
 }
 
 bool ts_add_callback(topic_subscriber_t *ts,
                      const char *topic,
                      ts_message_callback_t callback,
                      void *user_data)
 {
     pthread_mutex_lock(&ts->subscriptions_lock);
 
     int idx = ts_find_sub(ts, topic);
     if (idx < 0) {
         fprintf(stderr, "[Error] Not subscribed to %s. Use ts_subscribe() first.\n", topic);
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     ts_subscription_t *sub = &ts->subscriptions[idx];
     if (sub->callback_count >= TS_MAX_CALLBACKS_PER_TOPIC) {
         fprintf(stderr, "[Error] Max callbacks reached for %s\n", topic);
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     sub->callbacks[sub->callback_count].fn        = callback;
     sub->callbacks[sub->callback_count].user_data = user_data;
     sub->callback_count++;
 
     printf("[Callback Added] Added callback for %s\n", topic);
     pthread_mutex_unlock(&ts->subscriptions_lock);
     return true;
 }
 
 bool ts_remove_callback(topic_subscriber_t *ts,
                         const char *topic,
                         ts_message_callback_t callback)
 {
     pthread_mutex_lock(&ts->subscriptions_lock);
 
     int idx = ts_find_sub(ts, topic);
     if (idx < 0) {
         pthread_mutex_unlock(&ts->subscriptions_lock);
         return false;
     }
 
     ts_subscription_t *sub = &ts->subscriptions[idx];
     for (int j = 0; j < sub->callback_count; j++) {
         if (sub->callbacks[j].fn == callback) {
             for (int k = j; k < sub->callback_count - 1; k++) {
                 sub->callbacks[k] = sub->callbacks[k + 1];
             }
             sub->callback_count--;
             printf("[Callback Removed] Removed callback for %s\n", topic);
             pthread_mutex_unlock(&ts->subscriptions_lock);
             return true;
         }
     }
 
     pthread_mutex_unlock(&ts->subscriptions_lock);
     return false;
 }
 
 int ts_get_subscriptions(topic_subscriber_t *ts,
                          char topics[][TS_MAX_TOPIC_LEN],
                          int qos_values[],
                          int max_count)
 {
     int n = 0;
     pthread_mutex_lock(&ts->subscriptions_lock);
     for (int i = 0; i < ts->subscription_count && n < max_count; i++) {
         if (ts->subscriptions[i].active) {
             strncpy(topics[n], ts->subscriptions[i].topic, TS_MAX_TOPIC_LEN);
             qos_values[n] = ts->subscriptions[i].qos;
             n++;
         }
     }
     pthread_mutex_unlock(&ts->subscriptions_lock);
     return n;
 }
 
 void ts_get_statistics(topic_subscriber_t *ts, ts_stats_t *out)
 {
     pthread_mutex_lock(&ts->stats_lock);
     *out = ts->stats;
     pthread_mutex_unlock(&ts->stats_lock);
 }
 
 bool ts_is_connected(topic_subscriber_t *ts)
 {
     pthread_mutex_lock(&ts->state_lock);
     bool c = ts->connected;
     pthread_mutex_unlock(&ts->state_lock);
     return c;
 }
 
 void ts_wait_for_messages(topic_subscriber_t *ts, double timeout_sec)
 {
     if (timeout_sec > 0) {
         struct timespec req;
         req.tv_sec  = (time_t)timeout_sec;
         req.tv_nsec = (long)((timeout_sec - (double)req.tv_sec) * 1e9);
         nanosleep(&req, NULL);
     } else {
         while (ts->running && ts->connected) {
             struct timespec one_sec = {1, 0};
             nanosleep(&one_sec, NULL);
         }
     }
 }