/**
 * @file mqtt_luajit_adapter.c
 * @brief Adapter wrapping original C MQTT libraries for LuaJIT FFI
 *
 * Link: -lmqtt_kv_store -lmqtt_queue -lmosquitto -lpthread
 */

 #define _GNU_SOURCE  /* strdup */

 #include "mqtt_luajit_adapter.h"
 
 #include "kv_store_writer.h"
 #include "kv_store_reader.h"
 #include "mqtt_queue.h"
 
 #include <mosquitto.h>
 #include <stdlib.h>
 #include <string.h>
 
 /* ================================================================== */
 /*  Library init / cleanup                                             */
 /* ================================================================== */
 
 void mqtt_adapter_lib_init(void)
 {
     mosquitto_lib_init();
 }
 
 void mqtt_adapter_lib_cleanup(void)
 {
     mosquitto_lib_cleanup();
 }
 
 /* ================================================================== */
 /*  KV Store Writer                                                    */
 /* ================================================================== */
 
 struct MqttKvWriter {
     kvw_store_writer_t kv;
 };
 
 MqttKvWriter *mqtt_kvw_create(const char *host, int port,
                                const char *client_id)
 {
     MqttKvWriter *w = calloc(1, sizeof(*w));
     if (!w) return NULL;
 
     kvw_config_t cfg;
     kvw_config_init(&cfg);
     if (host)      strncpy(cfg.host, host, sizeof(cfg.host) - 1);
     if (port > 0)  cfg.port = port;
     if (client_id) strncpy(cfg.client_id, client_id, sizeof(cfg.client_id) - 1);
 
     if (kvw_init(&w->kv, &cfg) != 0) {
         free(w);
         return NULL;
     }
     return w;
 }
 
 void mqtt_kvw_destroy(MqttKvWriter *w)
 {
     if (!w) return;
     kvw_destroy(&w->kv);
     free(w);
 }
 
 bool mqtt_kvw_connect(MqttKvWriter *w, double timeout_sec)
 {
     return w ? kvw_connect(&w->kv, timeout_sec) : false;
 }
 
 void mqtt_kvw_disconnect(MqttKvWriter *w)
 {
     if (w) kvw_disconnect(&w->kv);
 }
 
 bool mqtt_kvw_is_connected(MqttKvWriter *w)
 {
     return w ? kvw_is_connected(&w->kv) : false;
 }
 
 bool mqtt_kvw_write(MqttKvWriter *w, const char *topic,
                     const char *value, int qos, bool retain,
                     double timeout_sec)
 {
     return w ? kvw_write_single(&w->kv, topic, value, qos, retain, timeout_sec) : false;
 }
 
 bool mqtt_kvw_delete(MqttKvWriter *w, const char *topic, double timeout_sec)
 {
     return w ? kvw_delete_single(&w->kv, topic, timeout_sec) : false;
 }
 
 bool mqtt_kvw_update(MqttKvWriter *w, const char *topic,
                      const char *value, double timeout_sec)
 {
     return w ? kvw_write_single(&w->kv, topic, value, 1, true, timeout_sec) : false;
 }
 
 /* ================================================================== */
 /*  KV Store Reader                                                    */
 /* ================================================================== */
 
 struct MqttKvReader {
     kvr_store_reader_t kv;
 };
 
 MqttKvReader *mqtt_kvr_create(const char *host, int port,
                                const char *client_id)
 {
     MqttKvReader *r = calloc(1, sizeof(*r));
     if (!r) return NULL;
 
     kvr_config_t cfg;
     kvr_config_init(&cfg);
     if (host)      strncpy(cfg.host, host, sizeof(cfg.host) - 1);
     if (port > 0)  cfg.port = port;
     if (client_id) strncpy(cfg.client_id, client_id, sizeof(cfg.client_id) - 1);
 
     if (kvr_init(&r->kv, &cfg) != 0) {
         free(r);
         return NULL;
     }
     return r;
 }
 
 void mqtt_kvr_destroy(MqttKvReader *r)
 {
     if (!r) return;
     kvr_destroy(&r->kv);
     free(r);
 }
 
 bool mqtt_kvr_connect(MqttKvReader *r, double timeout_sec)
 {
     return r ? kvr_connect(&r->kv, timeout_sec) : false;
 }
 
 void mqtt_kvr_disconnect(MqttKvReader *r)
 {
     if (r) kvr_disconnect(&r->kv);
 }
 
 bool mqtt_kvr_is_connected(MqttKvReader *r)
 {
     return r ? kvr_is_connected(&r->kv) : false;
 }
 
 int mqtt_kvr_read_pattern(MqttKvReader *r, const char *pattern,
                           int qos, double timeout_sec,
                           MqttKvEntry *out_entries, int max_entries)
 {
     if (!r || !out_entries || max_entries <= 0) return 0;
     return kvr_read_pattern(&r->kv, pattern, qos, timeout_sec,
                             NULL, false,
                             (kvr_entry_t *)out_entries, max_entries);
 }
 
 bool mqtt_kvr_read_single(MqttKvReader *r, const char *topic,
                           double timeout_sec,
                           char *out_value, int out_value_len)
 {
     return r ? kvr_read_single(&r->kv, topic, timeout_sec,
                                out_value, out_value_len) : false;
 }
 
 int mqtt_kvr_read_all(MqttKvReader *r, const char *base_topic,
                       double timeout_sec,
                       MqttKvEntry *out_entries, int max_entries)
 {
     if (!r || !out_entries || max_entries <= 0) return 0;
     return kvr_read_all(&r->kv, base_topic, timeout_sec,
                         NULL, false,
                         (kvr_entry_t *)out_entries, max_entries);
 }
 
 /* ================================================================== */
 /*  Queue Publisher                                                    */
 /* ================================================================== */
 
 struct MqttPublisher {
     mqtt_publisher_t pub;
 };
 
 MqttPublisher *mqtt_pub_create(const char *host, int port,
                                 const char *client_id)
 {
     MqttPublisher *p = calloc(1, sizeof(*p));
     if (!p) return NULL;
 
     mqtt_queue_config_t cfg = MQTT_QUEUE_CONFIG_DEFAULTS;
     if (host)      cfg.host          = host;
     if (port > 0)  cfg.port          = port;
     if (client_id) cfg.client_id     = client_id;
     cfg.clean_session = true;
 
     if (mqtt_publisher_init(&p->pub, &cfg) != 0) {
         free(p);
         return NULL;
     }
     return p;
 }
 
 void mqtt_pub_destroy(MqttPublisher *p)
 {
     if (!p) return;
     mqtt_publisher_destroy(&p->pub);
     free(p);
 }
 
 bool mqtt_pub_connect(MqttPublisher *p, int timeout_ms)
 {
     return p ? (mqtt_publisher_connect(&p->pub, timeout_ms) == 0) : false;
 }
 
 void mqtt_pub_disconnect(MqttPublisher *p)
 {
     if (p) mqtt_publisher_disconnect(&p->pub);
 }
 
 bool mqtt_pub_is_connected(MqttPublisher *p)
 {
     return p ? mqtt_publisher_is_connected(&p->pub) : false;
 }
 
 bool mqtt_pub_publish(MqttPublisher *p, const char *topic,
                       const char *payload, int qos, bool retain)
 {
     return p ? (mqtt_publisher_publish(&p->pub, topic, payload, qos, retain) == 0) : false;
 }
 
 /* ================================================================== */
 /*  Queue Reader                                                       */
 /* ================================================================== */
 
 struct MqttQueueReader {
     mqtt_reader_t reader;
 };
 
 MqttQueueReader *mqtt_qr_create(const char *host, int port,
                                  const char *client_id,
                                  bool clean_session)
 {
     MqttQueueReader *r = calloc(1, sizeof(*r));
     if (!r) return NULL;
 
     mqtt_queue_config_t cfg = MQTT_QUEUE_CONFIG_DEFAULTS;
     if (host)      cfg.host          = host;
     if (port > 0)  cfg.port          = port;
     if (client_id) cfg.client_id     = client_id;
     cfg.clean_session = clean_session;
 
     if (mqtt_reader_init(&r->reader, &cfg) != 0) {
         free(r);
         return NULL;
     }
     return r;
 }
 
 void mqtt_qr_destroy(MqttQueueReader *r)
 {
     if (!r) return;
     mqtt_reader_destroy(&r->reader);
     free(r);
 }
 
 bool mqtt_qr_connect(MqttQueueReader *r, int timeout_ms)
 {
     return r ? (mqtt_reader_connect(&r->reader, timeout_ms) == 0) : false;
 }
 
 void mqtt_qr_disconnect(MqttQueueReader *r)
 {
     if (r) mqtt_reader_disconnect(&r->reader);
 }
 
 bool mqtt_qr_is_connected(MqttQueueReader *r)
 {
     return r ? r->reader.connected : false;
 }
 
 bool mqtt_qr_subscribe(MqttQueueReader *r, const char *topic,
                         int qos, int timeout_ms)
 {
     return r ? (mqtt_reader_subscribe(&r->reader, topic, qos, timeout_ms) == 0) : false;
 }
 
 MqttQueueMsg *mqtt_qr_read(MqttQueueReader *r, const char *topic,
                             int qos, int timeout_ms, int *out_count)
 {
     if (!r) { if (out_count) *out_count = 0; return NULL; }
 
     int count = 0;
     mqtt_msg_t *orig = mqtt_reader_read_queue(&r->reader, topic, qos,
                                                timeout_ms, &count);
     if (out_count) *out_count = count;
     if (!orig) return NULL;
 
     MqttQueueMsg *head = NULL, *tail = NULL;
     for (mqtt_msg_t *cur = orig; cur; cur = cur->next) {
         MqttQueueMsg *node = calloc(1, sizeof(*node));
         if (!node) break;
         node->topic   = cur->topic   ? strdup(cur->topic)   : NULL;
         node->payload = cur->payload ? strdup(cur->payload) : NULL;
         node->next    = NULL;
         if (!head) head = tail = node;
         else       { tail->next = node; tail = node; }
     }
     mqtt_msg_list_free(orig);
     return head;
 }
 
 void mqtt_qr_free_msgs(MqttQueueMsg *head)
 {
     MqttQueueMsg *cur = head;
     while (cur) {
         MqttQueueMsg *next = cur->next;
         free(cur->topic);
         free(cur->payload);
         free(cur);
         cur = next;
     }
 }