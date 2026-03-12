/**
 * @file mqtt_luajit_adapter.h
 * @brief Opaque-handle adapter for LuaJIT FFI
 *
 * Wraps stack-allocated C MQTT structs with heap-allocated opaque handles.
 *
 * Wrapped:
 *   kv_read_store  → MqttKvWriter, MqttKvReader
 *   job_queue      → MqttPublisher, MqttQueueReader
 *   pubsub         → MqttPubSub (lifecycle + stats; subscribe callbacks
 *                     fire on mosquitto's thread — not usable from LuaJIT)
 */

#ifndef MQTT_LUAJIT_ADAPTER_H
#define MQTT_LUAJIT_ADAPTER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================== */
/*  Library init / cleanup (call once per process)                     */
/* ================================================================== */

void mqtt_adapter_lib_init(void);
void mqtt_adapter_lib_cleanup(void);

/* ================================================================== */
/*  KV Store Writer                                                    */
/* ================================================================== */

typedef struct MqttKvWriter MqttKvWriter;

MqttKvWriter *mqtt_kvw_create(const char *host, int port,
                               const char *client_id);
void mqtt_kvw_destroy(MqttKvWriter *w);
bool mqtt_kvw_connect(MqttKvWriter *w, double timeout_sec);
void mqtt_kvw_disconnect(MqttKvWriter *w);
bool mqtt_kvw_is_connected(MqttKvWriter *w);
bool mqtt_kvw_write(MqttKvWriter *w, const char *topic,
                    const char *value, int qos, bool retain,
                    double timeout_sec);
bool mqtt_kvw_delete(MqttKvWriter *w, const char *topic,
                     double timeout_sec);
bool mqtt_kvw_update(MqttKvWriter *w, const char *topic,
                     const char *value, double timeout_sec);

/* ================================================================== */
/*  KV Store Reader                                                    */
/* ================================================================== */

typedef struct MqttKvReader MqttKvReader;

#define MQTT_KV_MAX_TOPIC  256
#define MQTT_KV_MAX_VALUE  4096

typedef struct {
    char  topic[MQTT_KV_MAX_TOPIC];
    char  value[MQTT_KV_MAX_VALUE];
    bool  active;
} MqttKvEntry;

MqttKvReader *mqtt_kvr_create(const char *host, int port,
                               const char *client_id);
void mqtt_kvr_destroy(MqttKvReader *r);
bool mqtt_kvr_connect(MqttKvReader *r, double timeout_sec);
void mqtt_kvr_disconnect(MqttKvReader *r);
bool mqtt_kvr_is_connected(MqttKvReader *r);
int  mqtt_kvr_read_pattern(MqttKvReader *r, const char *pattern,
                           int qos, double timeout_sec,
                           MqttKvEntry *out_entries, int max_entries);
bool mqtt_kvr_read_single(MqttKvReader *r, const char *topic,
                          double timeout_sec,
                          char *out_value, int out_value_len);
int  mqtt_kvr_read_all(MqttKvReader *r, const char *base_topic,
                       double timeout_sec,
                       MqttKvEntry *out_entries, int max_entries);

/* ================================================================== */
/*  Queue Publisher                                                    */
/* ================================================================== */

typedef struct MqttPublisher MqttPublisher;

MqttPublisher *mqtt_pub_create(const char *host, int port,
                                const char *client_id);
void mqtt_pub_destroy(MqttPublisher *p);
bool mqtt_pub_connect(MqttPublisher *p, int timeout_ms);
void mqtt_pub_disconnect(MqttPublisher *p);
bool mqtt_pub_is_connected(MqttPublisher *p);
bool mqtt_pub_publish(MqttPublisher *p, const char *topic,
                      const char *payload, int qos, bool retain);

/* ================================================================== */
/*  Queue Reader                                                       */
/* ================================================================== */

typedef struct MqttQueueReader MqttQueueReader;

typedef struct MqttQueueMsg {
    char *topic;
    char *payload;
    struct MqttQueueMsg *next;
} MqttQueueMsg;

MqttQueueReader *mqtt_qr_create(const char *host, int port,
                                 const char *client_id,
                                 bool clean_session);
void mqtt_qr_destroy(MqttQueueReader *r);
bool mqtt_qr_connect(MqttQueueReader *r, int timeout_ms);
void mqtt_qr_disconnect(MqttQueueReader *r);
bool mqtt_qr_is_connected(MqttQueueReader *r);
bool mqtt_qr_subscribe(MqttQueueReader *r, const char *topic,
                        int qos, int timeout_ms);
MqttQueueMsg *mqtt_qr_read(MqttQueueReader *r, const char *topic,
                            int qos, int timeout_ms, int *out_count);
void mqtt_qr_free_msgs(MqttQueueMsg *head);

/* ================================================================== */
/*  PubSub (lifecycle + stats; subscribe callbacks NOT usable)         */
/* ================================================================== */

typedef struct MqttPubSub MqttPubSub;

typedef struct {
    uint64_t messages_received;
    uint64_t callbacks_executed;
    uint64_t errors;
} MqttPubSubStats;

MqttPubSub *mqtt_ps_create(const char *host, int port,
                             const char *client_id,
                             bool clean_session,
                             bool auto_reconnect,
                             double reconnect_delay);
void mqtt_ps_destroy(MqttPubSub *ps);
bool mqtt_ps_connect(MqttPubSub *ps, double timeout_sec);
void mqtt_ps_disconnect(MqttPubSub *ps);
bool mqtt_ps_is_connected(MqttPubSub *ps);
void mqtt_ps_get_stats(MqttPubSub *ps, MqttPubSubStats *out);
void mqtt_ps_wait(MqttPubSub *ps, double timeout_sec);

#ifdef __cplusplus
}
#endif

#endif /* MQTT_LUAJIT_ADAPTER_H */
