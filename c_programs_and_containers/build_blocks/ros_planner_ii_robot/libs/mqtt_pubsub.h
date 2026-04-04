/**
 * @file mqtt_pubsub.h
 * @brief Streaming MQTT pub/sub with thread-safe message buffer.
 *
 * Subscribe once -> messages accumulate in a ring buffer via on_message
 * callback (mosquitto thread) -> main thread drains them with poll().
 * No re-subscribe per read.
 *
 * Copied from knowledge_base/mqtt/ for standalone robot deployment.
 */

#ifndef MQTT_PUBSUB_H
#define MQTT_PUBSUB_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum sizes for buffered messages */
#define MQPS_MAX_TOPIC   256
#define MQPS_MAX_PAYLOAD 8192
#define MQPS_RING_SIZE   256   /* must be power of 2 */

/* Single buffered message (fixed-size for lock-free ring) */
typedef struct {
    char     topic[MQPS_MAX_TOPIC];
    char     payload[MQPS_MAX_PAYLOAD];
    uint32_t payload_len;
    int      qos;
    bool     retain;
    bool     valid;
} MqpsMsgSlot;

/* Opaque handle */
typedef struct MqttPubSubStream MqttPubSubStream;

/* Stats */
typedef struct {
    uint64_t published;
    uint64_t received;
    uint64_t dropped;      /* ring full, message lost */
    uint64_t polled;
    uint32_t ring_used;    /* current occupancy */
} MqpsStats;

/* --- Lifecycle --- */

MqttPubSubStream *mqps_create(const char *host, int port,
                               const char *client_id);
void              mqps_destroy(MqttPubSubStream *ps);

bool mqps_connect(MqttPubSubStream *ps, int timeout_ms);
void mqps_disconnect(MqttPubSubStream *ps);
bool mqps_is_connected(MqttPubSubStream *ps);

/* --- Publish (from main thread) --- */

bool mqps_publish(MqttPubSubStream *ps, const char *topic,
                  const void *payload, int payload_len,
                  int qos, bool retain);

/* --- Subscribe (persistent, one-time setup) --- */

bool mqps_subscribe(MqttPubSubStream *ps, const char *topic,
                    int qos);

bool mqps_unsubscribe(MqttPubSubStream *ps, const char *topic);

/* --- Poll buffered messages (from main thread) --- */

/**
 * Run mosquitto network loop for up to timeout_ms, then drain
 * the ring buffer into out_msgs (up to max_msgs).
 * Returns number of messages written to out_msgs.
 */
int mqps_poll(MqttPubSubStream *ps, int timeout_ms,
              MqpsMsgSlot *out_msgs, int max_msgs);

/**
 * Non-blocking drain: return buffered messages without waiting.
 * Call after mqps_poll or standalone for zero-wait drain.
 */
int mqps_drain(MqttPubSubStream *ps,
               MqpsMsgSlot *out_msgs, int max_msgs);

/* --- Stats --- */

void mqps_get_stats(MqttPubSubStream *ps, MqpsStats *out);

#ifdef __cplusplus
}
#endif

#endif /* MQTT_PUBSUB_H */
