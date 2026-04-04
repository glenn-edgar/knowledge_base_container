/**
 * @file robot_mqtt.h
 * @brief MQTT transport layer: connect, subscribe, publish with wire format.
 *
 * Handles CBOR/JSON encoding on outbound messages and decoding on inbound.
 * Builds all response JSON strings (ack, heartbeat, kb_done, status).
 */

#ifndef ROBOT_MQTT_H
#define ROBOT_MQTT_H

#include "robot_state.h"
#include "../libs/mqtt_pubsub.h"
#include <stdbool.h>

/* ================================================================== */
/*  MQTT transport handle                                              */
/* ================================================================== */

typedef struct {
    MqttPubSubStream *ps;
    bool              use_cbor;   /* true if wire_format == "cbor" */
} robot_mqtt_t;

/* Initialize MQTT connection + subscribe to RPC topic */
int robot_mqtt_init(robot_mqtt_t *mqtt, const robot_state_t *state);

/* Disconnect and cleanup */
void robot_mqtt_destroy(robot_mqtt_t *mqtt);

/* ================================================================== */
/*  Publish helpers (auto-encode CBOR if configured)                   */
/* ================================================================== */

/* Publish a JSON string to a topic, encoding to CBOR if use_cbor */
bool robot_mqtt_publish(robot_mqtt_t *mqtt, const char *topic,
                        const char *json, int qos, bool retain);

/* ================================================================== */
/*  Protocol message builders + publishers                             */
/* ================================================================== */

/* Send ack response on stream_bus */
void robot_mqtt_send_ack(robot_mqtt_t *mqtt, const robot_state_t *state,
                          int seq, int test_id);

/* Send heartbeat on stream_bus */
void robot_mqtt_send_heartbeat(robot_mqtt_t *mqtt, const robot_state_t *state,
                                const char *phase);

/* Send kb_done on stream_bus */
void robot_mqtt_send_kb_done(robot_mqtt_t *mqtt, robot_state_t *state,
                              bool success, const char *fault_reason);

/* Publish status/state (retained) */
void robot_mqtt_publish_state(robot_mqtt_t *mqtt, const robot_state_t *state,
                               bool connected);

/* Publish status/energy (retained) */
void robot_mqtt_publish_energy(robot_mqtt_t *mqtt, const robot_state_t *state);

/* Publish status/bitmask (retained) */
void robot_mqtt_publish_bitmask(robot_mqtt_t *mqtt, const robot_state_t *state);

/* ================================================================== */
/*  Inbound message decoding                                           */
/* ================================================================== */

/**
 * Decode an inbound MQTT message payload to JSON string.
 * If CBOR wire format, decodes CBOR->JSON into out_json buffer.
 * If JSON wire format, copies payload directly.
 * Returns length of JSON string, or -1 on error.
 */
int robot_mqtt_decode_payload(robot_mqtt_t *mqtt,
                               const char *payload, int payload_len,
                               char *out_json, int max_out);

#endif /* ROBOT_MQTT_H */
