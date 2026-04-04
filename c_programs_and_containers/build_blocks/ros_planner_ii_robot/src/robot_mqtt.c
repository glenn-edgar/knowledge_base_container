/**
 * @file robot_mqtt.c
 * @brief MQTT transport: connect, subscribe, publish with CBOR/JSON wire format.
 */

#include "robot_mqtt.h"
#include "robot_protocol.h"
#include "../libs/cbor_codec.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

/* ================================================================== */
/*  Timestamp helper                                                   */
/* ================================================================== */

static void get_timestamp(char *buf, int max) {
    time_t now = time(NULL);
    struct tm *t = gmtime(&now);
    strftime(buf, max, "%Y-%m-%dT%H:%M:%SZ", t);
}

/* ================================================================== */
/*  Init / Destroy                                                     */
/* ================================================================== */

int robot_mqtt_init(robot_mqtt_t *mqtt, const robot_state_t *state) {
    memset(mqtt, 0, sizeof(robot_mqtt_t));
    mqtt->use_cbor = (strcmp(state->config.wire_format, "cbor") == 0);

    /* Generate unique client ID */
    char client_id[128];
    snprintf(client_id, sizeof(client_id), "c_robot_%s_%d",
             state->config.robot_id, (int)time(NULL));

    mqtt->ps = mqps_create(state->config.mqtt_host,
                            state->config.mqtt_port,
                            client_id);
    if (!mqtt->ps) {
        fprintf(stderr, "[mqtt] failed to create pubsub stream\n");
        return -1;
    }

    if (!mqps_connect(mqtt->ps, 5000)) {
        fprintf(stderr, "[mqtt] failed to connect to %s:%d\n",
                state->config.mqtt_host, state->config.mqtt_port);
        mqps_destroy(mqtt->ps);
        mqtt->ps = NULL;
        return -1;
    }

    /* Subscribe to RPC topic */
    if (!mqps_subscribe(mqtt->ps, state->topic_rpc, 1)) {
        fprintf(stderr, "[mqtt] failed to subscribe to %s\n", state->topic_rpc);
        mqps_destroy(mqtt->ps);
        mqtt->ps = NULL;
        return -1;
    }

    printf("[mqtt] connected to %s:%d, subscribed to %s (wire=%s)\n",
           state->config.mqtt_host, state->config.mqtt_port,
           state->topic_rpc, state->config.wire_format);

    return 0;
}

void robot_mqtt_destroy(robot_mqtt_t *mqtt) {
    if (mqtt->ps) {
        mqps_disconnect(mqtt->ps);
        mqps_destroy(mqtt->ps);
        mqtt->ps = NULL;
    }
}

/* ================================================================== */
/*  Publish with wire format encoding                                  */
/* ================================================================== */

bool robot_mqtt_publish(robot_mqtt_t *mqtt, const char *topic,
                        const char *json, int qos, bool retain) {
    if (!mqtt->ps) return false;

    if (mqtt->use_cbor) {
        uint8_t cbor_buf[MQPS_MAX_PAYLOAD];
        int cbor_len = cbor_from_json(json, cbor_buf, sizeof(cbor_buf));
        if (cbor_len < 0) {
            fprintf(stderr, "[mqtt] CBOR encode failed for: %.80s...\n", json);
            return false;
        }
        return mqps_publish(mqtt->ps, topic, cbor_buf, cbor_len, qos, retain);
    } else {
        return mqps_publish(mqtt->ps, topic, json, (int)strlen(json), qos, retain);
    }
}

/* ================================================================== */
/*  Inbound decoding                                                   */
/* ================================================================== */

int robot_mqtt_decode_payload(robot_mqtt_t *mqtt,
                               const char *payload, int payload_len,
                               char *out_json, int max_out) {
    if (mqtt->use_cbor) {
        return cbor_to_json((const uint8_t *)payload, payload_len,
                            out_json, max_out);
    } else {
        int copy = payload_len;
        if (copy >= max_out) copy = max_out - 1;
        memcpy(out_json, payload, copy);
        out_json[copy] = '\0';
        return copy;
    }
}

/* ================================================================== */
/*  Protocol message builders                                          */
/* ================================================================== */

void robot_mqtt_send_ack(robot_mqtt_t *mqtt, const robot_state_t *state,
                          int seq, int test_id) {
    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
             "{\"type\":\"ack\",\"seq\":%d,\"test_id\":%d,\"status\":\"ok\"}",
             seq, test_id);
    robot_mqtt_publish(mqtt, state->topic_stream, json, 1, false);
}

void robot_mqtt_send_heartbeat(robot_mqtt_t *mqtt, const robot_state_t *state,
                                const char *phase) {
    const worker_state_t *w = &state->worker;
    double dx = state->pose.x - w->start_pose.x;
    double dy = state->pose.y - w->start_pose.y;
    double dz = state->pose.z - w->start_pose.z;
    double dh = state->pose.heading - w->start_pose.heading;
    double da = state->pose.arm_angle - w->start_pose.arm_angle;

    const char *worker_name = (w->packet_type >= 1 && w->packet_type <= 12)
                              ? packet_type_names[w->packet_type] : "unknown";

    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
        "{\"type\":\"heartbeat\","
        "\"phase\":\"%s\","
        "\"test_id\":%d,"
        "\"delta_x\":%.6f,\"delta_y\":%.6f,\"delta_z\":%.6f,"
        "\"delta_heading\":%.6f,\"delta_arm_angle\":%.6f,"
        "\"global_x\":%.6f,\"global_y\":%.6f,\"global_z\":%.6f,"
        "\"global_heading\":%.6f,\"global_arm_angle\":%.6f,"
        "\"watchdog_ticks\":%d,"
        "\"worker\":\"%s\"}",
        phase, w->test_id,
        dx, dy, dz, dh, da,
        state->pose.x, state->pose.y, state->pose.z,
        state->pose.heading, state->pose.arm_angle,
        w->elapsed, worker_name);

    robot_mqtt_publish(mqtt, state->topic_stream, json, 1, false);
}

void robot_mqtt_send_kb_done(robot_mqtt_t *mqtt, robot_state_t *state,
                              bool success, const char *fault_reason) {
    const worker_state_t *w = &state->worker;
    double dx = state->pose.x - w->start_pose.x;
    double dy = state->pose.y - w->start_pose.y;
    double dz = state->pose.z - w->start_pose.z;
    double dh = state->pose.heading - w->start_pose.heading;
    double da = state->pose.arm_angle - w->start_pose.arm_angle;

    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
        "{\"type\":\"kb_done\","
        "\"test_id\":%d,"
        "\"success\":%s,"
        "\"delta_x\":%.6f,\"delta_y\":%.6f,\"delta_z\":%.6f,"
        "\"delta_heading\":%.6f,\"delta_arm_angle\":%.6f,"
        "\"fault_reason\":%s%s%s,"
        "\"energy_remaining\":%d,"
        "\"energy_max\":%d}",
        w->test_id,
        success ? "true" : "false",
        dx, dy, dz, dh, da,
        fault_reason ? "\"" : "",
        fault_reason ? fault_reason : "null",
        fault_reason ? "\"" : "",
        state->energy_remaining,
        state->config.energy_max);

    robot_mqtt_publish(mqtt, state->topic_stream, json, 1, false);
}

/* ================================================================== */
/*  Status publishers (retained messages)                              */
/* ================================================================== */

void robot_mqtt_publish_state(robot_mqtt_t *mqtt, const robot_state_t *state,
                               bool connected) {
    char ts[64];
    get_timestamp(ts, sizeof(ts));

    const char *worker_name = "";
    if (state->worker.active && state->worker.packet_type >= 1
        && state->worker.packet_type <= 12) {
        worker_name = packet_type_names[state->worker.packet_type];
    }

    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
        "{\"connected\":%s,"
        "\"robot_id\":\"%s\","
        "\"wire_format\":\"%s\","
        "\"active_kb\":\"%s\","
        "\"active_worker\":\"%s\","
        "\"%s\":\"%s\"}",
        connected ? "true" : "false",
        state->config.robot_id,
        state->config.wire_format,
        worker_name, worker_name,
        connected ? "started_at" : "stopped_at", ts);

    /* Status messages are always JSON — bridge reads them directly */
    mqps_publish(mqtt->ps, state->topic_status_state,
                 json, (int)strlen(json), 1, true);
}

void robot_mqtt_publish_energy(robot_mqtt_t *mqtt, const robot_state_t *state) {
    char ts[64];
    get_timestamp(ts, sizeof(ts));

    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
        "{\"energy_max\":%d,"
        "\"energy_remaining\":%d,"
        "\"robot_id\":\"%s\","
        "\"timestamp\":\"%s\"}",
        state->config.energy_max,
        state->energy_remaining,
        state->config.robot_id, ts);

    /* Status messages are always JSON — bridge reads them directly */
    mqps_publish(mqtt->ps, state->topic_status_energy,
                 json, (int)strlen(json), 1, true);
}

void robot_mqtt_publish_bitmask(robot_mqtt_t *mqtt, const robot_state_t *state) {
    char ts[64];
    get_timestamp(ts, sizeof(ts));

    const char *kb_name = "";
    if (state->worker.active && state->worker.packet_type >= 1
        && state->worker.packet_type <= 12) {
        kb_name = packet_type_names[state->worker.packet_type];
    }

    /* Bit 0 is always heartbeat */
    uint32_t bm = state->bitmask | 0x01;

    char json[JSON_BUF_MAX];
    snprintf(json, sizeof(json),
        "{\"kb_name\":\"%s\","
        "\"raw\":%u,"
        "\"fields\":{\"heartbeat\":true},"
        "\"robot_id\":\"%s\","
        "\"timestamp\":\"%s\"}",
        kb_name, bm, state->config.robot_id, ts);

    /* Note: bitmask JSON has nested "fields" object, but the CBOR codec
       only handles flat maps. For CBOR wire format, we send bitmask
       status as JSON (retained status messages are small). */
    mqps_publish(mqtt->ps, state->topic_status_bitmask,
                 json, (int)strlen(json), 1, true);
}
