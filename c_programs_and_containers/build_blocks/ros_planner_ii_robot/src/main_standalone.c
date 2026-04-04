/**
 * @file main.c
 * @brief C Robot main loop — MQTT + CBOR, compatible with Planner II bridge.
 *
 * Event loop:
 *   1. Poll MQTT ring buffer for inbound RPC commands
 *   2. Decode CBOR/JSON payload, extract packet_type
 *   3. Dispatch command: ack, start worker, simulate action
 *   4. Tick worker: interpolate pose, send heartbeats
 *   5. On completion: send kb_done, deduct energy
 *   6. Periodic: publish status (state, energy, bitmask)
 *
 * Usage: ./robot_main <config.json>
 */

#define _POSIX_C_SOURCE 199309L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

#include "robot_config.h"
#include "robot_state.h"
#include "robot_mqtt.h"
#include "robot_protocol.h"
#include "json_extract.h"
#include "../libs/mqtt_pubsub.h"

/* ================================================================== */
/*  Constants                                                          */
/* ================================================================== */

#define TICK_MS           10       /* main loop tick interval */
#define HEARTBEAT_TICKS   10       /* send heartbeat every N ticks */
#define BITMASK_TICKS     10       /* publish bitmask every N ticks */
#define ENERGY_TICKS      3000     /* publish energy every 30s at 100Hz */
#define MAX_POLL_MSGS     8        /* max messages per poll */

/* ================================================================== */
/*  Signal handling                                                    */
/* ================================================================== */

static volatile sig_atomic_t g_shutdown = 0;

static void signal_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* ================================================================== */
/*  Command dispatch                                                   */
/* ================================================================== */

/**
 * Extract movement deltas from command fields based on packet_type.
 * For path types: compute delta from from_x/y to to_x/y.
 * For arm types: compute delta from arm params.
 * For rotate: compute heading delta.
 */
static void extract_deltas(const char *json, int packet_type,
                            double *dx, double *dy, double *dz,
                            double *dheading, double *darm) {
    *dx = *dy = *dz = *dheading = *darm = 0.0;

    double from_x = 0, from_y = 0, to_x = 0, to_y = 0;
    double from_heading = 0, to_heading = 0;
    double arm_target = 0;

    switch (packet_type) {
    case TYPE_PATH_SPLINE:
    case TYPE_PATH_LINE:
    case TYPE_PATH_WALL:
        json_get_double(json, "from_x", &from_x);
        json_get_double(json, "from_y", &from_y);
        json_get_double(json, "to_x", &to_x);
        json_get_double(json, "to_y", &to_y);
        *dx = to_x - from_x;
        *dy = to_y - from_y;
        break;

    case TYPE_PATH_ROTATE:
        json_get_double(json, "from_heading", &from_heading);
        json_get_double(json, "to_heading", &to_heading);
        *dheading = to_heading - from_heading;
        break;

    case TYPE_DELIVER_PART:
    case TYPE_PAINT_SAMPLE:
    case TYPE_LOAD_SHIPPING:
        json_get_double(json, "arm_target", &arm_target);
        *darm = arm_target;
        break;

    default:
        break;
    }
}

static void handle_rpc_command(robot_state_t *state, robot_mqtt_t *mqtt,
                                const char *json) {
    int packet_type = 0;
    int seq = 0;
    int test_id = 0;

    if (!json_get_int(json, "packet_type", &packet_type)) {
        fprintf(stderr, "[rpc] missing packet_type in: %.80s\n", json);
        return;
    }
    json_get_int(json, "seq", &seq);
    json_get_int(json, "test_id", &test_id);

    printf("[rpc] packet_type=%d seq=%d test_id=%d\n", packet_type, seq, test_id);

    /* Shutdown command */
    if (packet_type == TYPE_SHUTDOWN) {
        printf("[rpc] shutdown requested\n");
        robot_mqtt_send_ack(mqtt, state, seq, test_id);
        state->running = false;
        return;
    }

    /* Validate packet_type range */
    if (packet_type < 1 || packet_type > 12) {
        fprintf(stderr, "[rpc] unknown packet_type %d\n", packet_type);
        return;
    }

    /* If worker already active, drop (no lookahead in C version) */
    if (state->worker.active) {
        fprintf(stderr, "[rpc] worker busy, dropping packet_type=%d\n", packet_type);
        return;
    }

    /* Send ack */
    robot_mqtt_send_ack(mqtt, state, seq, test_id);

    /* Extract movement deltas from command */
    double dx, dy, dz, dheading, darm;
    extract_deltas(json, packet_type, &dx, &dy, &dz, &dheading, &darm);

    /* Start worker */
    robot_state_start_worker(state, packet_type, seq, test_id,
                              dx, dy, dz, dheading, darm);

    /* Send initial heartbeat */
    robot_mqtt_send_heartbeat(mqtt, state, "initial");

    printf("[rpc] started worker: %s (duration=%d ticks)\n",
           packet_type_names[packet_type], state->worker.duration);
}

/* ================================================================== */
/*  Main loop                                                          */
/* ================================================================== */

static void run_robot(robot_state_t *state, robot_mqtt_t *mqtt) {
    MqpsMsgSlot msgs[MAX_POLL_MSGS];
    char json_buf[JSON_BUF_MAX];

    /* Publish initial status */
    robot_mqtt_publish_state(mqtt, state, true);
    robot_mqtt_publish_energy(mqtt, state);
    robot_mqtt_publish_bitmask(mqtt, state);

    printf("[robot] entering main loop (tick=%dms)\n", TICK_MS);

    while (state->running && !g_shutdown) {
        /* 1. Poll MQTT for inbound messages */
        int count = mqps_poll(mqtt->ps, TICK_MS, msgs, MAX_POLL_MSGS);

        /* 2. Process inbound RPC commands */
        for (int i = 0; i < count; i++) {
            /* Check if this is our RPC topic */
            if (strstr(msgs[i].topic, "/rpc") == NULL) continue;

            int jlen = robot_mqtt_decode_payload(mqtt,
                            msgs[i].payload, (int)msgs[i].payload_len,
                            json_buf, sizeof(json_buf));
            if (jlen < 0) {
                fprintf(stderr, "[mqtt] decode failed on topic %s\n", msgs[i].topic);
                continue;
            }

            handle_rpc_command(state, mqtt, json_buf);
        }

        /* 3. Tick active worker */
        if (state->worker.active) {
            robot_state_tick_worker(state);

            /* Heartbeat every N ticks */
            if (state->worker.elapsed % HEARTBEAT_TICKS == 0) {
                robot_mqtt_send_heartbeat(mqtt, state, "periodic");
            }

            /* Check completion */
            if (state->worker.elapsed >= state->worker.duration) {
                /* Send final heartbeat */
                robot_mqtt_send_heartbeat(mqtt, state, "final");

                /* Deduct energy */
                robot_state_deduct_energy(state, state->worker.packet_type);

                /* Send kb_done */
                robot_mqtt_send_kb_done(mqtt, state, true, NULL);

                printf("[worker] completed: %s (energy=%d/%d)\n",
                       packet_type_names[state->worker.packet_type],
                       state->energy_remaining, state->config.energy_max);

                /* Complete worker */
                robot_state_complete_worker(state);

                /* Publish updated energy immediately */
                robot_mqtt_publish_energy(mqtt, state);
            }
        }

        /* 4. Periodic bitmask publish */
        if (state->tick_count % BITMASK_TICKS == 0) {
            robot_mqtt_publish_bitmask(mqtt, state);
        }

        /* 5. Periodic energy publish */
        if (state->tick_count % ENERGY_TICKS == 0) {
            robot_mqtt_publish_energy(mqtt, state);
        }

        state->tick_count++;
    }

    printf("[robot] exiting main loop (ticks=%lu)\n",
           (unsigned long)state->tick_count);
}

/* ================================================================== */
/*  Entry point                                                        */
/* ================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <config.json>\n", argv[0]);
        return 1;
    }

    /* Install signal handlers */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("=== ROS Planner II — C Robot ===\n\n");

    /* 1. Load configuration */
    robot_config_t config;
    if (robot_config_load(argv[1], &config) != 0) {
        fprintf(stderr, "Failed to load config from %s\n", argv[1]);
        return 1;
    }
    robot_config_print(&config);
    printf("\n");

    /* 2. Initialize robot state */
    robot_state_t state;
    robot_state_init(&state, &config);

    printf("[state] topics:\n");
    printf("  rpc:     %s\n", state.topic_rpc);
    printf("  stream:  %s\n", state.topic_stream);
    printf("  state:   %s\n", state.topic_status_state);
    printf("  energy:  %s\n", state.topic_status_energy);
    printf("  bitmask: %s\n", state.topic_status_bitmask);
    printf("\n");

    /* 3. Initialize MQTT transport */
    robot_mqtt_t mqtt;
    if (robot_mqtt_init(&mqtt, &state) != 0) {
        fprintf(stderr, "Failed to initialize MQTT transport\n");
        return 1;
    }

    /* 4. Run main loop */
    run_robot(&state, &mqtt);

    /* 5. Cleanup */
    robot_mqtt_publish_state(&mqtt, &state, false);
    robot_mqtt_destroy(&mqtt);

    printf("\n[robot] shutdown complete\n");
    return 0;
}
