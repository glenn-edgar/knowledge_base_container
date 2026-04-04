/**
 * @file main.c
 * @brief C Robot with ChainTree runtime — MQTT + CBOR event-driven.
 *
 * Custom main loop replaces cfl_runtime_run():
 *   1. Poll MQTT ring buffer for inbound RPC commands
 *   2. Inject CBOR payload as streaming data event into ChainTree
 *   3. Generate timer events for active KBs
 *   4. Process all ChainTree events (CBOR sink decodes, dispatches)
 *   5. Check blackboard for shutdown
 *   6. Periodic status publishing (bitmask, energy)
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
#include "robot_context.h"
#include "ct_user_functions.h"

/* ChainTree runtime headers */
#include "cfl_runtime.h"
#include "cfl_chaintree_support.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_image_loader.h"
#include "cfl_cbor_functions.h"
#include "cfl_cbor_packets.h"
#include "../dsl/robot_handle_image.h"
#include "../dsl/robot_handle_blackboard.h"

/* MQTT pubsub */
#include "../libs/mqtt_pubsub.h"
#include "../libs/cbor_codec.h"  /* for decoding CBOR wire → JSON */

/* ================================================================== */
/*  Constants                                                          */
/* ================================================================== */

#define TICK_MS           10
#define BITMASK_TICKS     10
#define ENERGY_TICKS      3000
#define MAX_POLL_MSGS     8

/* Blackboard access macros */
#define BB_UINT16(rt, off) (*(uint16_t *)((uint8_t *)(rt)->blackboard + (off)))

/* ================================================================== */
/*  Global robot context (accessed by user functions)                  */
/* ================================================================== */

robot_context_t *g_robot_ctx = NULL;

/* ================================================================== */
/*  Signal handling                                                    */
/* ================================================================== */

static volatile sig_atomic_t g_shutdown = 0;

static void signal_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* ================================================================== */
/*  Helper: find KB index by name                                      */
/* ================================================================== */

static int find_kb_index(const cfl_chaintree_handle_t *fh, const char *name) {
    for (uint16_t k = 0; k < fh->kb_count; k++) {
        if (fh->kb_table[k].kb_name &&
            strcmp(fh->kb_table[k].kb_name, name) == 0) {
            return (int)k;
        }
    }
    return -1;
}

/* ================================================================== */
/*  Helper: find event ID by name from event string table              */
/* ================================================================== */

static uint16_t find_event_id(const cfl_chaintree_handle_t *fh,
                               const char *event_name) {
    if (!fh->event_strings) return 0xFFFF;
    for (uint16_t i = 0; i < fh->event_count; i++) {
        if (fh->event_strings[i] &&
            strcmp(fh->event_strings[i], event_name) == 0) {
            return i;  /* 0-based index matches node_dict.event_id */
        }
    }
    return 0xFFFF;  /* fallback */
}

/* ================================================================== */
/*  ChainTree setup                                                    */
/* ================================================================== */

static cfl_perm_t perm;
static char perm_buffer[0xffff];

static cfl_runtime_handle_t *setup_chaintree(cfl_image_loader_t *img) {
    /* 1. Load embedded binary image */
    int rc = cfl_embedded_load(robot_handle_image,
                               ROBOT_HANDLE_IMAGE_SIZE, img);
    if (rc != CFL_IMAGE_OK) {
        fprintf(stderr, "[ct] failed to load image (error %d)\n", rc);
        return NULL;
    }
    printf("[ct] loaded image (%u bytes)\n", ROBOT_HANDLE_IMAGE_SIZE);

    /* 2. Register all built-in functions */
    cfl_register_all_functions(img);

    /* 3. Register robot user functions */
    register_robot_user_functions(img);

    /* 4. Validate */
    int missing = cfl_image_validate(img);
    if (missing > 0) {
        fprintf(stderr, "[ct] %d function(s) not registered\n", missing);
        return NULL;
    }

    /* 5. Get handle */
    const cfl_chaintree_handle_t *flash = cfl_image_get_handle(img);
    if (!flash) {
        fprintf(stderr, "[ct] flash handle is NULL\n");
        return NULL;
    }

    printf("[ct] nodes=%d, kbs=%d\n", flash->node_count, flash->kb_count);

    /* 6. Initialize worker KB lookup */
    robot_ct_init_kb_lookup(flash);

    /* 7. Create runtime */
    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    params->perm = &perm;
    params->perm_buffer = perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(perm_buffer);
    params->heap_size = (uint16_t)32000;
    params->max_allocator_count = 15;
    params->total_node_count = flash->node_count;
    params->allocator_0_size = (uint16_t)4096;
    params->event_queue_high_priority_size = (uint16_t)8;
    params->event_queue_low_priority_size = (uint16_t)64;
    params->delta_time = (double)0.01;

    cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, flash);
    cfl_runtime_create_params_destroy(params);

    if (!handle) {
        fprintf(stderr, "[ct] failed to create runtime\n");
        return NULL;
    }

    cfl_runtime_reset(handle);

    /* 8. Activate controller KB */
    int ctrl_idx = find_kb_index(flash, "controller");
    if (ctrl_idx >= 0) {
        cfl_add_test_by_index(handle, (uint16_t)ctrl_idx);
        printf("[ct] activated controller KB (index=%d)\n", ctrl_idx);
    } else {
        fprintf(stderr, "[ct] controller KB not found!\n");
    }

    return handle;
}

/* ================================================================== */
/*  Main loop (replaces cfl_runtime_run)                               */
/* ================================================================== */

static void run_robot(robot_state_t *state, robot_mqtt_t *mqtt,
                       cfl_runtime_handle_t *handle) {
    MqpsMsgSlot msgs[MAX_POLL_MSGS];
    CFL_EVENT_DATA_T event_data;
    cfl_tick_result_t tick_result;

    const cfl_chaintree_handle_t *flash = handle->flash_handle;
    uint16_t rpc_event_id = find_event_id(flash, "MQTT_RPC_EVENT");

    /* Find controller KB start node for event injection */
    uint16_t controller_start = 0;
    int ctrl_idx = find_kb_index(flash, "controller");
    if (ctrl_idx >= 0) {
        controller_start = flash->kb_table[ctrl_idx].start_index;
    }

    /* Publish initial status */
    robot_mqtt_publish_state(mqtt, state, true);
    robot_mqtt_publish_energy(mqtt, state);
    robot_mqtt_publish_bitmask(mqtt, state);

    /* Initialize timer */
    cfl_timer_wait(handle->timer_handle, 0.001, &tick_result);
    handle->future_time_stamp =
        cfl_timer_get_timestamp(handle->timer_handle) + handle->delta_time;

    printf("[robot] entering ChainTree main loop (tick=%dms, event_id=0x%04x)\n",
           TICK_MS, rpc_event_id);

    /* Persistent CBOR buffer ref for event injection */
    static uint8_t cbor_inject_buf[MQPS_MAX_PAYLOAD];
    static cfl_cbor_buffer_t cbor_ref;

    uint64_t tick_count = 0;

    while (!g_shutdown) {
        /* 1. Poll MQTT */
        int count = mqps_poll(mqtt->ps, TICK_MS, msgs, MAX_POLL_MSGS);

        /* 2. Inject payloads as streaming data events
         * CBOR wire: payload is already CBOR from bridge — pass directly
         * JSON wire: encode JSON→CBOR using our codec (same RFC 8949 format)
         * The ChainTree CBOR sink decodes with cfl_cbor_to_json_text
         * which uses cJSON internally — both codecs produce standard CBOR.
         */
        for (int i = 0; i < count; i++) {
            if (strstr(msgs[i].topic, "/rpc") == NULL) continue;

            if (mqtt->use_cbor) {
                /* CBOR wire: payload is already CBOR from bridge */
                int len = (int)msgs[i].payload_len;
                if (len > (int)sizeof(cbor_inject_buf)) len = (int)sizeof(cbor_inject_buf);
                memcpy(cbor_inject_buf, msgs[i].payload, len);
                cbor_ref.data = cbor_inject_buf;
                cbor_ref.len = (size_t)len;
            } else {
                /* JSON wire: encode to CBOR using ChainTree codec */
                size_t ct_len = cfl_json_text_to_cbor(msgs[i].payload,
                                    cbor_inject_buf, sizeof(cbor_inject_buf));
                if (ct_len == 0) continue;
                cbor_ref.data = cbor_inject_buf;
                cbor_ref.len = ct_len;
            }

            printf("[main:inject] CBOR %zu bytes → controller node %d, event_id=0x%04x\n",
                   cbor_ref.len, controller_start, rpc_event_id);

            cfl_send_streaming_data_event(handle->event_queue,
                CFL_EVENT_PRIORITY_LOW,
                controller_start,
                rpc_event_id,
                &cbor_ref);
        }

        /* 3. Generate timer events + process */
        tick_result.changed_mask = 0;

        for (uint16_t kb_idx = 0; kb_idx < flash->kb_count; kb_idx++) {
            if (!TEST_IS_ACTIVE(handle, kb_idx)) continue;

            handle->current_kb_idx = kb_idx;
            handle->kb_start_index = flash->kb_table[kb_idx].start_index;
            handle->kb_node_count = flash->kb_table[kb_idx].node_count;
            handle->kb_max_level = flash->kb_table[kb_idx].max_depth + 1;

            /* Timer event */
            if (cfl_engine_node_is_enabled(handle, handle->kb_start_index)) {
                cfl_send_event(handle->event_queue, CFL_EVENT_PRIORITY_LOW,
                    handle->kb_start_index, CFL_TIMER_EVENT, false,
                    CFL_TIMER_EVENT, &tick_result);
            }

            handle->bitmask = handle->shaddow_bitmask;

            /* 4. Process all events */
            while (cfl_total_event_count(handle->event_queue) > 0) {
                cfl_pop_event(handle->event_queue, &event_data);

                if (event_data.event_id == CFL_TERMINATE_SYSTEM_EVENT) {
                    goto exit;
                }

                handle->event_data_ptr = &event_data;

                if (cfl_execute_event(handle) == false) {
                    cfl_delete_test_by_index(handle, handle->current_kb_idx);
                }
            }

            if (!cfl_engine_node_is_enabled(handle, handle->kb_start_index)) {
                cfl_delete_test_by_index(handle, kb_idx);
            }
        }

        handle->future_time_stamp += handle->delta_time;

        /* 5. Check shutdown from blackboard */
        if (BB_UINT16(handle, BB_SHUTDOWN_REQUESTED_OFFSET)) {
            printf("[robot] shutdown requested via ChainTree\n");
            goto exit;
        }

        /* 6. Periodic status */
        if (tick_count % BITMASK_TICKS == 0) {
            robot_mqtt_publish_bitmask(mqtt, state);
        }
        if (tick_count % ENERGY_TICKS == 0) {
            robot_mqtt_publish_energy(mqtt, state);
        }

        tick_count++;
    }

exit:
    printf("[robot] exiting main loop (ticks=%lu)\n", (unsigned long)tick_count);
}

/* ================================================================== */
/*  Entry point                                                        */
/* ================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <config.json>\n", argv[0]);
        return 1;
    }

    setbuf(stdout, NULL);  /* unbuffered for background process logging */
    setbuf(stderr, NULL);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("=== ROS Planner II — C Robot (ChainTree) ===\n\n");

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
    printf("\n");

    /* 3. Initialize MQTT transport */
    robot_mqtt_t mqtt;
    if (robot_mqtt_init(&mqtt, &state) != 0) {
        fprintf(stderr, "Failed to initialize MQTT transport\n");
        return 1;
    }

    /* 4. Setup ChainTree runtime */
    cfl_image_loader_t img;
    cfl_runtime_handle_t *handle = setup_chaintree(&img);
    if (!handle) {
        fprintf(stderr, "Failed to setup ChainTree runtime\n");
        robot_mqtt_destroy(&mqtt);
        return 1;
    }

    /* 5. Set global context for user functions */
    robot_context_t ctx = { .mqtt = &mqtt, .state = &state };
    g_robot_ctx = &ctx;

    /* 6. Run main loop */
    run_robot(&state, &mqtt, handle);

    /* 7. Cleanup */
    g_robot_ctx = NULL;
    robot_mqtt_publish_state(&mqtt, &state, false);
    robot_mqtt_destroy(&mqtt);
    cfl_image_free(&img);

    printf("\n[robot] shutdown complete\n");
    return 0;
}
