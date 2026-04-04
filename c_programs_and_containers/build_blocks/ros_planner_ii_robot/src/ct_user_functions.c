/**
 * @file ct_user_functions.c
 * @brief ChainTree user functions for C MQTT/CBOR robot.
 *
 * Controller: CBOR sink dispatch + completion
 * Workers:    init one-shots set blackboard, DSL handles wait + terminate
 * Outbound:   uses g_robot_ctx for MQTT publish
 */

#include <stdio.h>
#include <string.h>
#include "ct_user_functions.h"
#include "robot_context.h"
#include "robot_protocol.h"
#include "cfl_runtime.h"
#include "cfl_cbor_functions.h"
#include "cfl_json_packets.h"
#include "cfl_image_loader.h"
#include "../dsl/robot_handle_blackboard.h"

/* ================================================================== */
/*  Blackboard access macros                                           */
/* ================================================================== */

#define BB_INT32(rt, off)  (*(int32_t  *)((uint8_t *)(rt)->blackboard + (off)))
#define BB_UINT16(rt, off) (*(uint16_t *)((uint8_t *)(rt)->blackboard + (off)))
#define BB_FLOAT(rt, off)  (*(float    *)((uint8_t *)(rt)->blackboard + (off)))

/* ================================================================== */
/*  Worker KB name -> index lookup                                     */
/* ================================================================== */

static int worker_kb_index[TYPE_COUNT + 1];

static void init_worker_kb_lookup(const cfl_chaintree_handle_t *fh) {
    static const struct { int ptype; const char *name; } map[] = {
        { TYPE_INIT_CHECK,      "worker_init_check" },
        { TYPE_PATH_SPLINE,     "worker_path_spline" },
        { TYPE_PATH_LINE,       "worker_path_line" },
        { TYPE_PATH_WALL,       "worker_path_wall" },
        { TYPE_PATH_ROTATE,     "worker_path_rotate" },
        { TYPE_DELIVER_PART,    "worker_deliver_part" },
        { TYPE_PAINT_SAMPLE,    "worker_paint_sample" },
        { TYPE_LOAD_SHIPPING,   "worker_load_shipping" },
        { TYPE_PASS_GATE,       "worker_pass_gate" },
        { TYPE_INSPECTION_SCAN, "worker_inspection_scan" },
        { TYPE_IDLE,            "worker_idle" },
        { TYPE_RECHARGE,        "worker_recharge" },
        { 0, NULL }
    };

    for (int i = 0; i <= TYPE_COUNT; i++) worker_kb_index[i] = -1;

    for (int m = 0; map[m].name; m++) {
        for (uint16_t k = 0; k < fh->kb_count; k++) {
            if (fh->kb_table[k].kb_name &&
                strcmp(fh->kb_table[k].kb_name, map[m].name) == 0) {
                worker_kb_index[map[m].ptype] = (int)k;
                break;
            }
        }
    }
}

void robot_ct_init_kb_lookup(const cfl_chaintree_handle_t *fh) {
    init_worker_kb_lookup(fh);
}

/* ================================================================== */
/*  CBOR RPC Dispatch (boolean for CBOR sink)                          */
/* ================================================================== */

bool cbor_rpc_dispatch_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    (void)node_index; (void)event_type; (void)event_id;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    if (!event_data) return true;

    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    /* Debug: dump packet */
    char dump_buf[512];
    int dump_len = cfl_json_to_buffer(pkt, dump_buf, sizeof(dump_buf));
    printf("[ct:dispatch] pkt: %s\n", dump_len > 0 ? dump_buf : "EMPTY");

    int32_t packet_type = 0, seq = 0, test_id = 0;
    cfl_json_get_int(pkt, "packet_type", &packet_type);
    cfl_json_get_int(pkt, "seq", &seq);
    cfl_json_get_int(pkt, "test_id", &test_id);

    printf("[ct:dispatch] packet_type=%d seq=%d test_id=%d\n",
           packet_type, seq, test_id);

    /* Shutdown */
    if (packet_type == TYPE_SHUTDOWN) {
        BB_UINT16(rt, BB_SHUTDOWN_REQUESTED_OFFSET) = 1;
        if (g_robot_ctx)
            robot_mqtt_send_ack(g_robot_ctx->mqtt, g_robot_ctx->state, seq, test_id);
        return true;
    }

    /* Validate */
    if (packet_type < 1 || packet_type > 12) {
        printf("[ct:dispatch] unknown packet_type %d\n", packet_type);
        return true;
    }

    int kb_idx = worker_kb_index[packet_type];
    if (kb_idx < 0) {
        printf("[ct:dispatch] no worker KB for packet_type %d\n", packet_type);
        return true;
    }

    /* Send ack */
    if (g_robot_ctx)
        robot_mqtt_send_ack(g_robot_ctx->mqtt, g_robot_ctx->state, seq, test_id);

    /* Set blackboard for worker */
    BB_INT32(rt, BB_CURRENT_PACKET_TYPE_OFFSET) = packet_type;
    BB_INT32(rt, BB_CURRENT_TEST_ID_OFFSET) = test_id;
    BB_INT32(rt, BB_CURRENT_SEQ_OFFSET) = seq;
    BB_INT32(rt, BB_ACTIVE_WORKER_IDX_OFFSET) = kb_idx;
    BB_UINT16(rt, BB_WORKER_DONE_OFFSET) = 0;
    BB_UINT16(rt, BB_WORKER_SUCCESS_OFFSET) = 0;
    BB_INT32(rt, BB_FAULT_CODE_OFFSET) = 0;

    /* Activate worker KB */
    cfl_add_test_by_index(rt, (uint16_t)kb_idx);

    /* Initial heartbeat */
    if (g_robot_ctx) {
        g_robot_ctx->state->worker.active = true;
        g_robot_ctx->state->worker.packet_type = packet_type;
        g_robot_ctx->state->worker.test_id = test_id;
        g_robot_ctx->state->worker.seq = seq;
        g_robot_ctx->state->worker.elapsed = 0;
        g_robot_ctx->state->worker.start_pose = g_robot_ctx->state->pose;
        robot_mqtt_send_heartbeat(g_robot_ctx->mqtt, g_robot_ctx->state, "initial");
    }

    printf("[ct:dispatch] activated worker KB %d for packet_type=%d\n",
           kb_idx, packet_type);
    return true;
}

/* ================================================================== */
/*  Controller: Completion                                             */
/* ================================================================== */

void ctrl_completion_init_fn(void *handle, unsigned node_id) {
    (void)handle; (void)node_id;
}

unsigned ctrl_completion_main_fn(void *handle, unsigned bool_fn_idx,
    unsigned node_id, unsigned event_type, unsigned event_id, void *event_data) {
    (void)bool_fn_idx; (void)node_id; (void)event_id; (void)event_data;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    if (event_type != CFL_TIMER_EVENT) return CFL_CONTINUE;

    if (!BB_UINT16(rt, BB_WORKER_DONE_OFFSET)) return CFL_CONTINUE;
    int32_t active_idx = BB_INT32(rt, BB_ACTIVE_WORKER_IDX_OFFSET);
    if (active_idx < 0) return CFL_CONTINUE;

    int32_t pkt_type = BB_INT32(rt, BB_CURRENT_PACKET_TYPE_OFFSET);
    bool success = BB_UINT16(rt, BB_WORKER_SUCCESS_OFFSET) != 0;
    int32_t fault_code = BB_INT32(rt, BB_FAULT_CODE_OFFSET);

    if (g_robot_ctx) {
        robot_state_t *state = g_robot_ctx->state;
        robot_mqtt_t *mqtt = g_robot_ctx->mqtt;

        state->worker.elapsed = BB_INT32(rt, BB_WATCHDOG_TICKS_OFFSET);

        /* Deduct energy */
        robot_state_deduct_energy(state, pkt_type);

        /* Final heartbeat */
        robot_mqtt_send_heartbeat(mqtt, state, "final");

        /* kb_done */
        const char *fault_str = NULL;
        if (fault_code == 1) fault_str = "watchdog_timeout";
        robot_mqtt_send_kb_done(mqtt, state, success, fault_str);

        /* Publish energy */
        robot_mqtt_publish_energy(mqtt, state);

        printf("[ct:completion] %s pkt=%d energy=%d/%d\n",
               success ? "success" : "FAIL", pkt_type,
               state->energy_remaining, state->config.energy_max);

        state->worker.active = false;
    }

    /* Reset blackboard */
    BB_INT32(rt, BB_ACTIVE_WORKER_IDX_OFFSET) = -1;
    BB_UINT16(rt, BB_WORKER_DONE_OFFSET) = 0;
    BB_UINT16(rt, BB_WORKER_SUCCESS_OFFSET) = 0;

    return CFL_CONTINUE;
}

/* ================================================================== */
/*  Worker: Termination one-shot                                       */
/* ================================================================== */

void worker_term_fn(void *handle, unsigned node_id) {
    (void)node_id;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    BB_UINT16(rt, BB_WORKER_DONE_OFFSET) = 1;
    BB_UINT16(rt, BB_WORKER_SUCCESS_OFFSET) = 1;
}

/* ================================================================== */
/*  Worker init one-shots (set blackboard for each action type)        */
/* ================================================================== */

void wkr_init_check_init_fn(void *handle, unsigned node_id)      { (void)handle; (void)node_id; }
void wkr_path_spline_init_fn(void *handle, unsigned node_id)     { (void)handle; (void)node_id; }
void wkr_path_line_init_fn(void *handle, unsigned node_id)       { (void)handle; (void)node_id; }
void wkr_path_wall_init_fn(void *handle, unsigned node_id)       { (void)handle; (void)node_id; }
void wkr_path_rotate_init_fn(void *handle, unsigned node_id)     { (void)handle; (void)node_id; }
void wkr_deliver_part_init_fn(void *handle, unsigned node_id)    { (void)handle; (void)node_id; }
void wkr_paint_sample_init_fn(void *handle, unsigned node_id)    { (void)handle; (void)node_id; }
void wkr_load_shipping_init_fn(void *handle, unsigned node_id)   { (void)handle; (void)node_id; }
void wkr_pass_gate_init_fn(void *handle, unsigned node_id)       { (void)handle; (void)node_id; }
void wkr_inspection_scan_init_fn(void *handle, unsigned node_id) { (void)handle; (void)node_id; }
void wkr_idle_init_fn(void *handle, unsigned node_id)            { (void)handle; (void)node_id; }
void wkr_recharge_init_fn(void *handle, unsigned node_id)        { (void)handle; (void)node_id; }

/* ================================================================== */
/*  Registration                                                       */
/* ================================================================== */

void register_robot_user_functions(cfl_image_loader_t *img) {
    int rc;

    /* Boolean */
    rc = cfl_image_register_boolean(img, "cbor_rpc_dispatch_boolean", cbor_rpc_dispatch_fn);
    if (rc < 0) printf("WARN: boolean 'cbor_rpc_dispatch_boolean' not in image\n");

    /* Main */
    rc = cfl_image_register_main(img, "ctrl_completion_main_main", ctrl_completion_main_fn);
    if (rc < 0) printf("WARN: main 'ctrl_completion_main_main' not in image\n");

    /* One-shots */
    #define REG_OS(name, fn) do { \
        rc = cfl_image_register_one_shot(img, name, fn); \
        if (rc < 0) printf("WARN: one_shot '%s' not in image\n", name); \
    } while(0)

    REG_OS("ctrl_completion_init_one_shot", ctrl_completion_init_fn);
    REG_OS("worker_term_one_shot",          worker_term_fn);
    REG_OS("wkr_init_check_init_one_shot",      wkr_init_check_init_fn);
    REG_OS("wkr_path_spline_init_one_shot",     wkr_path_spline_init_fn);
    REG_OS("wkr_path_line_init_one_shot",       wkr_path_line_init_fn);
    REG_OS("wkr_path_wall_init_one_shot",       wkr_path_wall_init_fn);
    REG_OS("wkr_path_rotate_init_one_shot",     wkr_path_rotate_init_fn);
    REG_OS("wkr_deliver_part_init_one_shot",    wkr_deliver_part_init_fn);
    REG_OS("wkr_paint_sample_init_one_shot",    wkr_paint_sample_init_fn);
    REG_OS("wkr_load_shipping_init_one_shot",   wkr_load_shipping_init_fn);
    REG_OS("wkr_pass_gate_init_one_shot",       wkr_pass_gate_init_fn);
    REG_OS("wkr_inspection_scan_init_one_shot", wkr_inspection_scan_init_fn);
    REG_OS("wkr_idle_init_one_shot",            wkr_idle_init_fn);
    REG_OS("wkr_recharge_init_one_shot",        wkr_recharge_init_fn);

    #undef REG_OS
}
