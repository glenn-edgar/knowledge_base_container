/**
 * @file rob_publish.c
 * @brief Generic robot publish one-shots (rob_ prefix).
 *
 * Each one-shot reads blackboard fields and publishes via g_robot_ctx.
 * These are transport-agnostic — robot_mqtt_publish handles wire encoding.
 */

#include <stdio.h>
#include "rob_publish.h"
#include "robot_context.h"
#include "robot_mqtt.h"
#include "../dsl/robot_handle_blackboard.h"

/* ================================================================== */
/*  Blackboard access                                                  */
/* ================================================================== */

#define BB_INT32(rt, off)  (*(int32_t  *)((uint8_t *)(rt)->blackboard + (off)))

/* ================================================================== */
/*  rob_send_ack — publish ack on stream_bus                           */
/* ================================================================== */

void rob_send_ack_fn(void *handle, unsigned node_id) {
    (void)node_id;
    if (!g_robot_ctx) return;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    int seq     = BB_INT32(rt, BB_CURRENT_SEQ_OFFSET);
    int test_id = BB_INT32(rt, BB_CURRENT_TEST_ID_OFFSET);

    robot_mqtt_send_ack(g_robot_ctx->mqtt, g_robot_ctx->state, seq, test_id);
}

/* ================================================================== */
/*  rob_publish_state — publish status/state (retained, connected=true)*/
/* ================================================================== */

void rob_publish_state_fn(void *handle, unsigned node_id) {
    (void)handle; (void)node_id;
    if (!g_robot_ctx) return;
    robot_mqtt_publish_state(g_robot_ctx->mqtt, g_robot_ctx->state, true);
}

/* ================================================================== */
/*  rob_publish_energy — publish status/energy (retained)              */
/* ================================================================== */

void rob_publish_energy_fn(void *handle, unsigned node_id) {
    (void)handle; (void)node_id;
    if (!g_robot_ctx) return;
    robot_mqtt_publish_energy(g_robot_ctx->mqtt, g_robot_ctx->state);
}

/* ================================================================== */
/*  rob_publish_bitmask — publish status/bitmask (retained)            */
/* ================================================================== */

void rob_publish_bitmask_fn(void *handle, unsigned node_id) {
    (void)handle; (void)node_id;
    if (!g_robot_ctx) return;
    robot_mqtt_publish_bitmask(g_robot_ctx->mqtt, g_robot_ctx->state);
}

/* ================================================================== */
/*  Registration                                                       */
/* ================================================================== */

void register_rob_publish_functions(cfl_image_loader_t *img) {
    int rc;

    rc = cfl_image_register_one_shot(img, "rob_send_ack_one_shot", rob_send_ack_fn);
    if (rc < 0) printf("WARN: one_shot 'rob_send_ack_one_shot' not in image\n");

    rc = cfl_image_register_one_shot(img, "rob_publish_state_one_shot", rob_publish_state_fn);
    if (rc < 0) printf("WARN: one_shot 'rob_publish_state_one_shot' not in image\n");

    rc = cfl_image_register_one_shot(img, "rob_publish_energy_one_shot", rob_publish_energy_fn);
    if (rc < 0) printf("WARN: one_shot 'rob_publish_energy_one_shot' not in image\n");

    rc = cfl_image_register_one_shot(img, "rob_publish_bitmask_one_shot", rob_publish_bitmask_fn);
    if (rc < 0) printf("WARN: one_shot 'rob_publish_bitmask_one_shot' not in image\n");
}
