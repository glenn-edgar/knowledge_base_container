/**
 * @file rob_publish.h
 * @brief Generic robot publish one-shots (rob_ prefix).
 *
 * Each function is a ChainTree one-shot that reads blackboard state
 * and publishes via g_robot_ctx.  Registered as rob_*_one_shot.
 */

#ifndef ROB_PUBLISH_H
#define ROB_PUBLISH_H

#include "cfl_runtime.h"
#include "cfl_image_loader.h"

/* One-shot: publish ack on stream_bus using blackboard seq/test_id */
void rob_send_ack_fn(void *handle, unsigned node_id);

/* One-shot: publish status/state retained (connected=true) */
void rob_publish_state_fn(void *handle, unsigned node_id);

/* One-shot: publish status/energy retained */
void rob_publish_energy_fn(void *handle, unsigned node_id);

/* One-shot: publish status/bitmask retained */
void rob_publish_bitmask_fn(void *handle, unsigned node_id);

/* Register all rob_ one-shots into the image loader */
void register_rob_publish_functions(cfl_image_loader_t *img);

#endif /* ROB_PUBLISH_H */
