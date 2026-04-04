/**
 * @file ct_user_functions.h
 * @brief ChainTree user function declarations for C MQTT/CBOR robot.
 */

#ifndef CT_USER_FUNCTIONS_H
#define CT_USER_FUNCTIONS_H

#include <stdbool.h>
#include "cfl_image_loader.h"
#include "cfl_chaintree_support.h"

/* CBOR sink dispatch boolean */
bool cbor_rpc_dispatch_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data);

/* Controller completion main + init */
unsigned ctrl_completion_main_fn(void *handle, unsigned bool_fn_idx,
    unsigned node_id, unsigned event_type, unsigned event_id, void *event_data);
void ctrl_completion_init_fn(void *handle, unsigned node_id);

/* Worker termination (shared) */
void worker_term_fn(void *handle, unsigned node_id);

/* Worker init one-shots (set ticks_remaining on blackboard) */
void wkr_init_check_init_fn(void *handle, unsigned node_id);
void wkr_path_spline_init_fn(void *handle, unsigned node_id);
void wkr_path_line_init_fn(void *handle, unsigned node_id);
void wkr_path_wall_init_fn(void *handle, unsigned node_id);
void wkr_path_rotate_init_fn(void *handle, unsigned node_id);
void wkr_deliver_part_init_fn(void *handle, unsigned node_id);
void wkr_paint_sample_init_fn(void *handle, unsigned node_id);
void wkr_load_shipping_init_fn(void *handle, unsigned node_id);
void wkr_pass_gate_init_fn(void *handle, unsigned node_id);
void wkr_inspection_scan_init_fn(void *handle, unsigned node_id);
void wkr_idle_init_fn(void *handle, unsigned node_id);
void wkr_recharge_init_fn(void *handle, unsigned node_id);

/* Registration and init */
void register_robot_user_functions(cfl_image_loader_t *img);
void robot_ct_init_kb_lookup(const cfl_chaintree_handle_t *fh);

#endif /* CT_USER_FUNCTIONS_H */
