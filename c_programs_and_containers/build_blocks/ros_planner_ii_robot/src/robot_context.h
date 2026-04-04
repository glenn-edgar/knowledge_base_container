/**
 * @file robot_context.h
 * @brief Global robot context accessible by ChainTree user functions.
 *
 * User functions inside ChainTree are transport-agnostic. They access
 * the MQTT handle and robot state via this global context pointer.
 */

#ifndef ROBOT_CONTEXT_H
#define ROBOT_CONTEXT_H

#include "robot_state.h"
#include "robot_mqtt.h"

typedef struct {
    robot_mqtt_t  *mqtt;
    robot_state_t *state;
} robot_context_t;

extern robot_context_t *g_robot_ctx;

#endif /* ROBOT_CONTEXT_H */
